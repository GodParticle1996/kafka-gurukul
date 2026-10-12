/* =====================================================================
   Kafka Gurukul — ASLI mini-Kafka broker (terminal missions)

   ⚠️ Yeh scripted output NAHI hai. Andar sach mein ek partitioned log chalta
   hai. Agar tum saare messages ek hi key se bhejoge, ek partition SACH MEIN
   hot hoga aur baaki khaali rahenge. LAG hardcode nahi hai —
   `logEndOffset − committedOffset` se nikalta hai. Isi liye galti se sabak
   banta hai.

   Do hisse:
     1. KQ.createBroker(spec)  — pure logic, koi DOM nahi (node mein testable)
     2. KQ.renderers.terminal  — usko terminal UI mein chipkata hai

   ---------------------------------------------------------------------
   Mission stage shape (data files mein):
   {
     type: 'terminal', title, instructions,
     goal: 'HTML — kya karna hai',
     cluster: {
       brokers: 3,                       // ya [{id:1},{id:2},{id:3}]
       topics: {
         orders: {
           partitions: 3, rf: 3,
           config: { 'retention.ms': 604800000, 'cleanup.policy': 'delete',
                     'min.insync.replicas': 2 },
           messages: [ { key: 'u1', value: 'order-created' }, ... ],  // seed
         },
       },
       groups: {
         'order-workers': { members: 2, committed: { orders: [100, 95, 0] } },
       },
     },
     responses: [ { re: /regex/, out: 'text' | fn(s, m), mutate?: fn(s, m) } ],
     tasks: [ { text, check(s) -> bool } ],
     success: fn(s) -> bool,  successText: 'HTML',
     hints: ['pehla hint', ...],
   }

   `s` (sim) = broker object + { history: [raw cmds], stage, hintsUsed, lastOut }
   Handy success() helpers:  s.lag(group, topic) · s.totalLag(group) ·
   s.topic(name) · s.group(name) · s.leo(topic, p) · s.partitionCounts(topic)
   ===================================================================== */

(function () {

KQ.renderers = KQ.renderers || {};

/* =====================================================================
   Kafka ka ASLI DefaultPartitioner hash (murmur2)
   Isi liye same key hamesha same partition par jaati hai — yeh guarantee
   hi Kafka mein "ordering" ka matlab hai.
   ===================================================================== */
function murmur2(bytes) {
  const length = bytes.length;
  const seed = 0x9747b28c;
  const m = 0x5bd1e995, r = 24;
  let h = (seed ^ length) | 0;
  const len4 = length >> 2;
  for (let i = 0; i < len4; i++) {
    const i4 = i * 4;
    let k = (bytes[i4] & 0xff) | ((bytes[i4 + 1] & 0xff) << 8) |
            ((bytes[i4 + 2] & 0xff) << 16) | ((bytes[i4 + 3] & 0xff) << 24);
    k = Math.imul(k, m); k ^= k >>> r; k = Math.imul(k, m);
    h = Math.imul(h, m); h ^= k;
  }
  const rem = length % 4, base = length & ~3;
  if (rem === 3) h ^= (bytes[base + 2] & 0xff) << 16;
  if (rem >= 2)  h ^= (bytes[base + 1] & 0xff) << 8;
  if (rem >= 1) { h ^= (bytes[base] & 0xff); h = Math.imul(h, m); }
  h ^= h >>> 13; h = Math.imul(h, m); h ^= h >>> 15;
  return h;
}
function utf8Bytes(str) {
  const out = [];
  for (let i = 0; i < str.length; i++) {
    let c = str.charCodeAt(i);
    if (c < 0x80) out.push(c);
    else if (c < 0x800) { out.push(0xc0 | (c >> 6), 0x80 | (c & 0x3f)); }
    else { out.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f)); }
  }
  return out;
}
function partitionForKey(key, numPartitions) {
  return (murmur2(utf8Bytes(String(key))) & 0x7fffffff) % numPartitions;
}
KQ.partitionForKey = partitionForKey;   // visuals/labs bhi isi se dikhate hain

/* =====================================================================
   ERRORS — asli Kafka error naam, taaki Google karna aasan ho
   ===================================================================== */
function kerr(msg) { const e = new Error(msg); e.kafkaErr = true; throw e; }

/* =====================================================================
   TEXT TABLE printer — asli CLI jaisa column layout
   ===================================================================== */
function table(headers, rows) {
  const all = [headers].concat(rows).map(r => r.map(c => String(c == null ? '' : c)));
  const w = headers.map((_, i) => Math.max(...all.map(r => (r[i] || '').length)));
  return all.map(r => r.map((c, i) => c.padEnd(w[i])).join('  ').replace(/\s+$/, '')).join('\n');
}

/* =====================================================================
   THE BROKER
   ===================================================================== */
KQ.createBroker = function createBroker(spec) {
  spec = spec || {};

  const DEFAULT_TOPIC_CONFIG = {
    'retention.ms': 604800000,          // 7 din
    'cleanup.policy': 'delete',
    'min.insync.replicas': 1,
    'max.message.bytes': 1048576,
    'unclean.leader.election.enable': 'false',
  };

  const b = {
    brokers: [],
    topics: {},
    groups: {},
    time: spec.startTime || Date.now(),
    log: [],            // internal event log (kya kya hua) — debugging/teaching ke liye
  };

  /* ---------------- brokers ---------------- */
  const brokerSpec = spec.brokers == null ? 3 : spec.brokers;
  if (typeof brokerSpec === 'number') {
    for (let i = 1; i <= brokerSpec; i++) b.brokers.push({ id: i, alive: true, host: `kafka-${i}:9092` });
  } else {
    brokerSpec.forEach(x => b.brokers.push({
      id: x.id, alive: x.alive !== false, host: x.host || `kafka-${x.id}:9092`,
    }));
  }
  const aliveIds = () => b.brokers.filter(x => x.alive).map(x => x.id);
  const isAlive = id => { const br = b.brokers.find(x => x.id === id); return !!(br && br.alive); };

  /* ---------------- KRaft controllers (ZooKeeper ki jagah) ---------------- */
  const ctrlSpec = spec.controllers == null ? 3 : spec.controllers;
  b.controllers = [];
  if (typeof ctrlSpec === 'number') {
    for (let i = 1; i <= ctrlSpec; i++) {
      b.controllers.push({ id: 9090 + i, alive: true, role: i === 1 ? 'leader' : 'follower' });
    }
  } else {
    ctrlSpec.forEach((x, i) => b.controllers.push({
      id: x.id, alive: x.alive !== false, role: x.role || (i === 0 ? 'leader' : 'follower'),
    }));
  }
  b.controllerEpoch = 1;
  b.metadataOffset = 120;
  b.connectors = {};

  /* ---------------- topic creation ---------------- */
  function makePartition(id, replicas) {
    return {
      id,
      log: [],                 // [{ offset, key, value, ts, headers }]
      logStartOffset: 0,       // retention/compaction se aage badhta hai
      replicas: replicas.slice(),
      isr: replicas.filter(isAlive),
      leader: replicas.find(isAlive) != null ? replicas.find(isAlive) : -1,
      rrCounter: 0,            // null-key round robin
    };
  }
  function assignReplicas(pIndex, rf) {
    // Kafka jaisa round-robin replica placement
    const ids = b.brokers.map(x => x.id);
    const out = [];
    for (let i = 0; i < Math.min(rf, ids.length); i++) out.push(ids[(pIndex + i) % ids.length]);
    return out;
  }

  b.createTopic = function (name, partitions, rf, config) {
    if (b.topics[name]) kerr(`Error while executing topic command : Topic '${name}' already exists.`);
    if (!/^[a-zA-Z0-9._-]+$/.test(name)) kerr(`InvalidTopicException: Topic name '${name}' is illegal — sirf a-z A-Z 0-9 . _ - chalega`);
    partitions = partitions || 1;
    rf = rf || 1;
    if (rf > b.brokers.length) {
      kerr(`Error while executing topic command : Replication factor: ${rf} larger than available brokers: ${b.brokers.length}.`);
    }
    const t = {
      name, rf,
      config: Object.assign({}, DEFAULT_TOPIC_CONFIG, config || {}),
      partitions: [],
    };
    for (let p = 0; p < partitions; p++) t.partitions.push(makePartition(p, assignReplicas(p, rf)));
    b.topics[name] = t;
    return t;
  };

  b.topic = function (name) {
    const t = b.topics[name];
    if (!t) kerr(`UnknownTopicOrPartitionException: Topic '${name}' does not exist on the broker.`);
    return t;
  };
  b.hasTopic = name => !!b.topics[name];

  b.addPartitions = function (name, count) {
    const t = b.topic(name);
    if (count <= t.partitions.length) {
      kerr(`InvalidPartitionsException: Topic currently has ${t.partitions.length} partitions, which is higher or equal than the requested ${count}. Partitions KABHI kam nahi ho sakte.`);
    }
    for (let p = t.partitions.length; p < count; p++) t.partitions.push(makePartition(p, assignReplicas(p, t.rf)));
    // 🔑 asli dard: purani keys ab naye partition par ja sakti hain → ordering toot gayi
    t.repartitioned = true;
    return t;
  };

  b.deleteTopic = function (name) { b.topic(name); delete b.topics[name]; };

  /* ---------------- leader / ISR ka hisaab ---------------- */
  function refreshLeadership() {
    Object.values(b.topics).forEach(t => {
      const minIsr = parseInt(t.config['min.insync.replicas'], 10) || 1;
      const unclean = String(t.config['unclean.leader.election.enable']) === 'true';
      t.partitions.forEach(p => {
        const aliveReplicas = p.replicas.filter(isAlive);
        // ISR = jo replicas zinda hain aur pehle se ISR mein the (ya wapas join kar gaye)
        p.isr = aliveReplicas.slice();
        if (p.leader === -1 || !isAlive(p.leader)) {
          const newLeader = p.isr[0];
          if (newLeader != null) p.leader = newLeader;
          else if (unclean && aliveReplicas.length) p.leader = aliveReplicas[0];
          else p.leader = -1;
        }
        p.underMinIsr = p.isr.length < minIsr;
        p.underReplicated = p.isr.length < p.replicas.length;
      });
    });
  }
  refreshLeadership();

  b.killBroker = function (id) {
    const br = b.brokers.find(x => x.id === id);
    if (!br) kerr(`Broker ${id} exist hi nahi karta.`);
    br.alive = false;
    refreshLeadership();
  };
  b.startBroker = function (id) {
    const br = b.brokers.find(x => x.id === id);
    if (!br) kerr(`Broker ${id} exist hi nahi karta.`);
    br.alive = true;
    refreshLeadership();
  };

  /* ---------------- PRODUCE ----------------
     Yahi jagah hai jahan acks / min.insync.replicas ka sabak milta hai. */
  b.produce = function (topicName, key, value, opts) {
    opts = opts || {};
    const acks = opts.acks == null ? 'all' : String(opts.acks);
    const t = b.topic(topicName);
    const n = t.partitions.length;

    let pIdx;
    if (opts.partition != null) pIdx = opts.partition;
    else if (key == null || key === '') {
      // null key → sticky/round-robin (koi ordering guarantee NAHI)
      pIdx = t.rrCounter = ((t.rrCounter || 0) + 1) % n;
    } else {
      pIdx = partitionForKey(key, n);       // asli murmur2
    }
    if (pIdx >= n || pIdx < 0) kerr(`UnknownTopicOrPartitionException: partition ${pIdx} does not exist (topic has ${n})`);
    const p = t.partitions[pIdx];

    // leader hi nahi bacha?
    if (p.leader === -1) {
      if (acks === '0') { b.lost = (b.lost || 0) + 1; return { lost: true, partition: pIdx }; }
      kerr(`LeaderNotAvailableException: ${topicName}-${pIdx} ka koi leader zinda nahi hai. Message bheja hi nahi ja sakta.`);
    }
    // acks=all + ISR kam → REJECT (yeh feature hai, bug nahi — data loss se bachata hai)
    const minIsr = parseInt(t.config['min.insync.replicas'], 10) || 1;
    if (acks === 'all' && p.isr.length < minIsr) {
      kerr(`NotEnoughReplicasException: ${topicName}-${pIdx} — ISR mein ${p.isr.length} replica hain par min.insync.replicas=${minIsr}. acks=all reject kar raha hai (taaki data gum na ho).`);
    }
    // acks=0 → fire and forget. Leader zinda hai toh chala jaayega, warna chup-chaap gum.
    const rec = {
      offset: p.logStartOffset + p.log.length,
      key: key == null ? null : String(key),
      value: value == null ? null : String(value),
      ts: b.time,
      headers: opts.headers || {},
    };
    // offset hamesha continuous — logStartOffset + index se nikalta hai
    rec.offset = leo(p);
    p.log.push(rec);
    return { partition: pIdx, offset: rec.offset };
  };

  /* log end offset = agla likhne wala offset */
  function leo(p) { return p.logStartOffset + p.log.length; }
  b.leo = function (topicName, pIdx) { return leo(b.topic(topicName).partitions[pIdx]); };

  /* ---------------- CONSUMER GROUPS ---------------- */
  b.createGroup = function (name, memberCount) {
    if (!b.groups[name]) {
      b.groups[name] = { name, members: [], committed: {}, state: 'Empty', generation: 0, assignor: 'range' };
    }
    const g = b.groups[name];
    for (let i = 0; i < (memberCount || 0); i++) b.addMember(name);
    return g;
  };
  b.group = function (name) {
    const g = b.groups[name];
    if (!g) kerr(`Consumer group '${name}' does not exist.`);
    return g;
  };

  let memberSeq = 0;
  b.addMember = function (groupName, clientId) {
    const g = b.groups[groupName] || b.createGroup(groupName, 0);
    const id = `${clientId || 'consumer'}-${groupName}-${++memberSeq}`;
    g.members.push({ id, host: `/10.0.1.${20 + (memberSeq % 200)}`, client: clientId || 'consumer', assignment: [] });
    b.rebalance(groupName);
    return id;
  };
  b.removeMember = function (groupName, memberId) {
    const g = b.group(groupName);
    if (!g.members.length) return null;
    const idx = memberId ? g.members.findIndex(m => m.id === memberId) : g.members.length - 1;
    if (idx < 0) kerr(`Member '${memberId}' group '${groupName}' mein hai hi nahi.`);
    const gone = g.members.splice(idx, 1)[0];
    b.rebalance(groupName);
    return gone;
  };

  /* Group kaun kaun se topics consume karta hai — committed offsets se pata chalta hai,
     aur explicitly subscribe() se bhi set kar sakte ho. */
  b.subscribe = function (groupName, topicName) {
    const g = b.groups[groupName] || b.createGroup(groupName, 0);
    g.subscriptions = g.subscriptions || [];
    if (!g.subscriptions.includes(topicName)) g.subscriptions.push(topicName);
    if (!g.committed[topicName]) {
      g.committed[topicName] = b.topic(topicName).partitions.map(() => 0);
    }
    b.rebalance(groupName);
    return g;
  };
  function subsOf(g) {
    const s = new Set(g.subscriptions || []);
    Object.keys(g.committed).forEach(t => s.add(t));
    return [...s].filter(t => b.topics[t]);
  }

  /* ---------------- REBALANCE ----------------
     Range assignor (Kafka ka default). Yahin dikhta hai ki agar members >
     partitions ho toh extra members IDLE baithe rehte hain — partition hi
     parallelism ki asli limit hai. */
  b.rebalance = function (groupName) {
    const g = b.group(groupName);
    g.generation++;
    g.members.forEach(m => { m.assignment = []; });
    const topics = subsOf(g);
    if (!g.members.length) { g.state = 'Empty'; return g; }
    g.state = 'Stable';

    topics.forEach(tn => {
      const parts = b.topic(tn).partitions.map(p => p.id);
      const m = g.members.length;
      if (g.assignor === 'roundrobin') {
        parts.forEach((p, i) => g.members[i % m].assignment.push({ topic: tn, partition: p }));
      } else {
        // range: consecutive chunks
        const per = Math.floor(parts.length / m);
        const extra = parts.length % m;
        let cur = 0;
        g.members.forEach((mem, i) => {
          const take = per + (i < extra ? 1 : 0);
          for (let k = 0; k < take; k++) mem.assignment.push({ topic: tn, partition: parts[cur++] });
        });
      }
    });
    g.idleMembers = g.members.filter(m => !m.assignment.length).length;
    return g;
  };

  /* ---------------- LAG (asli calculation) ---------------- */
  b.committedOf = function (groupName, topicName, pIdx) {
    const g = b.group(groupName);
    const arr = g.committed[topicName];
    if (!arr) return null;
    return arr[pIdx] == null ? null : arr[pIdx];
  };
  b.lag = function (groupName, topicName) {
    const g = b.group(groupName);
    const t = b.topic(topicName);
    const arr = g.committed[topicName];
    if (!arr) return null;
    return t.partitions.reduce((sum, p) => sum + Math.max(0, leo(p) - (arr[p.id] == null ? leo(p) : arr[p.id])), 0);
  };
  b.lagPerPartition = function (groupName, topicName) {
    const g = b.group(groupName);
    const t = b.topic(topicName);
    const arr = g.committed[topicName] || [];
    return t.partitions.map(p => Math.max(0, leo(p) - (arr[p.id] == null ? leo(p) : arr[p.id])));
  };
  b.totalLag = function (groupName) {
    const g = b.group(groupName);
    return subsOf(g).reduce((s, tn) => s + (b.lag(groupName, tn) || 0), 0);
  };

  /* ---------------- CONSUME + COMMIT ----------------
     Yeh sirf padhta nahi — commit bhi karta hai, taaki lag SACH MEIN ghate. */
  b.consume = function (groupName, topicName, maxRecords, opts) {
    opts = opts || {};
    const g = b.groups[groupName] ? b.group(groupName) : b.createGroup(groupName, 0);
    const t = b.topic(topicName);
    if (!g.committed[topicName]) {
      // auto.offset.reset — naya group kahan se shuru kare
      const reset = opts.autoOffsetReset || 'latest';
      g.committed[topicName] = t.partitions.map(p => (reset === 'earliest' ? p.logStartOffset : leo(p)));
      if (!g.subscriptions) g.subscriptions = [];
      if (!g.subscriptions.includes(topicName)) g.subscriptions.push(topicName);
      if (g.members.length) b.rebalance(groupName);
    }
    const arr = g.committed[topicName];
    const out = [];
    let budget = maxRecords == null ? Infinity : maxRecords;

    // sirf un partitions se padho jo members ko assign hain (agar members hain)
    let allowed = null;
    if (g.members.length) {
      allowed = new Set();
      g.members.forEach(m => m.assignment.forEach(a => { if (a.topic === topicName) allowed.add(a.partition); }));
    }

    for (const p of t.partitions) {
      if (allowed && !allowed.has(p.id)) continue;
      if (p.leader === -1) continue;             // offline partition — padha hi nahi ja sakta
      let cur = arr[p.id] == null ? p.logStartOffset : arr[p.id];

      // 🔥 OffsetOutOfRange — retention ne woh offset kha liya jahan consumer tha
      if (cur < p.logStartOffset) {
        const reset = opts.autoOffsetReset || 'latest';
        out.push({ __reset: true, topic: topicName, partition: p.id, from: cur,
                   to: reset === 'earliest' ? p.logStartOffset : leo(p), reset });
        cur = reset === 'earliest' ? p.logStartOffset : leo(p);
      }
      while (cur < leo(p) && budget > 0) {
        const rec = p.log[cur - p.logStartOffset];
        if (rec) out.push({ topic: topicName, partition: p.id, offset: rec.offset, key: rec.key, value: rec.value, ts: rec.ts });
        cur++; budget--;
      }
      if (opts.commit !== false) arr[p.id] = cur;
    }
    return out;
  };

  /* ---------------- offset reset (asli CLI jaisa) ---------------- */
  b.resetOffsets = function (groupName, topicName, mode, arg) {
    const g = b.group(groupName);
    const t = b.topic(topicName);
    if (g.members.length && !g.allowLiveReset) {
      kerr(`Assignments can only be reset if the group '${groupName}' is inactive, but the current state is Stable.\n(Pehle consumers band karo — chalte group ke offsets nahi badal sakte.)`);
    }
    if (!g.committed[topicName]) g.committed[topicName] = t.partitions.map(() => 0);
    const arr = g.committed[topicName];
    t.partitions.forEach(p => {
      if (mode === 'earliest') arr[p.id] = p.logStartOffset;
      else if (mode === 'latest') arr[p.id] = leo(p);
      else if (mode === 'offset') arr[p.id] = Math.max(p.logStartOffset, Math.min(leo(p), arg));
      else if (mode === 'shift') arr[p.id] = Math.max(p.logStartOffset, Math.min(leo(p), arr[p.id] + arg));
    });
    return arr;
  };

  /* ---------------- TIME / RETENTION / COMPACTION ---------------- */
  b.advanceTime = function (ms) {
    b.time += ms;
    b.retentionSweep();
    return b.time;
  };
  b.retentionSweep = function () {
    Object.values(b.topics).forEach(t => {
      const policy = String(t.config['cleanup.policy'] || 'delete');
      if (policy.includes('compact')) b.compact(t.name);
      if (!policy.includes('delete')) return;
      const retMs = parseInt(t.config['retention.ms'], 10);
      if (!(retMs >= 0)) return;
      const cutoff = b.time - retMs;
      t.partitions.forEach(p => {
        let drop = 0;
        while (drop < p.log.length && p.log[drop].ts < cutoff) drop++;
        if (drop) { p.log.splice(0, drop); p.logStartOffset += drop; }
      });
    });
  };
  /* Log compaction — har key ki sirf AAKHRI value bachti hai.
     value === null (tombstone) → key poori tarah gayab. */
  b.compact = function (topicName) {
    const t = b.topic(topicName);
    let removed = 0;
    t.partitions.forEach(p => {
      const lastIdxOfKey = new Map();
      p.log.forEach((r, i) => { if (r.key != null) lastIdxOfKey.set(r.key, i); });
      const keep = [];
      p.log.forEach((r, i) => {
        if (r.key == null) { keep.push(r); return; }         // null-key record chhoda nahi jaata
        if (lastIdxOfKey.get(r.key) !== i) { removed++; return; }  // purani value — udao
        if (r.value === null) { removed++; return; }               // tombstone — key khatam
        keep.push(r);
      });
      // compaction offsets ko renumber NAHI karta — gaps reh jaate hain (yeh asli behaviour hai)
      p.log = keep;
      p.compacted = true;
    });
    return removed;
  };

  /* ---------------- convenience for success() ---------------- */
  b.partitionCounts = function (topicName) { return b.topic(topicName).partitions.map(p => p.log.length); };
  b.allRecords = function (topicName) {
    return b.topic(topicName).partitions.reduce((a, p) => a.concat(p.log), []);
  };

  /* ---------------- seed from spec ---------------- */
  Object.entries(spec.topics || {}).forEach(([name, t]) => {
    const topic = b.createTopic(name, t.partitions || 1, t.rf || Math.min(3, b.brokers.length), t.config);
    (t.messages || []).forEach(m => {
      if (m.ts != null) { const save = b.time; b.time = m.ts; b.produce(name, m.key, m.value, { acks: '1', partition: m.partition }); b.time = save; }
      else b.produce(name, m.key, m.value, { acks: '1', partition: m.partition });
    });
    if (t.explicitLog) {   // partition-wise exact log dena ho toh
      t.explicitLog.forEach((recs, pIdx) => {
        const p = topic.partitions[pIdx];
        p.log = recs.map((r, i) => ({ offset: i, key: r.key == null ? null : String(r.key), value: r.value == null ? null : String(r.value), ts: r.ts || b.time, headers: {} }));
      });
    }
  });
  Object.entries(spec.groups || {}).forEach(([name, g]) => {
    b.createGroup(name, 0);
    const grp = b.groups[name];
    grp.assignor = g.assignor || 'range';
    Object.entries(g.committed || {}).forEach(([tn, offs]) => { grp.committed[tn] = offs.slice(); });
    (g.subscriptions || Object.keys(g.committed || {})).forEach(tn => {
      if (b.topics[tn]) b.subscribe(name, tn);
    });
    for (let i = 0; i < (g.members || 0); i++) b.addMember(name, g.clientId);
  });
  refreshLeadership();

  /* =====================================================================
     CLI — asli kafka-*.sh commands
     ===================================================================== */
  function splitArgs(line) {
    const out = []; let cur = '', q = null;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (q) { if (c === q) q = null; else cur += c; continue; }
      if (c === '"' || c === "'") { q = c; continue; }
      if (/\s/.test(c)) { if (cur) { out.push(cur); cur = ''; } continue; }
      cur += c;
    }
    if (cur) out.push(cur);
    return out;
  }
  function parseFlags(args) {
    const f = { _: [], repeated: {} };
    for (let i = 0; i < args.length; i++) {
      const a = args[i];
      if (a.startsWith('--')) {
        const name = a.slice(2);
        const next = args[i + 1];
        if (next != null && !next.startsWith('--')) {
          f[name] = next;
          (f.repeated[name] = f.repeated[name] || []).push(next);
          i++;
        } else f[name] = true;
      } else f._.push(a);
    }
    return f;
  }
  function need(f, name, cmd) {
    if (f[name] == null || f[name] === true) kerr(`Missing required argument "[${name}]"\nUsage: ${cmd}`);
    return f[name];
  }

  b.run = function (rawLine) {
    const line = rawLine.trim();
    if (!line) return '';
    const args = splitArgs(line);
    let cmd = args.shift().replace(/\.sh$/, '');
    const f = parseFlags(args);

    switch (cmd) {
      case 'kafka-topics':          return cmdTopics(f);
      case 'kafka-console-producer':return cmdProducer(f);
      case 'kafka-console-consumer':return cmdConsumer(f);
      case 'kafka-consumer-groups': return cmdGroups(f);
      case 'kafka-configs':         return cmdConfigs(f);
      case 'kafka-run-class':       return cmdRunClass(f, args);
      case 'kafka-metadata-quorum':
      case 'kafka-metadata':        return cmdMetadataQuorum(f);
      case 'kafka-reassign-partitions': return cmdReassign(f);
      case 'connect':               return cmdConnect(f, args);
      case 'sim':                   return cmdSim(f, args);
      case 'dump':                  return cmdDump(f, args);
      case 'status':                return cmdStatus();
      case 'help':                  return HELP;
      default:
        kerr(`bash: ${cmd}: command not found\n('help' likho — supported commands ki poori list mil jaayegi.)`);
    }
  };

  /* ---- kafka-topics ---- */
  function cmdTopics(f) {
    if (f.create || f.alter || f.delete) requireQuorum('topic metadata change');
    if (f.create) {
      const name = need(f, 'topic', 'kafka-topics --create --topic <name> --partitions <n> --replication-factor <r>');
      const parts = parseInt(f.partitions, 10) || 1;
      const rf = parseInt(f['replication-factor'], 10) || Math.min(3, b.brokers.length);
      const config = {};
      (f.repeated.config || []).forEach(c => { const [k, v] = c.split('='); config[k] = v; });
      b.createTopic(name, parts, rf, config);
      return `Created topic ${name}.`;
    }
    if (f.list) return Object.keys(b.topics).sort().join('\n') || '(koi topic nahi hai)';
    if (f.delete) { const n = need(f, 'topic', 'kafka-topics --delete --topic <name>'); b.deleteTopic(n); return `Topic ${n} deleted.`; }
    if (f.alter) {
      const n = need(f, 'topic', 'kafka-topics --alter --topic <name> --partitions <n>');
      const to = parseInt(f.partitions, 10);
      b.addPartitions(n, to);
      return `Topic ${n} ke partitions ab ${to} hain.\n⚠️ Dhyan: purani keys ab NAYE partition par ja sakti hain — us key ki ordering guarantee TOOT gayi.`;
    }
    if (f.describe) {
      const names = f.topic && f.topic !== true ? [f.topic] : Object.keys(b.topics).sort();
      if (!names.length) return '(koi topic nahi hai)';
      return names.map(n => {
        const t = b.topic(n);
        const head = `Topic: ${t.name}\tTopicId: ${'kQ' + n.slice(0, 6).padEnd(6, 'x')}\tPartitionCount: ${t.partitions.length}\tReplicationFactor: ${t.rf}\tConfigs: ${Object.entries(t.config).filter(([k, v]) => DEFAULT_TOPIC_CONFIG[k] !== v).map(([k, v]) => k + '=' + v).join(',')}`;
        const rows = t.partitions.map(p =>
          `\tTopic: ${t.name}\tPartition: ${p.id}\tLeader: ${p.leader === -1 ? 'none' : p.leader}\tReplicas: ${p.replicas.join(',')}\tIsr: ${p.isr.join(',')}` +
          (p.underMinIsr ? '\t⚠️ UNDER-MIN-ISR' : p.underReplicated ? '\t⚠️ UNDER-REPLICATED' : ''));
        return [head].concat(rows).join('\n');
      }).join('\n');
    }
    kerr('kafka-topics: --create | --list | --describe | --alter | --delete mein se koi ek do.');
  }

  /* ---- kafka-console-producer ----
     Asli CLI stdin se padhta hai; yahan --message (baar baar) ya --count use karo. */
  function cmdProducer(f) {
    const topicName = need(f, 'topic', 'kafka-console-producer --topic <name> --message "key:value"');
    const t = b.topic(topicName);
    const props = {};
    (f.repeated.property || []).forEach(p => { const [k, v] = p.split('='); props[k] = v; });
    const parseKey = props['parse.key'] === 'true';
    const sep = props['key.separator'] || '\t';
    const acks = f.acks != null && f.acks !== true ? String(f.acks) : 'all';

    let msgs = (f.repeated.message || []).map(m => {
      if (parseKey && m.includes(sep)) {
        const i = m.indexOf(sep);
        return { key: m.slice(0, i), value: m.slice(i + 1) };
      }
      if (parseKey && m.includes(':')) {          // dost-bhaav: default separator ':' bhi chalne do
        const i = m.indexOf(':');
        return { key: m.slice(0, i), value: m.slice(i + 1) };
      }
      return { key: null, value: m };
    });

    // bulk mode — hot partition dikhane ke liye
    const count = parseInt(f.count, 10);
    if (count > 0) {
      const keyMode = f.key != null && f.key !== true ? String(f.key) : 'none';
      for (let i = 0; i < count; i++) {
        let k = null;
        if (keyMode === 'none' || keyMode === 'null') k = null;
        else if (keyMode === 'random') k = 'k' + Math.floor(Math.random() * 100000);
        else if (keyMode.includes('{i}')) k = keyMode.replace('{i}', i);
        else k = keyMode;                                  // ek hi fixed key = hot partition
        msgs.push({ key: k, value: `msg-${i}` });
      }
    }
    if (!msgs.length) kerr('Kuch bhejne ke liye do na — --message "k:v" (kai baar likh sakte ho) ya --count 1000 --key <mode>');

    const perPartition = t.partitions.map(() => 0);
    let sent = 0, lost = 0;
    const errs = [];
    msgs.forEach(m => {
      try {
        const r = b.produce(topicName, m.key, m.value, { acks });
        if (r.lost) { lost++; return; }
        perPartition[r.partition]++; sent++;
      } catch (e) {
        if (e.kafkaErr) { if (errs.length < 2) errs.push(e.message); }
        else throw e;
      }
    });
    let out = `>>> ${sent} record${sent === 1 ? '' : 's'} bheje (acks=${acks}).`;
    if (lost) out += `\n💀 ${lost} record CHUP-CHAAP GUM ho gaye — acks=0 tha, koi shikayat nahi aayi.`;
    if (errs.length) out += `\n❌ ${msgs.length - sent - lost} fail hue:\n   ` + errs.join('\n   ');
    out += `\n\n` + table(['PARTITION', 'RECORDS-ADDED', 'LOG-END-OFFSET'],
      t.partitions.map(p => [`${topicName}-${p.id}`, perPartition[p.id], leo(p)]));
    const nonZero = perPartition.filter(x => x > 0).length;
    if (sent >= 10 && nonZero === 1 && t.partitions.length > 1) {
      out += `\n\n🔥 HOT PARTITION: saare ${sent} records ek hi partition par gaye. Baaki ${t.partitions.length - 1} partitions khaali baithe hain — matlab tumhara parallelism 1 hai, chahe consumers kitne bhi laga lo.`;
    }
    return out;
  }

  /* ---- kafka-console-consumer ---- */
  function cmdConsumer(f) {
    const topicName = need(f, 'topic', 'kafka-console-consumer --topic <name> [--from-beginning] [--group <g>]');
    const max = parseInt(f['max-messages'], 10) || 10;
    const props = {};
    (f.repeated.property || []).forEach(p => { const [k, v] = p.split('='); props[k] = v; });
    const printKey = props['print.key'] === 'true';
    const groupName = f.group && f.group !== true ? f.group : `console-consumer-${Math.floor(Math.random() * 90000) + 10000}`;
    const ephemeral = !(f.group && f.group !== true);
    const reset = f['from-beginning'] ? 'earliest' : 'latest';

    if (ephemeral) b.createGroup(groupName, 0);
    const recs = b.consume(groupName, topicName, max, {
      autoOffsetReset: reset,
      commit: !ephemeral,
      partition: f.partition,
    });

    const lines = [];
    let shown = 0;
    recs.forEach(r => {
      if (r.__reset) {
        lines.push(`⚠️  OffsetOutOfRangeException: ${r.topic}-${r.partition} — committed offset ${r.from} ab log mein hai hi nahi (retention kha gaya).`);
        lines.push(`   auto.offset.reset=${r.reset} → ${r.to} se shuru kar rahe hain.` +
          (r.reset === 'earliest' ? `  💀 Matlab poora topic DOBARA process hoga — duplicates ke liye taiyaar raho.` : `  💀 Matlab beech ke messages HAMESHA ke liye skip ho gaye.`));
        return;
      }
      shown++;
      lines.push(printKey ? `${r.key == null ? 'null' : r.key}\t${r.value}` : String(r.value));
    });
    if (ephemeral) delete b.groups[groupName];
    lines.push(`^CProcessed a total of ${shown} messages`);
    return lines.join('\n');
  }

  /* ---- kafka-consumer-groups ---- */
  function cmdGroups(f) {
    if (f.list) return Object.keys(b.groups).sort().join('\n') || '(koi consumer group nahi hai)';

    if (f.describe) {
      const names = f['all-groups'] ? Object.keys(b.groups).sort()
        : [need(f, 'group', 'kafka-consumer-groups --describe --group <g>')];
      return names.map(gn => {
        const g = b.group(gn);
        const topics = subsOf(g);
        if (!topics.length) return `Consumer group '${gn}' has no committed offsets.`;
        const rows = [];
        topics.forEach(tn => {
          const t = b.topic(tn);
          const arr = g.committed[tn] || [];
          t.partitions.forEach(p => {
            const owner = g.members.find(m => m.assignment.some(a => a.topic === tn && a.partition === p.id));
            const cur = arr[p.id] == null ? '-' : arr[p.id];
            const end = leo(p);
            const lag = arr[p.id] == null ? '-' : Math.max(0, end - arr[p.id]);
            rows.push([gn, tn, p.id, cur, end, lag,
              owner ? owner.id : '-', owner ? owner.host : '-', owner ? owner.client : '-']);
          });
        });
        let out = '';
        if (g.state !== 'Stable') out += `\nWarning: Consumer group '${gn}' is ${g.state === 'Empty' ? 'rebalancing or has no active members' : g.state}.\n`;
        out += table(['GROUP', 'TOPIC', 'PARTITION', 'CURRENT-OFFSET', 'LOG-END-OFFSET', 'LAG', 'CONSUMER-ID', 'HOST', 'CLIENT-ID'], rows);
        const total = topics.reduce((s, tn) => s + (b.lag(gn, tn) || 0), 0);
        out += `\n\nTotal lag: ${total}   ·   Members: ${g.members.length}   ·   State: ${g.state}   ·   Generation: ${g.generation}`;
        if (g.idleMembers) out += `\n😴 ${g.idleMembers} consumer BEKAAR baithe hain — partitions se zyada consumers ho gaye. Partition hi parallelism ki limit hai.`;
        return out;
      }).join('\n\n');
    }

    if (f['reset-offsets']) {
      const gn = need(f, 'group', 'kafka-consumer-groups --reset-offsets --group <g> --topic <t> --to-earliest --execute');
      const tn = need(f, 'topic', 'kafka-consumer-groups --reset-offsets --group <g> --topic <t> --to-earliest --execute');
      const g = b.group(gn);
      let mode, arg;
      if (f['to-earliest']) mode = 'earliest';
      else if (f['to-latest']) mode = 'latest';
      else if (f['to-offset'] != null && f['to-offset'] !== true) { mode = 'offset'; arg = parseInt(f['to-offset'], 10); }
      else if (f['shift-by'] != null && f['shift-by'] !== true) { mode = 'shift'; arg = parseInt(f['shift-by'], 10); }
      else kerr('Reset kahan karna hai? --to-earliest | --to-latest | --to-offset <n> | --shift-by <n>');

      if (!f.execute && !f['dry-run']) {
        return `(dry run — kuch nahi badla. Sach mein karna hai toh --execute lagao.)\n` +
          preview(gn, tn, mode, arg);
      }
      if (f['dry-run']) return `(dry run)\n` + preview(gn, tn, mode, arg);
      b.resetOffsets(gn, tn, mode, arg);
      const arr = g.committed[tn];
      return table(['GROUP', 'TOPIC', 'PARTITION', 'NEW-OFFSET'],
        b.topic(tn).partitions.map(p => [gn, tn, p.id, arr[p.id]]));
    }

    if (f.delete) { const gn = need(f, 'group', 'kafka-consumer-groups --delete --group <g>'); b.group(gn); delete b.groups[gn]; return `Deletion of requested consumer groups ('${gn}') was successful.`; }
    kerr('kafka-consumer-groups: --list | --describe | --reset-offsets | --delete mein se koi ek do.');
  }
  function preview(gn, tn, mode, arg) {
    const t = b.topic(tn);
    const g = b.group(gn);
    const arr = g.committed[tn] || t.partitions.map(() => 0);
    return table(['GROUP', 'TOPIC', 'PARTITION', 'NEW-OFFSET'], t.partitions.map(p => {
      let v;
      if (mode === 'earliest') v = p.logStartOffset;
      else if (mode === 'latest') v = leo(p);
      else if (mode === 'offset') v = Math.max(p.logStartOffset, Math.min(leo(p), arg));
      else v = Math.max(p.logStartOffset, Math.min(leo(p), (arr[p.id] || 0) + arg));
      return [gn, tn, p.id, v];
    }));
  }

  /* ---- kafka-configs ---- */
  function cmdConfigs(f) {
    const name = need(f, 'entity-name', 'kafka-configs --describe --entity-type topics --entity-name <t>');
    const t = b.topic(name);
    if (f.describe) {
      return `Dynamic configs for topic ${name} are:\n` +
        Object.entries(t.config).map(([k, v]) =>
          `  ${k}=${v} sensitive=false synonyms={${DEFAULT_TOPIC_CONFIG[k] === v ? 'DEFAULT_CONFIG' : 'DYNAMIC_TOPIC_CONFIG'}:${k}=${v}}`).join('\n');
    }
    if (f.alter) {
      const add = f.repeated['add-config'] || [];
      if (!add.length) kerr('--add-config chahiye, jaise --add-config retention.ms=60000');
      add.forEach(c => c.split(',').forEach(pair => {
        const [k, v] = pair.split('=');
        t.config[k] = v;
      }));
      b.retentionSweep();
      return `Completed updating config for topic ${name}.`;
    }
    kerr('kafka-configs: --describe ya --alter do.');
  }

  /* ---- kafka-run-class (GetOffsetShell / DumpLogSegments) ---- */
  function cmdRunClass(f, args) {
    const cls = (f._[0] || '');
    if (/GetOffsetShell/.test(cls)) {
      const tn = need(f, 'topic', 'kafka-run-class kafka.tools.GetOffsetShell --topic <t>');
      const t = b.topic(tn);
      return t.partitions.map(p => `${tn}:${p.id}:${leo(p)}`).join('\n');
    }
    if (/DumpLogSegments/.test(cls)) return cmdDump(f, args);
    kerr(`Kaunsi class? kafka.tools.GetOffsetShell ya kafka.tools.DumpLogSegments try karo.`);
  }

  /* ---- dump: log ke ANDAR jhaanko ---- */
  function cmdDump(f, args) {
    const tn = (f.topic && f.topic !== true) ? f.topic : f._[0];
    if (!tn) kerr('dump <topic> [partition]  — kis topic ka log dekhna hai?');
    const t = b.topic(tn);
    const only = f.partition != null && f.partition !== true ? parseInt(f.partition, 10)
      : (f._[1] != null ? parseInt(f._[1], 10) : null);
    return t.partitions.filter(p => only == null || p.id === only).map(p => {
      const head = `Dumping ${tn}-${p.id}/00000000000000000000.log  (logStartOffset: ${p.logStartOffset}, logEndOffset: ${leo(p)}, leader: ${p.leader === -1 ? 'none' : p.leader}, isr: [${p.isr.join(',')}])`;
      if (!p.log.length) return head + '\n  (khaali)';
      const rows = p.log.slice(0, 30).map(r =>
        [`offset: ${r.offset}`, `key: ${r.key == null ? 'null' : r.key}`,
         `payload: ${r.value === null ? 'null 🪦 (tombstone)' : r.value}`].join('  '));
      const more = p.log.length > 30 ? `\n  ... aur ${p.log.length - 30} records` : '';
      return head + '\n  ' + rows.join('\n  ') + more;
    }).join('\n\n');
  }

  /* ---- status: ek nazar mein poora cluster ---- */
  /* =====================================================================
     KRaft — controller quorum (ZooKeeper ki jagah)
     Yahan ka asli sabak: quorum toot-ne par METADATA plane rukta hai,
     par DATA plane (produce/consume) chalta rehta hai.
     ===================================================================== */
  b.quorumMajority = () => Math.floor(b.controllers.length / 2) + 1;
  b.aliveControllers = () => b.controllers.filter(c => c.alive);
  b.hasQuorum = () => b.aliveControllers().length >= b.quorumMajority();

  function refreshControllerLeader() {
    const alive = b.aliveControllers();
    const hadLeader = b.controllers.find(c => c.role === 'leader' && c.alive);
    if (hadLeader) return;
    b.controllers.forEach(c => { if (c.role === 'leader') c.role = 'follower'; });
    if (b.hasQuorum() && alive.length) {
      alive[0].role = 'leader';
      b.controllerEpoch++;
    }
  }
  b.killController = function (id) {
    const c = b.controllers.find(x => x.id === id);
    if (!c) kerr(`Controller ${id} exist hi nahi karta. (voters: ${b.controllers.map(x => x.id).join(',')})`);
    c.alive = false;
    refreshControllerLeader();
  };
  b.startController = function (id) {
    const c = b.controllers.find(x => x.id === id);
    if (!c) kerr(`Controller ${id} exist hi nahi karta.`);
    c.alive = true;
    refreshControllerLeader();
  };
  /* metadata write — har admin operation isse hoke jaati hai */
  function requireQuorum(what) {
    if (!b.hasQuorum()) {
      kerr(`TimeoutException: ${what} — controller quorum unavailable.\n` +
        `   ${b.aliveControllers().length}/${b.controllers.length} voters zinda hain, majority ${b.quorumMajority()} chahiye.\n` +
        `   ⚠️ Dhyaan do: produce/consume abhi bhi CHAL RAHE hain — sirf metadata changes ruke hain.`);
    }
    b.metadataOffset += 1;
  }
  b.requireQuorum = requireQuorum;

  function cmdMetadataQuorum(f) {
    if (f.replication) {
      const leader = b.controllers.find(c => c.role === 'leader' && c.alive);
      const rows = b.controllers.map(c => [
        c.id,
        c.alive ? (c.role === 'leader' ? 'Leader' : 'Follower') : 'Follower',
        c.alive ? b.metadataOffset : Math.max(0, b.metadataOffset - 37),
        c.alive ? 0 : 37,
        c.alive ? 'Voter' : 'Voter (unreachable)',
      ]);
      return table(['NodeId', 'Role', 'LogEndOffset', 'Lag', 'Status'], rows) +
        (leader ? '' : '\n\n💀 Koi controller leader nahi hai — quorum toot chuka hai.');
    }
    /* --describe --status (default) */
    const leader = b.controllers.find(c => c.role === 'leader' && c.alive);
    const voters = b.controllers.map(c => c.id).join(',');
    const alive = b.aliveControllers().map(c => c.id).join(',') || '(koi nahi)';
    const lines = [
      `ClusterId:              kafka-gurukul-cluster`,
      `LeaderId:               ${leader ? leader.id : -1}`,
      `LeaderEpoch:            ${b.controllerEpoch}`,
      `HighWatermark:          ${b.metadataOffset}`,
      `CurrentVoters:          [${voters}]`,
      `LiveVoters:             [${alive}]`,
      `QuorumMajority:         ${b.quorumMajority()} of ${b.controllers.length}`,
      '',
    ];
    if (b.hasQuorum()) {
      lines.push('✅ Quorum healthy — metadata writes (topic create/alter/reassign) chalenge.');
    } else {
      lines.push('💀 QUORUM LOST — metadata writes FAIL karengi (TimeoutException).');
      lines.push('   Par existing partitions par produce/consume chalta rahega —');
      lines.push('   brokers apni aakhri maloom metadata se kaam chalate rehte hain.');
    }
    return lines.join('\n');
  }

  /* =====================================================================
     Partition reassignment — naye broker apne aap kaam nahi bantate
     ===================================================================== */
  function cmdReassign(f) {
    const tn = need(f, 'topic', 'kafka-reassign-partitions --topic <t> --broker-list 1,2,3 (--execute|--verify)');
    const t = b.topic(tn);

    if (f.verify) {
      if (!t.reassign || !t.reassign.pending) {
        return `Status of partition reassignment for topic '${tn}':\n` +
          t.partitions.map(p => `Reassignment of partition ${tn}-${p.id} is completed.`).join('\n') +
          `\n\n(koi reassignment pending nahi hai)`;
      }
      t.partitions.forEach((p, i) => {
        p.replicas = t.reassign.target[i].slice();
        p.isr = p.replicas.filter(isAlive);
      });
      refreshLeadership();
      const moved = t.reassign.movedRecords;
      const plan = t.reassign;
      t.reassign = { pending: false, target: null, movedRecords: 0 };
      return `Status of partition reassignment for topic '${tn}':\n` +
        t.partitions.map(p => `Reassignment of partition ${tn}-${p.id} is completed.`).join('\n') +
        `\n\n✅ Reassignment poori hui.\n` +
        `   ${plan.movedPartitions} partitions ke replicas badle, ~${moved} records network par copy hue.\n` +
        `   ⚠️ Production mein yahi step ghanton chalta hai aur cluster ka network kha jaata hai —\n` +
        `   isiliye --throttle ke bina kabhi mat chalana.\n\n` + cmdTopics({ describe: true, topic: tn });
    }

    if (!f.execute) kerr('kafka-reassign-partitions --topic <t> --broker-list 1,2,3 (--execute | --verify)');

    requireQuorum('reassign partitions');
    const listRaw = need(f, 'broker-list', 'kafka-reassign-partitions --topic <t> --broker-list 1,2,3 --execute');
    const ids = String(listRaw).split(',').map(x => parseInt(x.trim(), 10)).filter(x => !isNaN(x));
    if (!ids.length) kerr('--broker-list mein kam se kam ek broker id do, jaise --broker-list 1,2,3');
    ids.forEach(id => { if (!b.brokers.find(x => x.id === id)) kerr(`Broker ${id} cluster mein hai hi nahi.`); });
    if (ids.length < t.rf) kerr(`RF ${t.rf} hai par --broker-list mein sirf ${ids.length} broker hain.`);

    const target = t.partitions.map((p, i) => {
      const out = [];
      for (let k = 0; k < t.rf; k++) out.push(ids[(i + k) % ids.length]);
      return out;
    });
    let movedPartitions = 0, movedRecords = 0;
    t.partitions.forEach((p, i) => {
      const before = p.replicas.slice().sort().join(',');
      const after = target[i].slice().sort().join(',');
      if (before !== after) { movedPartitions++; movedRecords += p.log.length; }
    });
    t.reassign = { pending: true, target, movedPartitions, movedRecords };

    const rows = t.partitions.map((p, i) => [
      `${tn}-${p.id}`, p.replicas.join(','), target[i].join(','),
      target[i].slice().sort().join(',') === p.replicas.slice().sort().join(',') ? '—' : `${p.log.length} records`,
    ]);
    return `Current partition replica assignment saved for rollback.\n` +
      `Successfully started partition reassignment for ${t.partitions.map(p => `${tn}-${p.id}`).join(',')}\n\n` +
      table(['PARTITION', 'CURRENT', 'TARGET', 'TO COPY'], rows) +
      `\n\n⏳ Reassignment shuru ho gayi — ${movedPartitions} partitions, ~${movedRecords} records copy honge.\n` +
      `   Ab \`kafka-reassign-partitions --topic ${tn} --verify\` se poora karo.`;
  }

  /* =====================================================================
     KAFKA CONNECT — asli connector runtime (scripted nahi)

     Sabse bada sabak: SINK connector ek CONSUMER GROUP hi hota hai.
     `kafka-consumer-groups --list` mein woh 'connect-<name>' banke dikhega.
     ===================================================================== */
  const CONNECT_PLUGINS = {
    FileStreamSource:     { type: 'source', desc: 'file ki har line ek record' },
    JdbcSource:           { type: 'source', desc: 'DB table ko poll karke rows' },
    DatagenSource:        { type: 'source', desc: 'test data banata hai' },
    S3Sink:               { type: 'sink',   desc: 'records ko S3 par likhta hai' },
    ElasticsearchSink:    { type: 'sink',   desc: 'records ko Elasticsearch mein' },
    JdbcSink:             { type: 'sink',   desc: 'records ko DB table mein' },
  };
  const SMTS = {
    InsertField:  r => ({ ...r, value: r.value + '|ingested_at=' + new Date(b.time).toISOString().slice(0, 19) }),
    MaskField:    r => ({ ...r, value: String(r.value).replace(/\b\d{10,16}\b/g, '****MASKED****') }),
    ExtractField: r => ({ ...r, value: String(r.value).split('|')[0] }),
    Filter:       r => (String(r.value).includes('heartbeat') ? null : r),
  };

  b.connectorGroupName = name => 'connect-' + name;

  function connectorOrDie(name) {
    const c = b.connectors[name];
    if (!c) kerr(`Connector '${name}' nahi mila. (connect list se dekho)`);
    return c;
  }

  function cmdConnect(f, args) {
    const sub = f._[0];

    if (sub === 'plugins' || (sub === 'plugin' && f._[1] === 'list')) {
      return table(['CLASS', 'TYPE', 'KYA KARTA HAI'],
        Object.entries(CONNECT_PLUGINS).map(([k, v]) => [k, v.type, v.desc])) +
        `\n\nSMTs (Single Message Transforms): ${Object.keys(SMTS).join(', ')}`;
    }

    if (sub === 'create') {
      const name = need(f, 'name', 'connect create --name <n> --class <Plugin> --topic <t> [--tasks n]');
      const cls = need(f, 'class', 'connect create --name <n> --class <Plugin> --topic <t> [--tasks n]');
      const plugin = CONNECT_PLUGINS[cls];
      if (!plugin) kerr(`ClassNotFoundException: '${cls}'.\n   Available: ${Object.keys(CONNECT_PLUGINS).join(', ')}`);
      if (b.connectors[name]) kerr(`Connector '${name}' pehle se hai.`);
      const tn = need(f, 'topic', 'connect create ... --topic <t>');
      requireQuorum('create connector (config topic par likhna padta hai)');

      const transforms = (f.repeated.transform || []).filter(x => x !== true);
      transforms.forEach(t => { if (!SMTS[t]) kerr(`Unknown transform '${t}'. Available: ${Object.keys(SMTS).join(', ')}`); });

      if (plugin.type === 'sink' && !b.topics[tn]) kerr(`Sink connector ko maujooda topic chahiye — '${tn}' exist nahi karta.`);
      if (plugin.type === 'source' && !b.topics[tn]) b.createTopic(tn, parseInt(f.partitions, 10) || 3, Math.min(3, b.brokers.length));

      const maxTasks = parseInt(f.tasks, 10) || 1;
      const parts = b.topic(tn).partitions.length;
      /* Connect ka asli niyam: sink tasks partitions se zyada nahi ho sakte */
      const effective = plugin.type === 'sink' ? Math.min(maxTasks, parts) : maxTasks;

      const c = {
        name, cls, type: plugin.type, topic: tn,
        maxTasks, tasks: effective,
        transforms,
        errorsTolerance: f['errors-tolerance'] === 'all' ? 'all' : 'none',
        dlq: f.dlq && f.dlq !== true ? f.dlq : null,
        state: 'RUNNING',
        taskStates: Array.from({ length: effective }, () => 'RUNNING'),
        produced: 0, consumed: 0, dlqCount: 0, filtered: 0,
        lastError: null,
      };
      if (c.dlq && !b.topics[c.dlq]) b.createTopic(c.dlq, 1, Math.min(3, b.brokers.length));
      b.connectors[name] = c;

      /* sink connector = consumer group. Yahi sabse bada "aha" moment hai. */
      if (c.type === 'sink') {
        const gn = b.connectorGroupName(name);
        const g = b.groups[gn] || b.createGroup(gn, 0);
        g.subscriptions = [tn];
        if (!g.committed[tn]) g.committed[tn] = b.topic(tn).partitions.map(p => p.logStartOffset);
        for (let i = 0; i < effective; i++) b.addMember(gn, 'connector-task');
      }

      const warn = [];
      if (plugin.type === 'sink' && maxTasks > parts) {
        warn.push(`⚠️ tasks.max=${maxTasks} maanga par topic mein sirf ${parts} partitions hain → ${maxTasks - parts} task khaali baithenge.`);
        warn.push(`   Sink connector consumer group hi hota hai — partitions se zyada parallelism mumkin hi nahi.`);
      }
      if (plugin.type === 'sink' && c.errorsTolerance === 'none') {
        warn.push(`⚠️ errors.tolerance=none — ek bhi kharab record poore task ko FAILED kar dega.`);
      }
      return `Created connector '${name}'\n` +
        `   class    : ${cls} (${plugin.type})\n` +
        `   topic    : ${tn}\n` +
        `   tasks    : ${effective}${effective !== maxTasks ? ` (tasks.max=${maxTasks} maanga tha)` : ''}\n` +
        `   transforms: ${transforms.length ? transforms.join(' → ') : '(koi nahi)'}\n` +
        `   errors   : tolerance=${c.errorsTolerance}${c.dlq ? `, dlq=${c.dlq}` : ''}\n` +
        (c.type === 'sink' ? `   ➡️  consumer group '${b.connectorGroupName(name)}' ban gaya — kafka-consumer-groups --list se dekho!\n` : '') +
        (warn.length ? '\n' + warn.join('\n') : '');
    }

    if (sub === 'list') {
      const rows = Object.values(b.connectors).map(c => [
        c.name, c.cls, c.type, c.topic, `${c.taskStates.filter(s => s === 'RUNNING').length}/${c.tasks}`, c.state,
      ]);
      return rows.length ? table(['NAME', 'CLASS', 'TYPE', 'TOPIC', 'TASKS', 'STATE'], rows)
        : '(koi connector nahi — connect create se banao)';
    }

    if (sub === 'status') {
      const c = connectorOrDie(f._[1]);
      const rows = c.taskStates.map((s, i) => [i, s, s === 'FAILED' ? (c.lastError || 'error') : '—']);
      const lag = c.type === 'sink' ? b.totalLag(b.connectorGroupName(c.name)) : '—';
      return `Connector: ${c.name}   state=${c.state}\n` +
        `class=${c.cls}  type=${c.type}  topic=${c.topic}\n` +
        (c.type === 'sink' ? `consumer group: ${b.connectorGroupName(c.name)}   lag: ${lag}\n` : '') +
        `records: produced=${c.produced} consumed=${c.consumed} dlq=${c.dlqCount} filtered=${c.filtered}\n\n` +
        table(['TASK', 'STATE', 'TRACE'], rows) +
        (c.state === 'FAILED' || c.taskStates.includes('FAILED')
          ? `\n\n💀 Task fail hone par Connect usse APNE AAP restart NAHI karta.\n` +
            `   'connect restart ${c.name}' chalao — par pehle wajah theek karo,\n` +
            `   warna wahi record dobara task ko maar dega (poison pill loop).`
          : '');
    }

    if (sub === 'pause' || sub === 'resume' || sub === 'restart' || sub === 'delete') {
      const c = connectorOrDie(f._[1]);
      if (sub === 'pause') { c.state = 'PAUSED'; c.taskStates = c.taskStates.map(() => 'PAUSED'); return `Connector '${c.name}' PAUSED — offsets waheen rukе hain, lag badhega.`; }
      if (sub === 'resume') { c.state = 'RUNNING'; c.taskStates = c.taskStates.map(() => 'RUNNING'); return `Connector '${c.name}' RUNNING.`; }
      if (sub === 'restart') {
        c.state = 'RUNNING'; c.taskStates = c.taskStates.map(() => 'RUNNING'); c.lastError = null;
        return `Connector '${c.name}' aur uske ${c.tasks} tasks restart hue.\n` +
          (c.errorsTolerance === 'none' ? `⚠️ errors.tolerance abhi bhi 'none' hai — wahi poison record phir maar dega.` : '');
      }
      requireQuorum('delete connector');
      delete b.connectors[c.name];
      return `Connector '${c.name}' delete hua.` +
        (c.type === 'sink' ? `\n(consumer group '${b.connectorGroupName(c.name)}' ab orphan hai — kafka-consumer-groups --delete se hatao)` : '');
    }

    if (sub === 'poll' || sub === 'run') {
      const c = connectorOrDie(f._[1]);
      const n = parseInt(f.records, 10) || 50;
      if (c.state !== 'RUNNING') return `Connector '${c.name}' ${c.state} hai — kuch nahi hua.`;
      if (c.taskStates.every(s => s === 'FAILED')) return `Saare tasks FAILED hain — 'connect status ${c.name}' dekho.`;

      if (c.type === 'source') {
        let made = 0;
        for (let i = 0; i < n; i++) {
          let rec = { key: 'row-' + (c.produced + i), value: 'id=' + (c.produced + i) + '|card=4111111111111111|amount=' + (10 + (i % 90)) };
          for (const t of c.transforms) { rec = SMTS[t](rec); if (!rec) break; }
          if (!rec) { c.filtered++; continue; }
          b.produce(c.topic, rec.key, rec.value, { acks: 'all' });
          made++;
        }
        c.produced += made;
        return `Source connector '${c.name}' ne ${made} records '${c.topic}' mein likhe.` +
          (c.filtered ? `  (${c.filtered} Filter SMT se gire)` : '') +
          `\n${c.transforms.length ? 'SMT chain: ' + c.transforms.join(' → ') : ''}\n\n` +
          table(['PARTITION', 'LOG-END-OFFSET'],
            b.topic(c.topic).partitions.map(p => [`${c.topic}-${p.id}`, leo(p)]));
      }

      /* SINK — asli consumer group se padho */
      const gn = b.connectorGroupName(c.name);
      const snapshot = (b.groups[gn] && b.groups[gn].committed[c.topic] || []).slice();
      const recs = b.consume(gn, c.topic, n, { autoOffsetReset: 'earliest' }).filter(r => !r.__reset);
      const doneOffsets = snapshot.slice();
      let ok = 0, bad = 0;
      for (const r of recs) {
        const poison = String(r.value).includes('CORRUPT');
        if (poison) {
          if (c.errorsTolerance === 'all') {
            bad++;
            if (c.dlq) { b.produce(c.dlq, r.key, r.value, { acks: 'all' }); c.dlqCount++; }
            if (doneOffsets[r.partition] == null || doneOffsets[r.partition] < r.offset + 1) {
              doneOffsets[r.partition] = r.offset + 1;
            }
          } else {
            c.lastError = `org.apache.kafka.connect.errors.DataException: record at offset ${r.offset} deserialize nahi hua`;
            const idx = c.taskStates.findIndex(s => s === 'RUNNING');
            if (idx >= 0) c.taskStates[idx] = 'FAILED';
            if (c.taskStates.every(s => s === 'FAILED')) c.state = 'FAILED';
            else c.state = 'DEGRADED';
            c.consumed += ok;
            /* Task fail hua matlab offsets AAGE NAHI badhte — lag wahin atak jaata hai.
               Yahi production ka asli symptom hai: "connector chal raha hai par lag chadh raha hai". */
            if (b.groups[gn]) {
              doneOffsets[r.partition] = r.offset;          // poison record par ruke raho
              b.groups[gn].committed[c.topic] = doneOffsets;
            }
            return `💀 Sink connector '${c.name}' ka ek task FAILED.\n` +
              `   ${c.lastError}\n` +
              `   ${ok} records likhe gaye, phir kharab record par ruk gaya.\n\n` +
              `   errors.tolerance=none ka matlab yehi hai: EK kharab record poori pipeline rok deta hai.\n` +
              `   Ilaaj: connector ko errors.tolerance=all aur ek DLQ topic ke saath dobara banao.`;
          }
        } else {
          ok++;
          if (doneOffsets[r.partition] == null || doneOffsets[r.partition] < r.offset + 1) {
            doneOffsets[r.partition] = r.offset + 1;
          }
        }
      }
      c.consumed += ok;
      const lag = b.totalLag(gn);
      return `Sink connector '${c.name}' ne ${ok} records ${c.cls.replace('Sink', '')} par likhe.` +
        (bad ? `\n   ${bad} kharab records${c.dlq ? ` → DLQ topic '${c.dlq}' mein bheje` : ' → chup-chaap gire (koi DLQ nahi!)'}` : '') +
        `\n   consumer group '${gn}' ka lag ab: ${lag}` +
        (lag === 0 ? '  🎉 caught up' : '') +
        (bad && !c.dlq ? `\n\n⚠️ errors.tolerance=all bina DLQ ke = data CHUP-CHAAP kho raha hai. Yeh sabse khatarnak setting hai.` : '');
    }

    kerr('connect plugins | create | list | status <n> | poll <n> [--records N] | pause|resume|restart|delete <n>');
  }

  function cmdStatus() {
    const brokerLine = 'Brokers: ' + b.brokers.map(x => `${x.id}${x.alive ? '✅' : '💀DOWN'}`).join('  ');
    const topicRows = Object.values(b.topics).map(t => [
      t.name, t.partitions.length, t.rf,
      t.partitions.reduce((s, p) => s + p.log.length, 0),
      t.config['cleanup.policy'],
      t.partitions.some(p => p.underMinIsr) ? '⚠️ under-min-isr' : t.partitions.some(p => p.leader === -1) ? '💀 offline' : 'ok',
    ]);
    const groupRows = Object.values(b.groups).map(g => [
      g.name, g.members.length, g.state, b.totalLag(g.name),
    ]);
    return [
      brokerLine,
      `Clock: ${new Date(b.time).toISOString().replace('T', ' ').slice(0, 19)}`,
      '',
      table(['TOPIC', 'PARTITIONS', 'RF', 'RECORDS', 'CLEANUP', 'HEALTH'], topicRows),
      '',
      groupRows.length ? table(['GROUP', 'MEMBERS', 'STATE', 'TOTAL-LAG'], groupRows) : '(koi consumer group nahi)',
    ].join('\n');
  }

  /* ---- sim: woh cheezein jo asli CLI se nahi hotin ---- */
  function cmdSim(f, args) {
    const sub = f._[0];
    if (sub === 'consumers' || sub === 'consumer') {
      const action = f._[1], gn = f._[2];
      const n = parseInt(f._[3], 10) || 1;
      if (!gn) kerr('sim consumer add|remove <group> [count]');
      if (action === 'add') {
        for (let i = 0; i < n; i++) b.addMember(gn);
        const g = b.group(gn);
        return `${n} consumer add hue → REBALANCE (generation ${g.generation}).\n` +
          `Group '${gn}' ab ${g.members.length} members ka hai.\n\n` +
          assignmentTable(g) +
          (g.idleMembers ? `\n\n😴 ${g.idleMembers} consumer bilkul khaali baithe hain — inhe koi partition mila hi nahi.` : '');
      }
      if (action === 'remove') {
        for (let i = 0; i < n; i++) b.removeMember(gn);
        const g = b.group(gn);
        return `${n} consumer gaye → REBALANCE (generation ${g.generation}).\n` +
          `⏸️  Rebalance ke doraan poora group PROCESSING ROK deta hai (stop-the-world).\n\n` + assignmentTable(g);
      }
      kerr('sim consumer add|remove <group> [count]');
    }
    if (sub === 'broker') {
      const action = f._[1], id = parseInt(f._[2], 10);
      if (action === 'kill') {
        b.killBroker(id);
        const affected = [];
        Object.values(b.topics).forEach(t => t.partitions.forEach(p => {
          if (p.underMinIsr || p.leader === -1) affected.push(`${t.name}-${p.id}`);
        }));
        return `💀 Broker ${id} down.\n` + cmdTopics({ describe: true }) +
          (affected.length ? `\n\n⚠️ Ab in partitions par acks=all waale produce FAIL honge: ${affected.join(', ')}` : `\n\n✅ Baaki replicas ne sambhal liya — koi downtime nahi. Yahi replication ka fayda hai.`);
      }
      if (action === 'start') { b.startBroker(id); return `✅ Broker ${id} wapas zinda. ISR bhar gaya.\n` + cmdTopics({ describe: true }); }
      kerr('sim broker kill|start <id>');
    }
    if (sub === 'controller') {
      const action = f._[1], id = parseInt(f._[2], 10);
      if (action === 'kill') {
        b.killController(id);
        return `💀 Controller ${id} down.\n\n` + cmdMetadataQuorum({ describe: true });
      }
      if (action === 'start') {
        b.startController(id);
        return `\u2705 Controller ${id} wapas zinda.\n\n` + cmdMetadataQuorum({ describe: true });
      }
      kerr('sim controller kill|start <id>   (voters: ' + b.controllers.map(c => c.id).join(',') + ')');
    }
    if (sub === 'process') {
      const gn = f._[1] || (f.group !== true ? f.group : null);
      if (!gn) kerr('sim process <group> [--records N]');
      const g = b.group(gn);
      const n = parseInt(f.records, 10) || 100;
      const before = b.totalLag(gn);
      let got = 0;
      subsOf(g).forEach(tn => { got += b.consume(gn, tn, n, { autoOffsetReset: 'earliest' }).filter(r => !r.__reset).length; });
      const after = b.totalLag(gn);
      return `⚙️  Group '${gn}' ne ${got} records process kiye aur offsets commit kar diye.\n` +
             `Lag: ${before} → ${after}` + (after === 0 ? '  🎉 Bilkul caught up!' : '');
    }
    if (sub === 'time') {
      const arg = f._[1] || '';
      const m = arg.match(/^\+?(\d+)(ms|s|m|h|d)$/);
      if (!m) kerr('sim time +30s | +5m | +2h | +7d');
      const mult = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 }[m[2]];
      const before = Object.fromEntries(Object.keys(b.topics).map(n => [n, b.topic(n).partitions.reduce((s, p) => s + p.log.length, 0)]));
      b.advanceTime(parseInt(m[1], 10) * mult);
      const lines = [`⏩ Ghadi ${m[1]}${m[2]} aage badhi. Retention/compaction chali.`];
      Object.keys(b.topics).forEach(n => {
        const now = b.topic(n).partitions.reduce((s, p) => s + p.log.length, 0);
        if (now !== before[n]) lines.push(`   ${n}: ${before[n]} → ${now} records (${before[n] - now} udd gaye)`);
      });
      return lines.join('\n');
    }
    if (sub === 'lag') {
      const gn = f._[1];
      if (!gn) kerr('sim lag <group>');
      const g = b.group(gn);
      return subsOf(g).map(tn =>
        `${tn}: ` + b.lagPerPartition(gn, tn).map((l, i) => `p${i}=${l}`).join('  ') + `   TOTAL=${b.lag(gn, tn)}`
      ).join('\n');
    }
    if (sub === 'assignment') {
      const gn = f._[1];
      if (!gn) kerr('sim assignment <group>');
      return assignmentTable(b.group(gn));
    }
    if (sub === 'compact') {
      const tn = f._[1];
      if (!tn) kerr('sim compact <topic>');
      const t = b.topic(tn);
      if (!String(t.config['cleanup.policy']).includes('compact')) {
        return `⚠️ '${tn}' par cleanup.policy=${t.config['cleanup.policy']} hai — compaction chalega hi nahi.\nPehle: kafka-configs --alter --entity-type topics --entity-name ${tn} --add-config cleanup.policy=compact`;
      }
      const before = t.partitions.reduce((s, p) => s + p.log.length, 0);
      const removed = b.compact(tn);
      const after = t.partitions.reduce((s, p) => s + p.log.length, 0);
      return `🗜️  Compaction chali: ${before} → ${after} records (${removed} purani versions/tombstones hate).\n` +
             `Har key ki sirf AAKHRI value bachi hai. Offsets renumber NAHI hue — beech mein gaps dikhenge (yeh normal hai).`;
    }
    kerr(`sim ka yeh sub-command nahi hai. Options: consumer add/remove · broker kill/start · process · time · lag · assignment · compact`);
  }
  function assignmentTable(g) {
    if (!g.members.length) return '(group khaali hai — koi member nahi)';
    return table(['CONSUMER', 'ASSIGNED PARTITIONS'],
      g.members.map(m => [m.id, m.assignment.length ? m.assignment.map(a => `${a.topic}-${a.partition}`).join(', ') : '— (kuch nahi mila 😴)']));
  }

  const HELP = `📚 Supported commands (asli Kafka CLI jaise):

  kafka-topics --create --topic <t> --partitions <n> --replication-factor <r> [--config k=v]
  kafka-topics --list | --describe [--topic <t>] | --alter --topic <t> --partitions <n> | --delete --topic <t>

  kafka-console-producer --topic <t> [--property parse.key=true] --message "key:value" [--message ...]
                         [--acks all|1|0]  [--count <n> --key <fixed|none|random|user-{i}>]
  kafka-console-consumer --topic <t> [--from-beginning] [--group <g>] [--max-messages <n>]
                         [--property print.key=true]

  kafka-consumer-groups --list
  kafka-consumer-groups --describe --group <g>          ← 📉 ASLI lag table yahan
  kafka-consumer-groups --reset-offsets --group <g> --topic <t> (--to-earliest|--to-latest|--to-offset <n>|--shift-by <n>) --execute
  kafka-consumer-groups --delete --group <g>

  kafka-configs --describe --entity-type topics --entity-name <t>
  kafka-configs --alter --entity-type topics --entity-name <t> --add-config retention.ms=60000

  kafka-run-class kafka.tools.GetOffsetShell --topic <t>
  dump <topic> [partition]          ← log ke andar jhaanko (offsets, keys, tombstones)
  status                            ← ek nazar mein poora cluster

  kafka-metadata-quorum --describe [--status | --replication]   ← 🗳️ KRaft controller quorum
  kafka-reassign-partitions --topic <t> --broker-list 1,2,3 (--execute | --verify)

🔌 Kafka Connect (sink connector = ek consumer group!):
  connect plugins
  connect create --name <n> --class <Plugin> --topic <t> [--tasks <n>]
                 [--transform InsertField|MaskField|ExtractField|Filter]
                 [--errors-tolerance all] [--dlq <topic>]
  connect list | status <n> | poll <n> [--records N]
  connect pause|resume|restart|delete <n>

🎛️ Simulator-only (asli CLI mein nahi hote, seekhne ke liye hain):
  sim consumer add <group> [n]      ← rebalance apni aankhon se dekho
  sim consumer remove <group> [n]
  sim broker kill <id> | sim broker start <id>    ← ISR shrink / leader election
  sim process <group> [--records n] ← consumer ka kaam karo, lag ghatao
  sim time +5m | +7d                ← ghadi aage, retention/compaction chalao
  sim controller kill <id> | sim controller start <id>   ← quorum todo, dekho kya rukta hai
  sim lag <group> · sim assignment <group> · sim compact <topic>

  help · clear · hint`;

  return b;
};

/* =====================================================================
   RENDERER — broker ko terminal UI se jodta hai
   ===================================================================== */
KQ.renderers.terminal = function renderTerminal(stage, arena, onDone) {
  const sim = KQ.createBroker(stage.cluster || {});
  sim.history = [];
  sim.hintsUsed = 0;
  sim.done = false;
  sim.stage = stage;
  sim.lastOut = '';
  sim.lastError = null;

  /* ---------- UI ---------- */
  const goalBox = KQ.el('div', 'term-goal');
  goalBox.innerHTML = `<h4>🎯 Mission</h4><div>${stage.goal}</div>`;
  const taskList = KQ.el('ul', 'term-tasks');
  (stage.tasks || []).forEach(t => taskList.appendChild(KQ.el('li', '', t.text)));
  if (stage.tasks) goalBox.appendChild(taskList);

  const term = KQ.el('div', 'terminal');
  term.innerHTML = `
    <div class="term-bar">
      <span class="dot" style="background:#f87171"></span>
      <span class="dot" style="background:#ffb347"></span>
      <span class="dot" style="background:#34d399"></span>
      <span>&nbsp;shishya@kafkagurukul — kafka cli (bootstrap: localhost:9092)</span>
    </div>
    <div class="term-out"></div>
    <div class="term-in"><span class="ps1">$</span><input type="text" autocomplete="off"
      spellcheck="false" placeholder="kafka-topics --list        (madad ke liye: help)"></div>`;
  const out = term.querySelector('.term-out');
  const input = term.querySelector('input');

  const btns = KQ.el('div', 'term-btns');
  const hintBtn = KQ.el('button', 'btn small ghost', `💡 Hint (−${KQ.HINT_COST} XP)`);
  hintBtn.onclick = () => giveHint();
  btns.appendChild(hintBtn);

  arena.append(goalBox, term, btns);
  print('sys', `Mock Kafka cluster ready — ${sim.brokers.length} broker${sim.brokers.length > 1 ? 's' : ''}, ${Object.keys(sim.topics).length} topic${Object.keys(sim.topics).length === 1 ? '' : 's'}. ('help' se saari commands, 'status' se cluster ki halat.)`);
  setTimeout(() => input.focus(), 100);

  const cmdHistory = [];
  let histIdx = -1;
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      const raw = input.value.trim();
      input.value = '';
      if (raw) { cmdHistory.push(raw); histIdx = cmdHistory.length; run(raw); }
    } else if (e.key === 'ArrowUp') {
      if (histIdx > 0) { histIdx--; input.value = cmdHistory[histIdx] || ''; }
      e.preventDefault();
    } else if (e.key === 'ArrowDown') {
      if (histIdx < cmdHistory.length) { histIdx++; input.value = cmdHistory[histIdx] || ''; }
      e.preventDefault();
    }
  });

  function print(kind, text) {
    const div = KQ.el('div', 't-' + kind);
    div.textContent = text;
    out.appendChild(div);
    out.scrollTop = out.scrollHeight;
  }
  function giveHint() {
    const hints = stage.hints || [];
    if (sim.hintsUsed >= hints.length) { print('hint', '💡 Aur hints nahi hain — tum kar sakte ho!'); return; }
    print('hint', `💡 Hint ${sim.hintsUsed + 1}: ${hints[sim.hintsUsed]}`);
    sim.hintsUsed++;
  }

  function refresh() {
    if (stage.tasks) {
      stage.tasks.forEach((t, i) => {
        if (t.check && t.check(sim)) taskList.children[i].classList.add('done');
      });
    }
    if (!sim.done && stage.success && stage.success(sim)) {
      sim.done = true;
      input.disabled = true;
      const xp = Math.max(10, KQ.TERMINAL_XP - sim.hintsUsed * KQ.HINT_COST);
      KQ.addXP(xp);
      const win = KQ.el('div', 'stage-summary pop');
      win.innerHTML = `
        <div class="sum-emoji">🎉</div>
        <div class="sum-title">Mission complete! +${xp} XP${sim.hintsUsed ? ` (${sim.hintsUsed} hint use hue)` : ' — bina hint ke! 🔥'}</div>
        <div class="sum-detail" style="text-align:left;line-height:1.6;margin-top:8px">${stage.successText || ''}</div>`;
      arena.appendChild(win);
      win.scrollIntoView({ behavior: 'smooth', block: 'center' });
      setTimeout(() => onDone({ firstTry: sim.hintsUsed === 0 ? 1 : 0, total: 1 }), 300);
    }
  }

  function run(raw) {
    print('cmd', raw);
    if (sim.done) return;
    if (raw === 'clear') { out.innerHTML = ''; return; }
    if (raw === 'hint') { giveHint(); return; }
    sim.history.push(raw);
    sim.lastError = null;
    try {
      // stage-specific scripted commands builtins se PEHLE match hote hain
      const scripted = (stage.responses || []).find(r => r.re.test(raw));
      if (scripted) {
        const m = raw.match(scripted.re);
        if (scripted.mutate) scripted.mutate(sim, m);
        const text = typeof scripted.out === 'function' ? scripted.out(sim, m) : scripted.out;
        if (text) { sim.lastOut = text; print('ok', text); }
      } else {
        const text = sim.run(raw);
        sim.lastOut = text || '';
        if (text) print('ok', text);
      }
    } catch (e) {
      if (e.kafkaErr) { sim.lastError = e.message; print('err', e.message); }
      else { console.error('kafkacli error:', e); print('err', `simulator error: ${e.message} — command dobara likho ya 'help' dekho`); }
    }
    refresh();
  }

  refresh();
};

/* node harness ke liye (browser mein koi asar nahi) */
if (typeof module !== 'undefined' && module.exports) module.exports = { createBroker: KQ.createBroker, partitionForKey };

})();
