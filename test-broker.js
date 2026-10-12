#!/usr/bin/env node
/* kafkacli.js engine smoke test — browser ke bina */
global.KQ = { renderers: {}, el: () => ({}), HINT_COST: 5, TERMINAL_XP: 30, addXP() {} };
const { createBroker, partitionForKey } = require('./kafkacli.js');

let pass = 0, fail = 0;
function ok(name, cond, extra) {
  if (cond) { pass++; console.log('  ✅', name); }
  else { fail++; console.log('  ❌', name, extra != null ? '\n     → ' + extra : ''); }
}
function section(s) { console.log('\n=== ' + s + ' ==='); }

/* ---------- 1. partitioning is real murmur2 ---------- */
section('1. Key → partition (asli murmur2)');
const p1 = partitionForKey('user-42', 3);
ok('same key → same partition (deterministic)', partitionForKey('user-42', 3) === p1);
ok('partition in range', p1 >= 0 && p1 < 3, 'got ' + p1);
const spread = new Set();
for (let i = 0; i < 200; i++) spread.add(partitionForKey('user-' + i, 6));
ok('different keys spread across all 6 partitions', spread.size === 6, 'hit ' + spread.size);

/* ---------- 2. topics + produce + real LEO ---------- */
section('2. Produce & log end offset');
const b = createBroker({ brokers: 3 });
b.run('kafka-topics --create --topic orders --partitions 3 --replication-factor 3');
ok('topic created', b.hasTopic('orders'));
b.produce('orders', 'u1', 'a'); b.produce('orders', 'u1', 'b'); b.produce('orders', 'u1', 'c');
const counts = b.partitionCounts('orders');
ok('same key → all 3 msgs on ONE partition', counts.filter(c => c > 0).length === 1, JSON.stringify(counts));
ok('offsets are 0,1,2 on that partition',
  JSON.stringify(b.topic('orders').partitions.find(p => p.log.length).log.map(r => r.offset)) === '[0,1,2]');

/* ---------- 3. hot partition detection ---------- */
section('3. Hot partition (asli, hardcode nahi)');
const hot = createBroker({ brokers: 3, topics: { evt: { partitions: 4, rf: 3 } } });
const hotOut = hot.run('kafka-console-producer --topic evt --count 50 --key SAME');
ok('bulk same-key → hot partition warning', /HOT PARTITION/.test(hotOut));
ok('exactly one partition has all 50', hot.partitionCounts('evt').filter(c => c === 50).length === 1,
  JSON.stringify(hot.partitionCounts('evt')));
const cool = createBroker({ brokers: 3, topics: { evt: { partitions: 4, rf: 3 } } });
cool.run('kafka-console-producer --topic evt --count 200 --key user-{i}');
ok('varied keys → all partitions used', cool.partitionCounts('evt').every(c => c > 0),
  JSON.stringify(cool.partitionCounts('evt')));

/* ---------- 4. LAG is computed, not stored ---------- */
section('4. Consumer lag (LEO − committed)');
const lb = createBroker({
  brokers: 3,
  topics: { orders: { partitions: 3, rf: 3 } },
  groups: { 'order-workers': { members: 2, committed: { orders: [0, 0, 0] } } },
});
lb.run('kafka-console-producer --topic orders --count 300 --key u-{i}');
const totalRecords = lb.partitionCounts('orders').reduce((a, c) => a + c, 0);
ok('300 records produced', totalRecords === 300, String(totalRecords));
ok('lag === 300 (nothing consumed)', lb.lag('order-workers', 'orders') === 300,
  String(lb.lag('order-workers', 'orders')));
const desc = lb.run('kafka-consumer-groups --describe --group order-workers');
ok('describe shows LAG column', /LAG/.test(desc) && /Total lag: 300/.test(desc));
lb.run('sim process order-workers --records 1000');
ok('after processing, lag === 0', lb.lag('order-workers', 'orders') === 0,
  String(lb.lag('order-workers', 'orders')));

/* ---------- 5. rebalance + idle consumers ---------- */
section('5. Rebalance & partition = parallelism limit');
const rb = createBroker({ brokers: 3, topics: { t: { partitions: 3, rf: 3 } },
  groups: { g: { members: 1, committed: { t: [0, 0, 0] } } } });
ok('1 member owns all 3 partitions', rb.group('g').members[0].assignment.length === 3);
rb.run('sim consumer add g 2');
ok('3 members → 1 partition each', rb.group('g').members.every(m => m.assignment.length === 1));
const idleOut = rb.run('sim consumer add g 2');
ok('5 members on 3 partitions → 2 idle', rb.group('g').idleMembers === 2, String(rb.group('g').idleMembers));
ok('idle warning shown', /khaali baithe/.test(idleOut));
ok('generation bumped on each rebalance', rb.group('g').generation >= 3);

/* ---------- 6. acks + min.insync.replicas ---------- */
section('6. acks=all + min.insync.replicas + broker death');
const ab = createBroker({
  brokers: 3,
  topics: { pay: { partitions: 1, rf: 3, config: { 'min.insync.replicas': 2 } } },
});
ab.killBroker(2);
let threw = null;
try { ab.produce('pay', 'k', 'v', { acks: 'all' }); } catch (e) { threw = e.message; }
ok('2 replicas alive, minISR=2 → produce still OK', threw === null, threw);
ab.killBroker(3);
try { ab.produce('pay', 'k', 'v', { acks: 'all' }); threw = null; } catch (e) { threw = e.message; }
ok('1 replica alive, minISR=2 → NotEnoughReplicas', threw && /NotEnoughReplicas/.test(threw), threw);
const before = ab.topic('pay').partitions[0].log.length;
ab.produce('pay', 'k', 'v', { acks: '1' });
ok('acks=1 still writes (leader zinda hai)', ab.topic('pay').partitions[0].log.length === before + 1);
ab.killBroker(1);
const r0 = ab.produce('pay', 'k', 'v', { acks: '0' });
ok('acks=0 with no leader → silently LOST', r0.lost === true);

/* ---------- 7. retention eats offsets → OffsetOutOfRange ---------- */
section('7. Retention → OffsetOutOfRange (asli 3AM incident)');
const rt = createBroker({
  brokers: 1,
  topics: { logs: { partitions: 1, rf: 1, config: { 'retention.ms': 60000 } } },
  groups: { slow: { members: 0, committed: { logs: [0] } } },
});
rt.run('kafka-console-producer --topic logs --count 100 --key none');
ok('100 records in log', rt.partitionCounts('logs')[0] === 100);
rt.advanceTime(5 * 60 * 1000);
ok('retention swept old records', rt.partitionCounts('logs')[0] === 0, JSON.stringify(rt.partitionCounts('logs')));
ok('logStartOffset moved to 100', rt.topic('logs').partitions[0].logStartOffset === 100,
  String(rt.topic('logs').partitions[0].logStartOffset));
const recs = rt.consume('slow', 'logs', 10, { autoOffsetReset: 'earliest' });
ok('consumer at offset 0 gets OffsetOutOfRange reset', recs.some(r => r.__reset), JSON.stringify(recs));

/* ---------- 8. log compaction ---------- */
section('8. Log compaction + tombstones');
const cb = createBroker({
  brokers: 1,
  topics: { profiles: { partitions: 1, rf: 1, config: { 'cleanup.policy': 'compact' } } },
});
cb.produce('profiles', 'u1', 'name=A'); cb.produce('profiles', 'u2', 'name=B');
cb.produce('profiles', 'u1', 'name=A2'); cb.produce('profiles', 'u1', 'name=A3');
cb.produce('profiles', 'u2', null);   // tombstone
ok('5 records before compaction', cb.partitionCounts('profiles')[0] === 5);
cb.compact('profiles');
const kept = cb.topic('profiles').partitions[0].log;
ok('compaction keeps 1 record (u1 latest; u2 tombstoned away)', kept.length === 1, JSON.stringify(kept));
ok('kept record is the LATEST value', kept[0] && kept[0].value === 'name=A3', JSON.stringify(kept[0]));

/* ---------- 9. offset reset guard + execute ---------- */
section('9. reset-offsets');
const ob = createBroker({
  brokers: 1, topics: { t: { partitions: 2, rf: 1 } },
  groups: { g: { members: 1, committed: { t: [0, 0] } } },
});
ob.run('kafka-console-producer --topic t --count 40 --key k-{i}');
let e9 = null;
try { ob.run('kafka-consumer-groups --reset-offsets --group g --topic t --to-latest --execute'); }
catch (e) { e9 = e.message; }
ok('cannot reset while group is live', e9 && /inactive/.test(e9), e9);
ob.removeMember('g');
const dry = ob.run('kafka-consumer-groups --reset-offsets --group g --topic t --to-latest');
ok('dry run does not change offsets', /dry run/.test(dry) && ob.lag('g', 't') === 40, String(ob.lag('g', 't')));
ob.run('kafka-consumer-groups --reset-offsets --group g --topic t --to-latest --execute');
ok('--execute sets lag to 0', ob.lag('g', 't') === 0, String(ob.lag('g', 't')));

/* ---------- 10. adding partitions breaks key ordering ---------- */
section('10. Partition badhane se ordering tootna');
const pb = createBroker({ brokers: 1, topics: { t: { partitions: 3, rf: 1 } } });
pb.run('kafka-topics --alter --topic t --partitions 6');
ok('topic now has 6 partitions', pb.topic('t').partitions.length === 6);
// Har key nahi hilti — par zyadatar hilti hain. Yahi ordering-break ka asli scale hai.
let moved = 0;
for (let i = 0; i < 500; i++) {
  if (partitionForKey('user-' + i, 3) !== partitionForKey('user-' + i, 6)) moved++;
}
ok('~half the keys land on a different partition (ordering broken)', moved > 200 && moved < 400,
  `${moved}/500 keys moved`);

/* ---------- 11. CLI error messages ---------- */
section('11. Errors');
const eb = createBroker({ brokers: 3 });
function expectErr(cmd, re, label) {
  try { eb.run(cmd); ok(label, false, 'koi error hi nahi aaya'); }
  catch (e) { ok(label, re.test(e.message), e.message); }
}
function expectErrOn(brk, cmd, re, label) {
  try { brk.run(cmd); ok(label, false, 'koi error hi nahi aaya'); }
  catch (e) { ok(label, re.test(e.message), e.message); }
}
expectErr('kafka-topics --describe --topic ghost', /UnknownTopicOrPartition/, 'unknown topic');
expectErr('kafka-topics --create --topic x --partitions 1 --replication-factor 9', /larger than available brokers/, 'rf > brokers');
expectErr('kafka-frobnicate --wat', /command not found/, 'unknown command');
eb.run('kafka-topics --create --topic x --partitions 4 --replication-factor 2');
expectErr('kafka-topics --alter --topic x --partitions 2', /higher or equal/, 'cannot shrink partitions');

/* ---------- 12. help/status don't crash ---------- */
section('12. help / status / dump');
ok('help works', /Supported commands/.test(eb.run('help')));
ok('status works', /Brokers:/.test(eb.run('status')));
eb.run('kafka-console-producer --topic x --message hello');
ok('dump shows log internals', /Dumping x-/.test(eb.run('dump x')));
ok('GetOffsetShell works', /^x:0:/m.test(eb.run('kafka-run-class kafka.tools.GetOffsetShell --topic x')));


/* ---------- 13. KRaft controller quorum ---------- */
section('13. KRaft — controller quorum');
const q_kb = createBroker({ brokers: 3, controllers: 3, topics: { orders: { partitions: 3, rf: 3 } } });
ok('3 controllers bane', q_kb.controllers.length === 3);
ok('majority 2 hai', q_kb.quorumMajority() === 2, 'got ' + q_kb.quorumMajority());
ok('shuru mein quorum healthy', q_kb.hasQuorum());
ok('quorum status leader dikhata hai', /LeaderId:\s+90/.test(q_kb.run('kafka-metadata-quorum --describe --status')));
ok('replication view chalta hai', /NodeId/.test(q_kb.run('kafka-metadata-quorum --describe --replication')));

q_kb.run('sim controller kill 9091');
ok('1 controller marne par quorum zinda (2/3)', q_kb.hasQuorum());
ok('quorum zinda par topic ban jaata hai',
  (() => { try { q_kb.run('kafka-topics --create --topic ok1 --partitions 1 --replication-factor 1'); return true; } catch (e) { return false; } })());

q_kb.run('sim controller kill 9092');
ok('2 controllers marne par quorum TOOTA', !q_kb.hasQuorum());
ok('quorum lost par LeaderId -1', /LeaderId:\s+-1/.test(q_kb.run('kafka-metadata-quorum --describe --status')));
expectErrOn(q_kb, 'kafka-topics --create --topic nope --partitions 1 --replication-factor 1',
  /TimeoutException/, 'metadata write quorum ke bina fail hoti hai');
ok('quorum lost par PRODUCE phir bhi chalta hai (data plane zinda)',
  (() => {
    try { q_kb.run('kafka-console-producer --topic orders --count 10 --key u1'); }
    catch (e) { return false; }
    return q_kb.partitionCounts('orders').reduce((a, c) => a + c, 0) === 10;
  })());
ok('quorum lost par CONSUME bhi chalta hai',
  (() => { try { q_kb.run('kafka-console-consumer --topic orders --from-beginning --group g1 --max-messages 5'); return true; } catch (e) { return false; } })());
ok('quorum lost par describe (read) chalta hai', /Topic: orders/.test(q_kb.run('kafka-topics --describe --topic orders')));

q_kb.run('sim controller start 9091');
ok('controller wapas aane par quorum bahaal', q_kb.hasQuorum());
ok('quorum bahaal hote hi topic phir ban jaata hai',
  (() => { try { q_kb.run('kafka-topics --create --topic ok2 --partitions 1 --replication-factor 1'); return true; } catch (e) { return false; } })());

/* ---------- 14. partition reassignment ---------- */
section('14. Partition reassignment');
const q_rb = createBroker({
  brokers: 5, controllers: 3,
  topics: { orders: { partitions: 6, rf: 3, messages: Array.from({ length: 120 }, (_, i) => ({ key: 'k' + i, value: 'v' })) } },
});
const beforeSet = new Set(q_rb.topics.orders.partitions.flatMap(p => p.replicas));
ok('shuru mein saare 5 brokers use nahi ho rahe (ya ho rahe hain — dono theek)', beforeSet.size <= 5);
const exec = q_rb.run('kafka-reassign-partitions --topic orders --broker-list 1,2,3,4,5 --execute');
ok('execute ne plan dikhaya', /Successfully started partition reassignment/.test(exec));
ok('execute ke baad reassignment pending hai', q_rb.topics.orders.reassign.pending === true);
const ver = q_rb.run('kafka-reassign-partitions --topic orders --verify');
ok('verify ne complete kiya', /is completed/.test(ver));
ok('verify ke baad pending clear', q_rb.topics.orders.reassign.pending === false);
const afterSet = new Set(q_rb.topics.orders.partitions.flatMap(p => p.replicas));
ok('reassignment ke baad saare 5 brokers replicas mein hain',
  [1, 2, 3, 4, 5].every(id => afterSet.has(id)), [...afterSet].join(','));
ok('reassignment se data nahi gaya',
  q_rb.partitionCounts('orders').reduce((a, c) => a + c, 0) === 120);
expectErrOn(q_rb, 'kafka-reassign-partitions --topic orders --broker-list 1,99 --execute',
  /cluster mein hai hi nahi/, 'anjaan broker id reject hota hai');

/* ---------- 15. Kafka Connect ---------- */
section('15. Kafka Connect');
const q_cb = createBroker({
  brokers: 3, controllers: 3,
  topics: {
    payments: {
      partitions: 3, rf: 3,
      messages: [
        ...Array.from({ length: 20 }, (_, i) => ({ key: 'p' + i, value: 'amount=' + i })),
        { key: 'bad', value: 'CORRUPT_PAYLOAD' },
        ...Array.from({ length: 15 }, (_, i) => ({ key: 'q' + i, value: 'amount=' + i })),
      ],
    },
  },
});
ok('plugins list chalti hai', /S3Sink/.test(q_cb.run('connect plugins')));
expectErrOn(q_cb, 'connect create --name x --class NoSuchThing --topic payments',
  /ClassNotFoundException/, 'anjaan connector class reject hoti hai');

/* source connector */
q_cb.run('connect create --name src --class JdbcSource --topic ingest --tasks 2 --transform MaskField');
ok('source ne topic bana diya', q_cb.hasTopic('ingest'));
q_cb.run('connect poll src --records 30');
ok('source ne records produce kiye', q_cb.partitionCounts('ingest').reduce((a, c) => a + c, 0) === 30);
ok('MaskField SMT ne card number chhupaya',
  q_cb.topics.ingest.partitions.some(p => p.log.some(r => /MASKED/.test(String(r.value)))));

/* sink = consumer group */
q_cb.run('connect create --name sink1 --class S3Sink --topic payments --tasks 10');
ok('sink tasks partitions par cap hote hain (10 maanga, 3 mile)', q_cb.connectors.sink1.tasks === 3,
  'got ' + q_cb.connectors.sink1.tasks);
ok('sink ka consumer group ban gaya', !!q_cb.groups['connect-sink1']);
ok('group kafka-consumer-groups --list mein dikhta hai', /connect-sink1/.test(q_cb.run('kafka-consumer-groups --list')));

/* poison pill — bina DLQ ke task marta hai */
const failOut = q_cb.run('connect poll sink1 --records 100');
ok('poison record par task FAILED hua', /FAILED/.test(failOut));
ok('connector DEGRADED/FAILED state mein hai', q_cb.connectors.sink1.state !== 'RUNNING');
ok('task fail hone par lag ATKA rehta hai (production symptom)',
  q_cb.totalLag('connect-sink1') > 0, 'lag ' + q_cb.totalLag('connect-sink1'));
const stuckLag = q_cb.totalLag('connect-sink1');
q_cb.run('connect poll sink1 --records 100');
ok('failed connector dobara poll par aage nahi badhta', q_cb.totalLag('connect-sink1') === stuckLag);

/* DLQ ke saath sab nikal jaata hai */
q_cb.run('connect delete sink1');
q_cb.run('connect create --name sink2 --class S3Sink --topic payments --tasks 3 --errors-tolerance all --dlq pay-dlq');
ok('DLQ topic apne aap ban gaya', q_cb.hasTopic('pay-dlq'));
const okOut = q_cb.run('connect poll sink2 --records 100');
ok('DLQ ke saath pipeline nahi rukhi', !/FAILED/.test(okOut));
ok('kharab record DLQ mein gaya', q_cb.connectors.sink2.dlqCount === 1, 'dlq ' + q_cb.connectors.sink2.dlqCount);
ok('DLQ topic mein sach mein record hai', q_cb.partitionCounts('pay-dlq').reduce((a, c) => a + c, 0) === 1);
ok('DLQ ke saath lag 0 ho gaya', q_cb.totalLag('connect-sink2') === 0, 'lag ' + q_cb.totalLag('connect-sink2'));
ok('connect status task table deta hai', /TASK/.test(q_cb.run('connect status sink2')));
q_cb.run('connect pause sink2');
ok('paused connector poll par kuch nahi karta', /PAUSED/.test(q_cb.run('connect poll sink2')));
q_cb.run('connect resume sink2');
ok('resume ke baad RUNNING', q_cb.connectors.sink2.state === 'RUNNING');

/* quorum aur Connect ka rishta */
q_cb.run('sim controller kill 9091');
q_cb.run('sim controller kill 9092');
expectErrOn(q_cb, 'connect create --name sink3 --class S3Sink --topic payments',
  /TimeoutException/, 'quorum ke bina connector nahi banta (config topic par likhna padta hai)');

console.log(`\n${'='.repeat(50)}\n  ${pass} pass · ${fail} fail\n${'='.repeat(50)}`);
process.exit(fail ? 1 : 0);
