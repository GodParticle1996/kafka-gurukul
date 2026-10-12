#!/usr/bin/env node
/* =====================================================================
   Kafka Gurukul — Content Validator

   Browser kholne se PEHLE yeh chalao. Yeh saara content structurally
   check karta hai aur har terminal mission / lab challenge ko ASLI
   engine par chala kar dekhta hai ki woh solve hone laayak hai ya nahi.

       node validate.js

   Naya level likhne ke baad hamesha chalao. Jo galtiyan yeh pakadta hai
   woh browser mein chup-chaap tootti hain (quiz ka answer index galat,
   fill ka answer tokens mein hai hi nahi, incident ka dead-end node…).
   ===================================================================== */

const fs = require('fs');
const path = require('path');
const vm = require('vm');

const DIR = __dirname;
const problems = [];
const warnings = [];
const notes = [];
const P = (where, msg) => problems.push(`${where}: ${msg}`);
const W = (where, msg) => warnings.push(`${where}: ${msg}`);

/* ---------- stubbed browser ---------- */
const noop = () => {};
const fakeEl = () => new Proxy({}, {
  get(t, k) {
    if (k === 'style' || k === 'classList' || k === 'dataset') return fakeEl();
    if (k === 'children' || k === 'childNodes') return [];
    if (k === 'innerHTML' || k === 'textContent' || k === 'value' || k === 'className') return '';
    if (k === Symbol.toPrimitive || k === 'toString') return () => '';
    return typeof k === 'string' ? fakeEl.fn : undefined;
  },
  set() { return true; },
});
fakeEl.fn = new Proxy(noop, { get: () => fakeEl.fn, apply: () => fakeEl() });

const sandbox = {
  console, Math, Date, JSON, Object, Array, String, Number, Boolean, Error,
  Map, Set, Proxy, Symbol, RegExp, isNaN, parseInt, parseFloat, setTimeout: noop,
  module: { exports: {} },
  localStorage: { getItem: () => null, setItem: noop, removeItem: noop },
  document: {
    getElementById: () => fakeEl(), createElement: () => fakeEl(),
    querySelector: () => fakeEl(), querySelectorAll: () => [],
    addEventListener: noop, body: fakeEl(),
  },
  window: { scrollTo: noop, addEventListener: noop },
  Element: { prototype: {} },
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);

/* core.js `const KQ` likhta hai, aur vm mein har script ka apna lexical scope
   hota hai — isliye alag-alag runInContext calls ek doosre ka KQ nahi dekh
   paatin (browser mein yeh kaam karta hai kyunki wahan sab ek hi script scope
   share karte hain). Isliye sab kuch JOD kar EK script ki tarah chalate hain,
   bilkul waise jaise browser karta hai. */
const FILES = ['core.js', 'data-core.js', 'visuals.js', 'games.js', 'kafkacli.js', 'lab.js',
               'data-beginner.js', 'data-intermediate.js', 'data-advanced.js', 'data-sysdesign.js'];

const parts = [];
FILES.forEach(f => {
  const p = path.join(DIR, f);
  if (!fs.existsSync(p)) { P('load', `${f} maujood hi nahi hai`); return; }
  parts.push(`/* ===== ${f} ===== */\n${fs.readFileSync(p, 'utf8')}`);
});
// epilogue — lexical bindings ko sandbox par le aao taaki bahar se dikh sakein
parts.push(`globalThis.__KQ = typeof KQ !== 'undefined' ? KQ : null;
globalThis.__VISUALS = typeof VISUALS !== 'undefined' ? VISUALS : {};
globalThis.__RANKS = typeof RANKS !== 'undefined' ? RANKS : [];`);

try { vm.runInContext(parts.join('\n'), sandbox, { filename: 'kafkagurukul-bundle.js' }); }
catch (e) {
  console.error('❌ Content load hi nahi hua:', e.message);
  if (e.stack) console.error(e.stack.split('\n').slice(0, 4).join('\n'));
  process.exit(1);
}

const KQ = sandbox.__KQ;
const VISUALS = sandbox.__VISUALS || {};
const RANKS = sandbox.__RANKS || [];
if (!KQ) { console.error('❌ KQ load hi nahi hua — aage nahi badh sakte.'); process.exit(1); }

/* ---------- collect ---------- */
const allLevels = [];
KQ.tracks.forEach(t => (KQ.levelSets[t.id] || []).forEach(l => allLevels.push({ ...l, _track: t.id })));

/* =====================================================================
   1. STRUCTURE
   ===================================================================== */
const seenIds = new Set();
allLevels.forEach(lvl => {
  const at = `${lvl._track}/${lvl.id || '(no id!)'}`;
  ['id', 'icon', 'title', 'badge'].forEach(k => { if (!lvl[k]) P(at, `"${k}" missing hai`); });
  if (lvl.id) {
    if (seenIds.has(lvl.id)) P(at, `duplicate level id "${lvl.id}"`);
    seenIds.add(lvl.id);
    if (!/^[a-z0-9-]+$/.test(lvl.id)) P(at, `id kebab-case hona chahiye: "${lvl.id}"`);
    if (!VISUALS[lvl.id]) W(at, `VISUALS['${lvl.id}'] nahi hai — level bina diagram ke chalega`);
  }
  if (!Array.isArray(lvl.objectives) || !lvl.objectives.length) P(at, 'objectives khaali hain');
  if (!Array.isArray(lvl.lesson) || !lvl.lesson.length) P(at, 'lesson khaali hai');
  if (!Array.isArray(lvl.success) || !lvl.success.length) P(at, 'success criteria khaali hai');
  if (!Array.isArray(lvl.stages) || !lvl.stages.length) P(at, 'stages khaali hain');
  if (!lvl.interview || !Array.isArray(lvl.interview.questions) || !lvl.interview.questions.length)
    P(at, 'interview round missing hai (har level mein hona chahiye)');
  if (!Array.isArray(lvl.cards) || lvl.cards.length < 3)
    W(at, `sirf ${(lvl.cards || []).length} flashcards — 3-5 rakho (Revision Dojo ke liye)`);
  (lvl.cards || []).forEach((c, i) => {
    if (!c.front || !c.back) P(at, `card[${i}] mein front/back missing`);
  });
});

/* =====================================================================
   2. STAGES
   ===================================================================== */
const KNOWN = ['bins', 'order', 'match', 'quiz', 'fill', 'hotspot', 'incident', 'terminal', 'lab'];
const terminalMissions = [];
const labChallenges = [];

function checkStage(st, at) {
  if (!st.type) { P(at, 'stage ka type missing'); return; }
  if (!KNOWN.includes(st.type)) { P(at, `anjaan stage type "${st.type}"`); return; }
  if (!st.title && st.type !== 'terminal') W(at, `${st.type} stage ka title nahi hai`);

  switch (st.type) {
    case 'bins': {
      if (!st.bins || st.bins.length < 2) P(at, 'bins mein kam se kam 2 categories chahiye');
      const ids = new Set((st.bins || []).map(b => b.id));
      if (!st.items || !st.items.length) P(at, 'bins stage mein items nahi hain');
      (st.items || []).forEach((it, i) => {
        if (!ids.has(it.bin)) P(at, `item[${i}] "${it.text}" ka bin "${it.bin}" exist hi nahi karta`);
        if (!it.explain) W(at, `item[${i}] "${it.text}" mein explain nahi hai (kyun nahi bataya)`);
      });
      // har bin mein kam se kam ek item ho
      (st.bins || []).forEach(b => {
        if (!(st.items || []).some(it => it.bin === b.id)) W(at, `bin "${b.id}" khaali hai — koi item usme nahi jaata`);
      });
      break;
    }
    case 'order': {
      if (!st.steps || st.steps.length < 2) P(at, 'order stage mein kam se kam 2 steps chahiye');
      (st.steps || []).forEach((s, i) => {
        if (!s.text) P(at, `step[${i}] ka text missing`);
        if (!s.explain) W(at, `step[${i}] mein explain nahi hai`);
      });
      break;
    }
    case 'match': {
      if (!st.pairs || st.pairs.length < 2) P(at, 'match stage mein kam se kam 2 pairs chahiye');
      (st.pairs || []).forEach((p, i) => {
        if (!p.left || !p.right) P(at, `pair[${i}] mein left/right missing`);
      });
      break;
    }
    case 'quiz': {
      if (!st.questions || !st.questions.length) { P(at, 'quiz mein questions nahi hain'); break; }
      st.questions.forEach((q, i) => {
        if (!q.q) P(at, `q[${i}] ka sawal missing`);
        if (!Array.isArray(q.choices) || q.choices.length < 2) P(at, `q[${i}] mein 2+ choices chahiye`);
        if (typeof q.answer !== 'number') P(at, `q[${i}] ka answer number hona chahiye`);
        else if (q.answer < 0 || q.answer >= (q.choices || []).length)
          P(at, `q[${i}] ka answer index ${q.answer} range se bahar hai (choices: ${(q.choices || []).length})`);
        if (!q.explain) W(at, `q[${i}] mein explain nahi hai`);
      });
      break;
    }
    case 'fill': {
      if (!st.template) { P(at, 'fill stage mein template nahi hai'); break; }
      const slots = (st.template.match(/\{\d+\}/g) || []).map(s => parseInt(s.slice(1, -1), 10));
      const uniq = [...new Set(slots)].sort((a, b) => a - b);
      if (!st.blanks || st.blanks.length !== uniq.length)
        P(at, `template mein ${uniq.length} slots hain par blanks ${(st.blanks || []).length} hain`);
      uniq.forEach((n, i) => { if (n !== i) P(at, `template ke slots 0 se lagataar hone chahiye — {${n}} mila jahan {${i}} chahiye tha`); });
      (st.blanks || []).forEach((b, i) => {
        if (!b.answer) { P(at, `blank[${i}] ka answer missing`); return; }
        if (!(st.tokens || []).includes(b.answer))
          P(at, `blank[${i}] ka answer "${b.answer}" tokens mein hai hi nahi — khelna NAMUMKIN hai`);
        if (!b.explain) W(at, `blank[${i}] mein explain nahi hai`);
      });
      const decoys = (st.tokens || []).length - (st.blanks || []).length;
      if (decoys < 1) W(at, `koi decoy token nahi hai (2-4 rakho warna guess karna aasan hai)`);
      break;
    }
    case 'hotspot': {
      if (!st.visual) { P(at, 'hotspot stage mein visual id nahi hai'); break; }
      const v = VISUALS[st.visual];
      if (!v) { P(at, `hotspot ka visual "${st.visual}" VISUALS mein nahi hai`); break; }
      const targets = new Set([...(v.html.match(/data-hs="([^"]+)"/g) || [])]
        .map(m => m.match(/data-hs="([^"]+)"/)[1]));
      (st.questions || []).forEach((q, i) => {
        if (!targets.has(q.target))
          P(at, `hotspot q[${i}] ka target "${q.target}" diagram mein data-hs se match nahi karta (maujood: ${[...targets].join(', ')})`);
      });
      break;
    }
    case 'incident': {
      if (!st.nodes || !st.start) { P(at, 'incident mein nodes/start missing'); break; }
      if (!st.nodes[st.start]) { P(at, `start node "${st.start}" exist nahi karta`); break; }
      const reach = new Set(); const stack = [st.start];
      while (stack.length) {
        const id = stack.pop();
        if (reach.has(id)) continue;
        reach.add(id);
        const n = st.nodes[id];
        if (!n) { P(at, `node "${id}" reference hua par exist nahi karta`); continue; }
        if (n.end) continue;
        const good = (n.choices || []).filter(c => c.good);
        if (good.length !== 1) P(at, `node "${id}" mein exactly 1 good choice honi chahiye (mili ${good.length})`);
        (n.choices || []).forEach((c, ci) => {
          if (c.good && !c.next) P(at, `node "${id}" choice[${ci}] good hai par next nahi hai`);
          if (c.next) stack.push(c.next);
          if (!c.explain) W(at, `node "${id}" choice[${ci}] mein explain nahi hai`);
        });
      }
      Object.keys(st.nodes).forEach(id => { if (!reach.has(id)) W(at, `node "${id}" tak kabhi pahunch hi nahi sakte (dead code)`); });
      if (![...reach].some(id => st.nodes[id] && st.nodes[id].end)) P(at, 'incident mein koi end node reachable nahi hai');
      break;
    }
    case 'terminal': {
      if (!st.goal) P(at, 'terminal mission mein goal nahi hai');
      if (typeof st.success !== 'function') P(at, 'terminal mission mein success() nahi hai');
      else terminalMissions.push({ st, at });
      if (!st.hints || !st.hints.length) W(at, 'terminal mission mein koi hint nahi hai');
      break;
    }
    case 'lab': {
      if (!st.goal) P(at, 'lab mein goal nahi hai');
      if (typeof st.success !== 'function') P(at, 'lab mein success() nahi hai');
      else labChallenges.push({ st, at });
      if (!st.hints || !st.hints.length) W(at, 'lab mein koi hint nahi hai');
      break;
    }
  }
}

allLevels.forEach(lvl => {
  (lvl.stages || []).forEach((st, i) => checkStage(st, `${lvl.id}/stage[${i}]:${st.type}`));
  (lvl.interview ? lvl.interview.questions : []).forEach((q, i) => {
    const at = `${lvl.id}/interview[${i}]`;
    if (q.terminal) checkStage({ ...q.terminal, type: 'terminal' }, at);
    else if (q.choices) {
      if (typeof q.answer !== 'number' || q.answer < 0 || q.answer >= q.choices.length)
        P(at, `answer index ${q.answer} range se bahar hai`);
      if (!q.explain) W(at, 'explain nahi hai');
    } else P(at, 'na MCQ hai na terminal — kuch toh hona chahiye');
  });
});

/* =====================================================================
   3. TERMINAL MISSIONS — ASLI engine par chala kar dekho
   ===================================================================== */
terminalMissions.forEach(({ st, at }) => {
  let sim;
  try { sim = KQ.createBroker(st.cluster || {}); }
  catch (e) { P(at, `cluster spec se broker bana hi nahi — ${e.message}`); return; }
  sim.history = []; sim.hintsUsed = 0; sim.stage = st; sim.lastError = null; sim.lastOut = '';

  // fresh cluster par success() crash nahi karna chahiye, aur turant true nahi hona chahiye
  let fresh;
  try { fresh = st.success(sim); }
  catch (e) { P(at, `success() fresh cluster par CRASH kar gaya — ${e.message}`); return; }
  if (fresh) P(at, 'success() shuru mein hi true hai — mission bina kuch kiye complete ho jaayega');

  // har task ka check() bhi crash nahi karna chahiye
  (st.tasks || []).forEach((t, i) => {
    if (typeof t.check !== 'function') { W(at, `task[${i}] mein check() nahi hai`); return; }
    try { t.check(sim); }
    catch (e) { P(at, `task[${i}].check() fresh cluster par crash — ${e.message}`); }
  });

  /* Hints ki commands sach mein chalni chahiye.
     ⚠️ Inhe SEQUENCE mein chalao, ek hi probe broker par — kyunki hints
     ek dusre par bante hain (hint 1 topic banata hai, hint 3 usme produce
     karta hai). Har hint ko fresh cluster par chalane se jhoothe
     "UnknownTopicOrPartition" warnings aate hain. */
  const probe = KQ.createBroker(st.cluster || {});
  probe.history = [];
  (st.hints || []).forEach((h, i) => {
    const cmds = String(h).split(/\n|\s+phir\s+/).map(s => s.trim())
      .filter(s => /^(kafka-|sim |dump |status|connect )/.test(s) && !/</.test(s));
    cmds.forEach(cmd => {
      try { probe.run(cmd); }
      catch (e) {
        if (e.kafkaErr) W(at, `hint[${i}] ki command reject hui: "${cmd}" → ${String(e.message).split('\n')[0]}`);
        else P(at, `hint[${i}] ki command ne engine crash kiya: "${cmd}" → ${e.message}`);
      }
    });
  });
});

/* =====================================================================
   4. LAB CHALLENGES — har lab solve hone laayak hai?
   ===================================================================== */
const KNOB_VALUES = {
  consumers: [1, 2, 4, 8, 12, 16, 20, 24],
  processMs: [1, 5, 10, 20, 40, 80, 120, 250],
  retry: ['none', 'fixed', 'exponential', 'expJitterFull', 'expJitterEqual', 'decorrelated'],
  dlq: [false, true],
  breaker: [false, true],
  maxRetries: [0, 1, 3, 5, 10],
  prefetch: [50, 200, 1000, 5000, 19000],
  failRate: [0, 0.1, 0.3, 0.6],
  producerRate: [100, 500, 1200, 3000],
  baseDelay: [0.2, 1, 3],
  breakerThreshold: [0.2, 0.5, 0.8],
  breakerCooldown: [3, 10, 30],
  downstreamCapacity: [200, 900, 2000],
};

labChallenges.forEach(({ st, at }) => {
  const knobs = (st.knobs || []).filter(k => KNOB_VALUES[k]);
  if (!knobs.length) { W(at, 'koi tunable knob nahi hai — player kya badlega?'); return; }

  // default preset par success() crash na kare
  try {
    const r0 = KQ.runPipeline(st.preset || {}, st.duration || 120);
    if (st.success(r0)) P(at, 'default preset se hi success ho jaata hai — koi challenge hi nahi bacha');
    if (st.verdict) st.verdict(r0);
  } catch (e) { P(at, `default preset par crash — ${e.message}`); return; }

  // random search — kya koi combination jeet paata hai?
  let solved = null, tries = 0;
  const MAX = 220;
  for (let i = 0; i < MAX && !solved; i++) {
    const cfg = Object.assign({}, st.preset || {});
    knobs.forEach(k => {
      const vals = KNOB_VALUES[k];
      cfg[k] = vals[Math.floor(Math.random() * vals.length)];
    });
    tries++;
    try {
      const r = KQ.runPipeline(cfg, st.duration || 120);
      if (st.success(r)) solved = knobs.map(k => `${k}=${cfg[k]}`).join(' ');
    } catch (e) { P(at, `runPipeline crash — ${e.message}`); return; }
  }
  if (!solved) P(at, `${tries} random knob combinations try kiye, koi bhi success() ko satisfy nahi karta — lab SOLVE HI NAHI HO SAKTA`);
  else notes.push(`  🎛️ ${at}\n       solvable ✅  (jaise: ${solved})`);
});

/* =====================================================================
   5. RANKS / TRACKS
   ===================================================================== */
if (!RANKS.length) P('data-core', 'RANKS khaali hai');
RANKS.forEach((r, i) => {
  if (i && r.xp <= RANKS[i - 1].xp) P('data-core', `RANKS[${i}] "${r.name}" ka xp badhta hua nahi hai`);
  if (!r.name || !r.icon) P('data-core', `RANKS[${i}] mein name/icon missing`);
});
KQ.tracks.forEach(t => {
  ['id', 'icon', 'name', 'blurb', 'certTitle'].forEach(k => { if (!t[k]) P('data-core', `track "${t.id}" mein ${k} missing`); });
  if (!KQ.levelSets[t.id] || !KQ.levelSets[t.id].length) W('data-core', `track "${t.id}" mein abhi koi level nahi hai`);
});

/* orphan visuals */
Object.keys(VISUALS).forEach(k => {
  if (!seenIds.has(k)) W('visuals.js', `VISUALS['${k}'] hai par us id ka koi level nahi`);
});

/* =====================================================================
   REPORT
   ===================================================================== */
const lvlCount = allLevels.length;
const stageCount = allLevels.reduce((s, l) => s + (l.stages || []).length, 0);
const cardCount = allLevels.reduce((s, l) => s + (l.cards || []).length, 0);
const ivCount = allLevels.reduce((s, l) => s + ((l.interview && l.interview.questions) || []).length, 0);

console.log('\n📊 Kafka Gurukul — content report');
console.log('─'.repeat(58));
KQ.tracks.forEach(t => {
  const n = (KQ.levelSets[t.id] || []).length;
  console.log(`  ${t.icon} ${t.name.padEnd(38)} ${String(n).padStart(2)} levels`);
});
console.log('─'.repeat(58));
console.log(`  ${lvlCount} levels · ${stageCount} stages · ${terminalMissions.length} terminal missions · ` +
            `${labChallenges.length} labs · ${ivCount} interview Qs · ${cardCount} flashcards · ` +
            `${Object.keys(VISUALS).length} diagrams`);

if (notes.length) { console.log('\n🎛️  Lab solvability:'); notes.forEach(n => console.log(n)); }

if (warnings.length) {
  console.log(`\n⚠️  ${warnings.length} warning${warnings.length > 1 ? 's' : ''} (app chalega, par dekh lo):`);
  warnings.slice(0, 40).forEach(w => console.log('   ·', w));
  if (warnings.length > 40) console.log(`   … aur ${warnings.length - 40}`);
}

if (problems.length) {
  console.log(`\n❌ ${problems.length} PROBLEM${problems.length > 1 ? 'S' : ''} — yeh theek karne padenge:`);
  problems.forEach(p => console.log('   ·', p));
  console.log('');
  process.exit(1);
}
console.log('\n✅ Koi problem nahi. Content khelne laayak hai.\n');
