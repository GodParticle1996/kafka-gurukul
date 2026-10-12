/* =====================================================================
   Kafka Gurukul — Core: state, XP, streak, spaced repetition, registry
   Loaded FIRST. Everything lives under the global `KQ` namespace so
   content files (data-*.js) can self-register levels & cards.
   (Ported from sqlgurukul/core.js — Kafka & Message Queues edition)
   ===================================================================== */

/* kuch purane browsers/webviews mein scrollIntoView nahi hota — app na toote */
if (typeof Element !== 'undefined' && !Element.prototype.scrollIntoView)
  Element.prototype.scrollIntoView = function () {};

const KQ = {
  STORE_KEY: 'kafkagurukul-v1',
  XP_PER_ITEM: 10,
  PERFECT_BONUS: 20,
  TERMINAL_XP: 30,       // base XP for completing a broker mission
  HINT_COST: 5,          // XP shaved off a terminal mission per hint
  CARD_XP: 5,            // XP per due flashcard reviewed
  POP_XP: 5,             // bonus XP per correct pop-quiz answer
  SRS_INTERVALS: [1, 3, 7, 16, 35],   // days — SM-2-lite boxes

  levelSets: {},         // trackId -> [level, ...]  (filled by data-*.js)
  tracks: [],            // filled by data-core.js
  state: null,
  session: null,
};

/* ---------------- level registry (extensibility hook) ----------------
   A data file calls:  KQ.registerLevels('beginner', [ {...}, {...} ])
   Multiple calls append — so you can add ONE new level from ANY file. */
KQ.registerLevels = function (trackId, levels) {
  if (!KQ.levelSets[trackId]) KQ.levelSets[trackId] = [];
  KQ.levelSets[trackId].push(...levels);
};

/* ---------------- persistence ---------------- */
KQ.defaultState = function () {
  return {
    xp: 0,
    completed: [],        // level ids
    stars: {},            // levelId -> 1..3
    dojo: {},             // cardId -> { box, due (ms), front, back, tag }
    streak: { last: null, count: 0, best: 0 },
    levelStarts: 0,       // for pop-quiz cadence
  };
};

KQ.loadState = function () {
  try {
    const s = JSON.parse(localStorage.getItem(KQ.STORE_KEY));
    if (s && typeof s.xp === 'number' && Array.isArray(s.completed)) {
      // forward-compat: fill any missing keys added in later versions
      return Object.assign(KQ.defaultState(), s);
    }
  } catch (e) { /* corrupted store — start fresh */ }
  return KQ.defaultState();
};
KQ.saveState = function () { localStorage.setItem(KQ.STORE_KEY, JSON.stringify(KQ.state)); };
KQ.state = KQ.loadState();

/* ---------------- DOM helpers ---------------- */
KQ.el = function (tag, cls, html) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (html != null) e.innerHTML = html;
  return e;
};
KQ.shuffle = function (arr) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
};
KQ.shake = function (node) {
  node.classList.remove('shake');
  void node.offsetWidth;
  node.classList.add('shake');
};
KQ.escapeHtml = function (s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
};

/* ---------------- XP & ranks ---------------- */
KQ.currentRank = function () {
  let r = RANKS[0];
  for (const rank of RANKS) if (KQ.state.xp >= rank.xp) r = rank;
  return r;
};
KQ.nextRank = function () { return RANKS.find(r => r.xp > KQ.state.xp) || null; };
KQ.addXP = function (n) {
  if (KQ.session && KQ.session.replay) return;
  if (n <= 0) return;
  KQ.state.xp += n;
  KQ.touchStreak();
  KQ.saveState();
  if (typeof renderTopbar === 'function') renderTopbar();
  KQ.floatXP(n);
};
KQ.floatXP = function (n) {
  const bar = document.getElementById('topbar');
  const f = KQ.el('div', 'xp-float', `+${n} XP`);
  bar.appendChild(f);
  setTimeout(() => f.remove(), 1200);
};

/* ---------------- daily streak ---------------- */
KQ.todayStr = function () {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
};
KQ.touchStreak = function () {
  const s = KQ.state.streak;
  const today = KQ.todayStr();
  if (s.last === today) return;
  const y = new Date(); y.setDate(y.getDate() - 1);
  const yesterday = `${y.getFullYear()}-${String(y.getMonth() + 1).padStart(2, '0')}-${String(y.getDate()).padStart(2, '0')}`;
  s.count = (s.last === yesterday) ? s.count + 1 : 1;
  s.best = Math.max(s.best, s.count);
  s.last = today;
  KQ.saveState();
};
KQ.streakAlive = function () {
  const s = KQ.state.streak;
  if (!s.last) return false;
  const today = KQ.todayStr();
  const y = new Date(); y.setDate(y.getDate() - 1);
  const yesterday = `${y.getFullYear()}-${String(y.getMonth() + 1).padStart(2, '0')}-${String(y.getDate()).padStart(2, '0')}`;
  return s.last === today || s.last === yesterday;
};

/* ---------------- Revision Dojo: SM-2-lite spaced repetition ----------
   Card = { box, due, front, back, tag }.  box indexes SRS_INTERVALS.
   - level complete  -> level.cards enter the deck (due tomorrow)
   - wrong answer    -> auto "mistake card" (due tomorrow, box 0)
   - review correct  -> box+1 (capped), due = now + interval[box]
   - review wrong    -> box = 0, due tomorrow                      */
KQ.DAY = 24 * 60 * 60 * 1000;

KQ.addCard = function (id, front, back, tag) {
  if (KQ.state.dojo[id]) return; // already in the deck
  KQ.state.dojo[id] = { box: 0, due: Date.now() + KQ.DAY, front, back, tag: tag || '' };
  KQ.saveState();
};

/* called by game renderers on a wrong answer — turns the mistake into a card */
KQ.noteMistake = function (front, back, tag) {
  if (KQ.session && KQ.session.replay) return;
  if (!front || !back) return;
  // stable id from the front text so the same mistake never duplicates
  let h = 0;
  for (let i = 0; i < front.length; i++) h = ((h << 5) - h + front.charCodeAt(i)) | 0;
  KQ.addCard('mistake-' + Math.abs(h), front, back, tag || '🙈 Galti se seekho');
};

KQ.enrollLevelCards = function (level) {
  (level.cards || []).forEach((c, i) =>
    KQ.addCard(`${level.id}-c${i}`, c.front, c.back, `${level.icon} ${level.title}`));
};

KQ.dueCards = function () {
  const now = Date.now();
  return Object.entries(KQ.state.dojo)
    .filter(([, c]) => c.due <= now)
    .map(([id, c]) => ({ id, ...c }));
};

KQ.reviewCard = function (id, remembered) {
  const c = KQ.state.dojo[id];
  if (!c) return;
  if (remembered) {
    c.box = Math.min(c.box + 1, KQ.SRS_INTERVALS.length - 1);
    c.due = Date.now() + KQ.SRS_INTERVALS[c.box] * KQ.DAY;
  } else {
    c.box = 0;
    c.due = Date.now() + KQ.DAY;
  }
  KQ.saveState();
};

/* ---------------- pop-quiz question harvesting ----------------
   Pulls MCQs from already-completed levels (quiz stages + interview). */
KQ.harvestQuestions = function () {
  const qs = [];
  KQ.tracks.forEach(t => (KQ.levelSets[t.id] || []).forEach(lvl => {
    if (!KQ.state.completed.includes(lvl.id)) return;
    (lvl.stages || []).forEach(st => {
      if (st.type === 'quiz') (st.questions || []).forEach(q => qs.push({ ...q, from: lvl }));
    });
    if (lvl.interview) (lvl.interview.questions || []).forEach(q => {
      if (q.choices) qs.push({ ...q, from: lvl });
    });
  }));
  return qs;
};

/* ---------------- track helpers ---------------- */
KQ.trackOf = function (level) {
  return KQ.tracks.find(t => (KQ.levelSets[t.id] || []).includes(level));
};
KQ.levelsOf = function (track) { return KQ.levelSets[track.id] || []; };
KQ.trackProgress = function (track) {
  const levels = KQ.levelsOf(track);
  const done = levels.filter(l => KQ.state.completed.includes(l.id)).length;
  return { done, total: levels.length, complete: done === levels.length && levels.length > 0 };
};
KQ.trackUnlocked = function (track) {
  const idx = KQ.tracks.indexOf(track);
  if (idx <= 0) return true;
  return KQ.trackProgress(KQ.tracks[idx - 1]).complete;
};
