/* =====================================================================
   Kafka Gurukul — 🎛️ PIPELINE LAB (visual simulator)

   Kuch cheezein padh ke samajh nahi aatin — DEKHNI padti hain.
   "Jitter kyun chahiye?" ka jawab tab tak adhoora hai jab tak tum retry
   spikes ko ek doosre ke upar chadhte na dekh lo.

   Yeh ek discrete-event simulator hai. Tum knobs ghumate ho, ▶ Run dabate
   ho, aur lag / retry-rate / throughput ko live banate dekhte ho.
   Koi number hardcode nahi hai — sab model se nikalta hai.

   ---------------------------------------------------------------------
   Stage shape (data files mein):
   {
     type: 'lab', title, instructions,
     scenario: 'HTML — problem kya hai',
     goal: 'HTML — kya karke dikhana hai',
     duration: 120,                    // simulated seconds
     preset: { producerRate: 800, consumers: 4, processMs: 40, failRate: 0,
               retry: 'none', maxRetries: 5, dlq: false, prefetch: 500,
               breaker: false, breakerThreshold: 0.5, breakerCooldown: 10,
               downstreamCapacity: 1000, outage: [20, 60] },   // outage = [startSec, endSec]
     knobs: ['consumers', 'retry', 'dlq'],   // sirf yeh sliders dikhenge (baaki locked)
     charts: ['lag', 'retryRate', 'throughput'],   // default: teeno
     success: r => r.peakRetryRate < 3 * r.steadyRetryRate && r.dropped === 0,
     successText: 'HTML — jeetne par seekh',
     verdict: r => 'HTML — har run ke baad kya hua, kyun hua',   // optional
     hints: [...],
   }

   `r` (result) mein milta hai:
     produced · completed · dlq · dropped · peakLag · finalLag · avgLag
     peakRetryRate · steadyRetryRate · totalRetries · peakInflight
     oomEvents · breakerOpens · maxDownstreamLoad · downstreamCapacity
     series: { t[], lag[], retryRate[], throughput[], downstreamLoad[], breaker[] }
   ===================================================================== */

(function () {

KQ.renderers = KQ.renderers || {};

/* ---------------------------------------------------------------------
   CHART COLORS — dark surface (#0d0920) par validate kiye gaye
   (OKLCH lightness band + chroma floor + colour-blind separation +
    contrast — teeno series all-pairs CVD check paas karte hain)
   Status rang (laal/hara) RESERVED hain — woh kabhi series ka rang nahi
   bantay, hamesha icon + label ke saath aate hain.
   --------------------------------------------------------------------- */
const C = {
  lag:        '#2298c7',   // cyan
  retryRate:  '#cb7229',   // amber
  throughput: '#d15d9a',   // pink
  grid:       '#2f2a55',
  axis:       '#6b6790',
  critical:   '#f87171',   // status only
  good:       '#34d399',   // status only
};

const DEFAULTS = {
  producerRate: 500,        // msg/sec
  consumers: 2,
  processMs: 20,            // har message ko process karne mein
  failRate: 0,              // baseline downstream failure probability
  retry: 'none',            // none | fixed | exponential | expJitterFull | expJitterEqual | decorrelated
  baseDelay: 1,             // seconds
  maxDelay: 60,
  maxRetries: 5,
  dlq: false,
  prefetch: 500,            // har consumer kitne messages buffer karega
  memLimitMsgs: 20000,      // itne buffered messages par consumer OOM
  breaker: false,
  breakerThreshold: 0.5,
  breakerCooldown: 10,      // seconds
  downstreamCapacity: 1000, // req/sec — isse zyada maaroge toh woh aur girega
  outage: null,             // [startSec, endSec] — downstream poori tarah down
  spike: null,              // [startSec, endSec, multiplier] — traffic spike
};

const KNOB_SPECS = {
  producerRate:      { label: '📤 Producer rate', unit: ' msg/s', min: 0,   max: 5000, step: 50 },
  consumers:         { label: '📥 Consumers',     unit: '',       min: 1,   max: 24,   step: 1 },
  processMs:         { label: '⚙️ Process time',  unit: ' ms',    min: 1,   max: 500,  step: 1 },
  failRate:          { label: '💥 Failure rate',  unit: '',       min: 0,   max: 1,    step: 0.05, pct: true },
  maxRetries:        { label: '🔁 Max retries',   unit: '',       min: 0,   max: 15,   step: 1 },
  baseDelay:         { label: '⏱️ Base delay',    unit: ' s',     min: 0.1, max: 10,   step: 0.1 },
  prefetch:          { label: '🪣 Prefetch/consumer', unit: ' msg', min: 1, max: 20000, step: 50 },
  breakerThreshold:  { label: '⚡ Breaker threshold', unit: '',    min: 0.1, max: 0.95, step: 0.05, pct: true },
  breakerCooldown:   { label: '❄️ Breaker cooldown', unit: ' s',   min: 1,   max: 60,   step: 1 },
  downstreamCapacity:{ label: '🏭 Downstream capacity', unit: ' req/s', min: 50, max: 5000, step: 50 },
  retry: { label: '🔁 Retry policy', choices: [
    ['none',           'Koi retry nahi'],
    ['fixed',          'Fixed delay (har baar 1s)'],
    ['exponential',    'Exponential backoff (1,2,4,8…)'],
    ['expJitterFull',  'Exponential + FULL jitter'],
    ['expJitterEqual', 'Exponential + EQUAL jitter'],
    ['decorrelated',   'Decorrelated jitter (AWS waala)'],
  ] },
  dlq:     { label: '💀 Dead Letter Queue', bool: true },
  breaker: { label: '⚡ Circuit Breaker',    bool: true },
};

/* =====================================================================
   THE SIMULATOR — pure logic, koi DOM nahi (node mein testable)
   ===================================================================== */
KQ.runPipeline = function runPipeline(cfg, duration) {
  const c = Object.assign({}, DEFAULTS, cfg || {});
  duration = duration || 120;
  const dt = 0.1;                       // 100ms simulated ticks
  const steps = Math.round(duration / dt);

  /* Backlog ek simple number NAHI hai — har message ke saath uska attempt
     number chalna chahiye, warna "5 retries ke baad DLQ" ka hisaab hi na rahe.
     Par har failure ke liye naya cohort banaoge toh woh 24^n ki raftaar se
     phategi. Isliye attempt-wise MERGE karte hain: backlog = Map(attempt → count).
     Attempts maxRetries tak hi jaate hain, toh yeh map hamesha chhota rehta hai. */
  const backlog = new Map();            // attempt -> count
  const backlogOf = () => { let s = 0; backlog.forEach(v => { s += v; }); return s; };
  const addBacklog = (attempt, count) => {
    if (count > 0) backlog.set(attempt, (backlog.get(attempt) || 0) + count);
  };
  let done = 0, dlq = 0, dropped = 0, produced = 0, totalRetries = 0;
  let buffered = 0, oomEvents = 0, breakerOpens = 0;
  const retryBuckets = new Map();       // readyAtTick -> Map(attempt -> count)
  let breakerState = 'closed', breakerOpenedAt = -1, lastDecorr = c.baseDelay;
  const winOutcomes = [];               // rolling window: [okCount, failCount] per tick
  const OOM_STALL_S = 5;                // OOM ke baad restart + rebalance mein itne second
  let stalledUntil = -1;                // is waqt tak consumer band hai

  const S = { t: [], lag: [], retryRate: [], throughput: [], downstreamLoad: [], breaker: [], dlq: [] };
  let peakLag = 0, peakRetryRate = 0, peakInflight = 0, maxLoad = 0, lagSum = 0;

  function scheduleRetry(count, attempt, tick) {
    if (count <= 0) return;
    totalRetries += count;
    const n = attempt;                                   // 0-indexed attempt just failed
    const expDelay = Math.min(c.maxDelay, c.baseDelay * Math.pow(2, n));

    if (c.retry === 'fixed') { push(tick + Math.round(c.baseDelay / dt), count, attempt + 1); return; }
    if (c.retry === 'exponential') { push(tick + Math.round(expDelay / dt), count, attempt + 1); return; }

    /* Jitter waale policies — count ko kai buckets mein BAANT dete hain.
       Yahi poora raaz hai: bina jitter ke saare failures EK hi tick par
       wapas aate hain (spike), jitter ke saath woh time par phail jaate hain. */
    const SLICES = 24;
    const per = count / SLICES;
    for (let i = 0; i < SLICES; i++) {
      let d;
      if (c.retry === 'expJitterFull') d = Math.random() * expDelay;
      else if (c.retry === 'expJitterEqual') d = expDelay / 2 + Math.random() * (expDelay / 2);
      else if (c.retry === 'decorrelated') d = Math.min(c.maxDelay, c.baseDelay + Math.random() * (lastDecorr * 3 - c.baseDelay));
      else d = expDelay;
      push(tick + Math.max(1, Math.round(d / dt)), per, attempt + 1);
    }
    if (c.retry === 'decorrelated') lastDecorr = Math.min(c.maxDelay, lastDecorr * 3);
  }
  function push(tick, count, attempt) {
    if (count <= 0) return;
    if (!retryBuckets.has(tick)) retryBuckets.set(tick, new Map());
    const m = retryBuckets.get(tick);
    m.set(attempt, (m.get(attempt) || 0) + count);   // merge — warna buckets phat jaate hain
  }

  for (let i = 0; i < steps; i++) {
    const t = i * dt;

    /* 1. produce */
    let rate = c.producerRate;
    if (c.spike && t >= c.spike[0] && t < c.spike[1]) rate *= c.spike[2];
    const newMsgs = rate * dt;
    addBacklog(0, newMsgs);
    produced += newMsgs;

    /* 2. retries jinka waqt aa gaya — apne attempt number ke saath wapas */
    let retriedThisTick = 0;
    const due = retryBuckets.get(i);
    if (due) {
      due.forEach((count, attempt) => { addBacklog(attempt, count); retriedThisTick += count; });
      retryBuckets.delete(i);
    }

    const backlogNowPre = backlogOf();

    /* 3. consumer buffer (prefetch) — memory yahin bharti hai.
          Prefetch bada rakhoge toh consumer messages ko RAM mein bhar lega.
          Buffer memory limit paar kar gaya → consumer OOM se mar gaya. */
    const bufCap = c.prefetch * c.consumers;
    buffered = Math.min(bufCap, backlogNowPre);
    if (buffered >= c.memLimitMsgs && t >= stalledUntil) {
      // 💥 OOM ka asli natija: pod mar gaya → restart + rebalance → kuch der
      // kuch process NAHI hota. (Messages safe hain — woh backlog mein hi hain.)
      oomEvents++;
      stalledUntil = t + OOM_STALL_S;
    }
    const stalled = t < stalledUntil;

    /* 4. circuit breaker ka faisla */
    if (c.breaker) {
      if (breakerState === 'open' && t - breakerOpenedAt >= c.breakerCooldown) {
        breakerState = 'half';
      }
      if (breakerState === 'closed') {
        const ok = winOutcomes.reduce((s, w) => s + w[0], 0);
        const bad = winOutcomes.reduce((s, w) => s + w[1], 0);
        if (ok + bad > 50 && bad / (ok + bad) > c.breakerThreshold) {
          breakerState = 'open'; breakerOpenedAt = t; breakerOpens++;
          winOutcomes.length = 0;      // state badla → counters reset (asli breakers aisa hi karte hain)
        }
      }
    }

    /* 5. kitne attempts ho sakte hain */
    const capacity = c.consumers * (dt / (c.processMs / 1000));
    let attempts;
    if (stalled) attempts = 0;                                              // OOM ke baad restart ho raha hai
    else if (c.breaker && breakerState === 'open') attempts = 0;            // fail fast — downstream ko saans do
    else if (c.breaker && breakerState === 'half') attempts = Math.min(1, backlogNowPre); // ek probe
    else attempts = Math.min(capacity, buffered > 0 ? buffered : backlogNowPre);

    /* 6. downstream par load */
    const load = attempts / dt;
    maxLoad = Math.max(maxLoad, load);

    /* 7. fail hone ka chance — congestion collapse yahin hota hai:
          capacity se zyada maaroge toh downstream aur zyada girega */
    let pFail = c.failRate;
    if (c.outage && t >= c.outage[0] && t < c.outage[1]) pFail = 1;
    else if (load > c.downstreamCapacity) {
      pFail = Math.min(0.98, pFail + (load / c.downstreamCapacity - 1) * 0.8);
    }

    /* 8. Backlog se utne messages nikalo — sabse purane (sabse zyada retry
          ho chuke) pehle, taaki poison messages DLQ tak pahunch sakein */
    let budget = attempts, okTotal = 0, failTotal = 0;
    const attemptsDesc = [...backlog.keys()].sort((a, b) => b - a);
    for (const att of attemptsDesc) {
      if (budget <= 0.000001) break;
      const have = backlog.get(att);
      const take = Math.min(have, budget);
      budget -= take;
      if (have - take <= 0.000001) backlog.delete(att); else backlog.set(att, have - take);

      const failed = take * pFail;
      const ok = take - failed;
      okTotal += ok; failTotal += failed;
      done += ok;

      if (failed > 0.000001) {
        const noRetry = c.retry === 'none' || c.maxRetries === 0;
        if (noRetry || att + 1 > c.maxRetries) {
          // 🪦 saare retries khatam — ab ya toh DLQ, ya hamesha ke liye gum
          if (c.dlq) dlq += failed; else dropped += failed;
        } else {
          scheduleRetry(failed, att, i);
        }
      }
    }
    const backlogNow = backlogOf();
    winOutcomes.push([okTotal, failTotal]);
    if (winOutcomes.length > 200) winOutcomes.shift();

    if (c.breaker && breakerState === 'half' && attempts > 0) {
      // half-open probe: chala toh band karo, nahi chala toh phir se kholo.
      // Dono soorat mein purane counters phenk do — warna outage ke waqt ka
      // failure history breaker ko turant dobara khol dega aur woh kabhi
      // recover hi nahi karega.
      breakerState = okTotal > failTotal ? 'closed' : 'open';
      if (breakerState === 'open') { breakerOpenedAt = t; breakerOpens++; }
      winOutcomes.length = 0;
    }

    /* 9. metrics */
    const retryRate = retriedThisTick / dt;
    peakLag = Math.max(peakLag, backlogNow);
    peakRetryRate = Math.max(peakRetryRate, retryRate);
    peakInflight = Math.max(peakInflight, buffered);
    lagSum += backlogNow;
    S.t.push(t);
    S.lag.push(backlogNow);
    S.retryRate.push(retryRate);
    S.throughput.push(okTotal / dt);
    S.downstreamLoad.push(load);
    S.breaker.push(breakerState);
    S.dlq.push(dlq);
  }

  // steady-state retry rate = second half ka median (spikes ko ignore karke)
  const half = S.retryRate.slice(Math.floor(S.retryRate.length / 2)).slice().sort((a, b) => a - b);
  const steadyRetryRate = half.length ? half[Math.floor(half.length / 2)] : 0;

  return {
    produced: Math.round(produced), completed: Math.round(done),
    dlq: Math.round(dlq), dropped: Math.round(dropped),
    peakLag: Math.round(peakLag), finalLag: Math.round(backlogOf()),
    avgLag: Math.round(lagSum / steps),
    peakRetryRate: Math.round(peakRetryRate), steadyRetryRate: Math.round(steadyRetryRate),
    totalRetries: Math.round(totalRetries), peakInflight: Math.round(peakInflight),
    oomEvents, breakerOpens,
    maxDownstreamLoad: Math.round(maxLoad), downstreamCapacity: c.downstreamCapacity,
    caughtUp: backlogOf() < Math.max(10, c.producerRate * 0.05),
    cfg: c, duration, series: S,
  };
};

/* =====================================================================
   CHART — inline SVG line chart. Ek chart = EK series (small multiples),
   title hi uski pehchaan hai, isliye legend ki zaroorat nahi.
   Dual-axis kabhi nahi — do alag paimaane = do alag chart.
   ===================================================================== */
function lineChart(title, subtitle, xs, ys, color, opts) {
  opts = opts || {};
  const W = 100, H = 100;                 // viewBox units (CSS se scale hoga)
  const maxY = Math.max(opts.minTop || 1, ...ys) * 1.12;
  const maxX = xs[xs.length - 1] || 1;
  const px = t => (t / maxX) * W;
  const py = v => H - (v / maxY) * H;

  // har point plot karne ki zaroorat nahi — thoda downsample kar do
  const stride = Math.max(1, Math.floor(xs.length / 220));
  let d = '';
  for (let i = 0; i < xs.length; i += stride) d += (d ? 'L' : 'M') + px(xs[i]).toFixed(2) + ' ' + py(ys[i]).toFixed(2);
  const area = d + `L${W} ${H}L0 ${H}Z`;

  const gridLines = [0.25, 0.5, 0.75].map(f =>
    `<line x1="0" y1="${(H * f).toFixed(1)}" x2="${W}" y2="${(H * f).toFixed(1)}"
           stroke="${C.grid}" stroke-width="0.3" vector-effect="non-scaling-stroke"/>`).join('');

  // threshold reference line (jaise downstream capacity)
  let ref = '';
  if (opts.threshold != null && opts.threshold < maxY) {
    const y = py(opts.threshold).toFixed(1);
    ref = `<line x1="0" y1="${y}" x2="${W}" y2="${y}" stroke="${C.axis}" stroke-width="0.5"
             stroke-dasharray="2 2" vector-effect="non-scaling-stroke"/>`;
  }

  const fmt = v => v >= 1000000 ? (v / 1000000).toFixed(1) + 'M'
    : v >= 1000 ? (v / 1000).toFixed(v >= 10000 ? 0 : 1) + 'k' : Math.round(v);

  return `<figure class="lab-chart">
    <figcaption>
      <span class="lc-title"><i style="background:${color}"></i>${title}</span>
      <span class="lc-peak">peak ${fmt(Math.max(...ys))}${opts.unit || ''}</span>
    </figcaption>
    <div class="lc-plot">
      <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img" aria-label="${title} over time">
        ${gridLines}${ref}
        <path d="${area}" fill="${color}" opacity="0.13"/>
        <path d="${d}" fill="none" stroke="${color}" stroke-width="2"
              stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/>
      </svg>
      <div class="lc-ymax">${fmt(maxY)}${opts.unit || ''}</div>
      <div class="lc-y0">0</div>
      <div class="lc-cursor" hidden></div>
      <div class="lc-tip" hidden></div>
    </div>
    <div class="lc-sub">${subtitle}</div>
  </figure>`;
}

/* =====================================================================
   RENDERER
   ===================================================================== */
KQ.renderers.lab = function renderLab(stage, arena, onDone) {
  const cfg = Object.assign({}, DEFAULTS, stage.preset || {});
  const shown = stage.knobs || ['consumers', 'retry'];
  const charts = stage.charts || ['lag', 'retryRate', 'throughput'];
  const duration = stage.duration || 120;
  let hintsUsed = 0, solved = false, runs = 0;

  /* ---------- scenario + goal ---------- */
  const goalBox = KQ.el('div', 'term-goal');
  goalBox.innerHTML = `<h4>🎛️ Pipeline Lab</h4>
    ${stage.scenario ? `<div class="lab-scenario">${stage.scenario}</div>` : ''}
    <div><b>🎯 Karna kya hai:</b> ${stage.goal}</div>`;

  const wrap = KQ.el('div', 'lab');

  /* ---------- knobs ---------- */
  const knobBox = KQ.el('div', 'lab-knobs');
  shown.forEach(k => {
    const spec = KNOB_SPECS[k];
    if (!spec) return;
    const row = KQ.el('div', 'knob');
    if (spec.bool) {
      row.innerHTML = `<label class="knob-toggle">
        <input type="checkbox" ${cfg[k] ? 'checked' : ''}>
        <span>${spec.label}</span><b class="knob-val">${cfg[k] ? 'ON' : 'OFF'}</b></label>`;
      const inp = row.querySelector('input');
      inp.onchange = () => { cfg[k] = inp.checked; row.querySelector('.knob-val').textContent = inp.checked ? 'ON' : 'OFF'; };
    } else if (spec.choices) {
      row.innerHTML = `<label>${spec.label}</label>
        <select>${spec.choices.map(([v, l]) => `<option value="${v}"${cfg[k] === v ? ' selected' : ''}>${l}</option>`).join('')}</select>`;
      const sel = row.querySelector('select');
      sel.onchange = () => { cfg[k] = sel.value; };
    } else {
      const show = v => spec.pct ? Math.round(v * 100) + '%' : v + spec.unit;
      row.innerHTML = `<label>${spec.label} <b class="knob-val">${show(cfg[k])}</b></label>
        <input type="range" min="${spec.min}" max="${spec.max}" step="${spec.step}" value="${cfg[k]}">`;
      const inp = row.querySelector('input');
      inp.oninput = () => { cfg[k] = parseFloat(inp.value); row.querySelector('.knob-val').textContent = show(cfg[k]); };
    }
    knobBox.appendChild(row);
  });

  /* ---------- fixed facts strip (jo knobs nahi hain) ---------- */
  const facts = [];
  if (!shown.includes('producerRate')) facts.push(`📤 ${cfg.producerRate} msg/s aa rahe hain`);
  if (!shown.includes('processMs')) facts.push(`⚙️ ${cfg.processMs}ms per message`);
  if (!shown.includes('downstreamCapacity')) facts.push(`🏭 downstream ${cfg.downstreamCapacity} req/s tak jhel sakta hai`);
  if (cfg.outage) facts.push(`💥 ${cfg.outage[0]}s–${cfg.outage[1]}s tak downstream POORA down`);
  if (cfg.spike) facts.push(`📈 ${cfg.spike[0]}s–${cfg.spike[1]}s traffic ${cfg.spike[2]}×`);
  const factStrip = KQ.el('div', 'lab-facts', facts.map(f => `<span>${f}</span>`).join(''));

  /* ---------- controls ---------- */
  const ctrl = KQ.el('div', 'lab-ctrl');
  const runBtn = KQ.el('button', 'btn', '▶ Run karo');
  const hintBtn = KQ.el('button', 'btn small ghost', `💡 Hint (−${KQ.HINT_COST} XP)`);
  ctrl.append(runBtn, hintBtn);

  const output = KQ.el('div', 'lab-out');
  wrap.append(factStrip, knobBox, ctrl, output);
  arena.append(goalBox, wrap);

  hintBtn.onclick = () => {
    const hints = stage.hints || [];
    if (hintsUsed >= hints.length) { toast('💡 Aur hints nahi hain — soch ke dekho!'); return; }
    toast(`💡 Hint ${hintsUsed + 1}: ${hints[hintsUsed]}`);
    hintsUsed++;
  };
  function toast(msg) {
    const t = KQ.el('div', 'lab-hint', msg);
    output.insertBefore(t, output.firstChild);
  }

  runBtn.onclick = () => {
    runs++;
    runBtn.disabled = true;
    runBtn.textContent = '⏳ Chal raha hai…';
    const r = KQ.runPipeline(cfg, duration);
    // chhota sa delay — taaki "chala" wala ehsaas ho
    setTimeout(() => { draw(r); runBtn.disabled = false; runBtn.textContent = '▶ Phir se run karo'; }, 220);
  };

  const CHART_META = {
    lag:            { title: '📉 Consumer Lag', color: C.lag,
                      sub: 'Kitne messages queue mein pade intezaar kar rahe hain. Upar jaa raha hai matlab consumer peeche chhoot raha hai.' },
    retryRate:      { title: '🔁 Retry rate', color: C.retryRate, unit: '/s',
                      sub: 'Har second kitne retries downstream par ja rahe hain. <b>Nukeele spikes = thundering herd</b> — sab clients ek saath retry kar rahe hain.' },
    throughput:     { title: '✅ Throughput', color: C.throughput, unit: '/s',
                      sub: 'Har second kitne messages SAFALTA se process hue. Yahi asli kaam hai.' },
    downstreamLoad: { title: '🏭 Downstream load', color: C.retryRate, unit: '/s',
                      sub: 'Downstream par kitna load ja raha hai. Dashed line uski capacity hai — usse upar gaye toh woh aur tez girega.' },
  };

  function draw(r) {
    output.innerHTML = '';

    /* --- headline stat tiles (status colours yahan, series colours nahi) --- */
    const pctDone = r.produced ? Math.round((r.completed / r.produced) * 100) : 0;
    const tiles = [
      { icon: '📦', label: 'Process hue', value: `${fmtN(r.completed)} / ${fmtN(r.produced)}`, note: pctDone + '%' },
      { icon: '📉', label: 'Peak lag', value: fmtN(r.peakLag), note: r.caughtUp ? 'aakhir mein catch up ✅' : `aakhir mein ${fmtN(r.finalLag)} baaki 😰` },
      { icon: '💀', label: 'DLQ mein gaye', value: fmtN(r.dlq), status: r.dlq > 0 ? 'warn' : 'ok' },
      { icon: '🕳️', label: 'HAMESHA ke liye gum', value: fmtN(r.dropped), status: r.dropped > 0 ? 'bad' : 'ok' },
    ];
    if (r.oomEvents) tiles.push({ icon: '💥', label: 'Consumer OOM', value: r.oomEvents, status: 'bad' });
    if (r.breakerOpens) tiles.push({ icon: '⚡', label: 'Breaker khula', value: r.breakerOpens + ' baar', status: 'warn' });
    output.appendChild(KQ.el('div', 'lab-tiles', tiles.map(t => `
      <div class="lab-tile${t.status ? ' is-' + t.status : ''}">
        <div class="lt-icon">${t.icon}</div>
        <div class="lt-val">${t.value}</div>
        <div class="lt-label">${t.label}</div>
        ${t.note ? `<div class="lt-note">${t.note}</div>` : ''}
      </div>`).join('')));

    /* --- small-multiple charts (ek chart = ek series) --- */
    const grid = KQ.el('div', 'lab-charts');
    grid.innerHTML = charts.map(k => {
      const m = CHART_META[k];
      if (!m) return '';
      return lineChart(m.title, m.sub, r.series.t, r.series[k], m.color, {
        unit: m.unit || '',
        threshold: k === 'downstreamLoad' ? r.downstreamCapacity : null,
      });
    }).join('');
    output.appendChild(grid);

    /* --- retry spike ka scoreboard (jitter ka asli sabooth) --- */
    if (charts.includes('retryRate') && r.totalRetries > 0) {
      const ratio = r.steadyRetryRate ? (r.peakRetryRate / r.steadyRetryRate) : r.peakRetryRate;
      const spiky = ratio >= 3;
      output.appendChild(KQ.el('div', 'lab-verdict ' + (spiky ? 'is-bad' : 'is-good'), `
        <b>${spiky ? '🌊 THUNDERING HERD' : '🍃 Retries acche se phaile hue hain'}</b> —
        peak retry rate <b>${fmtN(r.peakRetryRate)}/s</b> hai aur steady rate
        <b>${fmtN(r.steadyRetryRate)}/s</b> — yaani peak steady se <b>${ratio.toFixed(1)}×</b> hai.
        ${spiky
          ? 'Matlab saare failed messages EK SAATH wapas aa rahe hain aur downstream ko dobara kuchal rahe hain. Jitter lagao — delay ko randomize karo.'
          : 'Jitter kaam kar raha hai: retries time par bikhre hue hain, downstream ko saans mil rahi hai.'}`));
    }

    /* --- stage ka apna verdict --- */
    if (stage.verdict) output.appendChild(KQ.el('div', 'lab-verdict', stage.verdict(r)));

    /* --- success check --- */
    if (!solved && stage.success && stage.success(r)) {
      solved = true;
      knobBox.querySelectorAll('input,select').forEach(x => { x.disabled = true; });
      runBtn.disabled = true;
      const xp = Math.max(10, KQ.TERMINAL_XP - hintsUsed * KQ.HINT_COST);
      KQ.addXP(xp);
      const win = KQ.el('div', 'stage-summary pop');
      win.innerHTML = `
        <div class="sum-emoji">🎉</div>
        <div class="sum-title">Lab solved! +${xp} XP${hintsUsed ? ` (${hintsUsed} hint use hue)` : ' — bina hint ke! 🔥'}</div>
        <div class="sum-detail" style="text-align:left;line-height:1.6;margin-top:8px">${stage.successText || ''}</div>`;
      output.appendChild(win);
      win.scrollIntoView({ behavior: 'smooth', block: 'center' });
      setTimeout(() => onDone({ firstTry: runs === 1 && hintsUsed === 0 ? 1 : 0, total: 1 }), 300);
    } else if (!solved) {
      output.appendChild(KQ.el('div', 'lab-retry', '🔄 Abhi goal poora nahi hua — knobs badlo aur phir se run karo.'));
    }
  }

  function fmtN(v) {
    return v >= 1000000 ? (v / 1000000).toFixed(1) + 'M'
      : v >= 1000 ? (v / 1000).toFixed(v >= 10000 ? 0 : 1) + 'k' : String(Math.round(v));
  }
};

if (typeof module !== 'undefined' && module.exports) module.exports = { runPipeline: KQ.runPipeline };

})();
