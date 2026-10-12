#!/usr/bin/env node
/* lab.js simulator — kya sabak SACH MEIN emerge hote hain? */
global.KQ = { renderers: {}, el: () => ({}), HINT_COST: 5, TERMINAL_XP: 30, addXP() {} };
const { runPipeline } = require('./lab.js');

let pass = 0, fail = 0;
function ok(name, cond, extra) {
  if (cond) { pass++; console.log('  ✅', name); }
  else { fail++; console.log('  ❌', name, extra != null ? '\n     → ' + extra : ''); }
}
const S = s => console.log('\n=== ' + s + ' ===');
const spike = r => r.steadyRetryRate ? r.peakRetryRate / r.steadyRetryRate : r.peakRetryRate;

/* ---------- 1. Baseline: healthy pipeline ---------- */
S('1. Healthy pipeline — kuch bura nahi hona chahiye');
const h = runPipeline({ producerRate: 400, consumers: 4, processMs: 5, failRate: 0,
  downstreamCapacity: 2000 }, 60);
ok('sab process ho gaya', h.completed > h.produced * 0.95, `${h.completed}/${h.produced}`);
ok('lag kabhi nahi badha', h.peakLag < 200, String(h.peakLag));
ok('kuch gum nahi hua', h.dropped === 0 && h.dlq === 0);

/* ---------- 2. Slow consumer → lag builds ---------- */
S('2. Consumer dheema → lag badhna CHAHIYE');
const slow = runPipeline({ producerRate: 1000, consumers: 1, processMs: 50,
  downstreamCapacity: 5000 }, 60);
ok('lag badh gaya (consumer 20/s, producer 1000/s)', slow.peakLag > 30000, String(slow.peakLag));
ok('catch up nahi kar paya', !slow.caughtUp);
// 12 consumers × (1000/50ms) = sirf 240 msg/s — 1000 ke liye kaafi nahi.
// processMs ghatao: 12 × (1000/10ms) = 1200 msg/s > 1000 ✔
const fixed = runPipeline({ producerRate: 1000, consumers: 12, processMs: 10,
  downstreamCapacity: 5000 }, 60);
ok('consumers badhane se lag control mein aaya', fixed.peakLag < slow.peakLag / 5,
  `${fixed.peakLag} vs ${slow.peakLag}`);

/* ---------- 3. 🌟 JITTER — app ka sabse zaroori sabak ---------- */
S('3. THUNDERING HERD — jitter ka asli sabooth');
// Thundering herd ke liye failure ka CORRELATED hona zaroori hai — sab ek saath
// giren. Bikhri hui random failures apne aap smear ho jaati hain (aur yeh sahi
// physics hai). Isliye asli scenario: downstream 10s–60s tak POORA down.
const base = { producerRate: 1200, consumers: 16, processMs: 10, failRate: 0.02,
  maxRetries: 10, dlq: true, downstreamCapacity: 900, baseDelay: 1, outage: [10, 60] };
const noJit  = runPipeline(Object.assign({}, base, { retry: 'exponential' }), 150);
const fullJ  = runPipeline(Object.assign({}, base, { retry: 'expJitterFull' }), 150);
const equalJ = runPipeline(Object.assign({}, base, { retry: 'expJitterEqual' }), 150);
const decorr = runPipeline(Object.assign({}, base, { retry: 'decorrelated' }), 150);
console.log(`     exponential (no jitter): peak ${noJit.peakRetryRate}/s  steady ${noJit.steadyRetryRate}/s  → ${spike(noJit).toFixed(1)}× spike`);
console.log(`     + FULL jitter:           peak ${fullJ.peakRetryRate}/s  steady ${fullJ.steadyRetryRate}/s  → ${spike(fullJ).toFixed(1)}× spike`);
console.log(`     + EQUAL jitter:          peak ${equalJ.peakRetryRate}/s  steady ${equalJ.steadyRetryRate}/s  → ${spike(equalJ).toFixed(1)}× spike`);
console.log(`     decorrelated:            peak ${decorr.peakRetryRate}/s  steady ${decorr.steadyRetryRate}/s  → ${spike(decorr).toFixed(1)}× spike`);
ok('bina jitter ke retry spike TEZ hai', spike(noJit) >= 3, `${spike(noJit).toFixed(1)}×`);
ok('bina jitter ke peak retry rate downstream capacity se KAAFI upar', 
  noJit.peakRetryRate > base.downstreamCapacity * 2,
  `${noJit.peakRetryRate}/s vs capacity ${base.downstreamCapacity}/s`);
ok('jitter peak ko kam se kam 2x neeche laata hai',
  fullJ.peakRetryRate < noJit.peakRetryRate / 2,
  `${fullJ.peakRetryRate} vs ${noJit.peakRetryRate}`);
ok('full jitter spike ko kaafi flat karta hai', spike(fullJ) < spike(noJit),
  `${spike(fullJ).toFixed(1)}× vs ${spike(noJit).toFixed(1)}×`);
ok('full jitter ka PEAK retry rate kam hai', fullJ.peakRetryRate < noJit.peakRetryRate,
  `${fullJ.peakRetryRate} vs ${noJit.peakRetryRate}`);
// ⚠️ Jitter ka fayda THROUGHPUT nahi hai — throughput toh consumer capacity se
// bandha hua hai. Jitter ka fayda PEAK LOAD hai: downstream par ek saath itna
// bojh na pade ki woh phir se gir jaaye. Isliye sahi assertion yeh hai:
ok('jitter throughput ko nuksan nahi karta (~barabar)',
  Math.abs(fullJ.completed - noJit.completed) < noJit.completed * 0.05,
  `${fullJ.completed} vs ${noJit.completed}`);

/* ---------- 4. Retry storm → congestion collapse ---------- */
S('4. Retry storm downstream ko aur girata hai');
const noRetry = runPipeline({ producerRate: 800, consumers: 10, processMs: 8, failRate: 0.3,
  retry: 'none', dlq: true, downstreamCapacity: 700 }, 60);
const madRetry = runPipeline({ producerRate: 800, consumers: 10, processMs: 8, failRate: 0.3,
  retry: 'fixed', baseDelay: 0.5, maxRetries: 15, dlq: true, downstreamCapacity: 700 }, 60);
ok('aggressive retry se downstream par load BADHTA hai',
  madRetry.maxDownstreamLoad >= noRetry.maxDownstreamLoad,
  `${madRetry.maxDownstreamLoad} vs ${noRetry.maxDownstreamLoad}`);
ok('load capacity se upar gaya (collapse zone)', madRetry.maxDownstreamLoad > madRetry.downstreamCapacity,
  `${madRetry.maxDownstreamLoad} > ${madRetry.downstreamCapacity}`);

/* ---------- 5. DLQ vs data loss ---------- */
S('5. DLQ on/off — poison messages kahan jaate hain');
const cfgD = { producerRate: 300, consumers: 6, processMs: 10, failRate: 1.0,
  retry: 'exponential', maxRetries: 2, baseDelay: 0.5, downstreamCapacity: 2000 };
const noDlq = runPipeline(Object.assign({}, cfgD, { dlq: false }), 60);
const yesDlq = runPipeline(Object.assign({}, cfgD, { dlq: true }), 60);
ok('DLQ off → messages HAMESHA ke liye gum', noDlq.dropped > 0 && noDlq.dlq === 0,
  `dropped=${noDlq.dropped} dlq=${noDlq.dlq}`);
ok('DLQ on → kuch bhi gum nahi, sab DLQ mein', yesDlq.dlq > 0 && yesDlq.dropped === 0,
  `dropped=${yesDlq.dropped} dlq=${yesDlq.dlq}`);
ok('maxRetries sach mein lagta hai (infinite loop nahi)', yesDlq.dlq > 0);

/* ---------- 6. Circuit breaker ---------- */
S('6. Circuit breaker — outage ke doraan downstream ko saans');
const cfgB = { producerRate: 500, consumers: 10, processMs: 8, failRate: 0.05,
  retry: 'expJitterFull', maxRetries: 10, dlq: true, baseDelay: 1,
  downstreamCapacity: 800, outage: [15, 45] };
const noBreak = runPipeline(Object.assign({}, cfgB, { breaker: false }), 90);
const withBreak = runPipeline(Object.assign({}, cfgB, { breaker: true, breakerThreshold: 0.5, breakerCooldown: 8 }), 90);
console.log(`     bina breaker: outage ke doraan max load ${noBreak.maxDownstreamLoad}/s`);
console.log(`     breaker ke saath: ${withBreak.breakerOpens} baar khula, max load ${withBreak.maxDownstreamLoad}/s`);
ok('breaker khula (failures dekh ke)', withBreak.breakerOpens > 0, String(withBreak.breakerOpens));
ok('breaker ne downstream par total load ghataya',
  withBreak.maxDownstreamLoad <= noBreak.maxDownstreamLoad,
  `${withBreak.maxDownstreamLoad} vs ${noBreak.maxDownstreamLoad}`);

/* ---------- 7. Back-pressure / prefetch OOM ---------- */
S('7. Prefetch bahut zyada → consumer OOM');
const bigPrefetch = runPipeline({ producerRate: 3000, consumers: 4, processMs: 40,
  prefetch: 19000, memLimitMsgs: 20000, downstreamCapacity: 5000 }, 60);
const safePrefetch = runPipeline({ producerRate: 3000, consumers: 4, processMs: 40,
  prefetch: 100, memLimitMsgs: 20000, downstreamCapacity: 5000 }, 60);
ok('bada prefetch → OOM hua', bigPrefetch.oomEvents > 0, String(bigPrefetch.oomEvents));
ok('chhota prefetch → koi OOM nahi', safePrefetch.oomEvents === 0, String(safePrefetch.oomEvents));

/* ---------- 8. Determinism / sanity ---------- */
S('8. Sanity — hisaab barabar hai?');
const s8 = runPipeline({ producerRate: 200, consumers: 4, processMs: 10, failRate: 0.2,
  retry: 'exponential', maxRetries: 3, dlq: true, downstreamCapacity: 2000 }, 60);
const accounted = s8.completed + s8.dlq + s8.dropped + s8.finalLag;
ok('completed + dlq + dropped + finalLag ≈ produced',
  Math.abs(accounted - s8.produced) < s8.produced * 0.12,
  `${accounted} vs ${s8.produced}`);
ok('series ki lambai duration se match karti hai', s8.series.t.length === 600, String(s8.series.t.length));
ok('koi NaN nahi', s8.series.lag.every(Number.isFinite) && s8.series.retryRate.every(Number.isFinite));
const zero = runPipeline({ producerRate: 0, consumers: 1, processMs: 10 }, 30);
ok('zero traffic par crash nahi', zero.produced === 0 && zero.peakLag === 0);

console.log(`\n${'='.repeat(50)}\n  ${pass} pass · ${fail} fail\n${'='.repeat(50)}`);
process.exit(fail ? 1 : 0);
