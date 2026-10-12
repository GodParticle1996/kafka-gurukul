# 🔧 Kafka Gurukul Extend Karna

Naya concept seekha (kahin blog/video/incident se) aur app mein daalna hai?
**Sirf data files edit karni hain — engine files (core/games/kafkacli/lab/app.js)
ko chhoona nahi padta.**

Kuch bhi badalne ke baad:

```bash
node validate.js     # content check — galtiyan yahin pakdi jaayengi
node boot-test.js    # UI check — sab screens render hote hain?
```

---

## 1. Naya level add karna

Kisi bhi `data-*.js` file mein (ya nayi file bana ke) bas yeh call karo —
registry **additive** hai, jitni baar chaaho call karo:

```js
KQ.registerLevels('intermediate', [   // 'beginner' | 'intermediate' | 'advanced' | 'sysdesign'
  {
    id: 'mera-topic',            // unique, kebab-case (visuals + storage isi se judte hain)
    icon: '🧩',
    title: 'Mera Naya Topic — Hinglish Title',
    badge: 'Topic Tycoon',
    objectives: [ 'Yeh seekhoge', 'Woh seekhoge' ],          // intro screen par
    lesson: [ /* neeche "3-card niyam" dekho */ ],
    stages: [ /* neeche game types dekho */ ],
    interview: { questions: [ /* MCQ ya terminal */ ] },
    success: [ 'Main ab X kar sakta hoon' ],                 // completion checklist
    homework: `<p>Asli Kafka par karne wala kaam…</p>`,      // optional (null bhi chalega)
    cards: [                                                  // Revision Dojo flashcards
      { front: 'Sawal?', back: 'Jawab — <code>code</code> aur <b>bold</b> chalta hai.' },
    ],
  },
]);
```

Level track ke aakhir mein jud jaata hai. **Bas file save karo, browser refresh — done.**
(Naya level tabhi khulega jab uske pehle wala complete ho.)

---

## 2. ⭐ Lesson ka 3-card niyam (is app ka sabse bada rule)

Yeh app isliye banaya gaya tha ki concepts **yaad rahen**, aur woh tabhi hota hai jab
tum problem ki **intuition** pakdo — definition nahi. Isliye har `lesson` array
**teen cards** ka hota hai:

```js
lesson: [
  // Card 1 — 😰 DARD: ek asli problem, ASLI numbers ke saath
  `<h3>😰 Dard: "checkout sale ke din mar gaya"</h3>
   <p>3,000 orders/sec aaye. 400 threads. Har thread 2.8s block…</p>
   <div class="war-story"><b>⚡ Aur asli qatal yeh tha:</b> email service
     (sabse kam zaroori cheez) ne poora checkout rok diya.</div>`,

  // Card 2 — 🪜 SEEDHI: naive → better → real, aur har rung KYUN toota
  `<h3>🪜 Solutions ki seedhi</h3>
   <p><b>❌ Naive:</b> "timeout ghata do" → paisa kata par order nahi bana</p>
   <p><b>⚠️ Better:</b> "threads badha do" → DB connections khatam</p>
   <p><b>✅ Real:</b> beech mein ek durable log rakho</p>`,

  // Card 3 — ⚖️ NAYI MUSIBAT: har solution nayi problem laata hai
  `<h3>⚖️ Nayi musibat</h3>
   <p>Ab duplicates aayenge, ordering gayi, aur "ho gaya" ab jhooth hai…</p>`,
]
```

Baaki niyam:
- Har `explain:` **"kyun"** bataye, sirf "kya" nahi
- Examples: ~60% generic tech (e-comm/payments/ride-hailing), ~30% famous outages, ~10% factory/MQTT
- `<div class="war-story">` = asli incident box
- `<pre>` blocks Hinglish comments ke saath — ASCII diagram yahin banao

---

## 3. Level ka diagram

`visuals.js` mein entry add karo — key = level `id`:

```js
VISUALS['mera-topic'] = {
  t: 'Chhota diagram title',
  html: `<div class="dg-flow">
    <div class="dg-node is-accent" data-hs="producer">
      <span class="ni">📤</span><span class="nl">Producer</span><span class="ns">subtitle</span>
    </div>
    <div class="dg-arrow flow"></div>
    <div class="dg-node is-good"><span class="ni">✓</span><span class="nl">Result</span></div>
  </div>
  <div class="dg-cap">Caption — <b>bold</b> allowed.</div>`,
};
```

Building blocks (`style.css` mein sab ready hai):

| Class | Kya |
|---|---|
| `dg-flow` `dg-col` `dg-split` `dg-stack` `dg-grid` | layout |
| `dg-node` + `is-good/is-bad/is-accent/is-gold/is-muted/pulse` | box (andar `ni`=icon, `nl`=label, `ns`=subtitle) |
| `dg-pill` + `good/bad/acc/gold` | chhota tag |
| `dg-arrow` + `flow/good/bad/gold/two` · `dg-down` | teer |
| `dg-box` + `bx-label` | dashed group box |
| `dg-ladder` / `dg-rung` | seedhi (naive→better→real ke liye best) |
| `dg-legend` · `dg-cap` | legend aur caption |

Hotspot game ke liye clickable nodes par `data-hs="target"` lagao.
Saare diagrams ek page par: **`gallery.html`**

---

## 4. Game types (stages)

```js
// 🗂️ BINS — categories mein sort karo
{ type: 'bins', title: '...', instructions: '...',
  bins: [ { id: 'a', label: '🅰️ Category A' }, { id: 'b', label: '🅱️ B' } ],
  items: [ { text: 'cheez', bin: 'a', explain: 'kyun A mein — Hinglish' } ] }

// 🔢 ORDER — sahi sequence banao (SAHI order mein likho)
{ type: 'order', title: '...', instructions: '...',
  steps: [ { text: 'pehla step', explain: 'kyun pehla' }, ... ] }

// 🔗 MATCH — jodi milao
{ type: 'match', title: '...', instructions: '...',
  pairs: [ { left: 'Term', right: 'Definition', explain: '...' } ] }

// 🧠 QUIZ — MCQ (context optional — terminal output dikhata hai)
{ type: 'quiz', title: '...',
  questions: [ { q: 'Sawal?', choices: ['A','B','C','D'], answer: 0,
                 explain: '...', context: 'ERROR [Consumer] Offset out of range' } ] }

// 📄 FILL — command/config/code ke blanks (drag tokens)
{ type: 'fill', title: '...', instructions: '...',
  template: 'kafka-topics --{0} --topic x --{1} 6',   // {0},{1}… = blanks
  blanks: [ { answer: 'create', explain: '...' }, { answer: 'partitions', explain: '...' } ],
  tokens: [ 'create', 'partitions', 'galat-option', 'aur-ek' ] }   // answers + 2-4 decoys

// 🎯 HOTSPOT — diagram ke sahi hisse par click
{ type: 'hotspot', title: '...', visual: 'partitions',
  questions: [ { q: 'Kahan click?', target: 'idle', explain: '...' } ] }

// 🚨 INCIDENT — 3AM pager decision tree
{ type: 'incident', title: '...', pager: 'ALERT text (HTML)', start: 'n1',
  nodes: {
    n1: { sitrep: 'consumer-groups --describe ka output…', text: 'Kya karoge?',
          choices: [ { t: 'Sahi move', good: true, next: 'n2', explain: '...' },
                     { t: 'Galat move', good: false, explain: 'kyun galat' } ] },
    end: { end: true, text: 'Resolved!', summary: 'Seekh (HTML)' },
  } }

// 🎁 BONUS — kisi bhi stage mein `bonus: true` daalo → golden bonus round
```

---

## 5. 🖥️ Terminal mission (asli Kafka broker)

⚠️ Yaad rakho: **yeh scripted output nahi hai.** Andar sach mein ek partitioned log
chalta hai, murmur2 se partition chunti hai, aur `LAG = logEndOffset − committed`
se calculate hota hai. Galat key doge toh **sach mein** hot partition banegi.

```js
{ type: 'terminal', title: '🖥️ Mission: ...', instructions: '...',
  goal: 'Kya karna hai (HTML)',
  cluster: {                      // shuruaati cluster — jo chahiye wahi do
    brokers: 3,                   // ya [{ id:1 }, { id:2, alive:false }]
    topics: {
      orders: {
        partitions: 6, rf: 3,
        config: { 'retention.ms': 60000, 'cleanup.policy': 'compact',
                  'min.insync.replicas': 2 },
        messages: [ { key: 'u1', value: 'order-created' } ],   // seed (auto-partitioned)
      },
    },
    groups: {
      billing: { members: 2, committed: { orders: [100, 95, 0, 0, 0, 0] } },
    },
  },
  tasks: [   // checklist — har command ke baad check hota hai
    { text: 'Lag dekho', check: s => s.history.some(h => /--describe/.test(h)) },
  ],
  responses: [   // KISI BHI command ko script karo (builtins se PEHLE match hota hai)
    { re: /^seed$/, out: s => { /* s.produce(...) */ return 'seeded'; }, mutate: s => {} },
  ],
  success: s => s.lag('billing', 'orders') === 0,   // mission complete kab
  successText: 'Jeetne par seekh (HTML)',
  hints: [ 'Hint 1', 'Hint 2' ] }   // har hint −5 XP
```

**`s` (sim) mein kya milta hai** — poora broker + mission state:

```js
s.history          // [raw commands…]      s.lastError / s.lastOut
s.brokers          // [{ id, alive, host }]
s.topics / s.groups
s.hasTopic(n) · s.topic(n) · s.group(n)
s.lag(group, topic) · s.lagPerPartition(group, topic) · s.totalLag(group)
s.leo(topic, p) · s.partitionCounts(topic) · s.allRecords(topic)
s.produce(topic, key, value, { acks }) · s.consume(group, topic, n)
s.addMember(g) · s.removeMember(g) · s.rebalance(g)
s.killBroker(id) · s.startBroker(id) · s.advanceTime(ms) · s.compact(topic)
s.run(cmdline)     // koi bhi CLI command programmatically
```

**Builtin commands jo free milte hain:**
`kafka-topics` (create/list/describe/alter/delete) · `kafka-console-producer`
(`--message`, `--count`, `--key`, `--acks`) · `kafka-console-consumer`
(`--from-beginning`, `--group`, `--max-messages`) · `kafka-consumer-groups`
(`--describe` = asli LAG table, `--reset-offsets`, `--delete`) · `kafka-configs` ·
`kafka-run-class` (GetOffsetShell / DumpLogSegments) · `dump` · `status` ·
`sim consumer add/remove` · `sim broker kill/start` · `sim process` · `sim time` ·
`sim lag` · `sim assignment` · `sim compact` · `help` · `clear` · `hint`

Kuch aur chahiye? → `responses` mein regex se script kar do.

---

## 6. 🎛️ Pipeline Lab (visual simulator)

Jab concept **dekhne** se samajh aata ho (jitter, backpressure, circuit breaker,
retry storm) — tab lab banao, terminal nahi.

```js
{ type: 'lab', title: '🎛️ Lab: ...', instructions: '...',
  scenario: 'HTML — problem kya hai',
  goal: 'HTML — kya karke dikhana hai',
  duration: 120,                       // simulated seconds
  preset: {                            // shuruaati settings
    producerRate: 1200, consumers: 8, processMs: 10, failRate: 0.02,
    retry: 'none', baseDelay: 1, maxRetries: 10, dlq: false,
    prefetch: 500, memLimitMsgs: 20000,
    breaker: false, breakerThreshold: 0.5, breakerCooldown: 10,
    downstreamCapacity: 900,
    outage: [10, 60],                  // downstream POORA down (sec)
    spike:  [30, 45, 4],               // traffic 4× (sec, sec, multiplier)
  },
  knobs: ['retry', 'dlq', 'consumers'],           // sirf yeh sliders dikhenge
  charts: ['lag', 'retryRate', 'throughput'],     // + 'downstreamLoad'
  success: r => r.dropped === 0 && r.peakRetryRate < 2000,
  verdict: r => `HTML — har run ke baad kya hua, kyun hua`,   // optional
  successText: 'Jeetne par seekh (HTML)',
  hints: [ '...' ] }
```

**`r` (result) mein kya milta hai:**

```js
produced · completed · dlq · dropped
peakLag · finalLag · avgLag · caughtUp
peakRetryRate · steadyRetryRate · totalRetries
peakInflight · oomEvents · breakerOpens
maxDownstreamLoad · downstreamCapacity
cfg · duration
series: { t[], lag[], retryRate[], throughput[], downstreamLoad[], breaker[], dlq[] }
```

Available knobs: `producerRate` `consumers` `processMs` `failRate` `maxRetries`
`baseDelay` `prefetch` `breakerThreshold` `breakerCooldown` `downstreamCapacity`
`retry` (none/fixed/exponential/expJitterFull/expJitterEqual/decorrelated)
`dlq` `breaker`

> 💡 **Thundering herd chahiye?** Steady `failRate` se woh **nahi** banta — random
> failures apne aap bikhar jaati hain (aur yeh sahi physics hai). Herd ke liye
> **correlated** failure chahiye: `outage: [10, 60]` lagao. Tab bina jitter ke
> retry spikes saaf dikhte hain (~5×) aur jitter unhe flat kar deta hai (~1.6×).

`validate.js` har lab ko 220 random knob-combinations par chala kar confirm karta
hai ki woh **solve hone laayak hai** — aur yeh bhi ki default preset se hi win na ho jaaye.

---

## 7. Interview round

`interview.questions` mein do type mix kar sakte ho:

```js
// MCQ (context optional):
{ q: '🧑‍💼 "Interviewer ka sawal?"', choices: [...], answer: 0, explain: '...' }

// Hands-on (embedded terminal mission — wahi shape jo section 5 mein hai):
{ terminal: { title: '...', goal: '🧑‍💼 Interviewer: "..."',
              cluster: {...}, tasks: [...], success: s => ..., hints: [...] } }
```

Achha interview MCQ likhne ka tareeqa: galat options **aam galat jawab** hon
(jo junior sach mein deta hai), aur sahi option **trade-off** bataye — sirf fact nahi.

---

## 8. Naya track add karna

1. `data-core.js` → `KQ.tracks` mein entry (`id`/`icon`/`name`/`blurb`/`certTitle`/`certSub`/`motto`/`formula`)
2. Nayi file `data-<track>.js` banao jo `KQ.registerLevels('<track-id>', [...])` kare
3. `index.html` mein `<script src="data-<track>.js"></script>` (baaki data files ke saath, `app.js` se pehle)

Tracks order mein unlock hote hain (`KQ.tracks` ka order = unlock order).
Khaali track home par "🚧 levels aa rahe hain" dikhata hai — app tootta nahi.

---

## 9. Sirf flashcards add karna (bina naye level ke)

Kisi level ke `cards` array mein naya card daalo — **dhyan:** cards level complete
karne par ek baar enroll hote hain (id = `levelId-c<index>`), toh naya card purane
players ko tab milega jab woh level REPLAY complete karenge, ya console se:

```js
KQ.addCard('custom-1', 'Front sawal?', 'Back jawab', '🏷️ Tag');
```

---

## 10. Ranks / XP tuning

- Ranks: `data-core.js` → `RANKS` (xp thresholds + naam)
- XP knobs: `core.js` top — `XP_PER_ITEM`, `PERFECT_BONUS`, `TERMINAL_XP`,
  `HINT_COST`, `CARD_XP`, `POP_XP`, `SRS_INTERVALS`

---

## 11. Chart ke rang (agar naya chart banao)

`lab.js` ke chart rang **validate kiye gaye hain** dark surface `#0d0920` par —
OKLCH lightness band, chroma floor, colour-blind (CVD) separation aur contrast, sab.
Inhe bina soche mat badalna:

```js
lag: '#2298c7' (cyan) · retryRate: '#cb7229' (amber) · throughput: '#d15d9a' (pink)
```

Laal (`#f87171`) aur hara (`#34d399`) **reserved status colours** hain — woh kabhi
series ka rang nahi bante, hamesha icon + label ke saath aate hain (DLQ 💀, healthy ✅).
Naya series rang chahiye toh pehle CVD separation check karo — amber↔laal jodi
colour-blind readers ke liye alag nahi dikhti.

---

## ✅ Checklist naye content ke liye

- [ ] Level `id` unique hai aur `VISUALS[id]` add kiya
- [ ] Lesson **3-card niyam** follow karta hai (dard → seedhi → nayi musibat)
- [ ] Har `explain` Hinglish mein hai aur **"kyun"** batata hai
- [ ] Fill stages: har `blanks[].answer` `tokens` mein maujood hai (+ 2-4 decoys)
- [ ] Quiz: `answer` index sahi hai (validator check karega)
- [ ] Incident: har non-end node mein exactly ek `good: true` + valid `next`
- [ ] Terminal: `success()` fresh cluster par crash nahi karta **aur** turant true nahi hai
- [ ] Lab: `success()` kisi knob combination se solve ho sakta hai
- [ ] 3-5 `cards` — front = sawal, back = crisp jawab
- [ ] Har level mein `interview` round hai
- [ ] `node validate.js` → 0 problems
- [ ] `node boot-test.js` → sab pass
- [ ] Browser mein khel ke dekha 😄
