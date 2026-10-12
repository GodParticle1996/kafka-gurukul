# 🛕 Kafka Gurukul

**Kafka aur Message Queues seekho, khel khel mein — Hinglish mein, hands-on,
"queue chahiye hi kyun?" se lekar "3AM pe lag 40 lakh hai" tak.**

KubeGurukul / AWS Gurukul / SQL Gurukul ka sibling. Pure HTML/CSS/JS —
no build, no dependencies, sab kuch browser mein.

## Run it

```bash
# Option 1: bas file kholo (sab local hai)
explorer.exe index.html        # WSL se

# Option 2: serve karo
cd kafkagurukul
python3 -m http.server 8000
# → http://localhost:8000
```

Diagram gallery: [`gallery.html`](gallery.html) — saare concept visuals ek page par.

## Ise alag kya banata hai

### 🎯 Har concept ek PROBLEM se shuru hota hai

Definitions bhool jaate hain, dard yaad rehta hai. Isliye har level ke lessons
ek fixed shape mein hain:

| Card | Kya |
|---|---|
| 😰 **Dard** | Ek asli failure, asli numbers ke saath — *"3,000 orders/sec, 400 threads, har ek 2.8s block → checkout mar gaya"* |
| 🪜 **Solutions ki seedhi** | ❌ naive → ⚠️ better → ✅ real, aur **har rung kyun toota** |
| ⚖️ **Nayi musibat** | Har solution nayi problem laata hai — yahi agle level ka hook |

### 🖥️ Terminal simulator jo SACH MEIN ek broker hai

`kafkacli.js` scripted output nahi deta — andar ek asli partitioned log chalta hai:

- Partition Kafka ke **asli murmur2 DefaultPartitioner** se chunti hai — ek hi key
  se 200 messages bhejoge toh **sach mein** ek partition hot hogi aur baaki khaali
- **LAG hardcode nahi hai** — `logEndOffset − committedOffset` se nikalta hai
- `sim broker kill 2` → ISR sach mein shrink hota hai, leader election hota hai,
  aur `acks=all` sach mein `NotEnoughReplicasException` deta hai
- `sim consumer add` → asli range-assignor chalta hai, partitions se zyada
  consumers sach mein khaali baithte hain
- `sim time +7d` → retention sach mein purane records kha jaati hai, aur peeche
  chhoot gaye consumer ko sach mein `OffsetOutOfRangeException` milta hai
- Log compaction, tombstones, offset reset, `dump` se log ke andar jhaankna — sab asli
- `sim controller kill 9091` → **KRaft quorum** sach mein toot-ta hai: metadata writes
  `TimeoutException` dene lagti hain par **produce/consume chalta rehta hai** — wahi
  "cluster down hai ya nahi?" wala 3AM confusion, khud karke dekho
- `kafka-reassign-partitions --execute/--verify` → naye brokers par partitions sach mein
  move hote hain, aur output batata hai **kitne records network par copy hue**
- `connect create ... --dlq` → **Kafka Connect** ka asli runtime: sink connector ek
  **consumer group** banta hai (`kafka-consumer-groups --list` mein dikhega), `tasks.max`
  partitions par cap hota hai, aur ek poison record sach mein task ko **FAILED** karke
  lag ko atka deta hai — jab tak DLQ na lagao

Asli CLI ke naam use hote hain (`kafka-topics`, `kafka-consumer-groups --describe`),
toh jo yahan seekha woh seedha production terminal par chalega.

### 🎛️ Pipeline Lab — jo cheezein padh ke samajh nahi aatin

`lab.js` ek discrete-event simulator hai jisme tum knobs ghumate ho aur natija
live charts mein banate dekhte ho: producer rate, consumers, retry policy,
jitter, DLQ, prefetch, circuit breaker, downstream capacity, outage windows.

**"Jitter kyun chahiye" ka jawab tab tak adhoora hai jab tak retry spikes ko
ek doosre par chadhte na dekh lo:**

| Retry policy | Peak retry rate | Spike ratio |
|---|---|---|
| exponential (bina jitter) | 5,212/s | **5.3×** |
| exponential + full jitter | 1,781/s | **1.6×** |

Yeh numbers hardcode nahi hain — simulator se nikalte hain. Aur dhyan do ki
jitter **throughput** nahi badhata (woh consumer capacity se bandha hai) —
woh **peak load** girata hai, taaki downstream dobara na gire.

## Kya hai isme

- **4 tracks, 37 levels** (sequential unlock, har track ka certificate):
  - 🌱 **Beginner — Sandesh Shishya (9):** sync ki maut → broker anatomy →
    topics/partitions/offsets → producer (acks) → consumer groups → keys & ordering →
    delivery semantics → schema/serialization → Boss: pehla pipeline
  - 🔥 **Intermediate — Vishwas Sadhak (10):** consumer lag, rebalance toofan,
    retries + idempotency, **exponential backoff + jitter**, **DLQ**,
    **back-pressure**, **circuit breaker**, ISR/replication, retention + compaction,
    Boss: Black Friday outage night
  - 🚀 **Advanced — Stream Guru (10):** exactly-once + transactions, transactional
    outbox + CDC, event-time windows + late data, Kafka Streams (KStream/KTable),
    Schema Registry, security + quotas, multi-DC DR, Boss: platform owner,
    **KRaft + cluster ops** (controller quorum, partition reassignment),
    **Kafka Connect** (source/sink, SMTs, poison pill + DLQ)
  - 🏛️ **Architect — System Design (8):** Log vs Queue (Kafka/RabbitMQ/SQS/MQTT/
    Redis Streams), RabbitMQ model, SQS visibility timeout, MQTT QoS + IoT,
    fanout & celebrity problem, saga & compensations, capacity/cost math,
    **Final Boss: Design Gauntlet**

- **Har level mein:** 🎯 objectives · 📖 3 problem-first lesson cards + ⚡ war story ·
  📊 animated diagram · 🎮 3-5 challenges · 🎤 **Interview Round** (MCQ + hands-on) ·
  ✔ success checklist · 🏋️ asli Kafka homework (Docker + kafkajs) · 🧘 flashcards

- **Game types:** 🗂️ bins · 🔢 sequence · 🔗 match · 🧠 scenario quiz ·
  📄 fill-in-the-blanks · 🎯 diagram hotspot · 🚨 3AM pager incident tree ·
  🖥️ **broker terminal** · 🎛️ **Pipeline Lab**

- **Bhoolne ka ilaaj:** 🧘 Revision Dojo (spaced repetition 1→3→7→16→35 din) ·
  🎲 surprise pop quiz · 🔥 daily streak · 🙈 galtiyan apne aap flashcard ban jaati hain

- **Progression:** XP, 11 ranks (🐣 Sandesh Shishya → 👑 Kafka Mahaguru),
  per-level badges + stars, har track ka certificate

- **Progress `localStorage` mein** — tab band karo, baad mein wahin se continue.

## Verify karna (browser ke bina)

```bash
node validate.js     # content: schemas, quiz answers, fill tokens, incident graphs,
                     # har terminal mission asli engine par, har lab solvable hai ya nahi
node boot-test.js    # UI: sab screens stubbed DOM par render hote hain
node test-broker.js  # kafkacli.js broker engine (42 tests)
node test-lab.js     # lab.js simulator (25 tests)
```

Abhi ki halat — sab green:

```
35 levels · 121 stages · 19 terminal missions · 7 labs · 67 interview Qs
175 flashcards · 35 diagrams   →   0 problems, 0 warnings
```

`validate.js` woh galtiyan pakadta hai jo browser mein **chup-chaap** tootti hain —
quiz ka `answer` index range se bahar, fill ka answer `tokens` mein hai hi nahi
(matlab level khelna namumkin), incident ka dead-end node, terminal mission jo
shuru mein hi complete ho jaaye, aur lab jo kisi bhi setting se solve na ho.

## App extend karna (naya concept aaya?)

Yeh app **ever-extending** design ki hai — naya level/track/game/lab/card add karna
sirf data files ka kaam hai, engine ko chhoona nahi padta.
Poora guide: [`EXTENDING.md`](EXTENDING.md)

## Files

| File | Kya hai |
|------|---------|
| `core.js` | State, XP, ranks, streak, spaced-repetition engine, level registry |
| `kafkacli.js` | **Asli mini-Kafka broker** — partitioned log, murmur2, groups, real lag, ISR, retention, compaction + CLI |
| `lab.js` | **Pipeline Lab** — discrete-event simulator + validated SVG charts |
| `games.js` | Game renderers (bins/order/match/quiz/fill/hotspot/incident) |
| `app.js` | Screens: home, track maps, stages, interview, Dojo, pop quiz, certificates |
| `visuals.js` | Concept diagrams (`VISUALS[levelId]`) |
| `data-core.js` | Ranks + track definitions |
| `data-beginner.js` … `data-sysdesign.js` | Level content |
| `validate.js` / `boot-test.js` | Content aur UI verification |
| `gallery.html` | Saare diagrams ek page par |
| `PLAN.md` | Poora build plan aur design decisions |
