# 🛕 Kafka Gurukul — Build Plan

**Kafka aur Message Queues seekho, khel khel mein — Hinglish mein, hands-on,
"kyun queue chahiye?" se lekar "3AM pe lag 40 lakh hai" tak.**

KubeGurukul / AWS Gurukul / SQL Gurukul ka sibling. Pure HTML/CSS/JS — no build,
no dependencies, sab kuch browser mein chalega.

---

## Decisions (user ke saath tay hue)

| Sawal | Faisla |
|-------|--------|
| **Naam / folder** | Kafka Gurukul 🛕 → `kafkagurukul/` |
| **Scope** | Kafka spine + comparative levels (RabbitMQ · SQS · MQTT · Redis Streams) |
| **Simulator** | **Dono** — asli mini-Kafka engine (`kafkacli.js`) + visual **Pipeline Lab** (`lab.js`) |
| **Code language** | Node.js (`kafkajs`, `amqplib`, `mqtt.js`) |
| **Examples ka mix** | ~60% generic tech-industry (e-comm/payments/ride-hailing) · ~30% famous public outages · ~10% AAS/MQTT factory duniya |
| **Language** | Hinglish throughout |
| **Namespace** | `KQ`, store key `kafkagurukul-v1` |

---

## Pedagogy — "Problem pehle, solution baad mein"

Yeh app ka **sabse bada rule** hai (user ki explicit demand). Har level ke lessons
ek fixed 3-card shape follow karenge, taaki intuition yaad rahe, definition nahi:

```
Card 1 — 😰 DARD (The Problem)
  Ek concrete failure, asli numbers ke saath.
  "Flipkart sale. Order service payment service ko sync HTTP call karti hai.
   Payment 800ms leta hai. 12,000 orders/sec aaye. Order service ke saare
   threads payment ka wait kar rahe hain → checkout page hi mar gaya."

Card 2 — 🪜 SOLUTIONS KI SEEDHI (naive → better → real)
  ❌ Naive:  "Timeout ghata do" → order fail hone lage, paisa kat gaya
  ⚠️ Better: "Thread pool badha do" → DB connections khatam
  ✅ Real:   Queue lagao — order likho, ack do, payment async process karo
  Har rung ke saath: yeh kyun tuta.

Card 3 — ⚖️ NAYI MUSIBAT (trade-off)
  Har solution nayi problem laata hai. Queue lagayi → ab ordering gayi,
  duplicate aa sakte hain, consumer peeche reh sakta hai (lag).
  → yahi agle level ka hook hai.
```

Iske upar:
- **⚡ War story box** — asli incident (public postmortem ya production kahani)
- **🧠 "Interview mein aise poochenge"** — har concept ka interview framing
- Har `explain:` string "kyun" batayegi, sirf "kya" nahi

---

## Tech Architecture

| File | Kya hai | Siblings se farak |
|------|---------|-------------------|
| `core.js` | State, XP, ranks, streak, SRS registry | Namespace `KQ`, Kafka ranks |
| `kafkacli.js` | **Asli mini-Kafka broker** — partitioned log, real offsets/lag | 🆕 Naya engine |
| `lab.js` | **Pipeline Lab** — animated tunable sim + live charts | 🆕 Naya game type |
| `games.js` | 7 game renderers (bins/order/match/quiz/fill/hotspot/incident) | Same engine |
| `visuals.js` | ~36 concept diagrams | Naya content, wahi `.dg-*` primitives |
| `app.js` | Screens (home/map/stage/interview/dojo/cert) | Same engine, Kafka branding |
| `style.css` | Broker-black + Kafka-purple `#7b5cff` + stream-cyan `#22d3ee` | Naya palette + `.lab-*` classes |
| `data-core.js` | Ranks + 4 tracks | — |
| `data-beginner/intermediate/advanced/sysdesign.js` | Level content | 4 files |
| `gallery.html` | Saare diagrams ek page par | — |
| `EXTENDING.md` | Naya level/lab/mission add karne ki guide | — |

### 🆕 `kafkacli.js` — asli broker, scripted output nahi

SQL Gurukul waali philosophy: **galat cheez sach mein galat result de**, warna sabak
nahi banta. Agar tum saare messages ek hi key se bhejo, sim ko *sach mein* ek partition
hot karke baaki 5 khaali dikhane chahiye.

Andar asli state:
```js
broker = {
  topics: { 'orders': {
    partitions: [ { log: [{offset, key, value, ts, headers}], leader: 1, isr: [1,2,3] } ],
    config: { 'retention.ms', 'cleanup.policy', 'min.insync.replicas' } } },
  groups: { 'order-workers': {
    members: [{ id, assignment: [0,1] }], committed: { 0: 42, 1: 17 }, state: 'Stable' } },
  brokers: [{ id: 1, alive: true }, ...],
}
```
Isse **LAG asli calculation se aata hai** (`logEndOffset − committedOffset`), hardcode nahi.

Supported commands (asli CLI ke naam):
- `kafka-topics.sh --create/--describe/--list/--alter` (partitions, RF, configs)
- `kafka-console-producer.sh` (`--property parse.key=true`) → asli partition hashing
- `kafka-console-consumer.sh` (`--from-beginning`, `--group`, `--max-messages`)
- `kafka-consumer-groups.sh --describe` → **asli LAG table**, `--reset-offsets`
- `kafka-configs.sh --alter` (retention, compaction)
- `kafka-run-class.sh kafka.tools.GetOffsetShell` / `DumpLogSegments`
- Sim-only helpers: `broker kill 2` (ISR shrink dekho), `consumer add/remove`
  (rebalance dekho), `produce --rate 5000 --key none` (hot partition banao)
- `rabbitmqctl` / `aws sqs` ke chhote sub-simulators comparative levels ke liye

### 🆕 `lab.js` — Pipeline Lab (visual sim)

Naya stage type. Discrete-event simulator + animated canvas/DOM, sliders ke saath.
**Jitter ka fayda tab tak samajh nahi aata jab tak retry-spikes ko align hote na dekho.**

- **Knobs:** producer rate · consumer count · processing time (+ variance) · failure rate
  · retry policy (`none` / `fixed` / `exponential` / `exponential+jitter`) · max retries
  · DLQ on/off · prefetch/max.poll · circuit breaker (threshold + cooldown)
- **Live meters:** consumer lag graph · in-flight · retry-attempts-per-second (thundering
  herd yahin dikhega) · DLQ counter · downstream API load · p99 latency
- **Challenge mode:** har lab ka goal + success criteria, e.g.
  - "Downstream 500s de raha hai. Retry storm ko flatten karo bina messages khoye."
    → success: `peakRetryRate < 3× steadyRate && dropped === 0`
  - "Lag 2 lakh hai, 10 min mein 0 karo — par DB ko mat maaro."
  - "Ek poison message pura consumer group rok raha hai. Chalu karo."
- Har run ke baad **"Kya hua aur kyun"** breakdown

---

## Tracks & Levels (4 tracks · 35 levels)

### 🌱 Track 1 — Sandesh Shishya Path (9)
*"Queue kyun chahiye" — sync ki maut se lekar pehla consumer group tak*

| # | Level | Problem-first hook |
|---|-------|--------------------|
| 1 | 📮 **Sync ki Maut** — queue kyun? | Checkout thread exhaustion; coupling, spikes, slow consumer |
| 2 | 🏢 **Broker ki Anatomy** | Ek process kaise hazaaron producers/consumers ko sambhalta hai (log + zero-copy) |
| 3 | 🗂️ **Topics, Partitions, Offsets** | "Parallelism chahiye" → partition; offset = bookmark, cursor nahi delete |
| 4 | 📤 **Producer** — acks, batching, keys | Fire-and-forget se data gum; `acks=all` vs latency |
| 5 | 📥 **Consumer & Consumer Groups** | 1 consumer 50k/s nahi kha sakta → group; partition = parallelism ki limit |
| 6 | 🔑 **Keys & Ordering** | "User ka `order-created` uske `order-cancelled` ke baad aaya" → key se ordering |
| 7 | 📬 **Delivery Semantics** | at-most-once (paisa gum) vs at-least-once (do baar charge) vs exactly-once |
| 8 | 📦 **Message Shape & Serialization** | JSON blob se schema tak; producer ne field hata di → consumer crash |
| 9 | 🏆 **Boss: Pehla Pipeline** | Order-events pipeline design + chalao (terminal + lab) |

### 🔥 Track 2 — Vishwas Sadhak Path / Reliability (10)
*Yahan saare "system design" hathiyaar hain — user ne inhe naam leke maanga*

| # | Level | Problem-first hook |
|---|-------|--------------------|
| 1 | 📉 **Consumer Lag** — pehla SLI | Lag kya hai, kyun badhta hai, lag vs latency, alert kaise |
| 2 | 🔄 **Rebalancing ka Toofan** | Deploy pe har pod restart → stop-the-world rebalance → 4 min downtime; sticky/cooperative, static membership |
| 3 | 🔁 **Retries & Idempotency** | Retry ne customer ko 3 baar charge kiya; idempotency key, dedup window |
| 4 | ⏳ **Exponential Backoff + JITTER** | 10k clients ek saath retry → synchronized herd; **jitter kyun** (full/equal/decorrelated) — Lab mein dekho |
| 5 | 💀 **Dead Letter Queue** | Ek poison message ne pura partition 6 ghante rok diya; DLQ, retry topics (5s/1m/10m), replay strategy, DLQ monitoring |
| 6 | 🌊 **Back-pressure & Flow Control** | Consumer 500/s kha sakta, producer 5000/s bhej raha → memory blow / OOM; pull vs push, prefetch, `max.poll.records`, pause/resume, rate limit vs load shedding |
| 7 | ⚡ **Circuit Breaker** | Downstream down, consumer 2 lakh retries phenk raha, recovery hi nahi ho paa raha; closed/open/half-open, bulkhead, timeout budget |
| 8 | 🛡️ **Replication, ISR, acks** | Broker mara → data gum; `acks=all` + `min.insync.replicas=2`, unclean leader election ka jaal |
| 9 | 🗄️ **Retention & Log Compaction** | Disk full → broker down; time/size retention, compaction (`cleanup.policy`), tombstones |
| 10 | 🏆 **Boss: Black Friday Outage Night** | Incident tree + lab + terminal — lag 4M, DLQ bhar raha, downstream flaky |

### 🚀 Track 3 — Stream Guru Path (8)
| # | Level | Problem-first hook |
|---|-------|--------------------|
| 1 | 🎯 **Exactly-Once (EOS) & Transactions** | "At-least-once se kaam chal jaayega" — jab tak accounting mismatch na aaye; idempotent producer, `transactional.id`, read-committed |
| 2 | 📤 **Transactional Outbox & CDC** | DB commit hua par Kafka publish fail → ghost order; dual-write problem, outbox, Debezium |
| 3 | ⏰ **Time, Windows & Late Data** | Event-time vs processing-time; mobile app 3 ghante baad offline events bhejta hai; watermarks, grace period |
| 4 | 🌊 **Stream Processing** (Kafka Streams / ksqlDB) | Stateful joins, KTable vs KStream, changelog topics, state store restore ka dard |
| 5 | 📜 **Schema Registry & Evolution** | Producer ne field rename ki → 40 consumers down; BACKWARD/FORWARD/FULL compat |
| 6 | 🔐 **Security & Multi-tenancy** | SASL/mTLS, ACLs, quotas; ek team ne pura cluster kha liya |
| 7 | 🌍 **Multi-DC, MirrorMaker & DR** | Region down; active-active ordering, offset translation, RPO/RTO |
| 8 | 🏆 **Boss: Ek Din Ka Platform Owner** | Cluster health, lag SLO, cost, schema break — sab ek saath |

### 🏛️ Track 4 — Architect Path / System Design (8)
| # | Level | Problem-first hook |
|---|-------|--------------------|
| 1 | ⚖️ **Log vs Queue** — Kafka vs RabbitMQ vs SQS vs MQTT vs Redis Streams | "Kaun kab?" — replay chahiye? fanout? per-message ack? IoT? Comparative matrix + har ek ka mini-sim |
| 2 | 🐰 **RabbitMQ ka Model** | Exchanges (direct/topic/fanout), per-message ack, prefetch, TTL+DLX, queue backlog = memory pressure |
| 3 | ☁️ **SQS/SNS ka Model** | Visibility timeout ka jaal (duplicate processing), FIFO vs standard, long polling, SQS ka apna DLQ, `maxReceiveCount` |
| 4 | 📡 **MQTT & IoT** | QoS 0/1/2 ka asli matlab, retained messages, LWT, 50k factory testers → broker fanout (AAS `kpm/web/session` wali kahani) |
| 5 | 📢 **Fanout & Notification Systems** | 1 tweet → 5 crore feeds; fanout-on-write vs read, celebrity problem, dedup |
| 6 | 🧩 **Event-Driven Microservices & Saga** | Distributed transaction ke bina consistency; choreography vs orchestration, compensating actions |
| 7 | 📐 **Capacity, Ordering vs Parallelism, Cost** | Kitne partitions? (throughput math), partition badhane se ordering tootna, over-partitioning ki keemat |
| 8 | 👑 **Final Boss: Design Gauntlet** | Ride-hailing / payments pipeline end-to-end — requirements se lekar failure modes tak |

---

## Per-Level Structure (siblings jaisa hi)

🎯 objectives · 📖 3 problem-first lesson cards (+ ⚡ war story + 🧠 interview framing) ·
📊 `VISUALS[id]` animated diagram · 🎮 4–5 stages (kam se kam ek terminal **ya** lab) ·
🎤 **Interview Round** (MCQ + hands-on) · ✔ success criteria checklist ·
🏋️ homework (asli `docker compose` Kafka + `kafkajs` script) · 🧘 5 flashcards → Revision Dojo

## Game Types (9)

🗂️ bins · 🔢 order · 🔗 match · 🧠 quiz · 📄 fill (Node/config blanks) · 🎯 hotspot ·
🚨 incident (3AM pager tree) · 🖥️ **terminal** (asli broker) · 🎛️ **lab** (🆕 Pipeline Lab)

## Ranks (11)

| XP | Rank | Icon |
|----|------|------|
| 0 | Sandesh Shishya | 🐣 |
| 200 | Producer Pujari | 📤 |
| 450 | Consumer Chela | 📥 |
| 750 | Partition Pandit | 🗂️ |
| 1100 | Offset Ojha | 🔖 |
| 1500 | Lag Lakshak | 📉 |
| 1950 | Retry Rishi | 🔁 |
| 2450 | DLQ Dwarpal | 💀 |
| 3000 | Stream Sadhak | 🌊 |
| 3600 | Exactly-Once Acharya | 🎯 |
| 4300 | Kafka Mahaguru | 👑 |

## Retention system (bhoolne ka ilaaj)

🧘 Revision Dojo (SRS 1→3→7→16→35 din) · 🎲 Surprise Pop Quiz · 🔥 Daily streak ·
🙈 galtiyan apne aap flashcard ban jaati hain

---

## Build Phases — STATUS

| Phase | Kya banega | Status |
|-------|-----------|--------|
| **1. Engine** | `core.js` `games.js` `app.js` `style.css` `data-core.js` `index.html` `gallery.html` | ✅ **done** — KQ namespace, Kafka purple palette, 4 tracks |
| **2. Simulators** | `kafkacli.js` (asli broker) + `lab.js` (Pipeline Lab) | ✅ **done** — 42 + 25 tests green |
| **3. Track 1** | 9 levels + 9 visuals | ✅ **done** — app khelne laayak hai |
| **4. Track 2** | 10 levels (DLQ, jitter, backpressure, circuit breaker — app ka dil) | ✅ **done** |
| **5. Track 3** | 8 levels | ✅ **done** |
| **6. Track 4** | 8 levels + comparative models | ✅ **done** |
| **7. Docs + QA** | `README.md`, `EXTENDING.md`, verification harness | ✅ **done** — `validate.js` + `boot-test.js` + 2 engine test suites |

### Final status (verified)

```
node validate.js   → 35 levels · 121 stages · 19 terminal missions · 7 labs
                     67 interview Qs · 175 flashcards · 35 diagrams
                     0 problems, 0 warnings
node boot-test.js  → 9/9 pass (sab screens render)
node test-broker.js→ 42/42 pass
node test-lab.js   → 25/25 pass
```

**Poora plan deliver ho gaya** — 4 tracks, 35 levels, dono simulators, aur
verification harness. Aage naya content add karna sirf `data-*.js` ka kaam hai
(`EXTENDING.md` mein poora shape).

### Build ke doraan do engine bugs pakde aur theek kiye (lab.js)

1. **OOM ka koi asar nahi tha** — prefetch bada karne se OOM count badhta tha par
   throughput bilkul same rehta tha, toh backpressure lab ka sabak jhootha hota.
   Fix: `stalledUntil` — OOM ke baad consumer 5s band (restart + rebalance).
   Ab bada prefetch throughput 36,000 → 3,040 gira deta hai.
2. **Circuit breaker kabhi recover nahi karta tha** — failure window state-transition
   par reset nahi hoti thi, toh woh flap karta rehta. Fix: `winOutcomes` ko
   open/half-open transition par clear karo. Ab breaker OFF (~50k barbaad retries)
   vs tuned ON (~4.5k, wahi throughput) ka saaf farak dikhta hai.

## Verification (browser ke bina, node + stubbed DOM se — SQL Gurukul waala tareeka)

1. **Structural validator** — har stage ka schema, quiz `answer` indices, bin ids,
   hotspot targets vs `data-hs`, incident graph reachability
2. **Mission runner** — har terminal mission ka solution asli broker engine mein chala ke
   `success()` verify
3. **Lab runner** — har lab challenge ko intended knob-settings se chala ke success verify
   (aur galat settings se fail hona bhi verify)
4. **Fill solver** — har fill stage programmatically solve
5. **Boot test** — app.js load + har track map, saare level intros, certificates, Dojo render

## Ever-extending design

Engine files ko chhue bina naya content add ho sake — `EXTENDING.md` mein poora guide:
naya level, naya track, naya diagram, naya lab challenge, naya broker command
(`responses` regex se), sirf flashcards, XP/rank tuning.
