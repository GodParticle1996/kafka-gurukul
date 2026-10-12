/* =====================================================================
   Kafka Gurukul — 🏛️ TRACK 4: Architect Path (System Design)
   8 levels — ab interview waali table par baitho.

   Yeh track comparative hai: Kafka ke saath RabbitMQ, SQS, MQTT aur
   Redis Streams. "Kaun kab" ka jawab dene ke liye har ek ka MODEL
   samajhna padta hai, feature-list ratt-na nahi.

   Lesson ka 3-card niyam (EXTENDING.md section 2):
     Card 1 — 😰 DARD · Card 2 — 🪜 SEEDHI · Card 3 — ⚖️ NAYI MUSIBAT
   ===================================================================== */

KQ.registerLevels('sysdesign', [

/* =====================================================================
   1. LOG vs QUEUE — kaun kab?
   ===================================================================== */
{
  id: 'log-vs-queue',
  icon: '⚖️',
  title: 'Log vs Queue — "hum har jagah Kafka use karte hain"',
  badge: 'Tool Chunne Wala',
  objectives: [
    'Log-model aur Queue-model ka bunyaadi farak samjho',
    'Kafka, RabbitMQ, SQS, MQTT, Redis Streams ko unke MODEL se pehchano',
    'Char sawal seekho jinse sahi tool chuna jaata hai',
    'Jaano ki "Kafka har jagah" kab galat hai',
  ],
  lesson: [
    `<h3>😰 Dard: "Ek email bhejne ke liye humne Kafka cluster chalaya"</h3>
     <p>Ek startup ne "sab kuch Kafka" decide kiya kyunki woh "industry standard" hai.
     Unka use-case tha: ek admin panel se background jobs chalana — PDF banao, email
     bhejo, report generate karo. Din mein ~500 jobs.</p>
     <pre>Kya chahiye tha:
   • ek job ek hi worker kare
   • job fail ho toh usse ALAG se retry karo (baaki na rukein)
   • job priority (urgent PDF pehle)
   • ~500 jobs/din (kuch bhi nahi)

Kya mila Kafka se:
   ❌ ek poison job ne partition block ki (baaki jobs ruk gaye)
   ❌ per-message retry mushkil — offset ek line mein badhta hai
   ❌ priority ka koi concept nahi
   ❌ 3-broker cluster + ZooKeeper/KRaft maintain karna pada
      ...500 jobs/din ke liye 😅</pre>
     <div class="war-story"><b>⚡ Yeh Kafka ki kami nahi thi — galat tool ka chunav tha.</b>
     Unhe ek <b>task queue</b> chahiye thi (RabbitMQ / SQS / BullMQ), aur unhone ek
     <b>event log</b> chuna. Kafka replay, fanout aur bade throughput ke liye banaya gaya
     hai; per-message ack, priority aur selective retry uska model hi nahi hai.
     Baad mein woh RabbitMQ par shift hue aur code aadha ho gaya.</div>`,

    `<h3>🪜 Do models — sab kuch isi se nikalta hai</h3>
     <p>Track 1 mein tumne yeh dekha tha; ab ise <b>decision framework</b> banate hain:</p>
     <pre>📜 LOG MODEL (Kafka, Redis Streams, Pulsar)
   messages ek APPEND-ONLY log mein, padhne se MITTE NAHI
   consumer apna OFFSET aage badhata hai
   → replay mumkin ✅ · multi-consumer fanout muft ✅
   → bahut high throughput ✅
   → par: per-message ack/retry mushkil ❌ · priority nahi ❌
          ek poison message partition block karta hai ❌

📬 QUEUE MODEL (RabbitMQ, SQS, ActiveMQ, BullMQ)
   broker messages ko HOLD karta hai, consumer ack deta hai,
   ack milte hi message DELETE
   → per-message ack/nack/retry ✅ · priority ✅
   → ek message ka fail hona baaki ko nahi rokta ✅
   → par: replay nahi (delete ho gaya) ❌
          fanout ke liye alag queues banani padti hain ❌</pre>
     <p><b>Char sawal jinse tool chunte ho:</b></p>
     <pre>1️⃣ REPLAY chahiye? ("naya consumer purana data padhe", "bug ke baad
   dobara process karo") → LOG (Kafka)

2️⃣ FANOUT kaisa? (ek message kai independent consumers ko)
   → LOG mein muft (alag groups); QUEUE mein exchange/topic setup

3️⃣ PER-MESSAGE control chahiye? (ek message ko nack karo, retry karo,
   priority do) → QUEUE (RabbitMQ/SQS)

4️⃣ THROUGHPUT kitna? (lakhon/sec) → LOG; (hazaar/sec) → dono theek</pre>`,

    `<h3>⚖️ Panch tools ek nazar mein</h3>
     <pre>🟣 KAFKA (log)
   partitioned log, offsets, consumer groups
   💪 bahut high throughput, replay, fanout, stream processing
   😖 per-message retry mushkil, operational weight, no priority
   👉 event streaming, analytics pipelines, event sourcing, CDC

🐰 RABBITMQ (queue + routing)
   exchanges → bindings → queues; per-message ack; prefetch
   💪 flexible routing, priority, per-message ack/nack, TTL+DLX
   😖 replay nahi, bahut bada backlog memory pressure banata hai
   👉 task queues, RPC, complex routing, work distribution

☁️ SQS (managed queue)
   visibility timeout, standard (at-least-once) ya FIFO
   💪 zero ops, infinite scale, AWS-native, built-in DLQ
   😖 replay nahi, FIFO throughput limited, vendor lock-in
   👉 AWS mein simple decoupling, background jobs

📡 MQTT (pub-sub for IoT)
   lightweight, QoS 0/1/2, retained messages, LWT
   💪 chhote devices, kharab network, lakhon connections
   😖 storage/replay nahi (broker-dependent), analytics ke liye nahi
   👉 IoT devices, sensors, mobile push, telemetry ingest

🔴 REDIS STREAMS (log-lite)
   log + consumer groups, par memory-first
   💪 bahut kam latency, simple, Redis already ho toh muft
   😖 memory-bound (persistence limited), Kafka jaisi durability nahi
   👉 chhote-medium streams, jahan Redis pehle se hai</pre>
     <p><b>Nayi musibat — sabse aam architecture galti:</b> "ek tool sab kuch karega".
     Asli systems mein aksar <b>dono</b> hote hain: Kafka event backbone ke liye
     (analytics, CDC, event sourcing) aur RabbitMQ/SQS task queues ke liye
     (background jobs, retries, priority). Inhe competitors samajhna hi galti hai —
     woh alag kaam ke tools hain.</p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Log ya Queue — kaunsa model?',
      instructions: 'Char sawal yaad rakho: replay chahiye? fanout? per-message control? throughput?',
      bins: [
        { id: 'log', label: '📜 Log (Kafka / Redis Streams)' },
        { id: 'queue', label: '📬 Queue (RabbitMQ / SQS)' },
      ],
      items: [
        { text: 'Order events jo billing, warehouse aur analytics teeno ko chahiye', bin: 'log',
          explain: 'Multi-consumer fanout + har team apni raftaar par. Log mein yeh muft hai (alag consumer groups). Queue mein teen alag queues banani padti.' },
        { text: 'Background jobs: PDF banao, email bhejo (ek job ek worker)', bin: 'queue',
          explain: 'Classic task queue — per-message ack, ek job fail ho toh usse alag retry karo. Kafka mein poison job partition block kar deta.' },
        { text: 'Clickstream analytics (lakhs events/sec)', bin: 'log',
          explain: 'Bahut high throughput + baad mein replay/reprocess. Yahi Kafka ka core use-case hai.' },
        { text: 'Urgent orders ko normal orders se pehle process karna', bin: 'queue',
          explain: 'PRIORITY chahiye — Kafka mein iska concept hi nahi (log ek line mein hai). RabbitMQ priority queues deta hai.' },
        { text: 'Event sourcing — poora itihaas se state rebuild karna', bin: 'log',
          explain: 'Replay hi poora point hai. Queue mein messages delete ho jaate hain, toh rebuild namumkin.' },
        { text: 'Ek fail hua message baaki ko block na kare', bin: 'queue',
          explain: 'Per-message nack/retry queue model ki taakat hai. Kafka mein offset ek line mein badhta hai, isliye DLQ pattern banana padta hai.' },
        { text: 'Naya microservice chahiye ki pichle 7 din ka data padhe', bin: 'log',
          explain: 'Replay + naya consumer bina producer ko chhue. Log ki signature capability.' },
        { text: 'Video encoding jobs jinme har job 20 minute leti hai', bin: 'queue',
          explain: 'Lambi jobs + per-message ack (visibility timeout). Kafka mein 20-min processing max.poll.interval ko todegi aur rebalance loop banegi.' },
      ],
    },
    {
      type: 'match',
      title: '🔗 Tool aur uska signature use-case',
      instructions: 'Har tool ko us kaam se jodo jiske liye woh SABSE achha hai.',
      pairs: [
        { left: 'Analytics pipeline, replay chahiye, lakhs/sec', right: '🟣 Kafka',
          explain: 'High throughput + replay + fanout = log model ka core. Yahi Kafka ka ghar hai.' },
        { left: 'Complex routing + per-message ack + priority', right: '🐰 RabbitMQ',
          explain: 'Exchanges/bindings se flexible routing, priority queues, per-message ack/nack. Task queues ka raja.' },
        { left: 'AWS mein zero-ops background jobs', right: '☁️ SQS',
          explain: 'Managed, infinite scale, koi cluster nahi sambhaalna. AWS-native decoupling ke liye default.' },
        { left: 'Lakhs IoT devices, kharab network, chhote packets', right: '📡 MQTT',
          explain: 'Lightweight protocol, QoS levels, LWT — constrained devices aur unreliable networks ke liye bana.' },
        { left: 'Redis pehle se hai, chhota stream, ultra-low latency', right: '🔴 Redis Streams',
          explain: 'Log-lite: consumer groups + offsets, par memory-first. Chhote scale par simple aur bahut tez.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Tool selection ke sawal',
      questions: [
        {
          q: 'Kafka aur RabbitMQ mein bunyaadi farak (feature list nahi, MODEL)?',
          choices: [
            'Kafka tez hai, RabbitMQ dheema',
            'Kafka ek append-only LOG hai — messages padhne se mitte nahi, consumer offset aage badhata hai (isliye replay, fanout, multi-consumer muft). RabbitMQ ek QUEUE broker hai — woh messages hold karta hai aur ack milte hi DELETE kar deta hai (isliye per-message ack/nack, priority, aur ek message ka fail hona baaki ko nahi rokta). Baaki saare fayde-nuksan isi ek design farak se nikalte hain.',
            'Kafka Java mein hai',
            'RabbitMQ purana hai',
          ],
          answer: 1,
          explain: 'Yeh interview ka #1 messaging sawal hai. "Kafka tez hai" junior jawab hai. Model ka farak (log vs delete-on-ack) batana senior jawab hai — kyunki har feature usi se derive hota hai.',
        },
        {
          q: 'Background jobs (PDF/email) ke liye Kafka use karne mein kya dikkat hai?',
          choices: [
            'Kafka dheema hai',
            'Teen: (1) ek poison job partition block kar deta hai (offset ek line mein badhta hai), (2) per-message retry mushkil — ek job ko alag se nack nahi kar sakte, (3) priority ka concept hi nahi. Yeh sab queue model ki strengths hain. Chhote job volumes ke liye Kafka ka operational weight bhi over-kill hai.',
            'Kafka mein jobs nahi bhej sakte',
            'Kafka mahenga hai',
          ],
          answer: 1,
          explain: 'Kafka jobs bhej SAKTA hai, par uska model task-queue semantics ke liye nahi bana. Yeh galat-tool wala classic case hai — technically possible, practically dukhdayi.',
        },
        {
          q: 'Ek company Kafka AUR RabbitMQ dono use karti hai. Kya yeh galat architecture hai?',
          choices: [
            'Haan, ek hi tool hona chahiye',
            'Nahi — yeh aksar SAHI architecture hai. Woh alag kaam ke tools hain: Kafka event backbone ke liye (analytics, CDC, event sourcing, high-throughput streaming) aur RabbitMQ/SQS task queues ke liye (background jobs, per-message retry, priority). Inhe competitors maanna hi galti hai.',
            'Haan, cost double ho jaayegi',
            'Sirf agar teams alag hon',
          ],
          answer: 1,
          explain: 'Yeh interview mein achha signal hai — tum tools ko "religion" nahi, use-case fit se dekhte ho. Bade systems mein dono hona bilkul normal hai.',
        },
      ],
    },
    {
      type: 'order',
      title: '🔢 Tool chunne ka sahi kram',
      instructions: 'Ek naye system ke liye messaging tool chunte waqt sahi kram lagao.',
      steps: [
        { text: '📋 Requirements likho: throughput, replay, fanout, per-message control, ordering',
          explain: 'Pehle yeh. Bina requirements ke tool chunna "jo popular hai woh le lo" ban jaata hai — aur wahi 500-jobs-par-Kafka wala incident hota hai.' },
        { text: '❓ Poocho: REPLAY chahiye? (naya consumer purana data, ya bug ke baad reprocess)',
          explain: 'Yeh sabse decisive sawal hai. Haan → log model (Kafka). Nahi → queue bhi chal sakti hai.' },
        { text: '❓ Poocho: FANOUT kaisa? (ek message kitne independent consumers ko)',
          explain: 'Multiple independent consumers → log mein muft (alag groups). Queue mein har consumer ke liye alag queue + routing setup.' },
        { text: '❓ Poocho: PER-MESSAGE control chahiye? (nack, retry, priority)',
          explain: 'Haan → queue model (RabbitMQ/SQS). Kafka mein yeh DLQ pattern se banana padta hai, native nahi.' },
        { text: '📊 Throughput aur operational capacity dekho',
          explain: 'Lakhs/sec → Kafka. Hazaar/sec → dono theek, toh operational simplicity dekho (managed SQS vs self-hosted Kafka).' },
        { text: '✅ Chuno — aur yeh bhi accept karo ki DONO chahiye ho sakte hain',
          explain: 'Bade systems mein Kafka (backbone) + queue (tasks) dono hona normal hai. Ek tool sab kuch karega — yeh sabse aam architecture galti hai.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Kafka ya RabbitMQ — tum kaunsa chunoge?" (yeh trap question hai)',
        choices: [
          'Kafka, woh industry standard hai',
          'RabbitMQ, woh simple hai',
          'Yeh use-case par depend karta hai, aur main pehle char sawal poochunga: (1) Replay chahiye — naya consumer purana data padhe ya bug ke baad reprocess? (2) Fanout — ek message kitne independent consumers ko? (3) Per-message control — nack/retry/priority chahiye? (4) Throughput kitna? Replay + fanout + high throughput → Kafka (log model). Per-message ack + priority + task distribution → RabbitMQ (queue model). Aur yeh bhi bataunga ki bade systems mein aksar DONO hote hain — Kafka event backbone, RabbitMQ task queues.',
          'Dono same hain',
        ],
        answer: 2,
        explain: 'Yeh trap question hai — jo candidate turant ek naam bolta hai woh fail ho gaya. Achha jawab requirements poochta hai, dono ke MODEL se justify karta hai, aur "dono" ko valid answer maanta hai.',
      },
      {
        q: '🧑‍💼 "Tumhe background jobs chahiye (email, PDF). Kafka theek rahega?"',
        choices: [
          'Haan, Kafka sab kar sakta hai',
          'Technically haan, par model fit nahi baithta: (a) ek poison job partition block kar dega kyunki offset ek line mein badhta hai, (b) per-message retry/nack native nahi — DLQ pattern khud banana padega, (c) priority ka concept hi nahi. Task queues ke liye main RabbitMQ, SQS ya ek simple job queue (BullMQ) chunuunga — wahan per-message ack, retry aur priority built-in hain. Kafka ko event streaming ke liye rakhunga.',
          'Nahi, Kafka jobs support nahi karta',
          'Haan agar partitions zyada hon',
        ],
        answer: 1,
        explain: 'Yeh dikhata hai ki tum "possible" aur "appropriate" mein farak karte ho. Kafka par jobs chala sakte ho — par uska model tumhare against kaam karega. Yeh maturity ka signal hai.',
      },
    ],
  },
  success: [
    'Main log-model aur queue-model ka bunyaadi farak bata sakta hoon',
    'Main char sawalon se sahi tool chun sakta hoon',
    'Main Kafka/RabbitMQ/SQS/MQTT/Redis Streams ko unke model se pehchan sakta hoon',
    'Main jaanta hoon ki "ek tool sab kuch" kab galat hai',
  ],
  homework: `<p>Apne kisi asli (ya kaalpanik) system ke liye tool-selection likho:</p>
    <pre>System: ______________________

1. Replay chahiye?           [ ] haan  [ ] nahi
   (naya consumer purana data / bug ke baad reprocess)

2. Fanout kaisa?             [ ] 1 consumer  [ ] kai independent

3. Per-message control?      [ ] nack/retry/priority chahiye
                             [ ] batch processing kaafi

4. Throughput?               [ ] lakhs/sec  [ ] hazaar/sec  [ ] kam

5. Operational capacity?     [ ] self-host OK  [ ] managed chahiye

→ Faisla: ______________  Kyun: ______________</pre>
    <p><b>Asli kaam:</b> yeh char sawal apne current project par chalao. Agar tum
    already Kafka use kar rahe ho, imaandari se dekho ki kya char sawalon ke jawab
    Kafka ki taraf point karte hain — ya tumne use isliye chuna kyunki woh popular tha.
    Dono jawab theek hain, par pata hona chahiye.</p>`,
  cards: [
    { front: 'Log vs Queue model — ek line mein?', back: '<b>Log</b> (Kafka): messages padhne se <b>mitte nahi</b>, consumer offset badhata hai → replay + fanout muft.<br><b>Queue</b> (RabbitMQ/SQS): ack milte hi <b>delete</b> → per-message ack/nack/priority, par replay nahi.' },
    { front: 'Tool chunne ke CHAR sawal?', back: '1) <b>Replay</b> chahiye? → log<br>2) <b>Fanout</b> kaisa? (kai independent consumers) → log<br>3) <b>Per-message control</b> (nack/retry/priority)? → queue<br>4) <b>Throughput</b> (lakhs/sec)? → log' },
    { front: 'Background jobs ke liye Kafka kyun galat hai?', back: '1) Poison job <b>partition block</b> karta hai (offset ek line mein)<br>2) Per-message <b>retry/nack native nahi</b><br>3) <b>Priority ka concept hi nahi</b><br>Task queues (RabbitMQ/SQS/BullMQ) ka model yeh sab deta hai.' },
    { front: 'Panch tools — signature use-case?', back: '🟣 <b>Kafka</b>: streaming, replay, analytics, CDC<br>🐰 <b>RabbitMQ</b>: task queues, routing, priority<br>☁️ <b>SQS</b>: AWS zero-ops jobs<br>📡 <b>MQTT</b>: IoT, weak networks<br>🔴 <b>Redis Streams</b>: chhote streams, ultra-low latency' },
    { front: 'Kafka AUR RabbitMQ dono — galat architecture?', back: '<b>Nahi — aksar sahi hai.</b> Alag kaam ke tools: Kafka event backbone (analytics, CDC, streaming), RabbitMQ/SQS task queues (jobs, per-message retry, priority). "Ek tool sab kuch" hi sabse aam galti hai.' },
  ],
},

/* =====================================================================
   2. RABBITMQ KA MODEL
   ===================================================================== */
{
  id: 'rabbitmq-model',
  icon: '🐰',
  title: 'RabbitMQ — queue 40 lakh tak pahunchi, broker OOM',
  badge: 'Exchange Expert',
  objectives: [
    'Exchanges → bindings → queues ka routing model samjho',
    'Per-message ack/nack aur prefetch (QoS) seekho',
    'TTL + DLX se RabbitMQ ka DLQ pattern jaano',
    'Samjho ki queue backlog memory pressure kyun banata hai (Kafka se ulta)',
  ],
  lesson: [
    `<h3>😰 Dard: "Consumer 2 ghante down raha, RabbitMQ broker hi gir gaya"</h3>
     <p>Ek team ka notification consumer 2 ghante ke liye down ho gaya (ek bad deploy).
     Producers chalte rahe — 40 lakh messages queue mein jama ho gaye.</p>
     <pre>RabbitMQ queue messages ko MEMORY mein rakhta hai
(disk par sirf tab jab memory pressure ho — "paging", jo dheema hai)

40 lakh messages × ~2KB = ~8 GB
   → broker ka memory high-watermark paar
   → RabbitMQ ne PRODUCERS ko block karna shuru kiya (flow control)
   → producers hang ho gaye → upstream services timeout
   → ek consumer ke down hone se POORA system ruk gaya 💥</pre>
     <div class="war-story"><b>⚡ Yahan Kafka se ulta behaviour hai.</b> Kafka mein
     consumer down rehne se messages <b>disk par log mein</b> jama hote hain — broker
     ko koi farak nahi padta (bas lag badhta hai). RabbitMQ mein backlog
     <b>broker ki memory</b> par bojh hai. Isliye RabbitMQ ka mool mantra hai:
     <b>"queues should be short"</b> — woh transport hai, storage nahi.</div>`,

    `<h3>🪜 RabbitMQ ka routing model — teen hisse</h3>
     <p>Kafka mein producer seedha topic par likhta hai. RabbitMQ mein ek routing layer hai:</p>
     <pre>Producer → EXCHANGE → (binding rules) → QUEUE(s) → Consumer

Producer QUEUE ko jaanta hi nahi — woh sirf exchange ko deta hai.
Exchange ka type tay karta hai ki message kis queue mein jaayega:

📍 DIRECT     — routing key EXACT match
   routing_key="pdf" → queue jiska binding "pdf" hai

📢 FANOUT     — SAARI bound queues ko copy (routing key ignore)
   → yahi Kafka ke multi-consumer-group jaisa fanout hai

🌲 TOPIC      — pattern match (wildcards)
   "order.*.created" → order.in.created, order.us.created
   → sabse flexible, event routing ke liye

🗂️ HEADERS    — message headers se match (kam use hota hai)</pre>
     <p><b>Aur per-message control — yahi RabbitMQ ki asli taakat hai:</b></p>
     <pre>consumer.ack(msg)              → "ho gaya, delete kar do"
consumer.nack(msg, requeue=true)  → "fail hua, WAPAS queue mein daalo"
consumer.nack(msg, requeue=false) → "fail hua, DLX (dead letter) bhejo"

Kafka mein yeh nahi ho sakta — wahan offset ek line mein badhta hai.
Yahan har message ka apna faisla hai. Isliye ek fail hua message
baaki ko block NAHI karta.</pre>`,

    `<h3>⚖️ Prefetch, DLX, aur nayi musibat</h3>
     <p><b>Prefetch (QoS) — RabbitMQ ka back-pressure knob:</b></p>
     <pre>RabbitMQ PUSH model hai — broker consumer ko messages dhakelta hai.
Bina limit ke woh hazaaron messages ek slow consumer par daal dega
→ consumer memory blow → OOM  (Track 2 ka backpressure level yaad hai?)

channel.prefetch(10)   ← "ek saath max 10 unacked messages do"
→ consumer 10 process kare, ack de, tab agle 10 milenge
→ yahi RabbitMQ mein back-pressure ka tareeqa hai

⚠️ Kafka PULL hai (consumer maangta hai) → natural back-pressure
   RabbitMQ PUSH hai (broker dhakelta hai) → prefetch LAZMI hai</pre>
     <p><b>DLQ RabbitMQ style — TTL + DLX:</b></p>
     <pre>queue par do properties:
   x-message-ttl: 60000            → 60s baad message expire
   x-dead-letter-exchange: "dlx"   → expire/nack hua message DLX par jaaye

Isse retry-with-delay bhi banta hai:
   main queue → fail → nack → DLX → "retry-60s" queue (TTL 60s)
   → TTL expire → uska DLX = main queue → wapas try
   → yeh RabbitMQ ka "delayed retry" ka classic pattern hai</pre>
     <p><b>Nayi musibat:</b> RabbitMQ ka har fayda uske "delete on ack" model se aata
     hai, aur har nuksan bhi. <b>Replay namumkin</b> hai (message chala gaya), aur
     <b>bada backlog broker ke liye khatra</b> hai (memory). Isliye RabbitMQ ke saath
     hamesha: prefetch set karo, queues chhoti rakho, DLX lagao, aur queue-depth par
     alert lagao. Kafka mein bada backlog normal hai; RabbitMQ mein woh ek incident hai.</p>`,
  ],
  stages: [
    {
      type: 'match',
      title: '🔗 Exchange type aur uska kaam',
      instructions: 'Har routing zaroorat ko sahi exchange type se jodo.',
      pairs: [
        { left: 'Ek message SAARE subscribed services ko chahiye', right: '📢 Fanout exchange',
          explain: 'Fanout routing key ignore karke saari bound queues ko copy bhejta hai — Kafka ke multiple consumer groups jaisa.' },
        { left: 'routing key "pdf" wale messages sirf pdf-worker ko', right: '📍 Direct exchange',
          explain: 'Exact routing-key match. Simple work distribution ke liye.' },
        { left: '"order.*.created" pattern se route karo', right: '🌲 Topic exchange',
          explain: 'Wildcard pattern matching — sabse flexible. Event-driven routing ke liye standard choice.' },
        { left: 'Message headers ke basis par route karo', right: '🗂️ Headers exchange',
          explain: 'Routing key ke bajaye headers dekhta hai. Kam use hota hai par complex routing mein kaam aata hai.' },
        { left: 'Fail/expire hue messages ke liye alag jagah', right: '💀 Dead Letter Exchange (DLX)',
          explain: 'nack(requeue=false) ya TTL expire par message DLX par jaata hai — RabbitMQ ka DLQ mechanism.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 RabbitMQ ke sawal',
      questions: [
        {
          q: 'Consumer 2 ghante down raha aur RabbitMQ broker hi gir gaya. Kafka mein aisa kyun nahi hota?',
          choices: [
            'Kafka ke paas zyada memory hoti hai',
            'RabbitMQ queue messages ko MEMORY mein rakhta hai (disk paging dheemi hai), toh bada backlog broker ki memory kha jaata hai aur woh producers ko block karne lagta hai. Kafka mein messages DISK par append-only log mein jaate hain — backlog badhne se broker ko farak nahi padta, sirf consumer lag badhta hai. Isliye RabbitMQ ka mantra "queues should be short" hai — woh transport hai, storage nahi.',
            'Kafka messages delete kar deta hai',
            'RabbitMQ mein bug hai',
          ],
          answer: 1,
          explain: 'Yeh do models ka sabse practical farak hai. Kafka mein 10 crore ka backlog normal hai (disk); RabbitMQ mein 40 lakh ka backlog ek incident hai (memory). Yeh architecture decisions par seedha asar daalta hai.',
        },
        {
          q: 'RabbitMQ mein prefetch (QoS) kyun set karna zaroori hai?',
          choices: [
            'Speed badhane ke liye',
            'Kyunki RabbitMQ PUSH model hai — broker consumer ko messages dhakelta hai. Bina prefetch limit ke woh hazaaron messages ek slow consumer par daal dega aur uska memory blow ho jaayega. prefetch(N) kehta hai "ek saath max N unacked messages do". Kafka PULL hai (consumer maangta hai) toh wahan yeh natural hai, RabbitMQ mein explicitly set karna padta hai.',
            'Ordering ke liye',
            'Durability ke liye',
          ],
          answer: 1,
          explain: 'Push vs pull ka seedha natija. Kafka ka pull-model natural back-pressure deta hai; RabbitMQ ke push-model mein prefetch hi back-pressure hai. Yeh Track 2 ke backpressure level se jodta hai.',
        },
        {
          q: 'RabbitMQ mein ek message fail hua. Kafka se kya farak hai?',
          choices: [
            'Koi farak nahi',
            'RabbitMQ mein tum us EK message ko nack kar sakte ho — requeue=true (wapas queue) ya requeue=false (DLX par bhejo) — aur baaki messages bina ruke process hote rehte hain. Kafka mein offset ek line mein badhta hai, toh ek poison message poore partition ko block kar deta hai (isliye wahan DLQ pattern khud banana padta hai).',
            'Kafka mein bhi nack hota hai',
            'RabbitMQ message delete kar deta hai',
          ],
          answer: 1,
          explain: 'Per-message ack/nack queue model ki signature strength hai. Yahi wajah hai ki task queues ke liye RabbitMQ/SQS behtar fit hain — ek kharab job baaki ko nahi rokti.',
        },
      ],
    },
    {
      type: 'bins',
      title: '🗂️ RabbitMQ ka feature ya Kafka ka?',
      instructions: 'Har capability ko us system se jodo jismein woh NATIVE hai.',
      bins: [
        { id: 'rabbit', label: '🐰 RabbitMQ (queue model)' },
        { id: 'kafka', label: '🟣 Kafka (log model)' },
      ],
      items: [
        { text: 'Per-message ack/nack', bin: 'rabbit',
          explain: 'Har message ka apna faisla — ack, requeue, ya DLX. Queue model ki core strength.' },
        { text: 'Message replay (purana data dobara padhna)', bin: 'kafka',
          explain: 'Offset peeche karo aur dobara padho. RabbitMQ mein message ack ke baad delete ho jaata hai — replay namumkin.' },
        { text: 'Priority queues (urgent pehle)', bin: 'rabbit',
          explain: 'x-max-priority se priority queue banti hai. Kafka mein log ek line mein hai — priority ka concept hi nahi.' },
        { text: 'Consumer group offsets', bin: 'kafka',
          explain: 'Har group apna bookmark rakhta hai. Isse fanout aur independent consumption milti hai.' },
        { text: 'Exchange → binding → queue routing', bin: 'rabbit',
          explain: 'Producer queue ko jaanta hi nahi — exchange routing karta hai (direct/fanout/topic/headers). Kafka mein producer seedha topic par likhta hai.' },
        { text: 'Log compaction (har key ka latest)', bin: 'kafka',
          explain: 'cleanup.policy=compact — har key ka latest value. Queue model mein iska koi equivalent nahi.' },
        { text: 'TTL + DLX se delayed retry', bin: 'rabbit',
          explain: 'Message TTL expire hone par DLX par jaata hai — isse retry-with-delay chains banti hain. Kafka mein yeh retry-topics se manually banana padta hai.' },
        { text: 'Crore messages ka backlog bina broker ko gire', bin: 'kafka',
          explain: 'Disk par append-only log — backlog se broker ko farak nahi padta. RabbitMQ mein bada backlog memory pressure aur producer blocking laata hai.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "RabbitMQ mein queue bahut badi ho gayi toh kya hota hai? Kafka se farak?"',
        choices: [
          'Dono mein same hota hai',
          'RabbitMQ queues messages ko memory mein rakhti hain (disk paging dheemi hai), toh bada backlog broker ki memory kha jaata hai — high-watermark par woh producers ko BLOCK karne lagta hai (flow control), jisse upstream services bhi hang ho jaati hain. Ek consumer ke down hone se poora system ruk sakta hai. Kafka mein backlog disk par log mein jaata hai — broker ko farak nahi padta, sirf consumer lag badhta hai. Isliye RabbitMQ ke saath queue-depth par alert aur prefetch zaroori hai; "queues should be short" uska mantra hai.',
          'RabbitMQ messages delete kar deta hai',
          'Kafka crash ho jaata hai',
        ],
        answer: 1,
        explain: 'Yeh operational farak architecture decisions par seedha asar daalta hai. Kafka ko "buffer" ki tarah use kar sakte ho; RabbitMQ ko nahi — woh transport hai. Yeh senior-level distinction hai.',
      },
      {
        q: '🧑‍💼 "RabbitMQ mein retry-with-delay kaise banaoge?"',
        choices: [
          'Consumer mein sleep laga dunga',
          'TTL + DLX chain se. Main ek "retry-60s" queue banaunga jiska x-message-ttl=60000 ho aur uska x-dead-letter-exchange main queue ko point kare. Fail hua message main queue se nack(requeue=false) hoke retry queue mein jaayega, wahan 60s baithega, phir TTL expire hone par DLX se wapas main queue mein aa jaayega. Multiple tiers (5s/1m/10m) bana ke escalating backoff mil jaata hai, aur aakhri tier ka DLX ek asli DLQ ho. Consumer mein sleep lagana galat hai — woh consumer ko block karta hai aur throughput maar deta hai.',
          'Message ko baad mein publish karunga',
          'RabbitMQ mein delay possible nahi',
        ],
        answer: 1,
        explain: 'TTL+DLX chain RabbitMQ ka classic delayed-retry pattern hai (ya rabbitmq-delayed-message-exchange plugin). "Consumer mein sleep" wala jawab batata hai ki candidate ne production mein queues nahi chalayin — woh prefetch slot block kar deta hai.',
      },
    ],
  },
  success: [
    'Main exchange → binding → queue routing model samjha sakta hoon',
    'Main char exchange types aur unke use-cases bata sakta hoon',
    'Main prefetch (QoS) se RabbitMQ mein back-pressure samjha sakta hoon',
    'Main jaanta hoon ki RabbitMQ mein bada backlog kyun khatarnaak hai (Kafka se ulta)',
  ],
  homework: `<p>RabbitMQ ka routing khud dekho (amqplib):</p>
    <pre>const amqp = require('amqplib');
const conn = await amqp.connect('amqp://localhost');
const ch = await conn.createChannel();

// topic exchange + retry chain
await ch.assertExchange('orders', 'topic');
await ch.assertExchange('orders.dlx', 'topic');

await ch.assertQueue('order-worker', {
  deadLetterExchange: 'orders.dlx',       // nack → yahan jaayega
});
await ch.assertQueue('order-retry-60s', {
  messageTtl: 60000,                       // 60s baad expire
  deadLetterExchange: 'orders',            // expire → wapas main
});
await ch.bindQueue('order-worker', 'orders', 'order.*.created');
await ch.bindQueue('order-retry-60s', 'orders.dlx', '#');

// ⚠️ prefetch — back-pressure (yeh mat bhoolna!)
await ch.prefetch(10);

await ch.consume('order-worker', async (msg) => {
  try { await process(msg); ch.ack(msg); }
  catch (e) { ch.nack(msg, false, false); }   // → DLX → retry queue
});</pre>
    <p><b>Asli experiment:</b> prefetch hata do aur ek slow consumer chalao jabki
    producer tez bhej raha ho — dekho ki consumer ki memory kaise badhti hai.
    Phir <code>prefetch(10)</code> lagao — memory bounded ho jaayegi. Yahi push-model
    mein back-pressure hai.</p>`,
  cards: [
    { front: 'RabbitMQ ka routing model?', back: '<code>Producer → EXCHANGE → (bindings) → QUEUE(s) → Consumer</code>. Producer queue ko <b>jaanta hi nahi</b> — exchange type routing tay karta hai: <b>direct</b> (exact key), <b>fanout</b> (sab), <b>topic</b> (wildcards), <b>headers</b>.' },
    { front: 'RabbitMQ mein bada backlog khatarnaak kyun?', back: 'Queues messages ko <b>MEMORY</b> mein rakhti hain — bada backlog broker ki memory kha ke <b>producers ko block</b> kar deta hai (flow control). Kafka mein backlog <b>disk</b> par jaata hai, broker ko farak nahi. Mantra: "queues should be short".' },
    { front: 'prefetch (QoS) kyun zaroori hai?', back: 'RabbitMQ <b>PUSH</b> model hai — broker consumer par messages dhakelta hai. Bina prefetch ke slow consumer ka memory blow. <code>prefetch(N)</code> = "max N unacked messages ek saath". Kafka <b>PULL</b> hai toh wahan natural back-pressure.' },
    { front: 'RabbitMQ mein per-message control?', back: '<code>ack</code> (ho gaya, delete), <code>nack(requeue=true)</code> (wapas queue), <code>nack(requeue=false)</code> (DLX bhejo). Ek fail hua message <b>baaki ko block nahi karta</b> — Kafka mein offset ek line mein badhta hai isliye wahan poison partition rok deta hai.' },
    { front: 'RabbitMQ mein delayed retry kaise?', back: '<b>TTL + DLX chain</b>: retry queue with <code>x-message-ttl</code> aur uska <code>x-dead-letter-exchange</code> = main queue. Fail → nack → retry queue mein baithe → TTL expire → wapas main. Multiple tiers = escalating backoff.' },
  ],
},

/* =====================================================================
   3. SQS KA MODEL
   ===================================================================== */
{
  id: 'sqs-model',
  icon: '☁️',
  title: 'SQS — visibility timeout ne har order do baar process karwaya',
  badge: 'SQS Sipahi',
  objectives: [
    'Visibility timeout ka mechanism aur uska classic jaal samjho',
    'Standard vs FIFO queue ka farak aur throughput limits jaano',
    'Long polling se cost aur latency dono theek karna seekho',
    'SQS ka built-in DLQ (maxReceiveCount) samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "Har order do baar ship ho raha tha"</h3>
     <p>Ek team ne SQS se order processing banayi. Har order ko process karne mein
     ~45 second lagte the (inventory check + label print + notification). Unhone
     visibility timeout default (30 second) hi chhod diya.</p>
     <pre>SQS ka visibility timeout kaise kaam karta hai:
   consumer message "receive" karta hai
   → message queue mein RAHTA hai par INVISIBLE ho jaata hai
   → visibility timeout tak (default 30s)
   → consumer ko DELETE karna padta hai warna woh WAPAS VISIBLE ho jaata hai

Unka case:
   t=0s    worker-1 ne order receive kiya (invisible 30s ke liye)
   t=30s   processing abhi chal rahi hai (45s lagte hain)
           → message WAPAS VISIBLE ho gaya
   t=31s   worker-2 ne wahi order receive kiya → PROCESS karna shuru
   t=45s   worker-1 ne finish kiya, delete kiya
   t=76s   worker-2 ne bhi finish kiya → order DO BAAR ship ho gaya 📦📦</pre>
     <div class="war-story"><b>⚡ Visibility timeout &lt; processing time = guaranteed
     duplicates.</b> Yeh SQS ki sabse aam galti hai. Aur dhyan do — yeh at-least-once
     ka natural natija hai, bug nahi. SQS keh raha tha "tumne 30 second mein delete
     nahi kiya, toh maine maan liya ki tum mar gaye". Do fixes: timeout ko processing
     time se bada rakho, YA processing ke doraan heartbeat se timeout extend karo
     (<code>ChangeMessageVisibility</code>).</div>`,

    `<h3>🪜 SQS ka model — teen zaroori cheezein</h3>
     <pre>1️⃣ VISIBILITY TIMEOUT — "lease" jaisa
   receive → message invisible (default 30s, max 12 hours)
   delete kiya → gaya
   delete nahi kiya → wapas visible → koi aur consumer le lega

   ✅ Rules: timeout > p99 processing time rakho
            YA long jobs mein ChangeMessageVisibility se extend karo
            aur processing ke turant baad delete karo

2️⃣ STANDARD vs FIFO
   STANDARD: at-least-once, best-effort ordering,
             UNLIMITED throughput
   FIFO:     exactly-once processing (5-min dedup window),
             strict ordering per MessageGroupId,
             par ~300 msg/s (batching se 3,000)
   → FIFO ki ordering "message group" ke andar hai
     (Kafka ke partition-key jaisa hi idea)

3️⃣ LONG POLLING
   short polling: har request turant jawab (khaali bhi) → bahut
                  API calls → paisa + latency
   long polling (WaitTimeSeconds=20): message aane tak wait
                  → kam API calls, kam cost, kam latency
   → yeh lagbhag hamesha ON hona chahiye</pre>`,

    `<h3>⚖️ SQS ka DLQ, aur nayi musibat</h3>
     <p><b>SQS ka DLQ built-in hai</b> — Kafka jaisa manually banana nahi padta:</p>
     <pre>Redrive policy:
   maxReceiveCount: 5
   deadLetterTargetArn: arn:...:orders-dlq

Matlab: ek message 5 baar receive hua (yaani 5 baar processing fail
        hui aur woh wapas visible ho gaya) → 6th baar woh apne aap
        DLQ mein chala jaayega

⚠️ Jaal: "receive count" ka matlab hai receives, na ki tumhare
   application ke retries. Agar tumhara consumer crash hota hai
   ya visibility timeout chhota hai, toh receive count tezi se
   badhta hai aur healthy messages bhi DLQ mein ja sakte hain.</pre>
     <p><b>Nayi musibat — SQS ke trade-offs:</b></p>
     <pre>✅ Zero ops (managed), infinite scale, AWS-native, built-in DLQ
❌ REPLAY nahi — delete ke baad message gaya (Kafka jaisa itihaas nahi)
❌ Fanout ke liye SNS chahiye (SNS → kai SQS queues)
❌ FIFO throughput limited (~300/s, batching se 3,000/s)
❌ Vendor lock-in
❌ Per-message cost — bahut high volume par mahenga ho sakta hai

👉 SQS best hai: AWS mein simple decoupling, background jobs,
   jahan replay nahi chahiye aur ops team chhoti ho.
   Analytics/event-sourcing/replay chahiye → Kafka.</pre>
     <p><b>Fanout pattern:</b> SQS akela fanout nahi karta. Standard AWS pattern hai
     <b>SNS → multiple SQS queues</b> — SNS topic par publish karo, har subscriber
     ki apni SQS queue. Yeh Kafka ke multiple consumer groups jaisa hai, par har
     queue apna independent copy leti hai (aur replay phir bhi nahi).</p>`,
  ],
  stages: [
    {
      type: 'order',
      title: '🔢 SQS message ka lifecycle',
      instructions: 'Ek message ka SQS mein poora safar sahi kram mein lagao.',
      steps: [
        { text: '📤 Producer message ko queue mein bhejta hai (SendMessage)',
          explain: 'Message queue mein visible state mein aa jaata hai — koi bhi consumer use receive kar sakta hai.' },
        { text: '📥 Consumer ReceiveMessage karta hai → message INVISIBLE ho jaata hai',
          explain: 'Message delete NAHI hota — bas visibility timeout ke liye chhup jaata hai. Yeh ek "lease" hai.' },
        { text: '⏱️ Visibility timeout ki ghadi chalti hai (default 30s)',
          explain: 'Is dauran koi aur consumer use nahi dekh sakta. Agar processing lambi hai toh ChangeMessageVisibility se extend karo.' },
        { text: '⚙️ Consumer message process karta hai',
          explain: 'Yahan asli kaam. Yeh visibility timeout ke ANDAR khatam hona chahiye, warna message wapas visible ho jaayega aur duplicate processing hogi.' },
        { text: '🗑️ Consumer DeleteMessage karta hai — ab message poori tarah gaya',
          explain: 'Yahi "ack" hai. Delete na kiya toh timeout ke baad message wapas visible ho jaayega aur koi aur use uthayega.' },
        { text: '💀 (Agar baar-baar fail) receive count > maxReceiveCount → apne aap DLQ',
          explain: 'SQS ka built-in DLQ. Redrive policy se set hota hai. Dhyan: yeh RECEIVES ginta hai, tumhare app ke retries nahi.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 SQS ke sawal',
      questions: [
        {
          q: 'Har order do baar process ho raha hai. Processing 45s leti hai, visibility timeout 30s hai. Kyun?',
          choices: [
            'SQS mein bug hai',
            'Visibility timeout processing time se CHHOTA hai — 30s baad message wapas visible ho jaata hai jabki pehla worker abhi bhi kaam kar raha hai, toh doosra worker bhi use uthata hai. Fix: (a) visibility timeout ko p99 processing time se bada rakho, ya (b) processing ke doraan ChangeMessageVisibility se timeout extend karte raho (heartbeat). Aur processing idempotent banao — kyunki at-least-once hamesha duplicate de sakta hai.',
            'Do queues hain',
            'Consumers zyada hain',
          ],
          answer: 1,
          explain: 'Yeh SQS ki sabse aam galti hai. Visibility timeout ek "lease" hai — usse processing se lamba hona chahiye. Aur idempotency phir bhi chahiye (Track 2), kyunki crash/retry se duplicate aa hi sakta hai.',
        },
        {
          q: 'SQS Standard aur FIFO mein farak?',
          choices: [
            'FIFO tez hai',
            'Standard: at-least-once delivery, best-effort ordering, UNLIMITED throughput. FIFO: exactly-once processing (5-min dedup window), strict ordering per MessageGroupId, par throughput limited (~300 msg/s, batching se 3,000). FIFO ki ordering "message group" ke andar hoti hai — Kafka ke partition-key jaisa hi idea. Ordering/dedup ki keemat throughput hai.',
            'Standard mein ordering hai',
            'FIFO mein DLQ nahi hota',
          ],
          answer: 1,
          explain: 'FIFO ordering aur dedup deta hai par throughput ki keemat par. MessageGroupId Kafka ke partition key jaisa hai — same group = ordered. Yeh trade-off har messaging system mein dikhta hai.',
        },
        {
          q: 'Long polling (WaitTimeSeconds=20) kyun use karte hain?',
          choices: [
            'Ordering ke liye',
            'Short polling har request par turant jawab deta hai (khaali bhi) — isse bahut saari API calls hoti hain (paisa) aur message aane par latency bhi zyada hoti hai (agli poll ka intezaar). Long polling message aane tak wait karta hai — kam API calls (kam cost), aur message aate hi turant milta hai (kam latency). Yeh lagbhag hamesha ON hona chahiye.',
            'Duplicates rokne ke liye',
            'DLQ ke liye zaroori hai',
          ],
          answer: 1,
          explain: 'Long polling cost AUR latency dono theek karta hai — yeh unusual hai (aksar trade-off hota hai). Isliye yeh SQS ka lagbhag default best-practice hai.',
        },
      ],
    },
    {
      type: 'fill',
      title: '📄 SQS consumer theek karo',
      instructions: 'Woh double-shipping bug theek karo — timeout, long polling aur delete.',
      template: "const res = await sqs.receiveMessage({\n  QueueUrl: url,\n  MaxNumberOfMessages: 10,\n  WaitTimeSeconds: {0},        // long polling — cost + latency dono behtar\n  VisibilityTimeout: {1},      // processing 45s leti hai → isse BADA rakho\n});\n\nfor (const msg of res.Messages ?? []) {\n  await processOrder(msg);     // idempotent hona chahiye!\n  await sqs.{2}({          // ← yahi \"ack\" hai\n    QueueUrl: url,\n    ReceiptHandle: msg.ReceiptHandle,\n  });\n}\n\n// Redrive policy (queue par): maxReceiveCount: 5 → {3} mein bhej do",
      blanks: [
        { answer: '20', explain: 'Long polling ka max 20 second. Isse khaali polls kam hote hain (cost) aur message aate hi milta hai (latency).' },
        { answer: '60', explain: 'Processing 45s leti hai — visibility timeout usse bada (60s) rakho, warna message wapas visible ho jaayega aur duplicate processing hogi.' },
        { answer: 'deleteMessage', explain: 'Yahi SQS ka "ack" hai. Delete na karo toh timeout ke baad message wapas aa jaayega.' },
        { answer: 'DLQ', explain: '5 baar receive hone ke baad message apne aap dead-letter queue mein chala jaata hai — SQS ka built-in DLQ.' },
      ],
      tokens: ['20', '60', 'deleteMessage', 'DLQ', '0', '10', 'ackMessage', 'archive'],
    },
    {
      type: 'bins',
      title: '🗂️ SQS ya Kafka?',
      instructions: 'Har requirement ke liye sahi choice. Sochо: replay chahiye? throughput kitna? ops capacity?',
      bins: [
        { id: 'sqs', label: '☁️ SQS behtar' },
        { id: 'kafka', label: '🟣 Kafka behtar' },
      ],
      items: [
        { text: 'AWS mein background jobs, chhoti ops team', bin: 'sqs',
          explain: 'Zero ops, managed, built-in DLQ. Cluster maintain karne ki zaroorat nahi — chhoti team ke liye ideal.' },
        { text: 'Naya consumer ko pichle 7 din ka data chahiye', bin: 'kafka',
          explain: 'Replay chahiye — SQS mein delete ke baad message gaya. Yeh log model ki signature capability hai.' },
        { text: '10 lakh events/sec analytics pipeline', bin: 'kafka',
          explain: 'Bahut high throughput + per-message cost. SQS itne volume par mahenga aur Kafka isi ke liye bana hai.' },
        { text: 'Ek order ko process karna, 20 min ki job', bin: 'sqs',
          explain: 'Visibility timeout 12 ghante tak ja sakta hai — lambi jobs ke liye theek. Kafka mein 20-min processing max.poll.interval todegi.' },
        { text: 'Event sourcing — state ko events se rebuild karna', bin: 'kafka',
          explain: 'Poora itihaas replay karna — sirf log model mein mumkin.' },
        { text: 'Simple decoupling, din mein kuch hazaar messages', bin: 'sqs',
          explain: 'Kam volume + simple use-case = SQS ki simplicity jeetti hai. Kafka cluster is scale par over-engineering hai.' },
        { text: 'Stream processing (aggregations, joins, windows)', bin: 'kafka',
          explain: 'Kafka Streams/Flink ka ecosystem. SQS mein stream processing ka concept hi nahi.' },
        { text: 'Per-message retry aur DLQ bina extra code ke', bin: 'sqs',
          explain: 'SQS mein visibility timeout + maxReceiveCount se DLQ built-in hai. Kafka mein yeh pattern khud banana padta hai.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "SQS mein visibility timeout kya hai aur uska classic bug kya hai?"',
        choices: [
          'Woh message ka TTL hai',
          'Visibility timeout ek "lease" hai — jab consumer message receive karta hai toh woh delete nahi hota, bas utni der ke liye INVISIBLE ho jaata hai. Consumer ko us waqt ke andar DeleteMessage karna padta hai, warna message wapas visible ho jaata hai aur koi aur consumer use uthata hai. Classic bug: visibility timeout < processing time — tab pehla worker abhi kaam kar raha hota hai aur doosra bhi wahi message uthata hai → duplicate processing (jaise order do baar ship). Fix: timeout ko p99 processing time se bada rakho ya ChangeMessageVisibility se heartbeat karo, aur processing idempotent banao.',
          'Woh queue ka retention hai',
          'Woh polling ka interval hai',
        ],
        answer: 1,
        explain: 'Yeh SQS ka signature interview sawal hai. Achha jawab lease-model samjhata hai, classic bug (timeout < processing) batata hai, aur dono fixes + idempotency deta hai.',
      },
      {
        q: '🧑‍💼 "SQS mein ek message ko kai independent consumers ko kaise bhejoge?"',
        choices: [
          'Ek hi queue par kai consumers laga do',
          'SQS akela fanout nahi karta — ek queue par kai consumers lagane se woh messages BAANT lenge (har message ek hi consumer ko). Fanout ke liye standard AWS pattern hai SNS → multiple SQS queues: SNS topic par publish karo, har subscriber ki apni SQS queue subscribe kare, toh har queue ko poori copy milti hai. Yeh Kafka ke multiple consumer groups jaisa hai — par phir bhi replay nahi milta, kyunki har queue mein message ack ke baad delete ho jaata hai.',
          'FIFO queue use karo',
          'SQS mein fanout possible nahi',
        ],
        answer: 1,
        explain: 'SNS→SQS fanout AWS ka standard pattern hai. Achha jawab yeh bhi batata hai ki yeh Kafka groups jaisa DIKHTA hai par replay nahi deta — yeh gehri samajh dikhata hai.',
      },
    ],
  },
  success: [
    'Main visibility timeout ka mechanism aur uska classic bug samjha sakta hoon',
    'Main Standard aur FIFO queues ka trade-off bata sakta hoon',
    'Main long polling ka cost + latency fayda samjha sakta hoon',
    'Main SQS ke built-in DLQ (maxReceiveCount) ko samjha sakta hoon',
  ],
  homework: `<p>SQS ka visibility timeout khud dekho (localstack ya AWS free tier):</p>
    <pre>// long job ke liye heartbeat pattern
const { Messages } = await sqs.receiveMessage({
  QueueUrl: url,
  WaitTimeSeconds: 20,          // long polling
  VisibilityTimeout: 60,
});

for (const msg of Messages ?? []) {
  // lambi processing — beech mein timeout extend karte raho
  const heartbeat = setInterval(async () => {
    await sqs.changeMessageVisibility({
      QueueUrl: url,
      ReceiptHandle: msg.ReceiptHandle,
      VisibilityTimeout: 60,     // aur 60s do
    });
  }, 30_000);

  try {
    await longRunningJob(msg);   // 5 minute lag sakti hai
    await sqs.deleteMessage({ QueueUrl: url, ReceiptHandle: msg.ReceiptHandle });
  } finally {
    clearInterval(heartbeat);
  }
}</pre>
    <p><b>Asli experiment:</b> ek queue banao (visibility timeout 10s) aur ek consumer
    jo 30 second sleep kare bina delete kiye. Do consumers chalao — dekho ki dono ko
    WAHI message milega (duplicate). Phir heartbeat pattern lagao — ab sirf ek ko milega.
    Yahi woh double-shipping bug hai aur uska fix.</p>`,
  cards: [
    { front: 'SQS visibility timeout kya hai?', back: 'Ek <b>lease</b> — receive par message delete nahi hota, bas utni der <b>invisible</b> ho jaata hai. Consumer ko us waqt ke andar <code>DeleteMessage</code> karna padta hai, warna woh wapas visible ho jaata hai aur koi aur uthata hai.' },
    { front: 'SQS ka classic duplicate bug?', back: '<b>Visibility timeout &lt; processing time</b>. 30s timeout par 45s ki processing = message 30s par wapas visible → doosra worker bhi uthata hai → double processing. Fix: timeout > p99 processing, ya <code>ChangeMessageVisibility</code> heartbeat.' },
    { front: 'SQS Standard vs FIFO?', back: '<b>Standard</b>: at-least-once, best-effort ordering, unlimited throughput.<br><b>FIFO</b>: exactly-once (5-min dedup), strict ordering per <b>MessageGroupId</b> (Kafka partition-key jaisa), par ~300 msg/s (batching se 3,000). Ordering ki keemat throughput.' },
    { front: 'Long polling ka fayda?', back: '<code>WaitTimeSeconds=20</code> — message aane tak wait. Isse <b>kam API calls</b> (cost) AUR <b>kam latency</b> (message aate hi milta hai) — dono behtar. Lagbhag hamesha ON hona chahiye.' },
    { front: 'SQS mein fanout kaise?', back: 'SQS akela fanout nahi karta (ek queue par kai consumers messages <b>baant</b> lenge). Pattern: <b>SNS → multiple SQS queues</b> — har subscriber ki apni queue, sabko poori copy. Par replay phir bhi nahi.' },
  ],
},

]);

/* ===================== Levels 4-6: MQTT, Fanout, Saga ===================== */
KQ.registerLevels('sysdesign', [

/* =====================================================================
   4. MQTT & IoT
   ===================================================================== */
{
  id: 'mqtt-iot',
  icon: '📡',
  title: 'MQTT & IoT — QoS 1 ne har reading 4 baar bhej di',
  badge: 'IoT Acharya',
  objectives: [
    'MQTT ka QoS 0/1/2 ka asli matlab samjho (aur unki keemat)',
    'Retained messages aur Last Will (LWT) ka use-case jaano',
    'Samjho ki MQTT kyun banaya gaya (constrained devices, kharab network)',
    'MQTT → Kafka bridge pattern seekho (IoT ingest ka standard)',
  ],
  lesson: [
    `<h3>😰 Dard: "50,000 testers ne broker ko 4× traffic se maar diya"</h3>
     <p>Ek factory floor par testers (device testing machines) apne session events
     MQTT par bhejte the — <code>kpm/web/session</code> topic par. Network flaky tha
     (industrial WiFi), toh team ne "safety ke liye" <b>QoS 2</b> laga diya —
     "exactly once, sabse safe".</p>
     <pre>MQTT QoS 2 ka handshake (4 messages har publish ke liye):
   client → PUBLISH     → broker
   client ← PUBREC      ← broker
   client → PUBREL      → broker
   client ← PUBCOMP     ← broker

50,000 testers × har 5 second ek reading × 4 packets
= 40,000 packets/sec sirf HANDSHAKE ke liye

Aur flaky network par har dropped packet = poora handshake retry
→ broker CPU 100%, latency 8 second, kuch testers disconnect
→ jo "safety" ke liye kiya tha usi ne system gira diya</pre>
     <div class="war-story"><b>⚡ QoS 2 lagbhag kabhi zaroori nahi hota.</b> Yeh telemetry
     thi — ek sensor reading kho jaaye toh 5 second baad agli aa jaayegi. QoS 0 kaafi tha.
     Team QoS 1 par shift hui (2 packets) aur idempotent processing lagayi — traffic aadha
     ho gaya aur reliability wahi rahi. <b>Constrained environments mein "sabse safe option"
     chunna hi sabse bada risk ban jaata hai.</b></div>`,

    `<h3>🪜 MQTT ka QoS — teen levels, teen keematein</h3>
     <p>MQTT chhote devices aur kharab networks ke liye bana hai (header sirf 2 bytes!).
     Uska QoS Kafka ke acks se milta-julta hai par packet-level par:</p>
     <pre>📮 QoS 0 — "at most once" (fire and forget)
   1 packet: PUBLISH
   💪 sabse tez, sabse kam bandwidth/battery
   😖 network gira toh message gaya (koi retry nahi)
   👉 telemetry, sensor readings (agli 5 sec mein aa jaayegi)

📬 QoS 1 — "at least once"
   2 packets: PUBLISH → PUBACK
   💪 achha balance — message pahunchega
   😖 DUPLICATE aa sakte hain (PUBACK kho gaya toh retry)
   👉 DEFAULT choice — commands, important events
      (+ idempotent processing lagao)

🎯 QoS 2 — "exactly once"
   4 packets: PUBLISH → PUBREC → PUBREL → PUBCOMP
   💪 na loss, na duplicate
   😖 4× traffic, 4× latency, broker par state rakhna padta hai
   👉 sirf tab jab duplicate SACH MEIN nuksan kare
      (jaise "gate kholo" command) — aur woh bhi kam hi</pre>
     <p><b>Practical rule:</b> QoS 1 + idempotent processing lagbhag hamesha QoS 2 se
     behtar hai — sasta, tez, aur wahi asar. (Yeh bilkul wahi sabak hai jo Track 1 ke
     delivery semantics mein tha — "at-least-once + idempotency = effectively exactly once".)</p>`,

    `<h3>⚖️ Retained, LWT, aur MQTT → Kafka bridge</h3>
     <p><b>MQTT ke do khaas features jo aur kahin nahi milte:</b></p>
     <pre>📌 RETAINED MESSAGE
   broker har topic ka AAKHRI retained message rakhta hai
   naya subscriber connect kare → use turant woh mil jaata hai
   → "device ka current status" ke liye perfect
   → (yeh Kafka ke log compaction jaisa idea hai — har topic ka latest)

🪦 LAST WILL & TESTAMENT (LWT)
   client connect karte waqt bata deta hai:
   "agar main GANDE tareeqe se disconnect hoon (crash/network),
    toh mere behalf par yeh message publish kar dena"
   → device offline hone par apne aap "status: offline" publish
   → yeh IoT mein device-health ka standard tareeqa hai
   → (Kafka mein iska koi equivalent nahi — yeh MQTT-specific hai)</pre>
     <p><b>Nayi musibat — MQTT storage nahi hai:</b> MQTT ek <b>transport protocol</b> hai,
     storage system nahi. Zyadatar brokers messages ko retain nahi karte (retained flag ke
     alawa), replay nahi hota, aur analytics ka koi concept nahi. Toh production IoT
     architecture aksar dono use karta hai:</p>
     <pre>📡 Devices → MQTT broker → [BRIDGE] → 🟣 Kafka → analytics/storage

MQTT ka kaam: lakhs devices ko connect karna, kharab network jhelna,
              chhote packets, battery bachana
Kafka ka kaam: durable storage, replay, stream processing, fanout

Yahi IoT ingest ka standard pattern hai — MQTT edge par,
Kafka backbone par. Dono ko competitors samajhna galti hai.</pre>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Kaunsa QoS chahiye?',
      instructions: 'Har IoT use-case ke liye sahi QoS. Sochо: message kho jaaye toh kya hoga? duplicate aaye toh kya hoga?',
      bins: [
        { id: 'qos0', label: '📮 QoS 0 (kho jaaye toh chalega)' },
        { id: 'qos1', label: '📬 QoS 1 + idempotency (default)' },
        { id: 'qos2', label: '🎯 QoS 2 (duplicate NUKSAN kare)' },
      ],
      items: [
        { text: 'Temperature sensor har 5 second reading bhejta hai', bin: 'qos0',
          explain: 'Ek reading kho jaaye toh 5 second mein agli aa jaayegi. QoS 0 sabse sasta aur bilkul kaafi. QoS 2 yahan 4× traffic ka bekaar kharcha hai.' },
        { text: 'Device ka "firmware update complete" event', bin: 'qos1',
          explain: 'Yeh kho nahi sakta (server ko pata hona chahiye) par duplicate harmless hai (idempotent update status). QoS 1 + idempotency perfect.' },
        { text: '"Safety valve KHOLO" command', bin: 'qos2',
          explain: 'Duplicate command sach mein khatarnaak ho sakta hai (valve do baar khulna). Yeh un kam cases mein se hai jahan QoS 2 justify hota hai. (Behtar: command ko idempotent banao — "state = open" bhejo, "toggle" nahi.)' },
        { text: 'GPS location ping har 10 second', bin: 'qos0',
          explain: 'Purana location bekaar hai — bas latest chahiye. Kho jaaye toh agla aa jaayega. QoS 0.' },
        { text: 'Tester ka session-complete event (billing se juda)', bin: 'qos1',
          explain: 'Kho nahi sakta (billing), par duplicate ko sessionId se dedup kar sakte ho. QoS 1 + idempotent processing.' },
        { text: 'Machine ka error/alarm event', bin: 'qos1',
          explain: 'Alarm kho nahi sakta. Duplicate alarm annoying hai par khatarnaak nahi (dedup kar sakte ho). QoS 1.' },
        { text: 'Battery voltage telemetry (har minute)', bin: 'qos0',
          explain: 'Telemetry — ek data point khona chart mein chhota gap. Battery-powered device par QoS 0 battery bhi bachata hai.' },
        { text: '"Payment terminal ko ₹500 charge karo" command', bin: 'qos2',
          explain: 'Duplicate = double charge = asli paisa ka nuksan. QoS 2 (ya behtar: QoS 1 + idempotency key, jo aksar zyada practical hai).' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 MQTT ke sawal',
      questions: [
        {
          q: 'QoS 2 lagbhag kabhi kyun use nahi karna chahiye?',
          choices: [
            'Woh kaam nahi karta',
            'Woh 4-packet handshake maangta hai (PUBLISH→PUBREC→PUBREL→PUBCOMP) — yaani 4× traffic, 4× latency, aur broker par per-message state. Constrained devices aur kharab network par yeh bahut mehnga hai (battery + bandwidth + broker CPU). Zyadatar cases mein QoS 1 + idempotent processing wahi asar deta hai bahut kam keemat par — bilkul "at-least-once + idempotency" wala hi sabak.',
            'Woh purana hai',
            'Woh sirf Kafka mein hai',
          ],
          answer: 1,
          explain: 'Yeh MQTT ka signature trade-off hai. "Sabse safe option" chunna constrained environment mein hi system gira deta hai. QoS 1 + idempotency lagbhag hamesha sahi jawab hai.',
        },
        {
          q: 'Retained message kya hai aur kis kaam aata hai?',
          choices: [
            'Message jo delete nahi hota',
            'Broker har topic ka AAKHRI retained message rakhta hai, aur jab koi naya subscriber connect kare toh use turant woh mil jaata hai (agle message ka intezaar nahi). Use-case: "device ka current status" — naya dashboard connect kare toh use turant pata chal jaaye ki device online hai ya offline. Yeh Kafka ke log compaction jaisa idea hai (har key/topic ka latest value).',
            'Message jo encrypt hota hai',
            'Message jo priority mein hai',
          ],
          answer: 1,
          explain: 'Retained messages IoT ka "current state" problem solve karte hain. Iske bina naye subscriber ko device ka status jaanne ke liye agle update ka intezaar karna padta (jo ghanton baad ho sakta hai).',
        },
        {
          q: 'Last Will & Testament (LWT) kya karta hai?',
          choices: [
            'Message ko delete karta hai',
            'Client connect karte waqt broker ko ek message deta hai: "agar main GANDE tareeqe se disconnect hoon (crash/network fail, clean disconnect nahi), toh mere behalf par yeh publish kar dena". Isse device ke offline hone par apne aap "status: offline" publish ho jaata hai — yeh IoT mein device-health tracking ka standard tareeqa hai. Kafka mein iska koi equivalent nahi.',
            'Messages ko backup karta hai',
            'QoS badal deta hai',
          ],
          answer: 1,
          explain: 'LWT MQTT ka bahut elegant feature hai — device khud nahi bata sakta ki woh mar gaya, toh broker uske behalf par bata deta hai. Yeh unreliable networks ke liye specifically design kiya gaya.',
        },
      ],
    },
    {
      type: 'match',
      title: '🔗 MQTT feature aur uska kaam',
      instructions: 'Har MQTT capability ko uske use-case se jodo.',
      pairs: [
        { left: 'Naya subscriber ko device ka current status turant chahiye', right: '📌 Retained message',
          explain: 'Broker har topic ka aakhri retained message rakhta hai — connect karte hi mil jaata hai.' },
        { left: 'Device crash ho jaaye toh apne aap "offline" publish ho', right: '🪦 Last Will (LWT)',
          explain: 'Client pehle se broker ko bata deta hai ki gande disconnect par kya publish karna hai.' },
        { left: 'Battery-powered sensor, har reading matter nahi karti', right: '📮 QoS 0',
          explain: '1 packet — sabse kam battery/bandwidth. Kho jaaye toh agli reading aa jaayegi.' },
        { left: 'Important event, duplicate handle kar sakte hain', right: '📬 QoS 1 + idempotency',
          explain: '2 packets, message pahunchega. Duplicate ko dedup se harmless banao. Default choice.' },
        { left: 'Lakhs devices ka data durable storage + analytics', right: '🌉 MQTT → Kafka bridge',
          explain: 'MQTT transport hai, storage nahi. Bridge se Kafka mein daalo — wahan replay, retention, stream processing.' },
      ],
    },
    {
      type: 'order',
      title: '🔢 IoT ingest architecture ka flow',
      instructions: 'Lakhs devices se analytics tak ka sahi kram lagao.',
      steps: [
        { text: '📡 Devices MQTT broker se connect (QoS 1, LWT set, chhote packets)',
          explain: 'MQTT edge ke liye — 2-byte header, weak network jhelta hai, lakhs connections sambhaalta hai. LWT se device offline hone par pata chal jaata hai.' },
        { text: '🏢 MQTT broker messages receive karta hai (retained = current status)',
          explain: 'Broker connection management aur QoS handshakes sambhaalta hai. Retained messages se naye dashboards ko turant current state milta hai.' },
        { text: '🌉 Bridge MQTT topics ko Kafka topics par forward karta hai',
          explain: 'Yahi woh jagah hai jahan transport se storage mein transition hota hai. Bridge MQTT subscribe karta hai aur Kafka par produce karta hai.' },
        { text: '📜 Kafka mein durable log — retention, partitions, replication',
          explain: 'Ab data durable hai, replay ho sakta hai, aur multiple consumers independently padh sakte hain. MQTT yeh nahi de sakta.' },
        { text: '🌊 Stream processing — aggregations, windows, anomaly detection',
          explain: 'Kafka Streams/Flink se real-time analytics. Event-time windowing (Track 3) yahan bahut kaam aata hai kyunki IoT events der se aate hain.' },
        { text: '💾 Long-term storage + dashboards + alerts',
          explain: 'Aggregated data warehouse/timeseries DB mein, aur alerts operations team ko. Poora pipeline: edge → transport → backbone → processing → storage.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "MQTT ke QoS levels kya hain? Tum kaunsa chunoge?"',
        choices: [
          'QoS 2, woh sabse safe hai',
          'QoS 0 (at-most-once, 1 packet — fire and forget), QoS 1 (at-least-once, 2 packets — PUBLISH+PUBACK, duplicates possible), QoS 2 (exactly-once, 4-packet handshake — PUBLISH/PUBREC/PUBREL/PUBCOMP). Main lagbhag hamesha QoS 1 + idempotent processing chunuunga — QoS 2 ka 4× traffic aur broker state constrained devices/weak networks par bahut mehnga hai aur wahi asar idempotency se sasta mil jaata hai. Pure telemetry (sensor readings jo har 5 sec aati hain) ke liye QoS 0 kaafi hai — battery aur bandwidth dono bachte hain.',
          'QoS 0 hamesha, woh tez hai',
          'Sab same hain',
        ],
        answer: 1,
        explain: 'Yeh IoT ka signature sawal hai. Achha jawab teeno define karta hai, unki PACKET cost batata hai, aur "QoS 1 + idempotency" ko default recommend karta hai — yeh dikhata hai ki tumne constrained environments mein sach mein kaam kiya hai.',
      },
      {
        q: '🧑‍💼 "Lakhs IoT devices ka data ingest karna hai aur analytics bhi chahiye. Architecture?"',
        choices: [
          'Sab devices ko seedha Kafka se connect karo',
          'Do layers: MQTT edge par aur Kafka backbone par, ek bridge se jude. MQTT lakhs constrained devices ko sambhaalne ke liye bana hai — 2-byte header, weak network tolerance, QoS levels, LWT se device-health, retained messages se current status. Par MQTT transport hai, storage nahi — replay/retention/stream-processing nahi. Isliye bridge se data Kafka mein le jao, jahan durable log, replay, partitions aur stream processing milti hai. Devices ko seedha Kafka se connect karna kaam nahi karega — Kafka client heavy hai aur lakhs direct connections uske liye design nahi hain.',
          'Sab MQTT par hi rakho',
          'HTTP APIs use karo',
        ],
        answer: 1,
        explain: 'MQTT→Kafka bridge IoT ingest ka standard pattern hai. Achha jawab dono ke ROLE batata hai (transport vs storage) aur yeh bhi ki devices ko seedha Kafka se kyun nahi jodte — Kafka client constrained devices ke liye bhaari hai.',
      },
    ],
  },
  success: [
    'Main MQTT ke teen QoS levels aur unki packet-cost bata sakta hoon',
    'Main jaanta hoon ki QoS 2 lagbhag kabhi kyun nahi chahiye',
    'Main retained messages aur LWT ke use-cases samjha sakta hoon',
    'Main MQTT → Kafka bridge architecture design kar sakta hoon',
  ],
  homework: `<p>MQTT khud chala ke dekho (mqtt.js + koi local broker, jaise aedes/mosquitto):</p>
    <pre>const mqtt = require('mqtt');

const client = mqtt.connect('mqtt://localhost:1883', {
  clientId: 'tester-42',
  will: {                                  // ← LWT
    topic: 'factory/testers/42/status',
    payload: JSON.stringify({ status: 'offline' }),
    qos: 1,
    retain: true,
  },
});

client.on('connect', () => {
  // current status retained — naye subscribers ko turant milega
  client.publish('factory/testers/42/status',
    JSON.stringify({ status: 'online' }), { qos: 1, retain: true });

  // telemetry — QoS 0 kaafi hai
  setInterval(() => {
    client.publish('factory/testers/42/temp',
      JSON.stringify({ c: 42.5, t: Date.now() }), { qos: 0 });
  }, 5000);
});</pre>
    <p><b>Asli experiment:</b> client ko <b>Ctrl+C se nahi</b>, balki process ko
    <code>kill -9</code> se maaro (gande disconnect ki nakal). Ek doosra subscriber
    <code>factory/testers/+/status</code> par lagao — dekho ki LWT ne apne aap
    "offline" publish kar diya. Phir naya subscriber connect karo — use retained
    message turant milega bina agle publish ka intezaar kiye.</p>`,
  cards: [
    { front: 'MQTT ke teen QoS levels?', back: '<b>QoS 0</b>: at-most-once, 1 packet (fire-forget).<br><b>QoS 1</b>: at-least-once, 2 packets (PUBLISH+PUBACK), duplicates possible.<br><b>QoS 2</b>: exactly-once, <b>4-packet</b> handshake (PUBLISH/PUBREC/PUBREL/PUBCOMP).' },
    { front: 'QoS 2 lagbhag kyun avoid karte hain?', back: '4× traffic, 4× latency, broker par per-message state — constrained devices aur weak networks par bahut mehnga. <b>QoS 1 + idempotent processing</b> wahi asar deta hai bahut sasta (wahi "at-least-once + idempotency" sabak).' },
    { front: 'Retained message kya hai?', back: 'Broker har topic ka <b>aakhri</b> retained message rakhta hai — naya subscriber connect karte hi use turant mil jaata hai (agle publish ka intezaar nahi). "Device ka current status" ke liye perfect. Kafka ke log compaction jaisa idea.' },
    { front: 'Last Will & Testament (LWT)?', back: 'Client connect par broker ko bata deta hai: "gande disconnect (crash/network) par mere behalf par yeh publish karna". Device offline hone par apne aap "status: offline" — IoT device-health ka standard tareeqa. Kafka mein equivalent nahi.' },
    { front: 'IoT ingest ka standard architecture?', back: '<code>Devices → MQTT broker → bridge → Kafka → stream processing → storage</code>.<br><b>MQTT</b> = edge transport (lakhs devices, weak network, chhote packets).<br><b>Kafka</b> = backbone (durable log, replay, partitions, processing). MQTT storage nahi hai.' },
  ],
},

/* =====================================================================
   5. FANOUT & NOTIFICATION SYSTEMS
   ===================================================================== */
{
  id: 'fanout',
  icon: '📢',
  title: 'Fanout — ek celebrity tweet ne 5 crore writes trigger kiye',
  badge: 'Fanout Shilpi',
  objectives: [
    'Fanout-on-write aur fanout-on-read ka trade-off samjho',
    'Celebrity problem aur hybrid approach seekho',
    'Notification system ka dedup aur rate-limiting jaano',
    'Jaano ki fanout ka faisla read/write ratio se hota hai',
  ],
  lesson: [
    `<h3>😰 Dard: "Ek celebrity ke post ne poora feed system gira diya"</h3>
     <p>Ek social app ne "fanout-on-write" chuna — jab koi post kare, uske saare
     followers ki feed mein woh post <b>likh do</b> (pre-computed feeds, taaki read
     bahut tez ho).</p>
     <pre>Normal user: 200 followers
   1 post → 200 feed writes → ~20ms → bilkul theek ✅

Celebrity: 5 CRORE followers
   1 post → 5,00,00,000 feed writes 💥
   → ek post ne feed-writer service ko ghanton block kiya
   → us dauran BAAKI sabke posts bhi late (queue bhar gayi)
   → ek banda tweet kare aur poora platform slow ho jaaye</pre>
     <div class="war-story"><b>⚡ Isi ko "celebrity problem" (ya hot-key problem) kehte
     hain.</b> Yeh wahi hot-partition problem hai jo tumne Track 1 mein dekhi thi, par
     application level par. Aur yeh sirf social media ka masla nahi — koi bhi system
     jahan ek event ke kai receivers hon (notifications, webhooks, cache invalidation)
     yahi shape rakhta hai. Fix ek "hybrid" approach hai, jo Twitter ne bhi apnaya.</div>`,

    `<h3>🪜 Do fanout strategies</h3>
     <pre>✍️ FANOUT-ON-WRITE (push / pre-compute)
   post karte waqt SAARE followers ki feed mein likh do
   ✅ READ bahut tez (feed pehle se bani hai — bas padho)
   ❌ WRITE mehngi (N followers = N writes)
   ❌ celebrity problem: 5 crore writes ek post par
   👉 achha jab: followers kam, reads bahut zyada (read-heavy)

👁️ FANOUT-ON-READ (pull / on-demand)
   post sirf ek jagah likho. Jab user feed kholе, tab uske
   saare followees ke posts fetch karke MERGE karo
   ✅ WRITE sasti (1 write)
   ❌ READ mehngi (har feed open par N queries + merge)
   👉 achha jab: writes bahut zyada, ya followers bahut zyada

🔀 HYBRID (asli systems yahi karte hain)
   normal users → fanout-on-WRITE (unke followers kam hain)
   celebrities  → fanout-on-READ (unke posts read time par merge)

   User ki feed = (pre-computed feed)  ∪  (celebrities ke recent posts)
   → normal case tez, celebrity case bhi system nahi girata</pre>
     <p>Yahi Twitter ka asli approach hai — unhone dono ko mila diya, kyunki koi ek
     bhi akela kaam nahi karta us scale par.</p>`,

    `<h3>⚖️ Notification system ki nayi musibat</h3>
     <p>Fanout sirf feeds ke liye nahi — notification systems mein bhi wahi shape hai,
     aur wahan do aur problems aati hain:</p>
     <pre>🔁 DEDUP: ek user ko ek hi cheez ki kai notifications
   "Aapke post par A ne like kiya"
   "Aapke post par B ne like kiya"  … × 500
   → user ko 500 notifications? Nahi —
   → AGGREGATE karo: "A aur 499 aur logon ne like kiya"
   → time window mein group karo (jaise 5 min ka batch)

🚦 RATE LIMITING: ek user ko kitni notifications per hour
   → warna app uninstall ho jaayegi
   → per-user rate limit + priority (urgent vs normal)

📊 Aur fanout ka decision READ/WRITE RATIO se hota hai:
   reads >> writes (jaise feed) → fanout-on-write (pre-compute)
   writes >> reads (jaise audit log) → fanout-on-read
   dono heavy + skew (celebrities) → HYBRID</pre>
     <p><b>Kafka is architecture mein kahan hai?</b> Fanout ka <b>trigger</b> aksar
     ek Kafka event hota hai (<code>post-created</code>), aur fanout workers consumer
     group hote hain. Kafka yahan do cheezein deta hai: (1) fanout ka kaam async ho
     jaata hai (post karna turant, feed writes background mein), aur (2) celebrity spike
     ko queue <b>rok ke rakhti hai</b> (load leveling) taaki downstream apni raftaar
     se kaam kare. Par woh celebrity problem ko <b>hal nahi karta</b> — uske liye
     hybrid strategy chahiye. <b>Queue load ko smooth karti hai, kaam ko kam nahi karti.</b></p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Fanout-on-write ya fanout-on-read?',
      instructions: 'Har case ke liye sahi strategy. Sochо: read/write ratio kya hai, aur kya skew (celebrity) hai?',
      bins: [
        { id: 'write', label: '✍️ Fanout-on-write (pre-compute)' },
        { id: 'read', label: '👁️ Fanout-on-read (on-demand merge)' },
        { id: 'hybrid', label: '🔀 Hybrid (dono)' },
      ],
      items: [
        { text: 'Normal user (200 followers) ka post', bin: 'write',
          explain: '200 writes sasti hain, aur feed reads bahut zyada honge. Pre-compute karke read ko turant banao.' },
        { text: 'Celebrity (5 crore followers) ka post', bin: 'read',
          explain: '5 crore writes system gira degi. Uske posts read-time par merge karo — sirf 1 write.' },
        { text: 'Twitter/Instagram jaisa poora feed system', bin: 'hybrid',
          explain: 'Real systems dono use karte hain — normal users write-fanout, celebrities read-fanout. Koi ek akela us scale par kaam nahi karta.' },
        { text: 'Team chat channel (50 members)', bin: 'write',
          explain: 'Chhota fanout aur bahut reads. Har member ke liye pre-compute karna sasta aur read turant.' },
        { text: 'Audit log jo mahine mein ek baar padha jaata hai', bin: 'read',
          explain: 'Writes bahut, reads na ke barabar. Pre-compute karna barbaadi hai — read time par query kar lo.' },
        { text: 'Ek user jiske 50 lakh followers hain (mid-tier influencer)', bin: 'hybrid',
          explain: 'Threshold ke aas paas — isiliye hybrid systems ek cutoff rakhte hain (jaise 10,000 followers) aur uske upar read-fanout par shift kar dete hain.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Fanout ke sawal',
      questions: [
        {
          q: 'Celebrity problem kya hai aur kaise hal karte hain?',
          choices: [
            'Celebrities ko block kar do',
            'Fanout-on-write mein ek post ke saare followers ki feed mein likhna padta hai — 5 crore followers waale celebrity ka ek post 5 crore writes trigger karta hai, jo system ko ghanton block kar deta hai (aur baaki sabke posts bhi late ho jaate hain). Hal: HYBRID — normal users ke liye fanout-on-write (unke followers kam), celebrities ke liye fanout-on-read (unke posts user ke feed padhte waqt merge karo). Ek follower-count threshold se decide karo.',
            'Zyada servers lagao',
            'Posts ko rate limit karo',
          ],
          answer: 1,
          explain: 'Celebrity problem = application-level hot key. Hybrid approach woh standard hal hai jo Twitter aur baaki bade platforms use karte hain. Ek strategy akela us scale par kaam nahi karti.',
        },
        {
          q: 'Fanout-on-write vs fanout-on-read ka faisla kis cheez se hota hai?',
          choices: [
            'Database ke type se',
            'READ/WRITE RATIO se (aur skew se). Reads >> writes (jaise social feed jo baar-baar khulti hai) → fanout-on-write, kyunki ek baar pre-compute karke har read turant. Writes >> reads (jaise audit log) → fanout-on-read, kyunki pre-compute barbaadi hai. Aur agar bahut skew hai (celebrities) → hybrid.',
            'Programming language se',
            'Kafka partitions se',
          ],
          answer: 1,
          explain: 'Read/write ratio yeh classic caching-style trade-off decide karta hai: kaam ab karo (write time) ya baad mein (read time). Skew (celebrity) teesra factor hai jo hybrid ki taraf le jaata hai.',
        },
        {
          q: 'Kafka celebrity problem ko hal kar deta hai?',
          choices: [
            'Haan, partitions se',
            'Nahi. Kafka fanout ke kaam ko ASYNC bana deta hai (post karna turant, feed writes background mein) aur spike ko rok ke rakhta hai (load leveling) — par 5 crore feed writes phir bhi 5 crore hi hain. Queue load ko SMOOTH karti hai, kaam ko KAM nahi karti. Celebrity problem ke liye hybrid fanout strategy chahiye.',
            'Haan, consumer groups se',
            'Haan agar zyada consumers hon',
          ],
          answer: 1,
          explain: 'Yeh bahut zaroori distinction hai. Log sochte hain "Kafka laga do, scale ho jaayega" — par Kafka work ko distribute/smooth karta hai, use eliminate nahi karta. Algorithm (hybrid fanout) hi asli fix hai.',
        },
      ],
    },
    {
      type: 'order',
      title: '🔢 Notification pipeline ka sahi kram',
      instructions: 'Ek achhe notification system ka flow lagao — dedup aur rate-limit samet.',
      steps: [
        { text: '📤 Event Kafka par publish (post-liked, comment-added)',
          explain: 'Trigger event. Yeh turant hota hai — user ko wait nahi karna padta (Track 1: sync vs async).' },
        { text: '👥 Fanout worker: kis-kis ko notify karna hai woh nikaalo (hybrid strategy)',
          explain: 'Consumer group. Normal users ke liye pre-compute, celebrities ke liye read-time merge. Yahin celebrity problem handle hoti hai.' },
        { text: '🔁 DEDUP + AGGREGATE: ek user ke liye similar notifications ko group karo',
          explain: '"A ne like kiya" × 500 → "A aur 499 aur logon ne like kiya". Time window (jaise 5 min) mein aggregate karo, warna user ko 500 pings jaayenge.' },
        { text: '🚦 RATE LIMIT: per-user notification budget check karo',
          explain: 'Ek user ko ghante mein kitni notifications — warna app uninstall. Priority bhi dekho (urgent vs normal).' },
        { text: '📱 Delivery: push/email/SMS bhejo (idempotency key ke saath)',
          explain: 'Actual delivery. At-least-once hai (Track 1/2) toh idempotency key se duplicate delivery rok do — user ko do baar same push nahi jaana chahiye.' },
        { text: '💀 Fail hue notifications → retry with jitter → DLQ',
          explain: 'Push service down? Jitter-wala backoff (Track 2), phir DLQ. Ek fail hua notification baaki ko block na kare.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Twitter jaisa feed system design karo. Fanout kaise handle karoge?"',
        choices: [
          'Har post par saare followers ko likh dunga',
          'Hybrid fanout. Normal users (kam followers) ke liye fanout-on-WRITE — post karte waqt unke followers ki feed mein pre-compute kar do, taaki feed reads turant hon (aur feed bahut baar padhi jaati hai). Celebrities (lakhs/crore followers) ke liye fanout-on-READ — unka post ek jagah likho aur user ke feed kholte waqt merge karo, warna ek post 5 crore writes trigger karke poora system block kar dega (celebrity problem). Threshold (jaise 10,000 followers) se decide karo. Fanout ka kaam Kafka consumer groups par async chalega — par dhyan rahe ki Kafka load SMOOTH karta hai, kaam KAM nahi karta; celebrity problem ka asli hal yeh hybrid algorithm hai.',
          'Fanout-on-read hamesha, woh simple hai',
          'Cache laga dunga',
        ],
        answer: 1,
        explain: 'Yeh classic system-design sawal hai. Achha jawab dono strategies, unka read/write trade-off, celebrity problem, threshold-based hybrid, AUR yeh clarity ki Kafka work ko eliminate nahi karta — sab batata hai.',
      },
      {
        q: '🧑‍💼 "Notification system mein user ko 500 pings ja rahe hain. Kya karoge?"',
        choices: [
          'Notifications band kar do',
          'Do cheezein: (1) DEDUP + AGGREGATE — ek time window (jaise 5 min) mein similar notifications ko group karo: "A aur 499 aur logon ne like kiya" ek notification bane, 500 nahi. (2) RATE LIMIT — per-user notification budget (jaise max N per hour) with priority, taaki urgent cheezein jaayein aur baaki batch ho jaayein. Saath mein delivery par idempotency key taaki at-least-once se duplicate push na ho.',
          'Zyada servers lagao',
          'User ko settings do',
        ],
        answer: 1,
        explain: 'Notification systems mein aggregation aur rate-limiting core requirements hain, optional features nahi. 500 pings = uninstall. Achha jawab dono ko time-window ke saath deta hai.',
      },
    ],
  },
  success: [
    'Main fanout-on-write aur fanout-on-read ka trade-off bata sakta hoon',
    'Main celebrity problem pehchan sakta hoon aur hybrid se hal kar sakta hoon',
    'Main notification system mein dedup aur rate-limiting design kar sakta hoon',
    'Main jaanta hoon ki Kafka load smooth karta hai par kaam kam nahi karta',
  ],
  homework: `<p>Hybrid fanout ka logic likho (pseudocode):</p>
    <pre>const CELEBRITY_THRESHOLD = 10_000;

// POST karte waqt
async function onPostCreated(post) {
  const followerCount = await getFollowerCount(post.authorId);

  if (followerCount < CELEBRITY_THRESHOLD) {
    // fanout-on-WRITE: sabki feed mein likh do
    const followers = await getFollowers(post.authorId);
    await Promise.all(followers.map(f => feedStore.push(f, post.id)));
  }
  // celebrity → kuch mat karo, read time par merge hoga
}

// FEED padhte waqt
async function getFeed(userId) {
  const precomputed = await feedStore.get(userId);        // normal users
  const celebs = await getFollowedCelebrities(userId);
  const celebPosts = await Promise.all(
    celebs.map(c => getRecentPosts(c))                     // read-time merge
  );
  return merge(precomputed, celebPosts.flat())
    .sort(byTime).slice(0, 50);
}</pre>
    <p><b>Asli experiment (soch ke):</b> hisaab lagao — 1 crore users, average 200
    followers, 10 posts/din. Fanout-on-write mein kitne feed writes/din? (10 crore × 200
    = 200 crore). Ab isme 100 celebrities (50 lakh followers each) add karo — unke 10
    posts/din se kitne extra writes? Yeh number dekh ke tumhe khud samajh aa jaayega ki
    hybrid kyun zaroori hai.</p>`,
  cards: [
    { front: 'Fanout-on-write vs fanout-on-read?', back: '<b>On-write</b> (pre-compute): post par saare followers ki feed mein likho → read turant, par write mehngi (N followers = N writes).<br><b>On-read</b> (merge): 1 write, par har feed-open par N queries + merge. Faisla <b>read/write ratio</b> se.' },
    { front: 'Celebrity problem kya hai?', back: 'Fanout-on-write mein 5 crore followers waale ka <b>ek post = 5 crore writes</b> → system ghanton block, baaki sabke posts bhi late. Yeh application-level <b>hot key</b> problem hai.' },
    { front: 'Celebrity problem ka hal?', back: '<b>HYBRID</b>: normal users → fanout-on-write (followers kam), celebrities → fanout-on-read (read time par merge). Follower-count <b>threshold</b> se decide karo. Twitter yahi karta hai.' },
    { front: 'Kafka celebrity problem hal karta hai?', back: '<b>Nahi.</b> Woh fanout ko <b>async</b> banata hai aur spike ko <b>smooth</b> karta hai (load leveling) — par 5 crore writes phir bhi 5 crore hain. <b>Queue load smooth karti hai, kaam kam nahi karti.</b> Algorithm (hybrid) hi asli fix hai.' },
    { front: 'Notification system ki do zaroori cheezein?', back: '1) <b>Dedup + aggregate</b> — time window mein group karo ("A aur 499 aur logon ne like kiya"), warna 500 pings.<br>2) <b>Rate limiting</b> — per-user budget + priority, warna app uninstall.' },
  ],
},

/* =====================================================================
   6. EVENT-DRIVEN MICROSERVICES & SAGA
   ===================================================================== */
{
  id: 'saga',
  icon: '🧩',
  title: 'Saga — payment ho gaya par inventory khatam nikla',
  badge: 'Saga Shilpi',
  objectives: [
    'Samjho ki distributed transaction (2PC) microservices mein kyun nahi chalti',
    'Saga pattern aur compensating transactions seekho',
    'Choreography aur orchestration ka trade-off jaano',
    'Jaano ki saga eventual consistency deta hai, atomicity nahi',
  ],
  lesson: [
    `<h3>😰 Dard: "Paisa kat gaya, order cancel ho gaya, refund 5 din baad"</h3>
     <p>Ek e-commerce checkout ke 4 steps the, 4 alag services mein:</p>
     <pre>1. Order create        (order-service)
2. Payment charge      (payment-service)
3. Inventory reserve   (inventory-service)
4. Shipping schedule   (shipping-service)

Ek din step 3 fail hua — item sach mein out of stock tha
(kisi aur ne abhi abhi last piece le liya)

Par step 2 (payment) PEHLE ho chuka tha!
   → customer ka ₹4,999 kat gaya
   → order cancel ho gaya
   → refund manual process se 5 din baad
   → ek angry customer, ek support ticket, ek bad review</pre>
     <div class="war-story"><b>⚡ Monolith mein yeh ek DB transaction hoti —
     <code>BEGIN … COMMIT</code>, sab ya kuch nahi.</b> Par ab 4 alag services hain,
     4 alag databases. Ek transaction unke aar-paar nahi ja sakti. Yeh distributed
     systems ka core dard hai — aur iska jawab "transaction ko forcefully distribute
     karna" nahi, balki ek alag pattern hai.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Sab kuch ek hi service/DB mein daal do"</b><br>
     Yeh microservices ko wapas monolith bana dena hai. Kabhi kabhi yeh SAHI jawab hai
     (agar services ko alag karne ka koi asli karan nahi tha) — par agar teams, scaling,
     ya deployment ke karan alag hain, toh yeh unhe wapas jodna practical nahi.</p>

     <p><b>⚠️ Better: "2-Phase Commit (2PC) se distributed transaction karo"</b><br>
     Theory mein sahi: ek coordinator sabse "ready?" poochta hai, sab haan bolein toh
     "commit" bolta hai. Practice mein dukh:<br>
     • <b>Blocking</b> — coordinator mar gaya toh participants locks pakde baithe rehte hain<br>
     • <b>Dheema</b> — har step par 2 round-trips, aur locks lambe time tak<br>
     • <b>Availability kharab</b> — ek participant down = poori transaction ruki<br>
     • Aur third-party services (payment gateway) 2PC support karte hi nahi<br>
     Isliye industry ne 2PC ko lagbhag chhod diya hai.</p>

     <p><b>✅ Real: SAGA — chhote local transactions + compensations</b><br>
     Ek badi distributed transaction ke bajaye, <b>kai chhoti local transactions</b>,
     aur har ek ka ek <b>compensating action</b> (undo) taiyaar rakho:</p>
     <pre>Forward path:
   1. order created       →  compensate: order cancel
   2. payment charged     →  compensate: payment REFUND
   3. inventory reserved  →  compensate: inventory release
   4. shipping scheduled  →  compensate: shipping cancel

Agar step 3 fail ho:
   → step 2 ka compensation chalao (refund — apne aap, 5 din nahi)
   → step 1 ka compensation chalao (order cancel)
   → system consistent state mein wapas ✅</pre>`,

    `<h3>⚖️ Choreography vs Orchestration, aur nayi musibat</h3>
     <p>Saga ko implement karne ke do tareeqe hain:</p>
     <pre>💃 CHOREOGRAPHY (events se, koi central brain nahi)
   har service event sunti hai aur apna kaam karke naya event nikaalti hai
   order-created → payment-service sunti hai → payment-charged →
   inventory-service sunti hai → inventory-reserved → …
   ✅ loosely coupled, koi single point of failure nahi
   ❌ flow kahin LIKHA hua nahi — poora process samajhne ke liye
      5 services ka code padhna padta hai
   ❌ debugging mushkil, cyclic dependencies ka khatra
   👉 achha jab: steps kam (3-4), flow simple

🎭 ORCHESTRATION (ek coordinator saga chalata hai)
   ek "order-saga" service har step ko command bhejti hai aur
   reply ka intezaar karti hai; fail ho toh compensations chalati hai
   ✅ flow EK jagah likha hai — padhna, debug karna, badalna aasan
   ✅ state machine explicit hai
   ❌ ek aur service maintain karni padti hai (par woh SPOF nahi
      agar woh khud durable state rakhe)
   👉 achha jab: steps zyada, flow complex, visibility chahiye</pre>
     <p><b>Nayi musibat — saga atomicity NAHI deta:</b></p>
     <pre>Saga = EVENTUAL consistency, ATOMIC nahi.
Beech mein system ek "adhoora" state mein rehta hai:
   payment ho gaya par inventory abhi reserve nahi hua
   → us pal koi query kare toh use inconsistent data dikhega

Isliye:
   • UI ko yeh reflect karna chahiye ("processing…" na ki "confirmed")
   • compensations IDEMPOTENT hone chahiye (refund do baar na ho — Track 2!)
   • kuch compensations MUMKIN hi nahi hote
     (email bhej diya? "unsend" nahi kar sakte → toh us step ko
      saga ke AAKHIR mein rakho, jab sab confirm ho chuka ho)

👉 Golden rule: irreversible steps SABSE AAKHIR mein.</pre>`,
  ],
  stages: [
    {
      type: 'order',
      title: '🔢 Saga ka forward path + compensation',
      instructions: 'Checkout saga ka sahi kram lagao — irreversible step aakhir mein.',
      steps: [
        { text: '📝 Order create karo (status: PENDING) — compensate: cancel',
          explain: 'Pehle order banao par PENDING mein. Yeh aasan se cancel ho sakta hai (compensation simple).' },
        { text: '📦 Inventory RESERVE karo — compensate: release',
          explain: 'Payment se PEHLE inventory check/reserve karo! Yahi woh galti thi — agar stock nahi hai toh abhi pata chal jaaye, paisa katne se pehle. Release karna aasan hai.' },
        { text: '💳 Payment charge karo — compensate: refund',
          explain: 'Ab payment. Inventory pehle se reserved hai, toh "paisa kata par stock nahi" wali situation nahi aayegi. Refund mumkin hai par mehnga (fees, time).' },
        { text: '🚚 Shipping schedule karo — compensate: cancel shipment',
          explain: 'Ab shipping. Yeh bhi cancel ho sakta hai agar abhi dispatch nahi hua.' },
        { text: '✅ Order status CONFIRMED karo',
          explain: 'Sab steps safal — ab order confirm. Yahan tak sab reversible tha.' },
        { text: '📧 Confirmation email bhejo (IRREVERSIBLE — isliye sabse aakhir)',
          explain: 'Email "unsend" nahi ho sakta. Golden rule: irreversible steps sabse aakhir mein, jab sab kuch confirm ho chuka ho. Agar yeh pehle hota toh cancel hone par bhi customer ko "order confirmed" email chala jaata.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Saga ke sawal',
      questions: [
        {
          q: 'Microservices mein 2-Phase Commit (2PC) kyun avoid karte hain?',
          choices: [
            'Woh purana hai',
            'Char karan: (1) BLOCKING — coordinator mar gaya toh participants locks pakde baithe rehte hain, (2) dheema — har step par 2 round-trips aur lambe locks, (3) availability kharab — ek participant down = poori transaction ruki, (4) third-party services (payment gateway) 2PC support karte hi nahi. Isliye industry ne saga pattern apnaya — chhoti local transactions + compensations.',
            'Woh mahenga hai',
            '2PC Kafka mein nahi chalta',
          ],
          answer: 1,
          explain: '2PC theory mein sundar hai par distributed systems mein uski blocking nature aur availability cost usse impractical banati hai. Saga availability ke liye atomicity chhod deta hai — woh sahi trade-off hai.',
        },
        {
          q: 'Saga mein "compensating transaction" kya hai?',
          choices: [
            'Ek rollback',
            'Ek NAYA transaction jo pichle step ka asar ULTA karta hai — jaise "payment charged" ka compensation "payment refund" hai. Yeh database rollback NAHI hai (woh already commit ho chuka hai) — yeh ek naya business action hai jo pehle wale ko balance karta hai. Isliye compensations idempotent hone chahiye aur kuch actions (email bhej diya) compensate ho hi nahi sakte.',
            'Ek retry',
            'Ek DLQ entry',
          ],
          answer: 1,
          explain: 'Yeh key distinction hai: compensation = naya forward action, na ki rollback. Refund ek naya transaction hai, charge ka "undo" nahi. Isiliye woh visible hota hai (statement mein charge AUR refund dono dikhte hain).',
        },
        {
          q: 'Choreography aur orchestration mein kya chunoge?',
          choices: [
            'Choreography hamesha, woh decoupled hai',
            'Steps ki sankhya aur complexity par depend karta hai. Choreography (services events se react karti hain) simple flows (3-4 steps) ke liye achha hai — loosely coupled, koi central point nahi. Par flow kahin LIKHA hua nahi hota, toh 8-step process samajhne ke liye 8 services ka code padhna padta hai aur debugging mushkil hoti hai. Orchestration (ek coordinator saga chalata hai) complex flows ke liye behtar — flow ek jagah explicit hai, debug/change aasan.',
            'Orchestration hamesha',
            'Dono same hain',
          ],
          answer: 1,
          explain: 'Yeh trade-off complexity ka hai. Choreography chhote flows mein elegant hai; bade flows mein woh "invisible workflow" ban jaata hai jise koi samajh nahi paata. Orchestration visibility deta hai ek extra service ki keemat par.',
        },
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Choreography ya Orchestration?',
      instructions: 'Har situation ke liye sahi saga style chuno.',
      bins: [
        { id: 'choreo', label: '💃 Choreography (events, no central brain)' },
        { id: 'orch', label: '🎭 Orchestration (central coordinator)' },
      ],
      items: [
        { text: '3-step flow: order → payment → notify', bin: 'choreo',
          explain: 'Chhota aur simple flow — events se chain banana elegant hai, koi extra service nahi chahiye.' },
        { text: '12-step onboarding process with conditional branches', bin: 'orch',
          explain: 'Itna complex flow choreography mein "invisible" ho jaayega — koi nahi samjhega ki poora process kya hai. Orchestrator mein woh ek state machine hai.' },
        { text: 'Business team baar-baar flow badalti hai', bin: 'orch',
          explain: 'Flow ek jagah likha ho toh badalna aasan. Choreography mein badalne ke liye kai services chhedni padti hain.' },
        { text: 'Teams poori tarah independent rehna chahti hain', bin: 'choreo',
          explain: 'Choreography mein koi central service nahi — har team apni service ka event contract follow karti hai. Maximum decoupling.' },
        { text: 'Saga ka current state dashboard par dikhana hai', bin: 'orch',
          explain: 'Orchestrator ke paas explicit state hai (kaunsa step chal raha hai) — usse dashboard banana seedha hai. Choreography mein state bikhri hui hai.' },
        { text: 'Audit/compliance ke liye poora flow document karna hai', bin: 'orch',
          explain: 'Orchestrator ka code hi documentation hai — poora flow ek jagah. Choreography mein flow ko reverse-engineer karna padta hai.' },
      ],
    },
    {
      type: 'fill',
      title: '📄 Saga step + compensation likho',
      instructions: 'Checkout saga ka ek step, uske compensation ke saath.',
      template: "const checkoutSaga = {\n  steps: [\n    { name: 'reserveInventory',\n      action:     (o) => inventory.reserve(o.items, o.id),\n      {0}: (o) => inventory.release(o.id) },   // undo\n\n    { name: 'chargePayment',\n      action:     (o) => payment.charge(o.total, { idempotencyKey: o.{1} }),\n      compensate: (o) => payment.refund(o.id) },\n\n    // ⚠️ email IRREVERSIBLE hai → isliye sabse {2} mein\n    { name: 'sendConfirmation',\n      action:     (o) => email.send(o.customerEmail),\n      compensate: null },\n  ],\n};\n\n// fail hua? → ULTE kram mein compensations chalao\n// aur har compensation {3} honi chahiye (refund do baar na ho)",
      blanks: [
        { answer: 'compensate', explain: 'Har step ka apna undo action. Yeh rollback nahi — ek naya transaction jo asar ulta karta hai.' },
        { answer: 'id', explain: 'Idempotency key (order id) — taaki retry se double charge na ho (Track 2 ka sabak).' },
        { answer: 'aakhir', explain: 'Irreversible steps (email, SMS) sabse aakhir mein, jab sab confirm ho chuka ho — kyunki unka compensation exist hi nahi karta.' },
        { answer: 'idempotent', explain: 'Compensation bhi retry ho sakti hai — refund do baar nahi hona chahiye. Idempotency yahan bhi zaroori hai.' },
      ],
      tokens: ['compensate', 'id', 'aakhir', 'idempotent', 'rollback', 'total', 'shuru', 'ordered'],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Checkout ke 4 steps 4 alag services mein hain. Consistency kaise ensure karoge?"',
        choices: [
          'Distributed transaction (2PC) use karunga',
          'Saga pattern. 2PC avoid karunga — woh blocking hai (coordinator mara toh locks phanse), dheema hai, availability kharab karta hai, aur payment gateway jaise third-party 2PC support hi nahi karte. Saga mein har step ek chhoti LOCAL transaction hoti hai aur har ek ka ek COMPENSATING action hota hai (payment charged → refund, inventory reserved → release). Fail hone par ulte kram mein compensations chalao. Do zaroori baatein: (1) irreversible steps (email/SMS) SABSE AAKHIR mein rakho kyunki unka compensation exist nahi karta, aur (2) compensations idempotent hon (refund do baar na ho). Aur yaad rakho saga EVENTUAL consistency deta hai, atomicity nahi — toh UI ko "processing" dikhana chahiye, "confirmed" nahi.',
          'Sab kuch ek service mein daal dunga',
          'Retry logic laga dunga',
        ],
        answer: 1,
        explain: 'Yeh microservices ka signature sawal hai. Achha jawab 2PC reject karta hai (karan ke saath), saga deta hai, compensations explain karta hai, irreversible-steps-last rule batata hai, aur eventual-consistency ki honest limitation maanta hai.',
      },
      {
        q: '🧑‍💼 "Saga mein agar compensation KHUD fail ho jaaye toh?"',
        choices: [
          'Kuch nahi kar sakte',
          'Yeh saga ka sabse mushkil case hai. Pehle: compensations ko idempotent aur retry-able banao, aur unhe backoff+jitter ke saath retry karo (Track 2) — kyunki zyadatar failures transient hoti hain. Agar phir bhi fail ho toh use ek DLQ/manual-intervention queue mein bhejo aur ALERT karo — kyunki ab system ek inconsistent state mein atka hai aur usse insaan ko theek karna padega. Isliye design mein hi compensations ko yathasambhav SIMPLE aur reliable rakhte hain (jaise "status flag badlo" vs "external API call"), aur irreversible steps ko aakhir mein rakhte hain taaki unka compensation chahiye hi na.',
          'Poore saga ko dobara chalao',
          '2PC par shift kar do',
        ],
        answer: 1,
        explain: 'Yeh gehra follow-up sawal hai jo dikhata hai ki tumne saga ko sach mein socha hai. Compensation failure = stuck inconsistent state = human intervention. Isliye design-time par compensations ko simple rakhna hi asli bachav hai.',
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki 2PC microservices mein kyun avoid karte hain',
    'Main saga pattern aur compensating transactions design kar sakta hoon',
    'Main choreography aur orchestration mein sahi chunav kar sakta hoon',
    'Main jaanta hoon ki irreversible steps aakhir mein kyun rakhte hain',
  ],
  homework: `<p>Ek saga orchestrator ka skeleton likho:</p>
    <pre>async function runSaga(saga, ctx) {
  const done = [];
  try {
    for (const step of saga.steps) {
      await step.action(ctx);       // local transaction
      done.push(step);              // track — compensation ke liye
    }
    return { ok: true };
  } catch (err) {
    // ULTE kram mein compensations
    for (const step of done.reverse()) {
      if (!step.compensate) continue;         // irreversible — skip
      try {
        await step.compensate(ctx);           // idempotent hona chahiye
      } catch (compErr) {
        // ⚠️ compensation fail — manual intervention chahiye
        await alertOps({ saga: saga.name, step: step.name, ctx, compErr });
        await dlq.send({ saga: saga.name, step: step.name, ctx });
      }
    }
    return { ok: false, err };
  }
}</pre>
    <p><b>Asli experiment:</b> apne kisi asli flow (signup, checkout, booking) ke steps
    likho aur har ek ke saamne uska compensation. Tumhe do cheezein turant dikhengi:
    (1) kuch steps ka compensation exist hi nahi karta (email, SMS) — unhe aakhir mein
    le jao; (2) kuch compensations mehnge hain (payment refund mein fees lagti hain) —
    unhe baad mein karo, taaki pehle sasti cheezein fail ho jaayein. Yeh do insights
    hi saga design ka aadha kaam hain.</p>`,
  cards: [
    { front: '2PC microservices mein kyun avoid karte hain?', back: '1) <b>Blocking</b> — coordinator mara toh locks phanse<br>2) <b>Dheema</b> — 2 round-trips + lambe locks<br>3) <b>Availability kharab</b> — ek participant down = sab ruka<br>4) Third-party (payment gateway) 2PC support hi nahi karte' },
    { front: 'Saga pattern kya hai?', back: 'Ek badi distributed transaction ke bajaye <b>kai chhoti LOCAL transactions</b>, aur har ek ka ek <b>compensating action</b> (undo). Fail hone par ulte kram mein compensations chalao — system consistent state mein wapas.' },
    { front: 'Compensating transaction = rollback?', back: '<b>Nahi.</b> Woh ek <b>NAYA forward transaction</b> hai jo pichle ka asar ulta karta hai (charge → refund). DB rollback nahi (woh commit ho chuka). Isliye woh visible hota hai aur <b>idempotent</b> hona chahiye.' },
    { front: 'Choreography vs Orchestration?', back: '<b>Choreography</b>: services events se react karti hain — loosely coupled, par flow kahin likha nahi (3-4 steps tak achha).<br><b>Orchestration</b>: ek coordinator saga chalata hai — flow ek jagah explicit, debug/change aasan (complex flows ke liye).' },
    { front: 'Saga ka golden rule?', back: '<b>Irreversible steps SABSE AAKHIR mein</b> (email, SMS — inka compensation exist hi nahi karta). Aur saga <b>eventual consistency</b> deta hai, atomicity nahi — UI ko "processing" dikhana chahiye, "confirmed" nahi.' },
  ],
},

]);

/* ===================== Levels 7-8: Capacity math, Final Boss ===================== */
KQ.registerLevels('sysdesign', [

/* =====================================================================
   7. CAPACITY, ORDERING vs PARALLELISM & COST
   ===================================================================== */
{
  id: 'capacity-cost',
  icon: '📐',
  title: 'Capacity & Cost — 5,000 partitions ne failover 12 min ka kar diya',
  badge: 'Capacity Ganak',
  objectives: [
    'Partition count ka hisaab lagana seekho (throughput + parallelism)',
    'Samjho ki bahut ZYADA partitions bhi ek problem hai',
    'Ordering vs parallelism ka fundamental trade-off jaano',
    'Storage aur cost ka estimate karna seekho',
  ],
  lesson: [
    `<h3>😰 Dard: "Har broker failover 12 minute le raha tha"</h3>
     <p>Ek team ne socha "partitions zyada rakho, future-proof rahega" — unhone har topic
     par 500 partitions banaye. 10 topics × 500 = <b>5,000 partitions</b>, RF=3 ke saath
     <b>15,000 partition replicas</b>.</p>
     <pre>Ek broker gira (routine maintenance). Ab:
   → us broker ke ~5,000 partition replicas ke liye
     leader election chalani padi
   → controller ne ek-ek partition ka metadata update kiya
   → poore cluster ko naya metadata propagate hua
   → 12 MINUTE tak partial unavailability

Aur roz ke problems bhi:
   → har partition = broker par file handles + memory
   → producer har partition ke liye ek batch buffer rakhta hai
     → 5,000 partitions = producer memory bhi bahut zyada
   → end-to-end latency badhi (chhote batches per partition)</pre>
     <div class="war-story"><b>⚡ "Zyada partitions = zyada scale" ek myth hai.</b>
     Partitions parallelism dete hain, par har ek ki apni keemat hai: metadata,
     file handles, memory, replication traffic, aur sabse bada — <b>failover time</b>.
     Kafka ke modern versions (KRaft) mein yeh kaafi behtar hai, par principle wahi hai:
     partition count ek <b>soch-samajh kar liya gaya number</b> hona chahiye, "jitna zyada
     utna achha" nahi.</div>`,

    `<h3>🪜 Partition count ka asli hisaab</h3>
     <p>Track 1 mein formula dekha tha; ab poora picture:</p>
     <pre>partitions ≥ max(
    peak_throughput ÷ per_consumer_throughput,   ← consumer side
    peak_throughput ÷ per_partition_write_limit, ← producer side
    max_consumers_you_will_ever_run              ← parallelism ceiling
) × growth_headroom

Example:
   peak = 50,000 msg/s
   ek consumer = 2,000 msg/s   → 50,000 ÷ 2,000 = 25
   ek partition safely ~10 MB/s handle karti hai
   avg message 1 KB → 50,000 msg/s = 50 MB/s → 50 ÷ 10 = 5
   max consumers kabhi chalane pad sakte = 30

   → max(25, 5, 30) = 30, phir headroom (1.5×) → ~45 partitions</pre>
     <p><b>Upar ki taraf galti karo — par bahut zyada nahi:</b></p>
     <pre>Kam partitions:  ❌ parallelism ki limit (consumers khaali baithenge)
                 ❌ badhane par ORDERING toot-ti hai (breaking change)
Zyada partitions: ❌ failover/leader-election dheema
                 ❌ broker file handles + memory
                 ❌ producer memory (per-partition batch buffers)
                 ❌ end-to-end latency (chhote batches)

👉 Sweet spot: apne hisaab se nikaale number ka 1.5-2× headroom.
   Har topic par hazaaron partitions "just in case" mat banao.</pre>`,

    `<h3>⚖️ Ordering vs parallelism, aur cost ka hisaab</h3>
     <p><b>Yeh trade-off poore course ka nichod hai:</b></p>
     <pre>Ordering aur parallelism SEEDHE takraav mein hain:

   1 partition   → perfect global ordering, parallelism = 1
   N partitions  → N-way parallelism, ordering sirf partition ke ANDAR

   Aur key se ordering: same key = same partition = ordered
   → par tab us key ka poora traffic EK consumer par (hot key)

Isliye har design mein poocho:
   "Mujhe kis LEVEL par ordering chahiye?"
   • Global ordering?     → 1 partition (lagbhag hamesha over-kill)
   • Per-user/per-order?  → key = userId/orderId (99% cases yahi)
   • Koi ordering nahi?   → null key, max parallelism</pre>
     <p><b>Storage aur cost ka estimate:</b></p>
     <pre>storage = throughput × avg_msg_size × retention × RF

Example: 50,000 msg/s × 1 KB × 7 din × RF 3
       = 50 MB/s × 604,800 s × 3
       = ~87 TB  😳

Cost ghatane ke lever:
   📉 retention ghatao (7 din → 2 din = 3× kam)
   🗜️ compression (lz4/zstd — aksar 3-5× kam)
   🗃️ compaction (agar sirf latest state chahiye)
   🧊 tiered storage (purana data sasti object storage par)
   📏 message size ghatao (fat events ka trade-off yaad hai?)</pre>
     <p><b>Nayi musibat:</b> har lever ki apni keemat hai — retention ghatane se replay
     window chhoti hoti hai (aur OffsetOutOfRange ka khatra badhta hai), compression se
     CPU badhta hai, compaction se itihaas jaata hai. <b>Capacity planning ek balance hai,
     ek formula nahi</b> — par formula se shuru karna zaroori hai, warna tum "500 partitions
     rakh dete hain" wali galti karoge.</p>`,
  ],
  stages: [
    {
      type: 'quiz',
      title: '🧠 Partition math',
      questions: [
        {
          q: 'Peak 50,000 msg/s, ek consumer 2,000 msg/s handle karta hai, aur tum max 30 consumers chalaoge. Kitne partitions?',
          choices: [
            '25 (sirf throughput se)',
            'Kam se kam 30 (max consumers) — kyunki partition parallelism ki HARD limit hai, toh 30 consumers chalane ke liye 30 partitions chahiye. Throughput ke hisaab se 25 kaafi the, par max(25, 30) = 30 lena hoga. Phir growth headroom (1.5×) ke saath ~45 rakhna behtar hai, kyunki partitions badhana ordering todta hai aur ghatana namumkin hai.',
            '50,000',
            '3 — default',
          ],
          answer: 1,
          explain: 'Formula mein max() lena zaroori hai — throughput ya parallelism, jo bhi zyada maange. Aur headroom isliye kyunki badhana costly (ordering break) aur ghatana impossible hai.',
        },
        {
          q: 'Bahut ZYADA partitions (jaise 5,000) ka nuksan kya hai?',
          choices: [
            'Koi nuksan nahi, zyada hamesha behtar',
            'Char nuksan: (1) broker failover/leader-election bahut dheema (har partition ka metadata update), (2) broker par file handles aur memory, (3) producer memory — woh har partition ke liye batch buffer rakhta hai, (4) end-to-end latency badhti hai (per-partition batches chhote ho jaate hain). "Zyada partitions = zyada scale" ek myth hai — har partition ki apni keemat hai.',
            'Sirf disk zyada lagti hai',
            'Consumers confuse ho jaate hain',
          ],
          answer: 1,
          explain: 'Failover time sabse bada aur sabse kam samjha jaane wala nuksan hai. Modern KRaft mein behtar hai par principle wahi — partition count ek soch-samajh kar liya number hona chahiye.',
        },
        {
          q: '50,000 msg/s × 1 KB × 7 din retention × RF 3 = kitni storage?',
          choices: [
            '~1 TB',
            '~87 TB — 50 MB/s × 604,800 second (7 din) × 3 (replication factor). Yeh number aksar log bhool jaate hain kyunki woh RF ko multiply karna bhool jaate hain. Ghatane ke lever: retention kam karo, compression (lz4/zstd, aksar 3-5×), compaction agar sirf latest state chahiye, ya tiered storage.',
            '~10 GB',
            'Retention se koi farak nahi padta',
          ],
          answer: 1,
          explain: 'Storage = throughput × size × retention × RF. RF ko bhoolna sabse aam capacity-planning galti hai — woh storage ko 3× kar deta hai.',
        },
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Kitni ordering chahiye?',
      instructions: 'Har use-case ke liye sahi ordering level. Yaad rakho: zyada ordering = kam parallelism.',
      bins: [
        { id: 'global', label: '1️⃣ Global ordering (1 partition — parallelism 1)' },
        { id: 'key', label: '🔑 Per-key ordering (key set karo)' },
        { id: 'none', label: '🎲 Koi ordering nahi (null key, max parallelism)' },
      ],
      items: [
        { text: 'Ek user ke order events (created → paid → shipped)', bin: 'key',
          explain: 'Per-order ordering chahiye — key = orderId. 99% cases yahi hote hain. Global ordering ki zaroorat nahi.' },
        { text: 'Independent log lines (analytics ke liye)', bin: 'none',
          explain: 'Har log line independent hai — koi ordering nahi chahiye. Null key = max parallelism aur barabar distribution.' },
        { text: 'Ek bank account ka transaction ledger', bin: 'key',
          explain: 'Ek account ke transactions ordered hone chahiye (balance galat ho jaayega) — key = accountId. Alag accounts parallel chal sakte hain.' },
        { text: 'Ek chhota config-change stream jahan HAR change sequential ho', bin: 'global',
          explain: 'Yeh un kam cases mein se hai jahan global ordering sach mein chahiye — aur woh tabhi practical hai jab volume kam ho (kyunki parallelism 1 hoga).' },
        { text: 'IoT sensor readings (har device independent)', bin: 'key',
          explain: 'Ek device ke readings ordered chahiye (time series) — key = deviceId. Lakhs devices = achha parallelism.' },
        { text: 'Metrics data points (aggregate hi karna hai)', bin: 'none',
          explain: 'Aggregate karna hai toh order matter nahi karta. Null key se barabar distribution aur max throughput.' },
      ],
    },
    {
      type: 'fill',
      title: '📄 Capacity planning ka hisaab',
      instructions: 'Ek naye topic ke liye capacity nikaalo.',
      template: "// Requirements: peak 50,000 msg/s · 1 KB avg · 7 din retention · RF 3\n// Ek consumer 2,000 msg/s · max 30 consumers chalane pad sakte hain\n\npartitions = max(\n  50000 / 2000,        // = 25  (consumer throughput)\n  {0}                  // = 30  (max consumers — parallelism ceiling)\n) × 1.5                // growth {1}\n= ~45 partitions\n\nstorage = 50 MB/s × 604800 s × {2}    // ← RF mat bhoolna!\n        = ~87 TB\n\n// cost ghatane ke lever:\n//   retention ghatao · {3} (lz4/zstd, 3-5×) · compaction · tiered storage",
      blanks: [
        { answer: '30', explain: 'Max consumers = parallelism ceiling. Partition se zyada consumers khaali baithte hain, toh utni partitions chahiye.' },
        { answer: 'headroom', explain: 'Growth ke liye buffer — kyunki partitions badhana ordering todta hai aur ghatana namumkin hai. Upar ki taraf galti karo.' },
        { answer: '3', explain: 'Replication factor. Yeh multiply karna bhoolna sabse aam capacity galti hai — storage 3× ho jaati hai.' },
        { answer: 'compression', explain: 'lz4/zstd aksar 3-5× storage bachate hain, CPU ki thodi keemat par. Sabse aasan lever.' },
      ],
      tokens: ['30', 'headroom', '3', 'compression', '25', 'buffer', '1', 'encryption'],
    },
    {
      type: 'quiz',
      title: '🧠 Ordering vs parallelism',
      bonus: true,
      questions: [
        {
          q: 'Ordering aur parallelism ka rishta ek line mein?',
          choices: [
            'Dono saath badhte hain',
            'Woh SEEDHE takraav mein hain: 1 partition = perfect global ordering par parallelism 1; N partitions = N-way parallelism par ordering sirf partition ke andar. Key set karne se per-key ordering milti hai (same key = same partition) — par tab us key ka poora traffic ek consumer par jaata hai (hot key). Isliye har design mein poochna chahiye: "mujhe kis LEVEL par ordering chahiye?" — 99% cases mein per-entity (per-user/order) kaafi hota hai, global nahi.',
            'Ordering se parallelism badhta hai',
            'Koi rishta nahi',
          ],
          answer: 1,
          explain: 'Yeh poore course ka central trade-off hai — Track 1 (keys), Track 2 (skew), aur yahan capacity planning mein. Global ordering maangna lagbhag hamesha over-specification hoti hai.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Naye topic ke liye kitne partitions rakhoge? Poora hisaab dikhao."',
        choices: [
          'Hamesha 3 — default theek hai',
          'Formula: max(peak_throughput ÷ per_consumer_throughput, peak ÷ per_partition_limit, max_consumers_ever) × growth_headroom. Jaise 50k msg/s, ek consumer 2k/s → 25; par agar 30 consumers chalane pad sakte hain toh 30 (parallelism ceiling); headroom 1.5× → ~45. Upar ki taraf galti karunga kyunki partitions BADHANA ordering todta hai (hash % N badal jaata hai) aur GHATANA namumkin hai. Par bahut zyada bhi nahi — 5,000 partitions se failover/leader-election dheema hota hai, broker file-handles/memory aur producer batch buffers badhte hain, aur end-to-end latency bhi. "Zyada = behtar" myth hai.',
          'Jitne brokers hain utne',
          '1000 — future-proof',
        ],
        answer: 1,
        explain: 'Yeh classic capacity sawal hai. Achha jawab formula deta hai, DONO taraf ki keemat batata hai (kam = parallelism limit + ordering break on change; zyada = failover/memory/latency), aur ek concrete number nikaalta hai.',
      },
      {
        q: '🧑‍💼 "Tumhara Kafka storage bill bahut zyada hai. Kya karoge?"',
        choices: [
          'Kam data bhejo',
          'Pehle hisaab lagaunga: storage = throughput × avg_msg_size × retention × RF (log aksar RF bhoolte hain — woh 3× kar deta hai). Phir levers, sabse saste se: (1) COMPRESSION — lz4/zstd aksar 3-5× bachate hain, sirf thodi CPU ki keemat, yeh pehla step hai; (2) RETENTION review — kya sach mein 7 din chahiye ya 2 din kaafi hai (par dhyan: chhoti retention = chhota replay window aur OffsetOutOfRange ka zyada khatra); (3) COMPACTION agar us topic mein sirf latest state matter karta hai (itihaas nahi); (4) TIERED STORAGE — purana data sasti object storage par; (5) message size — fat events ka trade-off dekho. Har lever ki keemat hai, toh main per-topic decide karunga, blanket nahi.',
          'RF 3 se 1 kar dunga',
          'Purana data delete kar dunga',
        ],
        answer: 1,
        explain: 'Achha jawab formula se shuru karta hai, levers ko cost-benefit ke kram mein deta hai, aur har lever ki keemat batata hai. Dhyan: "RF 3 se 1" (option 3) durability bech dena hai — woh sabse khatarnaak "cost saving" hai.',
      },
    ],
  },
  success: [
    'Main partition count ka poora hisaab laga sakta hoon',
    'Main jaanta hoon ki bahut zyada partitions bhi ek problem hai',
    'Main ordering vs parallelism ka trade-off samjha sakta hoon',
    'Main storage/cost estimate karke sahi lever chun sakta hoon',
  ],
  homework: `<p>Apne kisi topic ka capacity plan likho:</p>
    <pre>Topic: ______________

peak throughput:        ______ msg/s
avg message size:       ______ KB
ek consumer handle:     ______ msg/s
max consumers (kabhi):  ______
retention:              ______ din
RF:                     ______

partitions = max(peak/per_consumer, max_consumers) × 1.5
           = max( ____ , ____ ) × 1.5 = ______

storage = (peak × size) × (retention × 86400) × RF
        = ______ MB/s × ______ s × ____ = ______ TB

Ordering chahiye?  [ ] global  [ ] per-key (key = ______)  [ ] koi nahi
Cost levers:       [ ] compression  [ ] retention  [ ] compaction  [ ] tiered</pre>
    <p><b>Asli kaam:</b> yeh apne current system par chalao. Do cheezein aksar
    surprise deti hain: (1) storage mein RF multiply karna bhool jaate hain — bill
    3× nikalta hai; (2) log "global ordering chahiye" bolte hain par asal mein
    per-entity kaafi hota hai — aur woh realization parallelism 1 se N kar deti hai.</p>`,
  cards: [
    { front: 'Partition count ka formula?', back: '<code>max(peak ÷ per_consumer_throughput, peak ÷ per_partition_limit, max_consumers_ever) × growth_headroom (1.5-2×)</code>. Upar ki taraf galti karo — badhana ordering todta hai, ghatana <b>namumkin</b>.' },
    { front: 'Bahut ZYADA partitions ka nuksan?', back: '1) <b>Failover/leader-election dheema</b> (sabse bada)<br>2) Broker file handles + memory<br>3) <b>Producer memory</b> (per-partition batch buffers)<br>4) End-to-end latency (chhote batches). "Zyada = behtar" myth hai.' },
    { front: 'Storage ka formula?', back: '<code>throughput × avg_msg_size × retention × RF</code>. <b>RF bhoolna sabse aam galti hai</b> — woh storage 3× kar deta hai. 50k msg/s × 1KB × 7din × RF3 = ~87 TB.' },
    { front: 'Storage cost ghatane ke lever?', back: '1) <b>Compression</b> (lz4/zstd, 3-5×, sirf CPU) — pehla step<br>2) <b>Retention</b> ghatao (par replay window chhota + OffsetOutOfRange khatra)<br>3) <b>Compaction</b> (agar sirf latest state)<br>4) <b>Tiered storage</b><br>❌ RF ghatana = durability bechna' },
    { front: 'Ordering vs parallelism ka rishta?', back: '<b>Seedhe takraav mein.</b> 1 partition = global ordering, parallelism 1. N partitions = N-way parallelism, ordering sirf partition ke andar. Key se per-key ordering (par hot-key ka khatra). 99% cases mein <b>per-entity</b> ordering kaafi hai, global nahi.' },
  ],
},

/* =====================================================================
   8. FINAL BOSS: DESIGN GAUNTLET
   ===================================================================== */
{
  id: 'boss-gauntlet',
  icon: '👑',
  title: 'FINAL BOSS: Design Gauntlet — poora system, shuru se ant tak',
  badge: 'Kafka Mahaguru',
  objectives: [
    'Ek poora real-world system end-to-end design karo',
    'Sahi tool chuno aur har faisle ko justify karo',
    'Failure modes khud pehchano aur unke liye design karo',
    'Poore course ke saare 4 tracks ek jagah use karo',
  ],
  lesson: [
    `<h3>👑 Final imtihaan — RideNow ka pura platform</h3>
     <p>Tum <b>RideNow</b> (ek ride-hailing app) ke lead architect ho. Interview panel
     baithi hai. Tumhe poora messaging architecture design karna hai — aur har faisle
     ka <b>kyun</b> batana hai.</p>
     <pre>Requirements:
   🚗 5 lakh active drivers, har 4 second location bhejte hain
      → ~1,25,000 location updates/sec
   📱 20 lakh riders, ride book karte hain (peak 5,000 rides/min)
   💳 Har ride ke end par payment (kabhi fail nahi hona chahiye)
   📊 Analytics team ko sab kuch chahiye (replay bhi)
   🔔 Riders ko real-time notifications (driver aa raha hai)
   🌍 3 regions, ek region gire toh service chalti rahe</pre>
     <p>Yeh gauntlet tumhe poore course se guzarega:</p>
     <ul>
       <li><b>Track 4:</b> kaunsa data kis tool par? (location vs payments vs notifications)</li>
       <li><b>Track 1:</b> partition count, keys, acks, consumer groups</li>
       <li><b>Track 2:</b> lag, DLQ, retries, backpressure, circuit breaker</li>
       <li><b>Track 3:</b> EOS/outbox, event-time, schema, DR</li>
     </ul>
     <p><b>Yaad rakho jo poore course ka nichod hai:</b> har design faisle ki keemat
     hoti hai, aur senior wahi hai jo apne design ki <b>nayi problems khud batata hai</b>.</p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Round 1: kaunsa data, kaunsa tool?',
      instructions: 'RideNow ke har data stream ke liye sahi tool chuno. Char sawal yaad hain? (replay, fanout, per-message control, throughput)',
      bins: [
        { id: 'kafka', label: '🟣 Kafka (log — replay, high throughput)' },
        { id: 'queue', label: '📬 Queue (per-message ack, jobs)' },
        { id: 'mqtt', label: '📡 MQTT (constrained devices, weak network)' },
      ],
      items: [
        { text: 'Driver location pings (1.25 lakh/sec, mobile network)', bin: 'mqtt',
          explain: 'Mobile devices + flaky network + bahut high frequency + chhote packets = MQTT ka ghar. QoS 0 kaafi hai (purana location bekaar). Phir bridge se Kafka mein analytics ke liye.' },
        { text: 'Ride events (requested → matched → started → completed)', bin: 'kafka',
          explain: 'Multiple consumers (payments, analytics, notifications), replay chahiye, aur per-ride ordering (key = rideId). Classic Kafka use-case.' },
        { text: 'Payment processing jobs (har ride ke end par)', bin: 'queue',
          explain: 'Per-message retry chahiye (payment gateway flaky ho sakta hai), ek fail hua payment baaki ko block na kare, aur priority bhi chahiye ho sakti hai. Queue model behtar fit.' },
        { text: 'Analytics events (sab kuch, replay chahiye)', bin: 'kafka',
          explain: 'Replay + high throughput + multiple independent consumers. Log model ka core.' },
        { text: 'Push notification delivery jobs', bin: 'queue',
          explain: 'Per-message retry (push service fail ho sakti hai), DLQ, rate limiting. Task queue ka classic case.' },
        { text: 'Driver app se trip telemetry (speed, route, fuel)', bin: 'mqtt',
          explain: 'Mobile device, continuous telemetry, weak network. MQTT QoS 0/1, phir Kafka bridge.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Round 2: design ke faisle',
      questions: [
        {
          q: '🚗 Location pings: 1.25 lakh/sec. Consumer 5,000/s handle karta hai. Kitne partitions aur kaunsi key?',
          choices: [
            '3 partitions, key = city',
            '~25+ partitions (1,25,000 ÷ 5,000 = 25, phir headroom se ~40). Key = driverId — kyunki ek driver ke pings ordered chahiye (route banane ke liye) aur driverIds lakhs hain (achhi cardinality, load barabar bantega). City key GALAT hoti — kuch cities mein 50% traffic = hot partition.',
            '1 partition, global ordering',
            '1000 partitions, no key',
          ],
          answer: 1,
          explain: 'Track 1 (partition math + key selection) ka direct application. City = low cardinality + skewed = hot partition. driverId = high cardinality + exactly woh ordering jo chahiye.',
        },
        {
          q: '💳 Payment events kabhi kho nahi sakte. Kaunsi settings?',
          choices: [
            'acks=1, RF=1 — tez rahega',
            'acks=all + RF=3 + min.insync.replicas=2 + unclean.leader.election=false. Teeno saath chahiye: RF akela guarantee nahi deta, acks=all akela ISR ke 1 tak girne par chup-chaap acks=1 ban jaata hai, aur unclean leader election committed data truncate kar sakta hai. Yeh availability ki keemat par durability chunna hai — payments ke liye sahi trade-off.',
            'acks=0 — throughput ke liye',
            'Sirf RF=5 kaafi hai',
          ],
          answer: 1,
          explain: 'Track 2 (ISR/replication) ka direct application. Durability teen settings ki jodi hai, koi ek akela nahi.',
        },
        {
          q: '💸 Ride complete hua: DB mein ride save karni hai AUR payment event Kafka par bhejna hai. Kaise?',
          choices: [
            'await db.save() phir await kafka.publish()',
            'Yeh dual-write problem hai — beech mein crash se ride DB mein hogi par payment event nahi (ya ulta). Transactional Outbox use karunga: ride aur event dono EK DB transaction mein (event outbox table mein), phir relay/CDC se Kafka par. At-least-once hoga toh payment consumer idempotent hona chahiye (rideId par unique constraint).',
            'Do baar publish karo',
            'Transaction dono ke around laga do',
          ],
          answer: 1,
          explain: 'Track 3 (outbox/CDC) ka direct application. "await db then await kafka" ghost payments banata — ride complete par payment kabhi nahi.',
        },
        {
          q: '📉 Payment consumer ka lag badh raha hai kyunki payment gateway dheema hai. Kya karoge?',
          choices: [
            'Consumers double kar do',
            'Consumers badhana GALAT hai — bottleneck downstream gateway hai, aur zyada consumers use aur tez maarenge (congestion collapse). Sahi: (1) circuit breaker taaki gateway sach mein gir jaaye toh usse maarna band ho, (2) retry with jitter taaki thundering herd na bane, (3) rate limiting/back-pressure taaki gateway ki capacity se zyada na jaaye, (4) DLQ taaki permanently-fail payments pipeline block na karein. Lag badhega par woh theek hai — messages log mein safe hain.',
            'Retries band kar do',
            'Offsets latest par reset karo',
          ],
          answer: 1,
          explain: 'Track 2 (backpressure + circuit breaker) ka direct application. Jab bottleneck downstream ho toh consumers badhana ulta padta hai — yeh sabse aam galti hai.',
        },
        {
          q: '🌍 3 regions, ek gire toh service chale. DR ka sabse bada gotcha?',
          choices: [
            'Bandwidth cost',
            'Offset translation — offsets clusters ke beech match nahi karte (har cluster apne offsets independently assign karta hai), toh failover par consumers skip ya duplicate kar sakte hain. MirrorMaker 2 ka offset translation setup karna padega. Aur sabse zaroori: DR ko REGULARLY TEST karna (game-day drill) — untested failover pehli baar production mein toot-ta hai. RPO/RTO bhi define karunga: payments ke liye RPO ~0 chahiye, location pings ke liye 5 min bhi chalega.',
            'Zyada partitions chahiye',
            'Latency badh jaayegi',
          ],
          answer: 1,
          explain: 'Track 3 (multi-DC DR) ka direct application. Offset translation DR ka signature gotcha hai, aur untested DR ek dua hai plan nahi.',
        },
        {
          q: '📊 Analytics team ko "har ghante kitni rides" chahiye. Driver app offline bhi ho sakta hai. Kya dhyan rakhoge?',
          choices: [
            'Processing-time se count karo',
            'EVENT-TIME windowing use karo, processing-time nahi. Driver app offline ho sakta hai (tunnel, no signal) toh ride-complete event ghanton baad aa sakta hai — processing-time se count karoge toh woh galat ghante mein jaayegi aur report baad mein badal jaayega. Event-time + watermark + grace period (jaise 30 min) se late events sahi window mein jaayenge. Grace ke baad aaye events ko side-output mein bhejunga (drop nahi) taaki pata rahe kitne late hue.',
            'Rides ko turant count kar do',
            'Analytics ko real-time mat do',
          ],
          answer: 1,
          explain: 'Track 3 (event-time windows) ka direct application. Ride-hailing mein offline drivers ek certainty hai — processing-time reports ko galat kar dega.',
        },
      ],
    },
    {
      type: 'terminal',
      title: '🖥️ FINAL MISSION: RideNow ka poora platform banao',
      instructions: 'Yeh aakhri mission hai. Poore course ka gyaan ek jagah — har topic ka config apne design ke hisaab se.',
      goal: `<b>RideNow ke saare topics banao:</b><br><br>
        1️⃣ <code>ride-events</code> — <b>≥12 partitions</b>, RF=3, <code>min.insync.replicas=2</code>
        (core event stream, durable)<br>
        2️⃣ <code>payments</code> — <b>≥6 partitions</b>, RF=3, <code>min.insync.replicas=2</code>
        (paisa — durability par samjhauta nahi)<br>
        3️⃣ <code>payments.DLQ</code> — RF=3 (permanently-fail payments)<br>
        4️⃣ <code>driver-state</code> — RF=3, <code>cleanup.policy=compact</code>
        (har driver ka current status — KTable style)<br>
        5️⃣ <code>ride-events</code> mein <b>rideId key ke saath</b> data daalo aur
        confirm karo ki saare partitions use ho rahe hain<br>
        6️⃣ Ek broker maar do — dekho ki RF=3 ke saath sab chalta rehta hai`,
      cluster: { brokers: 3 },
      tasks: [
        { text: 'ride-events (≥12 part, RF=3, minISR=2)',
          check: s => s.hasTopic('ride-events') && s.topics['ride-events'].partitions.length >= 12 &&
                      s.topics['ride-events'].rf === 3 &&
                      String(s.topics['ride-events'].config['min.insync.replicas']) === '2' },
        { text: 'payments (≥6 part, RF=3, minISR=2)',
          check: s => s.hasTopic('payments') && s.topics.payments.partitions.length >= 6 &&
                      s.topics.payments.rf === 3 &&
                      String(s.topics.payments.config['min.insync.replicas']) === '2' },
        { text: 'payments.DLQ (RF=3)',
          check: s => s.hasTopic('payments.DLQ') && s.topics['payments.DLQ'].rf === 3 },
        { text: 'driver-state (compact)',
          check: s => s.hasTopic('driver-state') && String(s.topics['driver-state'].config['cleanup.policy']).includes('compact') },
        { text: 'ride-events mein key ke saath data (sab partitions par)',
          check: s => s.hasTopic('ride-events') && s.partitionCounts('ride-events').every(c => c > 0) },
        { text: 'ek broker maaro (sab chalta rahe)', check: s => s.brokers.some(b => !b.alive) },
      ],
      success: s => s.hasTopic('ride-events') && s.topics['ride-events'].partitions.length >= 12 &&
                    s.topics['ride-events'].rf === 3 &&
                    String(s.topics['ride-events'].config['min.insync.replicas']) === '2' &&
                    s.hasTopic('payments') && s.topics.payments.partitions.length >= 6 &&
                    s.topics.payments.rf === 3 &&
                    String(s.topics.payments.config['min.insync.replicas']) === '2' &&
                    s.hasTopic('payments.DLQ') && s.topics['payments.DLQ'].rf === 3 &&
                    s.hasTopic('driver-state') &&
                    String(s.topics['driver-state'].config['cleanup.policy']).includes('compact') &&
                    s.partitionCounts('ride-events').every(c => c > 0) &&
                    s.brokers.some(b => !b.alive),
      successText: `👑 <b>TUMNE POORA KAFKA GURUKUL PAAR KAR LIYA!</b><br><br>
        Dekho tumne is ek mission mein poore course ka gyaan use kiya:<br><br>
        🌱 <b>Track 1:</b> partition count (throughput ÷ per-consumer), rideId key
        (ordering + cardinality), RF=3 + acks durability, alag consumer groups<br>
        🔥 <b>Track 2:</b> DLQ topic (poison payments), aur tumne broker maar ke dekha
        ki min.insync=2 ke saath ek failure jheli jaati hai<br>
        🚀 <b>Track 3:</b> compacted <code>driver-state</code> (KTable-style current
        state), EOS-ready durable topics<br>
        🏛️ <b>Track 4:</b> har data stream ke liye sahi tool (MQTT location, Kafka
        events, queue jobs), capacity math, aur poora system design<br><br>
        <b>Par sabse zaroori jo tumne seekha woh yeh hai:</b><br>
        📌 Har concept ek asli <b>PROBLEM</b> se shuru hota hai<br>
        📌 Har solution ki ek <b>KEEMAT</b> hoti hai<br>
        📌 Aur senior wahi hai jo apne design ki <b>NAYI problems khud bataye</b><br><br>
        🐣 Sandesh Shishya se 👑 <b>Kafka Mahaguru</b> tak ka safar poora hua.
        Ab jao aur asli cluster par yeh sab karke dekho. 🛕`,
      hints: [
        'kafka-topics --create --topic ride-events --partitions 12 --replication-factor 3 --config min.insync.replicas=2',
        'kafka-topics --create --topic payments --partitions 6 --replication-factor 3 --config min.insync.replicas=2',
        'kafka-topics --create --topic payments.DLQ --partitions 1 --replication-factor 3',
        'kafka-topics --create --topic driver-state --partitions 6 --replication-factor 3 --config cleanup.policy=compact',
        'kafka-console-producer --topic ride-events --count 200 --key ride-{i}     phir     sim broker kill 2',
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '👑 FINAL INTERVIEW: "RideNow ka messaging architecture design karo. 10 minute."',
        choices: [
          'Sab kuch Kafka par daal dunga',
          'Main data streams ko unki requirements se alag karunga: (1) Driver location pings (1.25 lakh/s, mobile, weak network) → MQTT QoS 0, phir bridge se Kafka mein analytics ke liye — kyunki MQTT constrained devices ke liye bana hai aur purana location bekaar hai. (2) Ride events → Kafka, key=rideId (per-ride ordering + high cardinality), ~25-40 partitions (throughput math), RF=3 + acks=all + min.insync=2, aur har consumer (payments/analytics/notifications) ka alag group. (3) Payment aur notification JOBS → queue (SQS/RabbitMQ) — per-message retry aur DLQ chahiye, ek fail baaki ko block na kare. (4) DB+Kafka dual-write se bachne ke liye transactional outbox. (5) Reliability: idempotent consumers, retry with jitter, circuit breaker gateway ke liye, DLQ + monitoring. (6) Analytics: event-time windowing (drivers offline hote hain). (7) DR: MM2 with offset translation, RPO/RTO per data-class define, aur game-day se TEST. Aur main yeh bhi bataunga ki isme nayi problems kya aayengi — duplicates (isliye idempotency), eventual consistency (isliye UI "processing" dikhaye), aur celebrity/hot-key risk (isliye key selection soch-samajh kar).',
          'Microservices bana dunga',
          'Ek badi queue laga dunga',
        ],
        answer: 1,
        explain: '👑 Yeh poore Kafka Gurukul ka capstone jawab hai. Dhyan do ki isme (1) har data stream ke liye ALAG tool uski requirement se, (2) concrete numbers aur configs with justification, (3) reliability patterns, (4) consistency (outbox), (5) DR — aur sabse zaroori (6) apne hi design ki NAYI problems. Yahi ek Mahaguru ko ek beginner se alag karta hai.',
      },
    ],
  },
  success: [
    'Maine ek poora real-world messaging platform design kiya',
    'Maine har data stream ke liye sahi tool chuna aur justify kiya',
    'Maine partition count, keys, durability aur DR sab decide kiya',
    'Main apne design ki nayi problems khud bata sakta hoon',
    '👑 Maine Kafka Gurukul poora kar liya',
  ],
  homework: `<p><b>Aakhri homework — apna khud ka system design karo:</b></p>
    <pre>Koi ek system chuno (jo tum banate ho ya banana chahte ho):
   • food delivery · ticket booking · chat app · IoT platform
   • ya tumhara asli project

Aur yeh document likho:

1. DATA STREAMS — har ek ke liye:
   naam · volume · ordering requirement · replay chahiye? · tool

2. TOPICS — har ek ke liye:
   partitions (hisaab dikhao) · key · RF · acks · min.insync
   · cleanup.policy (delete/compact) · retention

3. CONSUMERS:
   kaun kaun se groups · kya karte hain · lag SLO

4. RELIABILITY:
   idempotency kaise · retry policy · DLQ · circuit breaker kahan

5. CONSISTENCY:
   koi dual-write? → outbox kahan chahiye
   EOS kahan chahiye (agar kahin)

6. FAILURE MODES — yeh sabse zaroori hissa hai:
   "agar X gir jaaye toh kya hoga?" har component ke liye likho
   aur uska mitigation

7. NAYI PROBLEMS jo tumhare design ne banayi:
   (duplicates? eventual consistency? hot keys? cost?)</pre>
    <p><b>Section 7 hi asli imtihaan hai.</b> Koi bhi design nayi problems banata hai —
    unhe khud pehchanna aur likhna hi woh cheez hai jo interview mein aur asli kaam mein
    senior banati hai. Yahi poore Kafka Gurukul ka nichod hai. 🛕</p>`,
  cards: [
    { front: 'System design ka pehla kadam?', back: 'Data streams ko unki <b>requirements se alag</b> karo — har stream ka volume, ordering, replay, aur per-message control alag hota hai. Ek tool sab ke liye chunna sabse aam galti hai.' },
    { front: 'RideNow: location pings ke liye kaunsa tool aur kyun?', back: '<b>MQTT</b> — mobile devices, weak network, bahut high frequency, chhote packets. QoS 0 kaafi (purana location bekaar). Phir <b>bridge se Kafka</b> mein analytics/replay ke liye — MQTT storage nahi hai.' },
    { front: 'Payment jobs Kafka par kyun nahi?', back: 'Per-message <b>retry aur DLQ</b> chahiye, aur ek fail hua payment baaki ko block na kare. Kafka mein offset ek line mein badhta hai (poison blocks partition). <b>Queue model</b> (SQS/RabbitMQ) yeh natively deta hai.' },
    { front: 'Downstream gateway dheema hai, lag badh raha hai — consumers badhao?', back: '<b>Nahi!</b> Bottleneck downstream hai — zyada consumers use aur tez maarenge (congestion collapse). Sahi: <b>circuit breaker + retry with jitter + rate limiting + DLQ</b>. Lag badhega par messages log mein safe hain.' },
    { front: 'Poore course ka nichod?', back: '1) Har concept ek asli <b>PROBLEM</b> se shuru hota hai<br>2) Har solution ki ek <b>KEEMAT</b> hoti hai<br>3) Senior wahi hai jo apne design ki <b>NAYI problems khud bataye</b> 👑' },
  ],
},

]);
