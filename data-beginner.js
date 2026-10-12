/* =====================================================================
   Kafka Gurukul — 🌱 TRACK 1: Sandesh Shishya Path
   9 levels — "queue chahiye hi kyun" se lekar pehle poore pipeline tak.

   Har level ka lesson 3 cards ka hai (yeh app ka sabse bada niyam hai):
     Card 1 — 😰 DARD      : ek asli problem, asli numbers ke saath
     Card 2 — 🪜 SEEDHI    : naive → better → real, har rung KYUN toota
     Card 3 — ⚖️ NAYI MUSIBAT: har solution nayi problem laata hai

   registerLevels additive hai — naya level kisi bhi file se add kar sakte ho.
   ===================================================================== */

KQ.registerLevels('beginner', [

/* =====================================================================
   1. SYNC KI MAUT — queue chahiye hi kyun?
   ===================================================================== */
{
  id: 'sync-death',
  icon: '📮',
  title: 'Sync ki Maut — Queue chahiye hi kyun?',
  badge: 'Coupling Todne Wala',
  objectives: [
    'Samjho ki seedhi HTTP call kab aur KYUN maar deti hai',
    'Thread pool exhaustion ka cascade apni aankhon se dekho',
    'Jaano ki queue teen cheezein deti hai: decoupling, buffering, retry',
    'Yeh bhi jaano ki queue MUFT nahi hai — kya keemat deni padti hai',
  ],
  lesson: [
    `<h3>😰 Dard: Sale ke din checkout mar gaya</h3>
     <p>Ek e-commerce app. <code>OrderService</code> jab order banati hai toh
     seedha <b>sync HTTP call</b> karti hai:</p>
     <pre>POST /orders
  └─ payment-service par call    (~800ms)
  └─ inventory-service par call  (~200ms)
  └─ email-service par call      (~1500ms  ← SMTP dheema hai)
  └─ analytics-service par call  (~300ms)
────────────────────────────────────────
  Total: ~2.8 second har order</pre>
     <p>Normal din mein 200 orders/sec — sab theek. Sale ke din
     <b>3,000 orders/sec</b> aa gaye.</p>
     <p>OrderService ke paas 400 threads the. Har thread 2.8s tak <b>BLOCK</b> raha.
     Matlab capacity = 400 ÷ 2.8 = <b>sirf 142 orders/sec</b>. Baaki 2,858 requests
     queue mein khade hokar timeout hone lage.</p>
     <div class="war-story"><b>⚡ Aur asli qatal yeh tha:</b> email-service (sabse
     kam zaroori cheez!) dheema hua, aur usne <b>checkout hi band karwa diya</b>.
     Order lena email bhejne par depend kar gaya tha. Customer paisa dena chahta tha,
     system lene se mana kar raha tha — kyunki SMTP server busy tha. 🤦</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Timeout ghata do — 500ms kar do"</b><br>
     Ab payment call 800ms leti thi, toh woh हमेशा timeout hone lagi. Customer ka
     paisa <b>kat gaya</b> par order <b>ban hi nahi</b> — kyunki humne connection
     kaat diya, par payment-service ne kaam poora kar diya tha. Ab refund tickets
     ka pahaad. Yeh pehle se bhi bura hai.</p>

     <p><b>⚠️ Better: "Thread pool 400 se 4000 kar do"</b><br>
     Thodi der chala. Phir har thread ka apna DB connection chahiye tha →
     DB ka connection pool (max 500) khatam. Ab <b>database</b> gir gaya, jo
     baaki 12 services bhi use karti thin. Problem badi ho gayi, hal nahi hui.</p>

     <p><b>✅ Real: Beech mein ek LOG rakh do</b><br>
     OrderService sirf ek kaam kare — order ko <b>durably likh do</b> aur turant
     <code>201 Created</code> de do. Baaki kaam (payment, email, analytics)
     alag consumers baad mein karein:</p>
     <pre>POST /orders
  └─ orders topic par likho   (~2ms)  ✅ turant jawab
       ├─→ payment-worker    (apni raftaar se)
       ├─→ email-worker      (dheema hai? koi baat nahi)
       └─→ analytics-worker  (gir gaya? messages log mein safe hain)</pre>
     <p>Ab checkout <b>2.8s se 2ms</b> ka ho gaya. Email service poore ek ghante
     down rahi — <b>kisi customer ko pata bhi nahi chala</b>. Woh wapas aayi aur
     peeche ka saara backlog padh liya.</p>`,

    `<h3>⚖️ Nayi musibat: queue muft nahi hai</h3>
     <p>Jo bhi tumne jeeta, uski keemat hai. Ise pehle din se jaan lo:</p>
     <ul>
       <li><b>"Ho gaya" ab jhooth hai.</b> Customer ko <code>201 Created</code> mila,
       par order abhi tak process nahi hua. Agar payment 30 second baad fail hui toh
       user ko kaise bataoge? Ab tumhe status tracking chahiye.</li>
       <li><b>Ordering ki guarantee gayi.</b> <code>order-created</code> aur
       <code>order-cancelled</code> ulte kram mein process ho sakte hain.</li>
       <li><b>Duplicates aayenge.</b> Consumer crash hua commit se pehle → wahi message
       dobara. Bina care ke customer <b>do baar charge</b> ho jaayega.</li>
       <li><b>Ek nayi cheez jo khud gir sakti hai.</b> Ab broker bhi tumhari
       zimmedari hai — usko monitor, scale aur backup karna padega.</li>
     </ul>
     <p>Isliye asli sawal "queue lagayein ya nahi" nahi hai. Asli sawal hai:
     <b>"is call ka jawab user ko ABHI chahiye ya nahi?"</b></p>
     <p>💳 Payment approve hua ya nahi — <b>abhi chahiye</b> → sync raho.<br>
     📧 Email bhejna, analytics likhna, thumbnail banana — <b>abhi nahi chahiye</b> → async karo.</p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Sync ya Async?',
      instructions: 'Har kaam ko sahi jagah bhejo. Sawal ek hi hai: iska jawab user ko ABHI chahiye ya nahi?',
      bins: [
        { id: 'sync', label: '⚡ Sync rakho (jawab abhi chahiye)' },
        { id: 'async', label: '📮 Async karo (queue mein daal do)' },
      ],
      items: [
        { text: 'Credit card authorize karna', bin: 'sync',
          explain: 'User screen par khada hai aur "approved ya declined" jaanna chahta hai. Ise async kiya toh user ko pata hi nahi chalega ki order hua ya nahi.' },
        { text: 'Order confirmation email bhejna', bin: 'async',
          explain: 'Email 5 second baad jaaye ya 5 minute baad — kisi ko farak nahi padta. Par agar SMTP dheema hua toh isne poora checkout rok diya. Classic galti.' },
        { text: 'Login par password verify karna', bin: 'sync',
          explain: 'Bina jawab ke user ko andar bhej hi nahi sakte. Sync ke alawa koi chaara nahi.' },
        { text: 'Uploaded video ka thumbnail banana', bin: 'async',
          explain: 'Video processing mein minutes lagte hain. Sync kiya toh HTTP request kabhi ki timeout ho chuki hogi. YouTube bhi "processing..." dikhata hai.' },
        { text: 'Stock (inventory) reserve karna', bin: 'sync',
          explain: 'Yeh tricky hai. Agar async kiya aur stock khatam nikla, toh order confirm karne ke BAAD cancel karna padega — bahut kharab experience. Isliye reservation sync.' },
        { text: 'Data warehouse mein analytics event bhejna', bin: 'async',
          explain: 'Analytics kabhi bhi user request ko block nahi karni chahiye. Iske liye user intezaar kyun kare?' },
        { text: 'Warehouse ko shipping label print karne bolna', bin: 'async',
          explain: 'Warehouse mein maal ghante baad uthega. Yeh checkout ke rehne ka koi karan hi nahi.' },
        { text: 'Order ke liye fraud check', bin: 'async',
          explain: 'Zyadatar bade e-comm mein yeh async hota hai — order le lo, fraud check background mein, shak ho toh baad mein rok do. Sync karoge toh 2 second aur lag jaayenge har order par.' },
      ],
    },
    {
      type: 'order',
      title: '🔢 Cascade failure ka kram',
      instructions: 'Sale ke din jo hua tha, use sahi tarteeb mein lagao — email service dheemi hone se poora checkout kaise mara.',
      steps: [
        { text: '📧 Email service dheemi ho gayi — 1.5s se 9s per call',
          explain: 'Shuruaat hamesha kisi ek non-critical cheez se hoti hai. SMTP provider rate-limit kar raha tha.' },
        { text: '🧵 OrderService ke threads email ka wait karte hue block ho gaye',
          explain: 'Sync call ka matlab hai thread tab tak baitha hai jab tak jawab na aaye. 400 threads, har ek 9 second phansa hua.' },
        { text: '🚫 Thread pool khatam — naye requests ko thread hi nahi mila',
          explain: 'Yahi "thread pool exhaustion" hai. Server zinda hai par kuch kar nahi sakta.' },
        { text: '⏱️ Load balancer ko health check ka jawab nahi mila',
          explain: 'Health check bhi ek thread maangta hai. Thread hai hi nahi, toh health check bhi timeout.' },
        { text: '💀 Load balancer ne OrderService ko "unhealthy" maan ke hata diya',
          explain: 'Ab bache hue healthy servers par aur zyada load — yeh cascade ka sabse khatarnaak mod hai.' },
        { text: '🔥 Bacha hua traffic baaki servers par gaya, woh bhi usi tarah mare',
          explain: 'Isko "cascading failure" kehte hain. Ek chhoti si dheemi cheez ne poora checkout gira diya.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Socho zara',
      questions: [
        {
          q: 'Email service ko queue ke peeche daalne ka SABSE bada fayda kya hai?',
          choices: [
            'Email tezi se jaayenge',
            'Email service down hone par bhi checkout chalta rahega',
            'Email bhejna sasta ho jaayega',
            'Emails ka kram theek rahega',
          ],
          answer: 1,
          explain: 'Asli jeet hai FAILURE ISOLATION. Email tez nahi honge (shayad thode dheeme hi hon) — par ab email service ka girna checkout ko nahi giraayega. Queue speed ke liye nahi, ALAG-ALAG rakhne ke liye hai.',
        },
        {
          q: 'Payment authorization ko async karna aam taur par galat kyun hai?',
          choices: [
            'Payment gateways async support nahi karte',
            'Bahut dheema ho jaayega',
            'User ko turant pata hona chahiye ki card chala ya nahi — warna woh "order ho gaya" samajh ke chala jaayega jabki card decline tha',
            'Payment data queue mein daalna illegal hai',
          ],
          answer: 2,
          explain: 'Yeh business decision hai, technical nahi. User ko turant jawab chahiye. Async karoge toh "aapka order ho gaya" bolna padega, aur 10 second baad "sorry, card decline" — yeh sabse kharab experience hai.',
        },
        {
          q: 'Ek team ne queue lagayi aur ab customers ko kabhi kabhi DO confirmation email jaate hain. Kyun?',
          choices: [
            'Queue mein bug hai, replace kar do',
            'At-least-once delivery — consumer ne email bheja par offset commit hone se pehle crash ho gaya, toh restart par wahi message dobara mila',
            'Producer ne do baar bheja',
            'Do consumers chal rahe hain',
          ],
          answer: 1,
          explain: 'Yeh queue ki nayi musibat hai jo humne card 3 mein padhi. Zyadatar brokers at-least-once dete hain: message kam se kam ek baar aayega, kabhi kabhi zyada baar. Iska ilaaj idempotency hai — hum Track 2 mein karenge.',
          context: '[worker] email bheja ✉️  order=8821\n[worker] offset commit karne se pehle CRASH 💥\n[worker] restart... offset 8821 se hi shuru\n[worker] email bheja ✉️  order=8821   ← dobara!',
        },
      ],
    },
    {
      type: 'lab',
      title: '🎛️ Lab: Sync ki deewar apni aankhon se dekho',
      instructions: 'Yeh tumhara pehla Pipeline Lab hai. Knobs ghumao, ▶ Run dabao, aur natija banate dekho.',
      scenario: `<b>Sale ka din.</b> 1,200 orders/sec aa rahe hain. Tumhare paas
        consumers hain jo har order ko process karte hain. Abhi har order
        <b>120ms</b> leta hai (kyunki woh 4 services ko sync call kar raha hai).`,
      goal: `Consumers itne badhao ki lag control mein aa jaaye —
        <b>peak lag 20,000 se neeche</b> aur aakhir mein pipeline catch up kar le.
        Phir sochо: kya sirf consumers badhana hi asli hal hai?`,
      duration: 90,
      preset: { producerRate: 1200, consumers: 2, processMs: 120, failRate: 0,
                downstreamCapacity: 5000, retry: 'none', dlq: false, prefetch: 200 },
      knobs: ['consumers', 'processMs'],
      charts: ['lag', 'throughput'],
      success: r => r.peakLag < 20000 && r.caughtUp,
      verdict: r => {
        const cap = Math.round(r.cfg.consumers * (1000 / r.cfg.processMs));
        return `📐 <b>Hisaab:</b> ${r.cfg.consumers} consumers × (1000ms ÷ ${r.cfg.processMs}ms)
          = <b>${cap} msg/s</b> ki capacity, aur aa rahe hain <b>${r.cfg.producerRate} msg/s</b>.
          ${cap >= r.cfg.producerRate
            ? '✅ Capacity demand se zyada hai — isliye lag nahi badhta.'
            : `❌ Capacity kam hai, isliye har second <b>${r.cfg.producerRate - cap}</b> messages backlog mein jud rahe hain. Lag hamesha badhta rahega.`}`;
      },
      successText: `🎉 Tumne abhi "Little's Law" ka sabse kaam ka roop use kiya:
        <b>capacity = consumers ÷ processing time</b>.
        <br><br>Ab dhyan do: tumne processing time ghata ke bhi jeeta hoga.
        Yeh SABSE saste jeet hoti hai — 120ms se 30ms karna = 4× consumers lagane ke barabar,
        par server ka kharcha zero. Isliye asli duniya mein pehle <b>processing time</b>
        dekho, phir consumers badhao.
        <br><br>Aur yaad rakho: queue ne demand ko kam nahi kiya — usne bas usse
        <b>rok ke rakha</b> taaki tumhara system apni raftaar se kaam kar sake. Isi ko
        <b>load leveling</b> kehte hain.`,
      hints: [
        'Capacity ka formula: consumers × (1000 ÷ processMs) messages per second.',
        '1200 msg/s ke liye: agar processMs 120 hai toh 144 consumers chahiye — par slider max 24 hai! Matlab akela consumers badhana kaafi nahi.',
        'processMs ko 20 karo (matlab 4 sync calls hata ke async kiye), phir 24 consumers = 1200/s. Bilkul fit.',
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Humare checkout mein 5 downstream calls hain aur woh dheema hai. Tum kya karoge?"',
        choices: [
          'Sab kuch queue mein daal do',
          'Pehle poochunga ki kaunse call ka jawab user ko ABHI chahiye. Payment/inventory sync rahenge; email/analytics/fraud-check async. Sirf non-critical cheezein hatao.',
          'Thread pool badha do',
          'Microservices ko wapas monolith bana do',
        ],
        answer: 1,
        explain: 'Interviewer dekhna chahta hai ki tum andhadhund "sab async" nahi karte. Sahi jawab hamesha CLASSIFICATION se shuru hota hai: kaunsa call user ke jawab ka hissa hai aur kaunsa nahi. "Sab kuch queue mein" utna hi galat hai jitna "sab kuch sync".',
      },
      {
        q: '🧑‍💼 "Async karne ke baad tumhare system mein kya naya toot sakta hai jo pehle nahi tootta tha?"',
        choices: [
          'Kuch nahi — async hamesha behtar hai',
          'Duplicate processing, out-of-order events, aur "user ko success bola par kaam baad mein fail ho gaya" — teeno naye failure modes hain',
          'Sirf network latency badhegi',
          'Database dheema ho jaayega',
        ],
        answer: 1,
        explain: 'Yahi sawal junior aur senior ko alag karta hai. Junior fayde ginata hai; senior turant NAYI failure modes batata hai. Async ne problem hataayi nahi — usko BADAL diya hai.',
      },
      {
        terminal: {
          title: '🖥️ Interview: pehla topic banao',
          goal: `🧑‍💼 Interviewer: "Chalo practical karte hain. Humein orders ke liye ek topic
            chahiye. Traffic 1,200 msg/s hai, aur hum chahte hain ki kam se kam
            <b>6 consumers</b> parallel mein kaam kar sakein. Ek broker gir jaaye toh bhi
            data na jaaye.<br><br>
            Ek topic banao jiska naam <code>orders</code> ho, jisme <b>kam se kam 6 partitions</b>
            hon aur <b>replication factor 3</b> ho. Phir mujhe <code>--describe</code> karke dikhao."`,
          cluster: { brokers: 3 },
          tasks: [
            { text: 'orders topic banao (≥6 partitions, RF=3)',
              check: s => s.hasTopic('orders') && s.topics.orders.partitions.length >= 6 && s.topics.orders.rf === 3 },
            { text: 'describe karke dikhao', check: s => s.history.some(h => /kafka-topics.*--describe/.test(h)) },
          ],
          success: s => s.hasTopic('orders') && s.topics.orders.partitions.length >= 6 &&
                        s.topics.orders.rf === 3 && s.history.some(h => /kafka-topics.*--describe/.test(h)),
          successText: `Shabaash! Dhyan do ki tumne <b>6 partitions</b> kyun banaye —
            kyunki <b>partition hi parallelism ki limit hai</b>. 3 partitions par 6 consumers
            lagate toh 3 consumers khaali baithe rehte. Aur RF=3 ka matlab har partition ki
            3 copies — ek broker gaya toh bhi data safe.`,
          hints: [
            'Command: kafka-topics --create --topic <naam> --partitions <n> --replication-factor <r>',
            'kafka-topics --create --topic orders --partitions 6 --replication-factor 3',
          ],
        },
      },
    ],
  },
  success: [
    'Main bata sakta hoon ki koi call sync honi chahiye ya async',
    'Main thread-pool exhaustion ka cascade samjha sakta hoon',
    'Main jaanta hoon ki queue teen cheezein deti hai: decoupling, buffering, retry',
    'Main queue ki KEEMAT bhi bata sakta hoon: duplicates, ordering, eventual consistency',
  ],
  homework: `<p>Apne laptop par ek asli Kafka chalao (Docker chahiye):</p>
    <pre>docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \\
  --bootstrap-server localhost:9092 \\
  --create --topic orders --partitions 3</pre>
    <p>Phir Node se ek producer likho:</p>
    <pre>npm i kafkajs

const { Kafka } = require('kafkajs');
const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer();
await producer.connect();
await producer.send({ topic: 'orders',
  messages: [{ key: 'user-1', value: JSON.stringify({ amount: 499 }) }] });</pre>
    <p><b>Asli experiment:</b> ek consumer likho jo har message par
    <code>await sleep(2000)</code> kare. Phir 100 messages bhejo. Dekho ki
    producer turant khatam ho jaata hai par consumer 200 second leta hai —
    <b>yahi decoupling hai, apni aankhon se.</b></p>`,
  cards: [
    { front: 'Queue ka asli fayda kya hai — speed ya kuch aur?', back: 'Speed NAHI. Asli fayda <b>decoupling</b> hai — email service girne par checkout nahi girta. Async akser thoda DHEEMA hota hai end-to-end, par woh failures ko alag-alag rakhta hai.' },
    { front: 'Thread pool exhaustion kaise hota hai?', back: 'Sync call mein thread jawab aane tak BLOCK rehta hai. Downstream dheema hua → saare threads waiting → naye request ke liye thread hi nahi bacha → server zinda hai par kuch nahi kar sakta. Health check bhi fail ho jaata hai.' },
    { front: 'Kaise decide karein ki call sync ho ya async?', back: 'Ek hi sawal: <b>"iska jawab user ko ABHI chahiye?"</b> Haan (payment approve/decline) → sync. Nahi (email, analytics, thumbnail) → async.' },
    { front: 'Async karne se kaunsi 3 nayi problems aati hain?', back: '1) <b>Duplicates</b> (at-least-once delivery)<br>2) <b>Ordering</b> ki guarantee gayi<br>3) <b>Eventual consistency</b> — user ko "ho gaya" bola par kaam abhi baaki hai' },
    { front: 'Consumer capacity ka formula?', back: '<code>capacity = consumers × (1000 ÷ processMs)</code> messages/sec. Agar yeh producer rate se kam hai toh lag <b>hamesha</b> badhta rahega. processMs ghatana akser consumers badhane se sasta hota hai.' },
  ],
},

/* =====================================================================
   2. BROKER KI ANATOMY
   ===================================================================== */
{
  id: 'broker',
  icon: '🏢',
  title: 'Broker ki Anatomy — ek log jo sab kuch sambhalta hai',
  badge: 'Log Ka Pujari',
  objectives: [
    'Samjho ki Kafka messages ko delete kyun nahi karta',
    'Queue aur Log ka bunyaadi farak jaano',
    'Jaano ki ek broker lakhon messages/sec kaise nikaal leta hai (sequential I/O + zero copy)',
    'Broker, topic, partition aur segment ka rishta samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "Woh message hi gayab ho gaya"</h3>
     <p>Ek payments team ne classic message queue (RabbitMQ-style) use ki. Consumer
     message uthata, kaam karta, aur <b>ack</b> deta — ack milte hi broker message
     <b>delete</b> kar deta.</p>
     <p>Ek raat unke fraud-detection consumer mein bug aa gaya. 4 ghante tak usne
     har message ko "process" kiya, ack diya, aur <b>chup-chaap galat result</b> likha.
     Bug pakda gaya. Team ne bola: "koi baat nahi, un 4 ghante ke messages dobara
     chala lete hain."</p>
     <div class="war-story"><b>⚡ Par woh messages the hi nahi.</b> Ack ho chuke the,
     delete ho chuke the. 4 ghante ka fraud data hamesha ke liye gaya.
     Unhe production database se reverse-engineer karna pada — 3 din lage,
     aur poora data phir bhi wapas nahi aaya.</div>
     <p>Ek aur team ko doosri taraf ka dard mila: unhe wahi order events
     <b>teen alag systems</b> ko chahiye the (billing, analytics, search index).
     Classic queue mein ek message ek hi consumer ko milta hai — toh unhone
     <b>teen alag queues</b> banayi aur producer se teeno mein likhwaya.
     Ab ek naya consumer chahiye? Producer ka code badlo. 😩</p>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Consume kiye messages ko ek backup table mein likh do"</b><br>
     Ab har message do jagah likha ja raha hai. Dono ke beech consistency ka masla,
     double storage, aur backup table khud ek bottleneck ban gaya.</p>

     <p><b>⚠️ Better: "Har consumer ke liye alag queue bana do (fanout)"</b><br>
     Kaam karta hai, par har naye consumer par <b>producer ka code badalna</b> padta hai.
     Producer ko pata hona chahiye ki uske kitne sunne waale hain — yeh ulta coupling hai,
     jisse bhaagne ke liye humne queue lagayi thi.</p>

     <p><b>✅ Real: Message ko delete hi mat karo. Log rakho.</b><br>
     Kafka ka bunyaadi faisla: broker ek <b>append-only log</b> hai, ek mailbox nahi.</p>
     <pre>Partition ek file hai jisme sirf END mein likha jaata hai:

  offset:   0    1    2    3    4    5    6    7
          ┌────┬────┬────┬────┬────┬────┬────┬────┐
          │ m0 │ m1 │ m2 │ m3 │ m4 │ m5 │ m6 │ m7 │ ← naya yahan judta hai
          └────┴────┴────┴────┴────┴────┴────┴────┘
                  ▲              ▲         ▲
             analytics       billing    search
              (offset 2)    (offset 5) (offset 7)</pre>
     <p>Har consumer group ka <b>apna bookmark (offset)</b> hai. Padhne se message
     mitta nahi. Isliye:</p>
     <ul>
       <li>Naya consumer chahiye? Bas offset 0 se padhna shuru karo — <b>producer ko
       pata bhi nahi chalega</b>.</li>
       <li>Bug aa gaya? Offset peeche kar do, dobara chala lo. <b>Time travel.</b></li>
       <li>Ek hi data, 10 alag consumers, koi copy nahi.</li>
     </ul>`,

    `<h3>⚖️ Nayi musibat + speed ka raaz</h3>
     <p><b>Nayi musibat:</b> Agar delete nahi karte, toh disk bharegi nahi? Haan!
     Isliye Kafka mein <b>retention</b> hai (default 7 din) — waqt ya size ke hisaab
     se purana data udta hai. Aur yahin se ek naya 3AM incident paida hota hai:
     agar tumhara consumer 8 din band raha, toh uska offset ab <b>log mein hai hi nahi</b>
     (<code>OffsetOutOfRangeException</code>). Yeh hum Track 2 mein khelenge.</p>

     <h4>🚀 Aur yeh itna tez kaise hai?</h4>
     <p>Log hone ki wajah se hi teen bade fayde milte hain:</p>
     <ul>
       <li><b>Sequential I/O:</b> Sirf file ke end mein likhna hota hai. Disk par
       sequential write random write se <b>100× tez</b> hai — HDD par bhi.</li>
       <li><b>Page cache:</b> Kafka data ko apni JVM memory mein nahi rakhta — OS ke
       page cache par chhod deta hai. Recent messages RAM se hi mil jaate hain.</li>
       <li><b>Zero-copy (<code>sendfile</code>):</b> Disk se network tak data bina
       application memory mein aaye seedha chala jaata hai. Copy aur context-switch bachte hain.</li>
     </ul>
     <div class="war-story"><b>⚡ Asli pemaana:</b> LinkedIn (jahan Kafka bana tha) ne
     2019 mein roz <b>7 trillion messages</b> handle kiye. Yeh koi jaadu nahi tha —
     yeh "sirf file ke end mein likho aur OS ko apna kaam karne do" ka natija tha.</div>`,
  ],
  stages: [
    {
      type: 'match',
      title: '🔗 Log vs Queue',
      instructions: 'Har behaviour ko sahi model se jodo.',
      pairs: [
        { left: 'Padhne ke baad message mit jaata hai', right: '📬 Classic Queue (RabbitMQ/SQS)',
          explain: 'Ack ka matlab hai "delete kar do". Isi liye replay mumkin nahi.' },
        { left: 'Padhne se message nahi mitta, offset aage badhta hai', right: '📜 Log (Kafka)',
          explain: 'Consumer sirf apna bookmark aage karta hai. Data wahin rehta hai retention tak.' },
        { left: 'Naya consumer purana data bhi padh sakta hai', right: '📜 Log (Kafka)',
          explain: 'Offset 0 se shuru karo — poora itihaas mil jaayega. Producer ko badalne ki zaroorat nahi.' },
        { left: 'Har message ek hi consumer ko milta hai', right: '📬 Classic Queue (RabbitMQ/SQS)',
          explain: 'Competing consumers pattern. Fanout ke liye alag exchange/queue banani padti hai.' },
        { left: 'Per-message ack aur selective retry aasan hai', right: '📬 Classic Queue (RabbitMQ/SQS)',
          explain: 'Yeh queue ki asli taakat hai! Kafka mein ek message ko akele "nack" karna mushkil hai — offset toh ek hi aage badhta hai.' },
        { left: 'Purana data waqt ke hisaab se apne aap udta hai', right: '📜 Log (Kafka)',
          explain: 'retention.ms — default 7 din. Isi ki wajah se disk nahi bharti, par isi se OffsetOutOfRange bhi hota hai.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Log ka dimaag',
      questions: [
        {
          q: 'Kafka HDD par bhi itna tez kyun hai jabki hum sochte hain ki disk dheemi hoti hai?',
          choices: [
            'Kyunki sab kuch RAM mein rakhta hai',
            'Kyunki woh sirf SEQUENTIAL likhta hai (file ke end mein) — sequential disk I/O random se ~100× tez hai, aur OS page cache + zero-copy sendfile use karta hai',
            'Kyunki messages ko compress karta hai',
            'Kyunki SSD zaroori hai',
          ],
          answer: 1,
          explain: 'Yeh Kafka ka sabse khoobsurat insight hai. "Disk dheemi hai" adhoora sach hai — RANDOM disk access dheema hai. Sequential append RAM ke aas paas pahunch jaata hai. Kafka ne apna poora design isi ek baat par khada kiya.',
        },
        {
          q: 'Ek nayi analytics team ko pichle 3 din ke saare order events chahiye. Kafka mein kya karna padega?',
          choices: [
            'Producer se bolo ki ek naye topic mein bhi likhe',
            'Kuch nahi — bas naya consumer group banao aur --from-beginning se padho. Producer ko pata bhi nahi chalega.',
            'Purane messages ko backup se restore karo',
            'Topic ko duplicate karo',
          ],
          answer: 1,
          explain: 'Yahi log model ki superpower hai. Consumers ek doosre se aur producer se poori tarah azad hain. Classic queue mein iske liye producer ka code badalna padta.',
        },
        {
          q: 'Tumhara consumer 10 din band raha. Retention 7 din hai. Chalu karne par kya hoga?',
          choices: [
            'Wahin se chalu hoga jahan chhoda tha',
            'Uska committed offset ab log mein hai hi nahi → OffsetOutOfRangeException → auto.offset.reset ke hisaab se ya toh shuruaat se sab dobara padhega (duplicates) ya seedha latest par chala jaayega (beech ka data skip)',
            'Kafka apne aap retention badha dega',
            'Consumer chalu hi nahi hoga',
          ],
          answer: 1,
          explain: 'Yeh asli 3AM incident hai. Dono raaste dukhdayi hain: earliest = sab dobara process (duplicate emails/charges), latest = data chup-chaap gum. Isi liye lag ka alert lagana zaroori hai.',
          context: 'ERROR [Consumer clientId=analytics] Offset 45231 is out of range\n       for partition orders-2 (logStartOffset: 891004)\nINFO  Resetting offset to LATEST — 845,773 messages SKIPPED 💀',
        },
      ],
    },
    {
      type: 'terminal',
      title: '🖥️ Mission: log ke andar jhaanko',
      instructions: 'Ab asli broker par haath chalao. Yeh mock output nahi hai — andar sach mein ek log chal raha hai.',
      goal: `Ek <code>payments</code> topic hai jisme kuch messages pade hain.
        Tumhe <b>log ke andar dekhna hai</b> — offsets, keys, sab kuch.<br><br>
        1. Cluster ki halat dekho (<code>status</code>)<br>
        2. <code>payments</code> topic ko describe karo<br>
        3. Log ko dump karo aur dekho ki offsets kaise 0 se badhte hain<br>
        4. Ek consumer group se shuruaat se padho aur dekho ki <b>padhne ke baad bhi messages wahin hain</b>`,
      cluster: {
        brokers: 3,
        topics: {
          payments: { partitions: 2, rf: 3, messages: [
            { key: 'user-1', value: 'charge:499' },
            { key: 'user-2', value: 'charge:1299' },
            { key: 'user-1', value: 'refund:499' },
            { key: 'user-3', value: 'charge:250' },
            { key: 'user-2', value: 'charge:899' },
          ] },
        },
      },
      tasks: [
        { text: 'status dekho', check: s => s.history.some(h => /^status/.test(h)) },
        { text: 'payments topic describe karo', check: s => s.history.some(h => /kafka-topics.*--describe/.test(h)) },
        { text: 'log dump karke offsets dekho', check: s => s.history.some(h => /^(dump|kafka-run-class.*DumpLog)/.test(h)) },
        { text: 'shuruaat se messages padho', check: s => s.history.some(h => /kafka-console-consumer.*--from-beginning/.test(h)) },
      ],
      success: s => s.history.some(h => /^status/.test(h)) &&
                    s.history.some(h => /kafka-topics.*--describe/.test(h)) &&
                    s.history.some(h => /^(dump|kafka-run-class.*DumpLog)/.test(h)) &&
                    s.history.some(h => /kafka-console-consumer.*--from-beginning/.test(h)),
      successText: `Bahut khoob! Do cheezein dhyan se dekho jo tumne abhi ki:<br><br>
        1️⃣ <b>Messages padhne ke baad bhi log mein hain.</b> Dobara
        <code>dump payments</code> karo — sab wahin milenge. Yeh queue nahi, LOG hai.<br><br>
        2️⃣ <b><code>user-1</code> ke dono messages EK hi partition par gaye.</b>
        Yeh ittefaq nahi — Kafka key ka murmur2 hash le kar partition chunta hai,
        isliye ek key hamesha ek hi partition par jaati hai. <b>Yahi ordering ki
        guarantee ki jadd hai</b> (level 6 mein poora khelenge).`,
      hints: [
        'status · kafka-topics --describe --topic payments · dump payments',
        'Padhne ke liye: kafka-console-consumer --topic payments --from-beginning --property print.key=true',
      ],
    },
    {
      type: 'fill',
      title: '📄 Topic banane ki command',
      instructions: 'Ek topic banao: naam events, 6 partitions, aur 3 copies har partition ki.',
      template: 'kafka-topics.sh --bootstrap-server localhost:9092 \\\n  --{0} \\\n  --topic events \\\n  --{1} 6 \\\n  --{2} 3',
      blanks: [
        { answer: 'create', explain: 'Kafka CLI mein har command ek action flag leti hai: --create, --describe, --alter, --delete, --list.' },
        { answer: 'partitions', explain: 'Partitions = parallelism ki limit. 6 partitions matlab zyada se zyada 6 consumers ek group mein kaam kar sakte hain.' },
        { answer: 'replication-factor', explain: 'RF=3 matlab har partition ki 3 copies alag brokers par. 2 broker gir jaayein toh bhi data safe.' },
      ],
      tokens: ['create', 'partitions', 'replication-factor', 'produce', 'copies', 'replicas', 'shards', 'consumers'],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Kafka aur RabbitMQ mein FUNDAMENTAL farak kya hai? (koi feature list nahi chahiye)"',
        choices: [
          'Kafka tez hai',
          'Kafka ek durable LOG hai jahan consumers apna offset aage badhate hain aur data padhne se mitta nahi; RabbitMQ ek broker hai jo messages ko route karta hai aur ack milte hi DELETE kar deta hai. Isi ek farak se replay, fanout aur multi-consumer sab nikalta hai.',
          'Kafka Java mein hai, RabbitMQ Erlang mein',
          'Kafka partitions use karta hai',
        ],
        answer: 1,
        explain: '"Kafka tez hai" junior ka jawab hai. Senior MODEL ka farak batata hai: log vs broker-with-deletion. Baaki saare fayde (replay, multiple independent consumers, time travel) isi ek design faisle ka natija hain.',
      },
      {
        q: '🧑‍💼 "Kafka disk par likhta hai. Toh yeh in-memory queue se tez kaise ho sakta hai?"',
        choices: [
          'Woh nahi hai, bas sasta hai',
          'Kyunki woh SEQUENTIAL append karta hai (random access nahi), OS page cache use karta hai (isliye recent reads RAM se hi aate hain), aur zero-copy sendfile se disk→network bina user-space copy ke bhejta hai',
          'Woh sab RAM mein cache karta hai',
          'Kyunki NVMe SSD tez hain',
        ],
        answer: 1,
        explain: 'Yeh sawal check karta hai ki tumne Kafka ke design paper ko samjha hai ya sirf API use ki hai. "Disk dheemi hai" ek myth hai — sequential disk throughput random memory access ke kareeb pahunch jaata hai.',
      },
    ],
  },
  success: [
    'Main bata sakta hoon ki log aur queue mein bunyaadi farak kya hai',
    'Main samjha sakta hoon ki Kafka messages delete kyun nahi karta',
    'Main sequential I/O, page cache aur zero-copy ka role bata sakta hoon',
    'Main jaanta hoon ki retention se OffsetOutOfRange ka khatra kaise banta hai',
  ],
  homework: `<p>Asli Kafka par log ke andar jhaanko:</p>
    <pre># kuch messages bhejo
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \\
  --bootstrap-server localhost:9092 --topic orders

# phir asli log file dekho — yeh sach mein disk par ek file hai
docker exec -it kafka ls -lh /tmp/kraft-combined-logs/orders-0/

# aur uske andar:
docker exec -it kafka /opt/kafka/bin/kafka-run-class.sh \\
  kafka.tools.DumpLogSegments \\
  --files /tmp/kraft-combined-logs/orders-0/00000000000000000000.log \\
  --print-data-log</pre>
    <p><b>Asli experiment:</b> ek consumer chalao <code>--from-beginning</code> se.
    Khatam hone par usse dobara chalao. Sab messages <b>phir se</b> aayenge —
    kyunki tumne group nahi diya, toh koi offset commit hi nahi hua.
    Ab <code>--group test1</code> ke saath do baar chalao. Doosri baar kuch nahi aayega.
    <b>Yahi offset ka poora khel hai.</b></p>`,
  cards: [
    { front: 'Kafka aur classic queue mein sabse bunyaadi farak?', back: 'Kafka ek <b>append-only log</b> hai — padhne se message mitta nahi, consumer sirf apna <b>offset</b> aage badhata hai. Classic queue ack milte hi <b>delete</b> kar deti hai. Replay, fanout aur time-travel sab isi ek farak se aate hain.' },
    { front: 'Kafka disk par hote hue bhi tez kyun hai?', back: '1) <b>Sequential append</b> (random I/O se ~100× tez)<br>2) <b>OS page cache</b> — JVM heap mein data nahi rakhta<br>3) <b>Zero-copy sendfile</b> — disk→network bina user-space copy' },
    { front: 'Offset kya hai?', back: 'Partition ke andar har message ka <b>number</b> (0, 1, 2…). Consumer group apna offset commit karta hai = "main yahan tak padh chuka". Offset peeche karo = time travel; aage karo = skip.' },
    { front: 'Ek hi data ko 3 alag systems ko dena ho toh Kafka mein kya karoge?', back: '<b>3 alag consumer groups</b> bana do — bas. Sabko poora data milega, koi copy nahi banegi, aur <b>producer ka code bilkul nahi badlega</b>.' },
    { front: 'Retention kya hai aur uska khatra kya hai?', back: 'Purana data waqt/size ke hisaab se udta hai (default 7 din). <b>Khatra:</b> consumer retention se zyada der band raha toh uska offset log se hi gayab → <code>OffsetOutOfRangeException</code> → ya sab dobara padho (duplicates) ya beech ka data skip karo (data loss).' },
  ],
},

]);

/* =====================================================================
   Levels 3-5 — registry additive hai, isliye alag call bhi chalti hai.
   (Yahi tareeqa hai naya level add karne ka — engine ko chhue bina.)
   ===================================================================== */
KQ.registerLevels('beginner', [

/* =====================================================================
   3. TOPICS, PARTITIONS, OFFSETS
   ===================================================================== */
{
  id: 'partitions',
  icon: '🗂️',
  title: 'Topics, Partitions & Offsets — parallelism ki asli seema',
  badge: 'Partition Pandit',
  objectives: [
    'Samjho ki ek log kab bottleneck ban jaata hai',
    'Jaano ki partition = parallelism ki HARD limit hai',
    'Offset, log-end-offset aur committed offset ka farak samjho',
    'Jaano ki partitions badhaye toh kya toot jaata hai',
  ],
  lesson: [
    `<h3>😰 Dard: "Humne 20 consumers laga diye, phir bhi lag badh raha hai"</h3>
     <p>Ek food-delivery app. <code>order-events</code> topic par 8,000 msg/s aa rahe
     the. Ek consumer 500 msg/s handle kar sakta tha. Team ne hisaab lagaya:
     8000 ÷ 500 = 16 consumers chahiye. Unhone <b>20</b> laga diye — thoda extra buffer.</p>
     <p>Lag phir bhi badhta raha. Unhone 40 kar diye. <b>Koi farak nahi pada.</b></p>
     <pre>$ kafka-consumer-groups --describe --group order-workers

TOPIC          PARTITION  LAG        CONSUMER-ID
order-events   0          1,240,882  consumer-1
order-events   1          1,198,441  consumer-2
order-events   2          1,301,772  consumer-3
order-events   3          1,255,109  consumer-4
                                     consumer-5   ← kuch nahi mila
                                     consumer-6   ← kuch nahi mila
                                     ... 34 aur khaali baithe 😴</pre>
     <div class="war-story"><b>⚡ Topic mein sirf 4 partitions the.</b> Unke 40 mein
     se 36 consumers bilkul khaali baithe the — bas heartbeat bhej rahe the.
     Servers ka bill 10× ho gaya, throughput utna ka utna.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Aur consumers add karo"</b><br>
     Partition se zyada consumers <b>bekaar</b> hain. Ek partition ek waqt par
     ek hi consumer ko assign ho sakti hai (ek group ke andar). Yeh Kafka ki
     bunyaadi guarantee hai — isi se partition ke andar ordering bachti hai.</p>

     <p><b>⚠️ Better: "Partitions 4 se 40 kar do"</b><br>
     Throughput turant theek ho gaya... par do nayi problem aa gayi:<br>
     1. Purani keys ab <b>naye partition</b> par jaane lagin (<code>hash(key) % 40</code>
     ab <code>% 4</code> se alag hai) → us key ki <b>ordering guarantee toot gayi</b>.<br>
     2. Partitions kabhi <b>kam nahi</b> ho sakte. 40 galat lagne par wapas nahi ja sakte.</p>

     <p><b>✅ Real: Shuru se sahi hisaab lagao, thodi headroom rakho</b></p>
     <pre>partitions ≥ max( peak_throughput ÷ per_consumer_throughput,
                   jitne consumers kabhi chalane pad sakte hain )

Humara case: 8000 ÷ 500 = 16
             + growth headroom  → 24 ya 32 partitions</pre>
     <p>Aur agar ordering zaroori hai toh <b>partition count ko ek constant maano</b> —
     use badalna ek breaking change hai, config change nahi.</p>`,

    `<h3>⚖️ Teen alag "offset" — inhe kabhi mat gadbadao</h3>
     <pre>          log-start-offset        committed        log-end-offset
                 │                     │                  │
                 ▼                     ▼                  ▼
    ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
    │ ✗ │ ✗ │ ✗ │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │10 │11 │12 │13 │
    └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
      retention kha gaya     ↑ padh liya ↑    ↑ abhi baaki ↑
                                              LAG = 14 − 8 = 6</pre>
     <ul>
       <li><b>log-end-offset (LEO):</b> agla message jo likha jaayega. Producer isse badhata hai.</li>
       <li><b>committed offset:</b> consumer group ka bookmark — "yahan tak main kar chuka".</li>
       <li><b>log-start-offset:</b> sabse purana message jo abhi bhi maujood hai (retention ke baad).</li>
       <li><b>LAG = LEO − committed.</b> Yahi tumhara sabse zaroori production metric hai.</li>
     </ul>
     <p><b>Nayi musibat:</b> LAG bade hone ka matlab hamesha "consumer dheema hai" nahi hota.
     Kabhi kabhi consumer <b>zinda hi nahi</b> hai, ya woh ek hi partition par atka hai.
     Isliye hamesha <b>per-partition</b> lag dekho, sirf total nahi.</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: 36 khaali consumers ka raaz',
      instructions: 'Yeh wahi asli scenario hai. Khud pata lagao ki lag kyun nahi ghat raha.',
      goal: `<code>order-events</code> par bahut lag hai aur group mein bahut saare
        consumers hain — phir bhi lag ghat nahi raha.<br><br>
        1. Group ko describe karke lag aur consumer assignment dekho<br>
        2. Pata lagao ki kitne consumers ko sach mein kaam mila hai<br>
        3. <b>Partitions badha kar</b> problem theek karo (kam se kam 12 karo)`,
      cluster: {
        brokers: 3,
        topics: { 'order-events': { partitions: 4, rf: 3 } },
        groups: { 'order-workers': { members: 10, committed: { 'order-events': [0, 0, 0, 0] } } },
      },
      responses: [
        { re: /^kafka-console-producer --topic order-events --count 40000/, out: () => '' },
      ],
      tasks: [
        { text: 'group describe karo', check: s => s.history.some(h => /consumer-groups.*--describe/.test(h)) },
        { text: 'assignment dekho (kaun khaali hai)', check: s => s.history.some(h => /sim assignment|consumer-groups.*--describe/.test(h)) },
        { text: 'partitions 12+ karo', check: s => s.topics['order-events'].partitions.length >= 12 },
      ],
      success: s => s.topics['order-events'].partitions.length >= 12 &&
                    s.history.some(h => /consumer-groups.*--describe/.test(h)),
      successText: `💡 Ab describe dobara karo — 10 ke 10 consumers ko partitions mil gaye.<br><br>
        <b>Par ek keemat bhi chukayi hai:</b> jo keys pehle <code>hash % 4</code> se
        partition chunti thin, ab <code>hash % 12</code> se chunengi. Matlab ek user ke
        naye events <b>alag partition</b> par ja sakte hain, jabki uske purane events
        purani partition par pade hain. Us user ke liye <b>ordering ki guarantee toot gayi</b>.<br><br>
        Isi liye asli duniya mein partitions <b>shuru se</b> zyada rakhte hain —
        badalna sasta nahi hai.`,
      hints: [
        'kafka-consumer-groups --describe --group order-workers  — LAG aur CONSUMER-ID dono dekho',
        'Jin partitions ka consumer "-" hai woh kisi ko assign nahi... ya ulta: jin consumers ka koi partition nahi.',
        'kafka-topics --alter --topic order-events --partitions 12',
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Partition ka ganit',
      questions: [
        {
          q: 'Ek topic mein 6 partitions hain aur ek consumer group mein 10 consumers. Kya hoga?',
          choices: [
            'Har consumer ko 0.6 partition milega',
            '6 consumers ko ek-ek partition milega, baaki 4 BILKUL khaali baithenge (par heartbeat bhejte rahenge)',
            'Kafka apne aap partitions badha dega',
            'Group start hi nahi hoga',
          ],
          answer: 1,
          explain: 'Partition = parallelism ki HARD limit. Extra consumers standby ki tarah baithe rehte hain — ek partition owner marne par kaam aa jaate hain, par abhi ka throughput zero badhate hain.',
        },
        {
          q: 'LAG = 500,000 hai. Sabse pehle kya check karoge?',
          choices: [
            'Turant consumers double kar do',
            'PER-PARTITION lag dekho — agar sirf ek partition par lag hai toh problem hot key/skew hai, consumer count nahi. Aur yeh bhi dekho ki consumers zinda hain ya nahi.',
            'Retention badha do',
            'Broker restart karo',
          ],
          answer: 1,
          explain: 'Total lag jhooth bol sakta hai. Ek partition par 500k lag ka matlab hai skew (sab ek hi key par ja raha hai) — us case mein consumers badhane se kuch nahi hoga, kyunki ek partition ek hi consumer padh sakta hai.',
          context: 'TOPIC     PARTITION  LAG       \nevents    0          2         \nevents    1          1         \nevents    2          499,995   ← 🔥\nevents    3          2         ',
        },
        {
          q: 'Partitions 4 se 12 karne ke baad kya TOOT jaata hai?',
          choices: [
            'Kuch nahi tootta',
            'Purana data gum ho jaata hai',
            'Key-based ordering — kyunki partition = hash(key) % N, aur N badalne se wahi key ab alag partition par jaayegi jabki purane events purani partition par pade hain',
            'Consumer group reset ho jaata hai',
          ],
          answer: 2,
          explain: 'Purana data safe rehta hai, par uska LOCATION ab naye messages se match nahi karta. Us key ke liye "ek hi partition, isliye ordered" wali guarantee toot gayi. Isliye partition count ek breaking change hai.',
        },
      ],
    },
    {
      type: 'fill',
      title: '📄 Partition ka hisaab',
      instructions: 'Sahi shabd bharo — partition count ka faisla kaise lete hain.',
      template: 'partitions ≥ max(\n  peak_throughput ÷ {0},\n  {1} jo kabhi chalane pad sakte hain\n)\n\n# ⚠️ partitions kabhi {2} nahi ho sakte\n# ⚠️ badhane se {3} ki guarantee toot jaati hai',
      blanks: [
        { answer: 'per_consumer_throughput', explain: 'Ek consumer kitna kha sakta hai — yahi asli divisor hai.' },
        { answer: 'max_consumers', explain: 'Partition parallelism ki limit hai, isliye jitne consumers chahiye utni partitions honi chahiye.' },
        { answer: 'kam', explain: 'Kafka partitions ghata nahi sakta. Sirf naya topic bana ke migrate karna padta hai.' },
        { answer: 'ordering', explain: 'hash(key) % N — N badla toh key ka ghar badal gaya.' },
      ],
      tokens: ['per_consumer_throughput', 'max_consumers', 'kam', 'ordering', 'zyada', 'durability', 'brokers', 'replication'],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Naye topic ke liye kitne partitions rakhoge? Kaise decide karoge?"',
        choices: [
          'Hamesha 3 — default yahi hai',
          'Do cheezein: (a) peak throughput ÷ ek consumer ka throughput, (b) zyada se zyada kitne consumers parallel chalane pad sakte hain — dono ka max lo, phir growth ke liye headroom. Aur yeh bhi soch lo ki partition badhana ordering todta hai, isliye upar ki taraf galti karna behtar hai.',
          'Jitne brokers hain utne',
          'Jitne zyada ho sake — 1000 rakh do',
        ],
        answer: 1,
        explain: 'Interviewer trade-off sunna chahta hai. "Jitne zyada ho sake" bhi galat hai — har partition broker par file handles, memory aur replication traffic leti hai; 100k+ partitions par cluster ka rebalance aur failover dheema pad jaata hai.',
      },
      {
        terminal: {
          title: '🖥️ Interview: lag ka nidaan karo',
          goal: `🧑‍💼 Interviewer: "Yeh dekho — <code>checkout</code> topic par lag hai.
            Mujhe batao ki kitna lag hai aur <b>kaunsi partition</b> par sabse zyada hai.
            Phir group ko wahin le jao jahan lag khatam ho jaaye
            (hint: yeh recovery ka asli tareeqa hai jab tum peeche ka data chhod sakte ho)."`,
          cluster: {
            brokers: 3,
            topics: { checkout: { partitions: 3, rf: 3 } },
            groups: { 'checkout-workers': { members: 0, committed: { checkout: [0, 0, 0] } } },
          },
          responses: [
            { re: /^seed$/, out: s => { for (let i = 0; i < 300; i++) s.produce('checkout', 'k' + i, 'v'); return 'seeded'; } },
          ],
          tasks: [
            { text: 'lag dekho (--describe ya sim lag)', check: s => s.history.some(h => /consumer-groups.*--describe|sim lag/.test(h)) },
            { text: 'group ka lag 0 karo', check: s => s.groups['checkout-workers'] && s.lag('checkout-workers', 'checkout') === 0 },
          ],
          success: s => s.lag('checkout-workers', 'checkout') === 0 &&
                        s.history.some(h => /consumer-groups.*--describe|sim lag/.test(h)),
          successText: `Bilkul sahi. <code>--reset-offsets --to-latest</code> ka matlab hai
            "peeche ka sab chhod do, abhi se shuru karo". Yeh <b>data chhodne wala</b> faisla hai —
            production mein sirf tab karo jab woh data sach mein faltu ho (jaise metrics),
            ya jab tum use kisi aur tareeqe se recover kar sakte ho.
            Warna <code>sim process</code> se sach mein consume karke lag ghatao.`,
          hints: [
            'kafka-consumer-groups --describe --group checkout-workers',
            'Do raaste: sach mein process karo (sim process checkout-workers --records 9999) ya offsets aage badha do',
            'kafka-consumer-groups --reset-offsets --group checkout-workers --topic checkout --to-latest --execute',
          ],
        },
      },
    ],
  },
  success: [
    'Main jaanta hoon ki partition parallelism ki hard limit hai',
    'Main partition count ka hisaab laga sakta hoon',
    'Main LEO, committed offset aur log-start-offset mein farak kar sakta hoon',
    'Main jaanta hoon ki partitions badhane se ordering kyun tootti hai',
  ],
  homework: `<p>Skew apni aankhon se dekho:</p>
    <pre># 3 partitions ka topic banao
kafka-topics.sh --create --topic skew-test --partitions 3 --bootstrap-server localhost:9092

# ek hi key se 100 messages
for i in $(seq 1 100); do echo "SAME:msg-$i"; done | \\
  kafka-console-producer.sh --topic skew-test --bootstrap-server localhost:9092 \\
  --property parse.key=true --property key.separator=:

# ab dekho har partition par kitne gaye
kafka-run-class.sh kafka.tools.GetOffsetShell \\
  --bootstrap-server localhost:9092 --topic skew-test</pre>
    <p>Ek partition par 100, baaki do par 0. <b>Yahi hot partition hai.</b>
    Ab <code>key.separator</code> hata ke (null keys) dobara karo — ab teeno mein
    barabar bantega.</p>`,
  cards: [
    { front: 'Partition se zyada consumers lagane se kya hota hai?', back: 'Kuch nahi — extra consumers <b>khaali</b> baithte hain. Ek partition ek group mein ek hi consumer ko assign hoti hai. <b>Partition = parallelism ki hard limit.</b>' },
    { front: 'LAG ka formula?', back: '<code>LAG = log-end-offset − committed-offset</code>. Hamesha <b>per-partition</b> dekho — total lag skew ko chhupa deta hai.' },
    { front: 'Partitions badhane se kya toot jaata hai?', back: '<b>Key-based ordering.</b> Partition = hash(key) % N. N badla toh wahi key ab naye partition par jaayegi, jabki uske purane events purani partition par pade hain. Isliye partition count ek breaking change hai.' },
    { front: 'Kitne partitions rakhne chahiye?', back: '<code>max(peak_throughput ÷ per_consumer_throughput, max_consumers)</code> + growth headroom. Upar ki taraf galti karo — badhana mushkil hai, aur ghatana <b>namumkin</b>.' },
    { front: 'Total lag 500k hai. Pehla kadam?', back: '<b>Per-partition lag dekho.</b> Sirf ek partition par lag = skew/hot-key problem (consumers badhane se kuch nahi hoga). Sab par barabar lag = sach mein capacity kam hai.' },
  ],
},

/* =====================================================================
   4. PRODUCER — acks, batching, keys
   ===================================================================== */
{
  id: 'producer',
  icon: '📤',
  title: 'Producer — acks, batching aur "message chala gaya" ka jhooth',
  badge: 'Acks Ka Rakshak',
  objectives: [
    'Jaano ki acks=0, 1 aur all mein kya farak hai — aur kaunsa data kha jaata hai',
    'min.insync.replicas ka role samjho (acks=all akela kaafi nahi)',
    'Batching aur linger.ms ka throughput par asar samjho',
    'Producer ke retries se duplicate kaise bante hain — aur idempotent producer kya theek karta hai',
  ],
  lesson: [
    `<h3>😰 Dard: "Metrics dashboard 12% events kha gaya"</h3>
     <p>Ek analytics team ne apne event-collector ko "tez" banane ke liye
     <code>acks=0</code> laga diya. Latency 8ms se 0.4ms ho gayi. Sab khush.</p>
     <p>Teen hafte baad billing team ne shikayat ki ki unke numbers 12% kam hain.
     Investigation mein pata chala:</p>
     <pre>acks=0  →  producer message socket par likhta hai aur
           TURANT "ho gaya" maan leta hai.
           Broker ne liya ya nahi — usse koi matlab nahi.

Har broker restart / rolling deploy / network blip par
in-flight messages CHUP-CHAAP gayab. Koi error nahi.
Koi log nahi. Koi metric nahi. 💀</pre>
     <div class="war-story"><b>⚡ Sabse buri baat:</b> data loss ka koi <b>signal hi nahi</b> tha.
     Application bilkul healthy dikh rahi thi. Error rate 0%. Unhe pata isliye
     chala kyunki kisi INSAAN ne teen hafte baad numbers ka milaan kiya.</div>`,

    `<h3>🪜 acks ki seedhi</h3>
     <pre>acks=0   → "bhej diya, bas."
           latency: ~0.4ms   ·  data loss: 😱 CHUP-CHAAP
           kab theek hai: sirf tab jab kuch messages khona SACH MEIN chalega
                          (jaise debug logs, non-billing metrics)

acks=1   → "leader ne likh liya, theek hai."
           latency: ~5ms     ·  data loss: leader mar gaya usse pehle ki
                                followers copy karein → woh messages gaye
           kab theek hai: kabhi kabhi — logs, low-value telemetry

acks=all → "leader + saare in-sync replicas ne likh liya."
           latency: ~10ms    ·  data loss: bahut mushkil
           kab theek hai: DEFAULT. Orders, payments, koi bhi asli data.</pre>
     <p><b>⚠️ Par ek jaal hai jo bahut log nahi jaante:</b></p>
     <p>Akela <code>acks=all</code> kaafi <b>nahi</b> hai. "All in-sync replicas" ka matlab hai
     "jitne bhi abhi ISR mein hain". Agar 2 replicas gir gaye aur ISR mein sirf
     <b>leader akela</b> bacha, toh <code>acks=all</code> ka matlab ban jaata hai
     "sirf leader ne likha" — yaani chup-chaap <code>acks=1</code>.</p>
     <p>Isi liye <b>hamesha jodi banao</b>:</p>
     <pre>acks=all  +  min.insync.replicas=2   (RF=3 ke saath)

Ab agar ISR 2 se neeche gaya, toh producer ko
NotEnoughReplicasException milega — matlab
CHUP-CHAAP data khone ke bajaye SAAF error.</pre>
     <p>Yeh <b>availability ke badle durability</b> ka सौदा hai — aur payments ke liye
     yeh sahi sauda hai. Likhna ruk jaana, chup-chaap data khone se behtar hai.</p>`,

    `<h3>⚖️ Nayi musibat: retries se duplicates</h3>
     <p><code>acks=all</code> lagate hi ek nayi problem aati hai. Socho:</p>
     <pre>1. Producer ne message bheja
2. Broker ne use likh liya ✅
3. ACK wapas aate waqt network gir gaya 💥
4. Producer ne ack nahi dekha → usne RETRY kiya
5. Broker ne WAHI message DOBARA likh diya

Log mein: [order-123] [order-123]   ← duplicate!</pre>
     <p><b>Hal: idempotent producer</b> (<code>enable.idempotence=true</code> — modern
     Kafka mein default). Har producer ko ek <b>PID</b> aur har message ko ek
     <b>sequence number</b> milta hai. Broker duplicate sequence dekhte hi use
     chup-chaap gira deta hai — bina error ke.</p>
     <p><b>Aur batching ka jaadu:</b> producer messages ko turant nahi bhejta —
     woh <code>linger.ms</code> tak intezaar karta hai taaki ek batch ban jaaye:</p>
     <pre>linger.ms=0    → har message alag request  (latency kam, throughput kam)
linger.ms=10   → 10ms ka batch                (latency +10ms, throughput 10-50× ⬆)
batch.size=32KB→ itna bharte hi turant bhej do</pre>
     <p>Yeh sabse aam "muft" performance jeet hai: <code>linger.ms=5..20</code> +
     <code>compression.type=lz4</code> se throughput bahut badh jaata hai,
     latency par sirf kuch millisecond ka asar.</p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Kis data ko kaunsa acks?',
      instructions: 'Sawal ek hi hai: yeh message khoya toh kitna nuksan?',
      bins: [
        { id: 'all', label: '🛡️ acks=all + min.insync=2' },
        { id: 'one', label: '⚖️ acks=1 chal jaayega' },
        { id: 'zero', label: '🏃 acks=0 (khona manzoor hai)' },
      ],
      items: [
        { text: 'Payment transaction events', bin: 'all',
          explain: 'Paisa. Ek bhi message khona matlab audit fail aur customer ka nuksan. Yahan durability par koi samjhauta nahi.' },
        { text: 'Order created / cancelled events', bin: 'all',
          explain: 'Kho gaya toh customer ka order gayab. Business-critical.' },
        { text: 'Application debug logs', bin: 'zero',
          explain: 'Debug logs ke liye 0.1% khona bilkul theek hai — aur yeh volume bahut zyada hota hai, toh speed matter karti hai.' },
        { text: 'Mouse-movement / heatmap clicks', bin: 'zero',
          explain: 'Statistical data hai — kuch samples khone se conclusion nahi badalta. Volume bahut bada hota hai.' },
        { text: 'Non-billing service metrics (CPU, memory)', bin: 'one',
          explain: 'Ek data point khona chart mein ek chhota gap hai. acks=1 se latency aur durability ka theek balance milta hai.' },
        { text: 'User signup events', bin: 'all',
          explain: 'Naya user kho gaya toh welcome email, onboarding, analytics — sab galat. Kabhi bhi acks=0 nahi.' },
        { text: 'Inventory stock updates', bin: 'all',
          explain: 'Stock update khona matlab oversell — customer ko woh cheez bech doge jo hai hi nahi.' },
        { text: 'Access logs (nginx)', bin: 'one',
          explain: 'Aam taur par troubleshooting ke liye. acks=1 kaafi hai — par agar compliance/audit ke liye chahiye toh all karna padega.' },
      ],
    },
    {
      type: 'terminal',
      title: '🖥️ Mission: acks ka asli imtihaan',
      instructions: 'Broker ko maar ke dekho ki har acks setting kya karti hai. Yeh sach mein chalta hai — koi scripted output nahi.',
      goal: `<code>payments</code> topic hai: RF=3, <code>min.insync.replicas=2</code>.<br><br>
        1. Pehle normal haal mein <code>acks=all</code> se kuch messages bhejo — chalna chahiye<br>
        2. Ab <b>do brokers maar do</b> (<code>sim broker kill 2</code>, <code>sim broker kill 3</code>)<br>
        3. Phir <code>acks=all</code> se bhejo — dekho kya hota hai<br>
        4. Phir wahi message <code>acks=0</code> se bhejo — aur DHYAN SE dekho kya hota hai`,
      cluster: {
        brokers: 3,
        topics: { payments: { partitions: 1, rf: 3, config: { 'min.insync.replicas': 2 } } },
      },
      tasks: [
        { text: 'acks=all se bhejo (sab theek hai)', check: s => s.history.some(h => /--acks all/.test(h)) },
        { text: '2 brokers maaro', check: s => s.brokers.filter(b => !b.alive).length >= 2 },
        { text: 'acks=all try karo (fail hona chahiye)', check: s => !!s.lastError && /NotEnoughReplicas/.test(s.lastError || '') },
        { text: 'acks=0 try karo (aur natija dekho)', check: s => s.history.some(h => /--acks 0/.test(h)) },
      ],
      success: s => s.brokers.filter(b => !b.alive).length >= 2 &&
                    s.history.some(h => /--acks all/.test(h)) &&
                    s.history.some(h => /--acks 0/.test(h)),
      successText: `Yeh sabse zaroori sabak hai jo tum aaj seekhoge:<br><br>
        🛡️ <b>acks=all ne SAAF MANA kar diya</b> — <code>NotEnoughReplicasException</code>.
        Yeh <b>feature hai, bug nahi</b>. Broker keh raha hai: "main is message ko surakshit
        nahi rakh sakta, isliye jhooth nahi bolunga."<br><br>
        💀 <b>acks=0 ne "ho gaya" bol diya</b> — jabki message kahin nahi pahuncha.
        Koi error nahi, koi warning nahi. <b>Yahi woh 12% hai jo analytics team kho rahi thi.</b><br><br>
        Isliye yaad rakho: <code>acks=all</code> + <code>min.insync.replicas=2</code> ka
        matlab hai <b>"chup-chaap data khone se behtar hai ki likhna ruk jaaye"</b>.`,
      hints: [
        'kafka-console-producer --topic payments --message "test" --acks all',
        'sim broker kill 2   phir   sim broker kill 3',
        'Ab dobara --acks all se bhejo, phir --acks 0 se. Dono ka output compare karo.',
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Producer ke jaal',
      questions: [
        {
          q: 'RF=3 hai aur acks=all hai, par min.insync.replicas set nahi kiya (default 1). Do brokers gir gaye. Kya hoga?',
          choices: [
            'Producer ruk jaayega',
            'Producing chalti rahegi — kyunki ab ISR mein sirf leader hai, aur "all in-sync replicas" ka matlab ban gaya "sirf leader". Yaani chup-chaap acks=1 ban gaya, aur leader marne par woh messages ja sakte hain.',
            'Kafka apne aap naye replicas bana dega',
            'Error aayega',
          ],
          answer: 1,
          explain: 'Yeh sabse aam production galti hai. acks=all AKELA durability nahi deta — woh "jitne bhi ISR mein hain" ka wada hai. min.insync.replicas=2 lagaye bina woh number 1 tak gir sakta hai.',
        },
        {
          q: 'linger.ms=0 se linger.ms=10 karne par kya hota hai?',
          choices: [
            'Sab kuch 10ms dheema ho jaayega, koi fayda nahi',
            'Har message par ~10ms latency judegi, PAR messages batch mein jaayenge — throughput 10-50× tak badh sakta hai aur network requests bahut kam ho jaayenge',
            'Messages 10ms baad delete honge',
            'Kuch nahi badlega',
          ],
          answer: 1,
          explain: 'Yeh classic latency-vs-throughput trade-off hai. Zyadatar systems ke liye 5-20ms ka linger bilkul invisible hai par throughput bahut badha deta hai. Compression bhi batch par behtar kaam karta hai.',
        },
        {
          q: 'Producer ne retry kiya aur wahi message DO baar log mein aa gaya. Ise kaise rokte hain?',
          choices: [
            'Retries band kar do',
            'enable.idempotence=true — producer ko ek PID aur har message ko sequence number milta hai, broker duplicate sequence ko chup-chaap gira deta hai',
            'acks=0 kar do',
            'Consumer mein duplicate check karo',
          ],
          answer: 1,
          explain: 'Retries band karna data loss laayega — ilaaj bimari se bura. Idempotent producer (modern Kafka mein default ON) broker par hi duplicate ko rok deta hai. Consumer-side dedup bhi chahiye hoti hai, par woh alag layer hai.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "acks=all laga diya. Ab tumhara data safe hai?"',
        choices: [
          'Haan, bilkul safe hai',
          'Nahi. acks=all ka matlab hai "jitne replicas abhi ISR mein hain unhone likh liya" — aur ISR ghat kar 1 ho sakta hai. Iske saath min.insync.replicas=2 aur RF=3 chahiye. Aur unclean.leader.election.enable=false bhi, warna out-of-sync replica leader ban ke data truncate kar dega.',
          'Haan agar RF=3 hai',
          'Sirf tab jab retries=0 ho',
        ],
        answer: 1,
        explain: 'Yeh senior-level jawab hai. Durability ek setting se nahi aati — yeh teen settings ka combination hai: acks=all + min.insync.replicas≥2 + unclean.leader.election=false.',
      },
      {
        q: '🧑‍💼 "Producer throughput kam hai. Kya dekhoge?"',
        choices: [
          'Turant acks=0 kar dunga',
          'Pehle batching dekhunga: linger.ms (0 hai toh badhao), batch.size, compression.type=lz4/zstd. Phir partitions kam toh nahi. acks ko durability requirement ke bina nahi ghatunga — woh performance knob nahi, correctness knob hai.',
          'Aur brokers add karunga',
          'Message chhote kar dunga',
        ],
        answer: 1,
        explain: 'Sabse bada red flag hota hai performance ke liye acks ghata dena. Batching + compression se aksar 10× jeet milti hai bina durability khoye. acks ko chhoona aakhri option hai, aur woh business decision hai.',
      },
    ],
  },
  success: [
    'Main acks=0/1/all ka farak aur unka data-loss risk bata sakta hoon',
    'Main jaanta hoon ki acks=all ko min.insync.replicas ke saath kyun chahiye',
    'Main linger.ms aur batch.size ka throughput par asar samjha sakta hoon',
    'Main jaanta hoon ki idempotent producer kya theek karta hai',
  ],
  homework: `<p>Asli producer configs ke saath khelo:</p>
    <pre>const producer = kafka.producer({
  idempotent: true,        // duplicate rokta hai (PID + sequence)
  maxInFlightRequests: 5,
});

await producer.send({
  topic: 'orders',
  acks: -1,                // -1 = all   (kafkajs mein yahi likhte hain)
  compression: CompressionTypes.LZ4,
  messages: [{ key: 'user-1', value: JSON.stringify(order) }],
});</pre>
    <p><b>Asli experiment:</b> 10,000 messages do baar bhejo —
    ek baar bina batching ke (har send ka alag await), aur ek baar
    ek hi <code>send()</code> mein 10,000 messages ka array de kar.
    Waqt naapo. Farak dekh ke tum hairaan ho jaoge.</p>`,
  cards: [
    { front: 'acks=0, 1, all — ek line mein farak?', back: '<b>0</b> = "bhej diya, bas" (chup-chaap data loss). <b>1</b> = leader ne likha (leader mara toh gaya). <b>all</b> = saare in-sync replicas ne likha (asli durability).' },
    { front: 'acks=all akela kaafi kyun nahi hai?', back: '"All in-sync replicas" ka matlab hai "jitne bhi ABHI ISR mein hain" — woh ghat kar 1 ho sakta hai, jisse acks=all chup-chaap acks=1 ban jaata hai. Isliye hamesha <code>min.insync.replicas=2</code> (RF=3 ke saath) lagao.' },
    { front: 'acks=0 ka sabse khatarnaak pehlu?', back: 'Data loss ka <b>koi signal nahi</b> milta. Error rate 0%, logs saaf, app healthy — par messages gayab. Pata tab chalta hai jab koi insaan mahine baad numbers milaata hai.' },
    { front: 'linger.ms kya karta hai?', back: 'Producer batch banane ke liye itne ms intezaar karta hai. <code>linger.ms=5..20</code> + <code>compression=lz4</code> = throughput mein bahut bada faayda, latency par bas kuch ms. Sabse sasti performance jeet.' },
    { front: 'Producer retry se duplicate kaise rukta hai?', back: '<code>enable.idempotence=true</code> — producer ko <b>PID</b> aur har message ko <b>sequence number</b> milta hai. Broker duplicate sequence chup-chaap gira deta hai. Modern Kafka mein default ON.' },
  ],
},

/* =====================================================================
   5. CONSUMER & CONSUMER GROUPS
   ===================================================================== */
{
  id: 'consumer-groups',
  icon: '📥',
  title: 'Consumer Groups — kaam baantne ka tareeqa',
  badge: 'Group Ka Sardar',
  objectives: [
    'Samjho ki consumer group scaling aur fanout dono kaise deta hai',
    'Jaano ki partitions consumers mein baantne ka kaam kaun karta hai',
    'poll loop, auto-commit aur "processed" ka jhooth samjho',
    'Jaano ki galat commit se data loss ya duplicate kaise banta hai',
  ],
  lesson: [
    `<h3>😰 Dard: "Notification service ne sabko 4 baar message bhej diya"</h3>
     <p>Ek team ne notification service ke 4 replicas chalaye taaki load baant sake.
     Har replica apne aap ko Kafka se jodta tha. Deploy ke baad customers ko
     <b>har notification 4 baar</b> mila.</p>
     <pre>❌ Jo unhone kiya:
   replica-1  → group.id = "notif-1"    ← alag group!
   replica-2  → group.id = "notif-2"    ← alag group!
   replica-3  → group.id = "notif-3"
   replica-4  → group.id = "notif-4"

   4 alag groups = har group ko POORA data milta hai
   = har message 4 baar process hua 📧📧📧📧</pre>
     <div class="war-story"><b>⚡ Yeh galti itni aam hai</b> ki iska ek naam hai:
     "group.id ko instance id samajh lena". <code>group.id</code> <b>service ka naam</b>
     hai, instance ka nahi. Ek hi group.id ke saare instances kaam <b>baantte</b> hain;
     alag group.id waale sab apni-apni poori copy lete hain.</div>`,

    `<h3>🪜 Group ka model samjho</h3>
     <pre>Topic: orders  (4 partitions)

  GROUP "billing"  (2 consumers)     GROUP "analytics" (1 consumer)
  ┌──────────────────────────┐       ┌──────────────────────────┐
  │ consumer-A → p0, p1      │       │ consumer-X → p0,p1,p2,p3 │
  │ consumer-B → p2, p3      │       │                          │
  └──────────────────────────┘       └──────────────────────────┘
     kaam BAANT rahe hain               poora data akela le raha

  ✅ Ek group ke ANDAR   → load balancing (har message ek hi consumer ko)
  ✅ Alag groups ke BEECH → fanout (har group ko poori copy)</pre>
     <p>Toh upar wali galti ka hal ek line ka tha: <b>chaaron replicas ko ek hi
     <code>group.id = "notification-service"</code> do.</b> Ab woh 4 partitions
     aapas mein baant lenge aur har message sirf ek baar process hoga.</p>

     <h4>Assignment karta kaun hai?</h4>
     <p>Group ka ek member <b>group leader</b> ban jaata hai (pehla join karne wala).
     Broker ka <b>group coordinator</b> members ki list bhejta hai, aur leader
     <b>assignment strategy</b> chala kar partitions baant deta hai:</p>
     <ul>
       <li><b>range</b> (default) — har topic ki partitions consecutive chunks mein.
       Kai topics ho toh skew kar sakta hai.</li>
       <li><b>roundrobin</b> — sab partitions ko baari-baari baanto. Behtar balance.</li>
       <li><b>sticky / cooperative-sticky</b> — rebalance par jitni ho sake purani
       assignment bachao. <b>Modern default yahi hona chahiye</b> (Track 2 mein kyun).</li>
     </ul>`,

    `<h3>⚖️ Nayi musibat: "commit" ka matlab kya hai?</h3>
     <p>Consumer ek <b>poll loop</b> chalata hai:</p>
     <pre>while (true) {
  const batch = await consumer.poll(500)   // messages lo
  for (const msg of batch) process(msg)    // kaam karo
  await consumer.commit()                  // "yahan tak ho gaya"
}</pre>
     <p>Ab sabse important sawal: <b>commit KAB karte ho?</b></p>
     <pre>⏱️ AUTO-COMMIT (enable.auto.commit=true, har 5 sec)
   → poll ke baad time hone par apne aap commit ho jaata hai,
     chahe processing poori hui ho ya nahi
   → 💀 DATA LOSS: commit ho gaya, phir crash → woh messages
     dobara nahi aayenge, par process bhi nahi hue

✅ COMMIT AFTER PROCESSING (manual)
   → pehle kaam poora, phir commit
   → duplicate ho sakta hai (kaam hua, commit se pehle crash)
   → par kuch KHOTA nahi. Yahi at-least-once hai.

❌ COMMIT BEFORE PROCESSING
   → at-most-once. Tez hai, par crash par message gaya.
     Sirf tab jab khona manzoor ho.</pre>
     <p><b>Niyam:</b> zyadatar systems ke liye — <b>manual commit, processing ke baad</b>.
     Isse duplicates aayenge, aur unka ilaaj <b>idempotency</b> hai (Track 2).
     Duplicate ko sambhalna aasan hai; gum hua data wapas nahi aata.</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: rebalance apni aankhon se',
      instructions: 'Consumers add/remove karo aur dekho ki partitions kaise redistribute hoti hain.',
      goal: `<code>orders</code> topic mein 6 partitions hain aur group
        <code>billing</code> mein abhi <b>1</b> consumer hai.<br><br>
        1. Assignment dekho — akele consumer ke paas kitni partitions hain?<br>
        2. 2 aur consumers add karo, assignment phir dekho<br>
        3. Ab itne consumers add karo ki <b>koi khaali baithe</b> — aur woh dekho<br>
        4. Sochо: partitions se zyada consumers ka kya fayda hai (agar hai toh)?`,
      cluster: {
        brokers: 3,
        topics: { orders: { partitions: 6, rf: 3 } },
        groups: { billing: { members: 1, committed: { orders: [0, 0, 0, 0, 0, 0] } } },
      },
      tasks: [
        { text: 'shuruaati assignment dekho', check: s => s.history.some(h => /sim assignment|--describe/.test(h)) },
        { text: 'consumers add karo', check: s => s.groups.billing.members.length > 1 },
        { text: 'itne consumers karo ki koi khaali baithe', check: s => s.groups.billing.idleMembers > 0 },
      ],
      success: s => s.groups.billing.idleMembers > 0 &&
                    s.history.some(h => /sim assignment|--describe/.test(h)),
      successText: `Ab tumne dono cheezein dekh li:<br><br>
        📈 <b>6 tak consumers badhane se</b> har ek ko kaam mila — asli scaling.<br>
        😴 <b>6 ke baad</b> naye consumers bilkul khaali baithe — sirf paisa jal raha hai.<br><br>
        <b>Par ek fayda phir bhi hai:</b> khaali consumers <b>hot standby</b> hain.
        Koi active consumer mara toh agle rebalance mein uski partition turant
        kisi khaali consumer ko mil jaayegi. Yaani recovery tez.
        Bas yeh jaan lo ki woh <b>throughput</b> nahi badha rahe.`,
      hints: [
        'sim assignment billing   — kaun kaunsi partition sambhal raha hai',
        'sim consumer add billing 2',
        '6 partitions hain — 7 ya zyada consumers karo, phir assignment dekho',
      ],
    },
    {
      type: 'order',
      title: '🔢 Consumer poll loop ka sahi kram',
      instructions: 'At-least-once delivery ke liye sahi kram lagao (kuch bhi khona nahi chahiye).',
      steps: [
        { text: '📥 poll() — broker se messages ka batch lo',
          explain: 'Consumer PULL karta hai, broker push nahi karta. Yahi natural back-pressure deta hai — consumer utna hi maangega jitna sambhal sake.' },
        { text: '⚙️ Har message ko process karo (DB likho, API call karo)',
          explain: 'Yahan asli kaam hota hai. Yeh commit se PEHLE hona zaroori hai.' },
        { text: '💾 Kaam ka natija durably save karo (DB commit)',
          explain: 'Agar iske baad crash hua, toh kaam ho chuka hai — dobara aaya toh duplicate hoga, par kuch khoya nahi.' },
        { text: '✅ Offset commit karo — "yahan tak ho gaya"',
          explain: 'Sabse aakhir mein. Yeh kram at-least-once deta hai. Ulta karoge (pehle commit) toh at-most-once ho jaayega aur crash par data gum.' },
        { text: '🔁 Loop dobara — agla batch',
          explain: 'Aur dhyan rakho: poll ke beech bahut der lagi toh broker samjhega ki consumer mar gaya (max.poll.interval.ms) aur rebalance kar dega.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Group ke jaal',
      questions: [
        {
          q: 'Ek service ke 4 instances hain. Har message SIRF EK baar process hona chahiye. group.id kaise set karoge?',
          choices: [
            'Har instance ko alag group.id (notif-1, notif-2, …)',
            'Chaaron ko EK HI group.id — tab woh partitions aapas mein baant lenge aur har message ek hi instance process karega',
            'group.id khaali chhod do',
            'group.id ko hostname par set karo',
          ],
          answer: 1,
          explain: 'group.id = SERVICE ka naam, instance ka nahi. Alag group.id dene ka matlab hai "mujhe poori copy chahiye" — isi se woh 4-baar-notification wala incident hua tha.',
        },
        {
          q: 'enable.auto.commit=true ke saath sabse bada khatra kya hai?',
          choices: [
            'Duplicate messages',
            'Offsets processing poori hone se PEHLE commit ho sakte hain — phir crash hua toh woh messages dobara nahi aayenge par process bhi nahi hue. Yeh CHUP-CHAAP data loss hai.',
            'Commit dheema hota hai',
            'Broker par load badhta hai',
          ],
          answer: 1,
          explain: 'Auto-commit timer par chalta hai, tumhare kaam ke hisaab se nahi. Isliye production mein akser manual commit (processing ke baad) use hota hai — duplicate sambhalna aasan hai, gum hua data wapas nahi aata.',
        },
        {
          q: 'Ek consumer ne 500 messages poll kiye aur har ek par 2 second laga (kul ~16 min). Kya hoga?',
          choices: [
            'Sab theek chalega',
            'max.poll.interval.ms (default 5 min) paar ho jaayega → broker samjhega consumer mar gaya → REBALANCE → uska kaam kisi aur ko mil jaayega aur yeh batch dobara process hoga',
            'Consumer tez ho jaayega',
            'Broker messages delete kar dega',
          ],
          answer: 1,
          explain: 'Yeh bahut aam production bug hai. Hal: max.poll.records ghatao (500 se 50), ya max.poll.interval.ms badhao, ya processing ko tez/parallel karo. Warna consumer infinite rebalance loop mein phans jaata hai.',
          context: 'WARN [Consumer clientId=worker-3 groupId=billing]\n  This member will leave the group because consumer poll timeout has expired.\n  This means the time between subsequent calls to poll() was longer than\n  the configured max.poll.interval.ms (300000).',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Ek hi topic ko do alag teams ko chahiye — billing aur analytics. Kaise doge?"',
        choices: [
          'Do alag topics bana do aur producer se dono mein likhwao',
          'Bas do alag consumer GROUPS — group.id=billing aur group.id=analytics. Dono ko poora data milega, dono apni raftaar se chalenge, aur producer ko kuch pata bhi nahi chalega.',
          'Topic ko duplicate kar do',
          'Ek group banao aur code se filter karo',
        ],
        answer: 1,
        explain: 'Yeh log model ka sabse seedha fayda hai. Alag groups = poori tarah azad consumers, apne apne offsets. Kal teesri team aayi toh bas ek naya group — producer ka code bilkul nahi badlega.',
      },
      {
        terminal: {
          title: '🖥️ Interview: group ki halat batao',
          goal: `🧑‍💼 Interviewer: "Yeh cluster dekho. Mujhe teen cheezein batao:
            (1) <code>orders</code> topic mein kitni partitions hain,
            (2) <code>billing</code> group mein kitne members hain aur kitna lag hai,
            (3) koi consumer khaali toh nahi baitha.<br><br>
            Aur agar khaali baithe hain, toh unhe kaam do — <b>bina partitions badhaye</b>
            yeh mumkin nahi, toh partitions 8 kar do."`,
          cluster: {
            brokers: 3,
            topics: { orders: { partitions: 3, rf: 3 } },
            groups: { billing: { members: 8, committed: { orders: [0, 0, 0] } } },
          },
          tasks: [
            { text: 'topic describe karo', check: s => s.history.some(h => /kafka-topics.*--describe/.test(h)) },
            { text: 'group describe karo', check: s => s.history.some(h => /consumer-groups.*--describe/.test(h)) },
            { text: 'partitions 8 karo', check: s => s.topics.orders.partitions.length >= 8 },
          ],
          success: s => s.topics.orders.partitions.length >= 8 &&
                        s.history.some(h => /consumer-groups.*--describe/.test(h)) &&
                        s.history.some(h => /kafka-topics.*--describe/.test(h)),
          successText: `Sahi. 3 partitions par 8 consumers matlab <b>5 bilkul khaali</b>.
            8 partitions karne ke baad sabko kaam mil gaya.<br><br>
            Interview mein yahan ek bonus point milta hai agar tum khud se bolo:
            <b>"par isse purani keys ka ordering toot jaayega, isliye pehle confirm
            karunga ki is topic par key-ordering ki zaroorat hai ya nahi."</b>`,
          hints: [
            'kafka-topics --describe --topic orders',
            'kafka-consumer-groups --describe --group billing',
            'kafka-topics --alter --topic orders --partitions 8',
          ],
        },
      },
    ],
  },
  success: [
    'Main jaanta hoon ki group ke andar load-balancing aur groups ke beech fanout hota hai',
    'Main group.id ko sahi tarah set kar sakta hoon (service ka naam, instance ka nahi)',
    'Main poll loop ka sahi kram bata sakta hoon (process → save → commit)',
    'Main auto-commit ka data-loss khatra samjha sakta hoon',
  ],
  homework: `<p>Woh "4 baar notification" wali galti khud reproduce karo:</p>
    <pre>// Terminal 1 aur 2 — ALAG group ids (galat tareeqa)
kafka-console-consumer.sh --topic orders --group g1 --bootstrap-server localhost:9092
kafka-console-consumer.sh --topic orders --group g2 --bootstrap-server localhost:9092
// → dono ko HAR message milega (fanout)

// Ab dono ko EK HI group do (sahi tareeqa)
kafka-console-consumer.sh --topic orders --group same --bootstrap-server localhost:9092
kafka-console-consumer.sh --topic orders --group same --bootstrap-server localhost:9092
// → messages aapas mein BANT jaayenge</pre>
    <p><b>Bonus:</b> doosre consumer ko chalate waqt pehle wale ka output dekho —
    tumhe rebalance ka log dikhega. Phir ek ko Ctrl+C karo aur dobara rebalance dekho.</p>`,
  cards: [
    { front: 'group.id kya hota hai — service ka naam ya instance ka?', back: '<b>Service ka naam.</b> Ek hi group.id ke saare instances partitions <b>baant</b> lete hain (load balancing). Alag group.id waale sab ko <b>poori copy</b> milti hai (fanout). Instance-wise group.id dena = har message N baar process.' },
    { front: 'At-least-once ke liye poll loop ka sahi kram?', back: '<code>poll() → process → durably save → commit</code>. Commit hamesha <b>aakhir mein</b>. Ulta karoge toh at-most-once ho jaayega aur crash par messages gum.' },
    { front: 'enable.auto.commit ka khatra?', back: 'Woh <b>timer</b> par commit karta hai, tumhare kaam ke hisaab se nahi. Processing poori hone se pehle commit ho gaya aur crash hua → woh messages na process hue na dobara aayenge = <b>chup-chaap data loss</b>.' },
    { front: 'max.poll.interval.ms exceed hone par kya hota hai?', back: 'Broker samajhta hai consumer mar gaya → <b>rebalance</b> → uski partitions kisi aur ko → woh batch dobara process. Hal: <code>max.poll.records</code> ghatao ya processing tez karo.' },
    { front: 'Partitions se zyada consumers ka koi fayda hai?', back: 'Throughput ka <b>nahi</b> — woh khaali baithte hain. Par woh <b>hot standby</b> hain: koi active consumer mara toh rebalance par uska kaam turant mil jaata hai, yaani recovery tez.' },
  ],
},

]);

/* ===================== Levels 6-9 ===================== */
KQ.registerLevels('beginner', [

/* =====================================================================
   6. KEYS & ORDERING
   ===================================================================== */
{
  id: 'keys-ordering',
  icon: '🔑',
  title: 'Keys & Ordering — "cancel" pehle aa gaya "create" se',
  badge: 'Ordering Ka Ustaad',
  objectives: [
    'Samjho ki Kafka ordering ki guarantee KAHAN tak deta hai (aur kahan nahi)',
    'Jaano ki key se partition kaise chunti hai (murmur2 hash)',
    'Hot partition / skew pehchano aur uska hal jaano',
    'Ordering vs parallelism ka asli trade-off samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "Cancelled order phir se zinda ho gaya"</h3>
     <p>Ek ride-hailing app. Ride ke events ek <code>ride-events</code> topic par jaate the,
     bina key ke (round-robin). Ek user ne ride book ki aur 2 second baad cancel kar di:</p>
     <pre>Producer ne bheja:
   t=0.0s   ride-created    → partition 2
   t=2.1s   ride-cancelled  → partition 0   ← alag partition!

Consumers ne process kiya:
   consumer-A (p0):  ride-cancelled  ← pehle process ho gaya
   consumer-B (p2):  ride-created    ← baad mein (p2 par lag tha)

Final state in DB:  status = CREATED  💀</pre>
     <p>Cancel ho chuki ride <b>zinda</b> ho gayi. Driver ko bheja gaya. Customer ka paisa kata.
     Support ticket. Refund.</p>
     <div class="war-story"><b>⚡ Aur yeh randomly hota tha</b> — sirf tab jab partitions par
     alag-alag lag ho. Testing mein kabhi nahi pakda gaya kyunki test mein lag hota hi nahi.
     Production mein hafte mein 3-4 baar. Sabse mushkil bugs yahi hote hain.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Ek hi partition rakh do — sab ordered ho jaayega"</b><br>
     Ordering perfect ho gayi... aur parallelism <b>1</b> ho gaya. Ek consumer, ek partition.
     8,000 msg/s ka traffic ek consumer nahi kha sakta. Lag infinite.</p>

     <p><b>⚠️ Better: "Consumer mein timestamp dekh ke sort kar lo"</b><br>
     Ab consumer ko har event ko buffer karna padega "shayad purana event abhi aayega" ke liye.
     Kitni der wait karein? Aur clock skew? Aur memory? Yeh problem ko consumer ke
     sar par daal dena hai, hal nahi.</p>

     <p><b>✅ Real: Key do — jo cheezein ordered chahiye, unki key ek rakho</b></p>
     <pre>producer.send({ topic: 'ride-events',
                messages: [{ key: rideId,      // ← YEH
                             value: JSON.stringify(event) }] })

partition = murmur2(key) & 0x7fffffff % numPartitions

ride-99 ka HAR event → hamesha WAHI partition
→ ek hi consumer, ek hi log, PERFECT ORDER ✅</pre>
     <p>Aur alag rides alag partitions par ja sakti hain — toh <b>parallelism bhi bacha raha</b>.</p>
     <p><b>Kafka ki asli guarantee, saaf shabdon mein:</b></p>
     <pre>✅ EK partition ke ANDAR — order poori tarah guaranteed
❌ Partitions ke BEECH  — koi guarantee NAHI, kabhi nahi

Toh: "same key = same partition = ordered"  ← yahi tumhara hathiyaar hai</pre>`,

    `<h3>⚖️ Nayi musibat: hot partition</h3>
     <p>Key laga di, ordering theek ho gayi. Par ab key <b>kya</b> chuni, yeh sabse
     zaroori faisla ban gaya:</p>
     <pre>❌ key = "IN"  (country)
   → 90% traffic India se → ek partition par 90% load
   → us partition ka consumer maro, baaki 5 khaali baithe
   → yeh "hot partition" hai

❌ key = orderStatus  ("PENDING"/"DONE")
   → sirf 2 alag values → sirf 2 partitions use hongi
   → chahe 20 partitions ho, parallelism 2

✅ key = rideId / userId / orderId
   → lakhon alag values → sab partitions par barabar
   → aur wahi ordering bhi mil gayi jo chahiye thi</pre>
     <p><b>Sahi key chunne ka niyam:</b> woh cheez jiske liye tumhe order chahiye,
     aur jiski <b>bahut saari alag values</b> hon (high cardinality).</p>
     <p><b>Aur ek baat:</b> agar ek hi key par sach mein bahut zyada traffic hai
     (jaise ek celebrity user), toh key ko <b>salt</b> kar sakte ho —
     <code>userId + ":" + (n % 4)</code> — par tab us user ke andar ordering
     4 sub-streams mein bant jaayegi. Har cheez ka mol hai.</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: hot partition banao aur theek karo',
      instructions: 'Yeh asli murmur2 hashing hai. Jo hoga, sach mein hoga.',
      goal: `<code>events</code> topic mein 4 partitions hain.<br><br>
        1. <b>200 messages</b> bhejo EK HI key se (<code>--count 200 --key SAME</code>)<br>
        2. Dekho ki woh kahan gaye — <code>dump events</code> ya producer ka output<br>
        3. Ab <b>200 messages</b> alag-alag keys se bhejo (<code>--key user-{i}</code>)<br>
        4. Dono ka farak dekho — <b>yahi hot partition vs achhi key ka farak hai</b>`,
      cluster: { brokers: 3, topics: { events: { partitions: 4, rf: 3 } } },
      tasks: [
        { text: 'ek hi key se 200 bhejo', check: s => s.history.some(h => /--count 200.*--key (?!user-\{i\})\S+/.test(h)) },
        { text: 'alag alag keys se 200 bhejo', check: s => s.history.some(h => /--key user-\{i\}|--key random/.test(h)) },
        { text: 'saare partitions par data pahuncha', check: s => s.partitionCounts('events').every(c => c > 0) },
      ],
      success: s => s.partitionCounts('events').every(c => c > 0) &&
                    s.history.some(h => /--key user-\{i\}|--key random/.test(h)),
      successText: `Dekha? Ek hi key = <b>ek hi partition</b>, chahe 200 messages ho ya 2 crore.
        Yeh koi bug nahi — yahi woh guarantee hai jo ordering deti hai.<br><br>
        Par isi guarantee ki keemat hai: <b>us key ka poora traffic ek consumer par</b>.
        Isliye key chunte waqt do cheezein saath dekho:<br>
        1️⃣ <b>Ordering:</b> kis cheez ke events ordered chahiye?<br>
        2️⃣ <b>Cardinality:</b> us cheez ki kitni alag values hain?<br><br>
        <code>rideId</code>/<code>userId</code> dono par khara utarta hai. <code>country</code> nahi.`,
      hints: [
        'kafka-console-producer --topic events --count 200 --key SAME',
        'kafka-console-producer --topic events --count 200 --key user-{i}     ← {i} har message par badalta hai',
        'dump events   — har partition mein kitne records hain, khud gin lo',
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Achhi key vs buri key',
      instructions: 'Har key ko sahi dabbe mein daalo. Do cheezein socho: ordering milegi? aur load barabar bantega?',
      bins: [
        { id: 'good', label: '✅ Achhi partition key' },
        { id: 'bad', label: '🔥 Buri key (hot partition)' },
      ],
      items: [
        { text: 'orderId', bin: 'good',
          explain: 'Lakhon alag values (barabar bantega) + ek order ke saare events ordered. Perfect.' },
        { text: 'userId', bin: 'good',
          explain: 'High cardinality aur "ek user ke events ordered" akser bilkul wahi hai jo chahiye.' },
        { text: 'country', bin: 'bad',
          explain: 'Sirf ~200 values, aur distribution bilkul barabar nahi — 90% traffic ek-do desh se. Classic hot partition.' },
        { text: 'orderStatus (PENDING/SHIPPED/DONE)', bin: 'bad',
          explain: 'Sirf 3-4 values → chahe 50 partitions ho, sirf 3-4 use hongi. Parallelism khatam.' },
        { text: 'deviceId (IoT sensors)', bin: 'good',
          explain: 'Har device ki apni ID, lakhon devices, aur ek device ke readings ordered chahiye. Bilkul sahi.' },
        { text: 'eventType (click/view/purchase)', bin: 'bad',
          explain: 'Bahut kam values aur bahut skewed — "view" events "purchase" se 1000× zyada honge.' },
        { text: 'accountId (banking ledger)', bin: 'good',
          explain: 'Ek account ke transactions ka ordered hona ZAROORI hai (balance galat ho jaayega), aur accounts lakhon hain.' },
        { text: 'currentDate ("2026-07-22")', bin: 'bad',
          explain: 'Sabse bura! Ek din ka POORA traffic ek hi partition par. Aur agle din poora load kisi doosri partition par shift ho jaayega.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Ordering ke sawal',
      questions: [
        {
          q: 'Kafka ordering ki guarantee kahan tak deta hai?',
          choices: [
            'Poore topic mein',
            'Sirf EK PARTITION ke andar. Partitions ke beech koi guarantee nahi — isliye "same key → same partition → ordered" hi asli hathiyaar hai.',
            'Poore cluster mein',
            'Ek consumer group ke andar',
          ],
          answer: 1,
          explain: 'Yeh Kafka ka sabse zaroori sentence hai. Topic-level ordering exist hi nahi karti. Jo cheez ordered chahiye, unki key ek rakho.',
        },
        {
          q: 'Ek partition par 95% messages aa rahe hain. Consumers badhane se kya hoga?',
          choices: [
            'Lag turant theek ho jaayega',
            'Kuch nahi — ek partition ko ek hi consumer padh sakta hai. Yeh key ki problem hai, capacity ki nahi. Key badalni padegi (ya salt karni padegi).',
            'Kafka apne aap rebalance kar dega',
            'Partition apne aap split ho jaayega',
          ],
          answer: 1,
          explain: 'Hot partition ka ilaaj kabhi bhi "aur consumers" nahi hota. Root cause key ka chunav hai. Yaad rakho: partition = parallelism ki limit.',
        },
        {
          q: 'Tumhe har user ke events ordered chahiye PAR ek user itna bada hai ki uski partition hot ho gayi. Kya karoge?',
          choices: [
            'Ordering chhod do',
            'Key ko salt karo — userId + ":" + (hash % 4) — us user ka load 4 partitions par bant jaayega, par tab us user ke andar ordering 4 sub-streams mein bant jaayegi. Yeh soch-samajh kar liya gaya trade-off hai.',
            'Partitions 1000 kar do',
            'Us user ko block kar do',
          ],
          answer: 1,
          explain: 'Yeh senior-level jawab hai: pehchano ki ordering aur load-balance mein SEEDHA takraav hai, aur explicit trade-off lo. Agar us user ke events aapas mein independent hain (jaise alag ride ids), toh salt karna bilkul safe hai.',
        },
      ],
    },
    {
      type: 'fill',
      title: '📄 Ordered producer likho',
      instructions: 'kafkajs se ek producer jisme ek order ke saare events ordered rahen.',
      template: "await producer.send({\n  topic: 'order-events',\n  messages: [{\n    key: order.{0},        // ← ordering ki jadd\n    value: JSON.stringify(event),\n  }],\n});\n\n// partition = murmur2(key) % numPartitions\n// same key  → same {1}  → ordered\n// alag keys → alag partitions → {2} bacha rehta hai",
      blanks: [
        { answer: 'id', explain: 'orderId — high cardinality aur exactly woh cheez jiske events ordered chahiye.' },
        { answer: 'partition', explain: 'Ordering partition ke andar hi milti hai, isliye same key ka same partition par jaana hi sab kuch hai.' },
        { answer: 'parallelism', explain: 'Yahi key-based partitioning ka jaadu hai — ordering bhi mili aur scaling bhi.' },
      ],
      tokens: ['id', 'partition', 'parallelism', 'status', 'topic', 'broker', 'offset', 'country'],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Kafka ordering guarantee karta hai — sach ya jhooth?"',
        choices: [
          'Sach — Kafka hamesha ordered hai',
          'Jhooth — Kafka kabhi order guarantee nahi karta',
          'Adhoora sach: Kafka ek PARTITION ke andar ordering guarantee karta hai, topic ke across nahi. Isliye jis cheez ka order chahiye uski key set karo — same key hamesha same partition par jaati hai.',
          'Sirf acks=all ke saath',
        ],
        answer: 2,
        explain: 'Interviewer yeh dekh raha hai ki tum guarantee ki SEEMA jaante ho. "Haan Kafka ordered hai" bolna production mein us ride-cancelled wale bug ki taraf le jaata hai.',
      },
      {
        q: '🧑‍💼 "Agar tumhe pure GLOBAL ordering chahiye poore topic par, toh?"',
        choices: [
          'Har partition ko sort kar do',
          'Ek hi partition rakhni padegi — par tab parallelism 1 ho jaayega aur throughput ek consumer tak simat jaayega. Isliye pehle main yeh challenge karunga ki kya sach mein GLOBAL ordering chahiye, ya per-entity ordering (per user/order) kaafi hai — 99% cases mein per-entity hi chahiye hota hai.',
          'Timestamp se sort karo',
          'Global ordering mumkin nahi hai',
        ],
        answer: 1,
        explain: 'Best jawab requirement ko hi challenge karta hai. Global ordering ki keemat (parallelism = 1) itni bhaari hai ki pehle confirm karna chahiye ki woh sach mein chahiye. Zyadatar log "global" bolte hain par matlab "per-entity" hota hai.',
      },
    ],
  },
  success: [
    'Main bata sakta hoon ki Kafka ordering sirf partition ke andar deta hai',
    'Main sahi partition key chun sakta hoon (ordering + cardinality dono dekh kar)',
    'Main hot partition pehchan sakta hoon aur jaanta hoon ki consumers badhane se woh theek nahi hota',
    'Main ordering aur parallelism ka trade-off samjha sakta hoon',
  ],
  homework: `<p>Ordering ka bug khud banao aur phir theek karo:</p>
    <pre># 3 partitions
kafka-topics.sh --create --topic ord-test --partitions 3 --bootstrap-server localhost:9092

# ❌ BINA key ke — order ki koi guarantee nahi
echo "ride-created"   | kafka-console-producer.sh --topic ord-test --bootstrap-server localhost:9092
echo "ride-cancelled" | kafka-console-producer.sh --topic ord-test --bootstrap-server localhost:9092

# ✅ key ke SAATH — dono ek hi partition par
printf "ride99:created\\nride99:cancelled\\n" | \\
  kafka-console-producer.sh --topic ord-test --bootstrap-server localhost:9092 \\
  --property parse.key=true --property key.separator=:</pre>
    <p>Har baar ke baad <code>GetOffsetShell</code> se dekho ki messages kis partition par gaye.
    Bina key ke woh bikhar jaayenge; key ke saath dono ek jagah.</p>`,
  cards: [
    { front: 'Kafka ordering kahan guarantee karta hai?', back: 'Sirf <b>ek partition ke andar</b>. Partitions ke beech <b>koi</b> guarantee nahi. Isliye: "same key → same partition → ordered".' },
    { front: 'Key se partition kaise chunta hai?', back: '<code>partition = murmur2(key) & 0x7fffffff % numPartitions</code>. Deterministic hai — isliye ek key hamesha ek hi partition par jaati hai (jab tak partition count na badle).' },
    { front: 'Achhi partition key ki 2 shartein?', back: '1) <b>Ordering:</b> woh cheez ho jiske events ordered chahiye (orderId, userId).<br>2) <b>High cardinality:</b> bahut saari alag values, taaki load barabar bante. <code>country</code>/<code>status</code>/<code>date</code> = hot partition.' },
    { front: 'Hot partition ka ilaaj consumers badhana kyun nahi hai?', back: 'Ek partition ko group mein <b>ek hi consumer</b> padh sakta hai. Root cause key ka chunav hai. Ilaaj: behtar key, ya key ko <b>salt</b> karna (<code>userId:n%4</code>) — par tab us entity ki ordering sub-streams mein bant jaayegi.' },
    { front: 'Global ordering chahiye toh?', back: 'Sirf <b>1 partition</b> — matlab parallelism 1. Keemat bahut bhaari hai. Pehle challenge karo: kya sach mein global chahiye ya <b>per-entity</b> ordering kaafi hai? 99% cases mein per-entity hi chahiye.' },
  ],
},

/* =====================================================================
   7. DELIVERY SEMANTICS
   ===================================================================== */
{
  id: 'delivery',
  icon: '📬',
  title: 'Delivery Semantics — "exactly once" ka sabse bada jhooth',
  badge: 'Semantics Ka Jaankar',
  objectives: [
    'At-most-once, at-least-once aur exactly-once ka asli farak samjho',
    'Jaano ki commit ka waqt hi semantics tay karta hai',
    'Samjho ki "exactly once" end-to-end kyun namumkin hai — aur log usse kaise haasil karte hain',
    'Idempotency ko asli hathiyaar ke roop mein pehchano',
  ],
  lesson: [
    `<h3>😰 Dard: "Customer se 3 baar paisa kat gaya"</h3>
     <p>Ek payment worker Kafka se <code>payment-requested</code> events padhta tha aur
     payment gateway ko charge karta tha. Ek raat gateway 8 second dheema ho gaya.
     Consumer ka <code>max.poll.interval.ms</code> paar ho gaya:</p>
     <pre>t=0s    consumer ne 100 payments poll kiye
t=0-30s consumer charge kar raha hai (gateway dheema)
t=300s  ⚠️ max.poll.interval paar → broker: "yeh mar gaya"
        → REBALANCE → partitions kisi aur consumer ko
t=305s  naya consumer WAHI offset se shuru
        → WAHI 100 payments DOBARA charge 💳💳
t=610s  phir wahi hua → TEESRI baar 💳💳💳</pre>
     <div class="war-story"><b>⚡ 1,400 customers se do ya teen baar paisa kata.</b>
     Refund mein 3 din lage. Sabse buri baat: system ne <b>bilkul waisa hi kaam kiya
     jaisa design kiya gaya tha</b>. At-least-once ka matlab hi yahi hai —
     "kam se kam ek baar", yaani <b>kabhi kabhi zyada baar</b>.</div>`,

    `<h3>🪜 Teen semantics — farak sirf COMMIT ke waqt ka hai</h3>
     <pre>1️⃣ AT-MOST-ONCE  (pehle commit, phir kaam)
   poll → commit → process
   crash beech mein? → woh message dobara nahi aayega
   💀 data LOSS  ·  ✅ koi duplicate nahi
   kab: metrics, logs — jahan khona chalega

2️⃣ AT-LEAST-ONCE  (pehle kaam, phir commit)  ← DEFAULT
   poll → process → commit
   crash beech mein? → wahi message DOBARA aayega
   ✅ kuch khota nahi  ·  💀 DUPLICATES
   kab: 95% systems. Duplicates ko idempotency se sambhalo.

3️⃣ EXACTLY-ONCE  (kaam aur commit ek hi transaction mein)
   begin → process → commit-offset → end   (sab atomic)
   ✅ na loss  ·  ✅ na duplicate
   💰 keemat: dheema, complex, aur SIRF Kafka→Kafka ke andar</pre>
     <p><b>Aur yahan sabse bada jhooth hai:</b> "Exactly-once" ka matlab yeh
     <b>nahi</b> hai ki tumhara code exactly ek baar chalega. Iska matlab hai ki
     <b>asar (effect) exactly ek baar dikhega</b>.</p>
     <p>Kafka ka EOS tab kaam karta hai jab source aur destination <b>dono Kafka</b> hon
     (kyunki tab offset commit aur output write ek hi Kafka transaction mein aa sakte hain).
     Par jaise hi tum <b>bahar</b> jaate ho — payment gateway, email, third-party API —
     woh transaction unhe cover nahi kar sakta. Woh gateway roll back nahi hoga.</p>`,

    `<h3>⚖️ Asli hal: idempotency (har jagah kaam karta hai)</h3>
     <p>Jab exactly-once nahi mil sakta, toh <b>duplicate ko harmless bana do</b>.
     Yeh distributed systems ka sabse taakatwar idea hai:</p>
     <pre>❌ NAHI (duplicate = double charge):
   charge(customer, 499)

✅ HAAN (duplicate = kuch nahi hota):
   chargeIfNotDone(idempotencyKey = paymentId, customer, 499)

   Gateway ki taraf:
     if (seen(paymentId)) return purana_result;   ← duplicate safe
     else { charge(); remember(paymentId); }</pre>
     <p><b>DB mein bhi wahi idea:</b></p>
     <pre>-- ❌ duplicate par galat
INSERT INTO payments (id, amount) VALUES (?, ?);

-- ✅ duplicate par kuch nahi hota
INSERT INTO payments (id, amount) VALUES (?, ?)
  ON DUPLICATE KEY UPDATE amount = amount;   -- no-op</pre>
     <p><b>Practical rule jo har jagah chalta hai:</b></p>
     <pre>at-least-once delivery  +  idempotent processing
                        =
        "effectively exactly once"  ✅

Yeh EOS se aasan bhi hai, tez bhi, aur Kafka ke bahar bhi chalta hai.</pre>
     <p>Isi liye asli production systems mein 95% jagah tum yahi dekhoge —
     poora Kafka transactions ka tamjhaam nahi.</p>`,
  ],
  stages: [
    {
      type: 'order',
      title: '🔢 At-least-once ka sahi kram',
      instructions: 'Payment worker ko is tarah likho ki kuch khoye bhi na, aur double charge bhi na ho.',
      steps: [
        { text: '📥 poll() — payment-requested events lo',
          explain: 'Batch chhota rakho (max.poll.records) taaki processing max.poll.interval ke andar poori ho jaaye.' },
        { text: '🔍 Check karo: kya yeh paymentId pehle process ho chuka hai?',
          explain: 'Yahi idempotency check hai. Iske bina at-least-once ka matlab hai double charge. Yeh kadam SABSE zaroori hai.' },
        { text: '💳 Agar naya hai toh gateway ko idempotency-key ke saath charge karo',
          explain: 'Gateway ko bhi idempotency key do — taaki agar tumhara check aur charge ke beech crash ho, toh gateway khud duplicate rok le. Do layer ki suraksha.' },
        { text: '💾 paymentId ko "done" mark karo (usi DB transaction mein)',
          explain: 'Charge ka result aur "done" flag ek saath commit hone chahiye. Alag hue toh beech mein crash par phir duplicate.' },
        { text: '✅ Kafka offset commit karo',
          explain: 'Sabse aakhir mein. Ab agar yahan crash hua toh message dobara aayega — par idempotency check use rok lega. Bilkul safe.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Semantics ke sawal',
      questions: [
        {
          q: '"Exactly-once" ka asli matlab kya hai?',
          choices: [
            'Tumhara code exactly ek baar chalega',
            'Message exactly ek baar network par jaayega',
            'ASAR (effect) exactly ek baar dikhega — code shayad kai baar chale, par natija ek baar ka hi ho. Aur Kafka ka EOS sirf Kafka→Kafka ke andar hi yeh de sakta hai.',
            'Duplicates apne aap delete ho jaate hain',
          ],
          answer: 2,
          explain: 'Yeh sabse bada misconception hai. Network par duplicate hamesha ho sakta hai (ack kho jaana). Sawal yeh hai ki ASAR kitni baar hua. Isi liye idempotency itni taakatwar hai.',
        },
        {
          q: 'Tumhara consumer Kafka se padh kar THIRD-PARTY email API ko call karta hai. Kya Kafka ka exactly-once yahan bachayega?',
          choices: [
            'Haan, transactions sab sambhal lenge',
            'Nahi. Kafka ka transaction sirf Kafka ke andar ka kaam (offset + output topic) atomic karta hai. Email API uss transaction ka hissa nahi — email bhej diya toh woh roll back nahi hoga. Yahan idempotency chahiye (email API ko dedup key do, ya khud track karo ki kaunse emails ja chuke).',
            'Haan agar acks=all ho',
            'Haan agar consumer transactional ho',
          ],
          answer: 1,
          explain: 'Yeh EOS ki sabse zaroori seema hai. Jaise hi tum Kafka ke bahar side-effect karte ho (email, payment, SMS, third-party API), EOS ki guarantee wahin khatam ho jaati hai.',
        },
        {
          q: 'Sabse aam production pattern kaunsa hai?',
          choices: [
            'At-most-once — sabse tez hai',
            'Kafka transactions se exactly-once — sabse safe hai',
            'At-least-once delivery + idempotent processing = "effectively exactly once". Aasan, tez, aur Kafka ke bahar bhi chalta hai.',
            'Har message ko do baar bhejo aur compare karo',
          ],
          answer: 2,
          explain: 'Yeh industry ka default hai. EOS ka apna jagah hai (stream processing, Kafka→Kafka pipelines), par jab bahari duniya se baat karni ho toh idempotency hi ek matra bharosemand raasta hai.',
        },
      ],
    },
    {
      type: 'match',
      title: '🔗 Kaunsa use-case, kaunsa semantic',
      instructions: 'Har scenario ko sahi delivery semantic se jodo.',
      pairs: [
        { left: 'Server CPU metrics har 10 sec', right: '1️⃣ At-most-once',
          explain: 'Ek data point khona chart mein chhota gap hai. Speed zyada zaroori hai, aur duplicate metrics graph ko galat kar dete.' },
        { left: 'Order processing pipeline', right: '2️⃣ At-least-once + idempotency',
          explain: 'Order kabhi khona nahi chahiye. Duplicate ka ilaaj orderId par idempotency se karo.' },
        { left: 'Payment charge karna', right: '2️⃣ At-least-once + idempotency',
          explain: 'Payment gateway Kafka transaction ka hissa nahi ho sakta, isliye EOS bekaar hai. Idempotency key hi ek matra hal hai.' },
        { left: 'Kafka topic se padh kar doosre Kafka topic mein aggregate likhna', right: '3️⃣ Exactly-once (Kafka transactions)',
          explain: 'Yahi woh ek jagah hai jahan Kafka ka EOS poori tarah kaam karta hai — source aur sink dono Kafka hain, toh offset aur output ek transaction mein aa sakte hain.' },
        { left: 'Debug logs collect karna', right: '1️⃣ At-most-once',
          explain: 'Volume bahut zyada, value per message bahut kam. Kuch khona bilkul theek hai.' },
      ],
    },
    {
      type: 'lab',
      title: '🎛️ Lab: duplicate ya data loss — chuno',
      instructions: 'Retries ON karo aur dekho ki kuch bhi na khoye. Phir sochо ki uski keemat kya hai.',
      scenario: `Payment worker chal raha hai. Downstream gateway <b>20s se 60s tak
        poori tarah down</b> rahega. Abhi <b>koi retry nahi</b> hai — matlab jo fail hua,
        woh <b>hamesha ke liye gaya</b>.`,
      goal: `Aisa configure karo ki <b>ek bhi message HAMESHA ke liye gum na ho</b>
        (dropped = 0). Hint: fail hone par messages ko kahin toh jaana chahiye.`,
      duration: 100,
      preset: { producerRate: 400, consumers: 8, processMs: 15, failRate: 0.02,
                retry: 'none', maxRetries: 5, dlq: false, baseDelay: 1,
                downstreamCapacity: 1200, outage: [20, 60] },
      knobs: ['retry', 'maxRetries', 'dlq'],
      charts: ['lag', 'retryRate', 'throughput'],
      success: r => r.dropped === 0 && r.produced > 0,
      verdict: r => {
        if (r.dropped > 0) return `💀 <b>${r.dropped.toLocaleString()} messages HAMESHA ke liye gum ho gaye.</b>
          Yeh at-most-once hai — fail hua toh khatam. Payments ke liye yeh bilkul manzoor nahi.`;
        const dup = r.totalRetries;
        return `✅ <b>Kuch bhi gum nahi hua.</b> Par dhyan do: <b>${dup.toLocaleString()} retry attempts</b>
          hue. Har retry ek <b>duplicate processing ka mauka</b> hai — kyunki ho sakta hai
          kaam pehli baar ho gaya ho aur sirf ack kho gaya ho.<br><br>
          <b>Isi liye at-least-once ke saath idempotency LAZMI hai.</b>
          ${r.dlq ? `Aur <b>${r.dlq.toLocaleString()}</b> messages DLQ mein gaye — woh gum nahi hue, alag rakhe gaye hain taaki tum baad mein dekh sako.` : ''}`;
      },
      successText: `Tumne abhi <b>at-most-once se at-least-once</b> ka safar khud kiya.<br><br>
        Dhyan do ki tumne kya trade kiya: <b>data loss</b> gaya, par uski jagah
        <b>duplicates</b> aa gaye (har retry ek duplicate ka mauka hai).<br><br>
        Yeh sauda lagbhag hamesha sahi hai — <b>duplicate ko idempotency se sambhala ja sakta hai,
        par gum hua data wapas nahi aata.</b><br><br>
        Aur agar tumne DLQ bhi ON kiya: ab jo messages baar-baar fail hote hain woh
        pipeline ko block kiye bina ek alag jagah chale jaate hain. Woh poora level
        Track 2 mein hai. 💀`,
      hints: [
        'Retry policy "Koi retry nahi" par hai — fail hua message seedha gayab ho raha hai.',
        'Koi bhi retry policy chuno (exponential behtar hai). Ab fail hone par message wapas try hoga.',
        'Agar phir bhi dropped > 0 hai: maxRetries khatam hone par messages kahan jaate hain? DLQ ON karo.',
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Kya Kafka exactly-once deta hai?"',
        choices: [
          'Haan, enable.idempotence=true se',
          'Nahi, kabhi nahi',
          'Kafka→Kafka ke andar haan (transactions + read_committed se offset aur output atomic ho jaate hain). Par jaise hi koi bahari side-effect ho — payment, email, third-party API — woh guarantee khatam. Wahan at-least-once + idempotency hi asli hal hai.',
          'Sirf Kafka Streams mein',
        ],
        answer: 2,
        explain: 'Best jawab "haan/na" nahi, SEEMA batata hai. Jo candidate bolta hai "haan Kafka exactly-once deta hai" woh production mein double-charge wala bug likhega.',
      },
      {
        q: '🧑‍💼 "Tumhara consumer duplicate messages process kar raha hai. Kya karoge?"',
        choices: [
          'Exactly-once ON kar dunga',
          'Auto-commit band kar dunga',
          'Pehle yeh maan lunga ki duplicates NORMAL hain (at-least-once ka natija) — unhe khatam karne ki koshish ke bajaye HARMLESS banaunga: processing ko idempotent karunga (unique key par upsert / dedup table / downstream ko idempotency-key). Saath mein root cause bhi dekhunga — agar rebalance loop ki wajah se ho raha hai toh max.poll.records bhi theek karunga.',
          'Consumers kam kar dunga',
        ],
        answer: 2,
        explain: 'Senior jawab duplicates ko bimari nahi, mausam maanta hai. Tum unhe poori tarah rok nahi sakte — tum unhe bekaar bana sakte ho. Saath mein root cause dekhna (rebalance storm) bonus point hai.',
      },
    ],
  },
  success: [
    'Main teeno delivery semantics ka farak bata sakta hoon',
    'Main jaanta hoon ki commit ka waqt hi semantics tay karta hai',
    'Main samjha sakta hoon ki exactly-once bahari systems ke saath kyun nahi chalta',
    'Main idempotent processing design kar sakta hoon',
  ],
  homework: `<p>Duplicate khud banao — at-least-once ko mehsoos karo:</p>
    <pre>const consumer = kafka.consumer({ groupId: 'dup-test' });
await consumer.run({
  autoCommit: false,                       // manual commit
  eachMessage: async ({ topic, partition, message }) => {
    console.log('processing:', message.value.toString());
    await doWork();
    // 👇 yahan Ctrl+C dabao — commit se PEHLE
    await consumer.commitOffsets([{ topic, partition,
      offset: (Number(message.offset) + 1).toString() }]);
  },
});</pre>
    <p>Processing ke baad par commit se pehle maaro. Restart karo — <b>wahi message
    dobara aayega</b>. Ab ek <code>Set</code> mein processed ids rakho aur dekho ki
    idempotency check use kaise harmless bana deta hai.</p>`,
  cards: [
    { front: 'Teen delivery semantics — farak kis cheez ka hai?', back: 'Sirf <b>commit ke waqt</b> ka!<br><b>At-most-once:</b> commit → process (data loss)<br><b>At-least-once:</b> process → commit (duplicates)<br><b>Exactly-once:</b> dono ek transaction mein' },
    { front: '"Exactly-once" ka asli matlab?', back: 'Code exactly ek baar chalega — yeh <b>galat</b> hai. Sahi: <b>ASAR exactly ek baar dikhega</b>. Aur Kafka ka EOS sirf <b>Kafka→Kafka</b> ke andar kaam karta hai.' },
    { front: 'Exactly-once bahari API ke saath kyun fail hota hai?', back: 'Kafka ka transaction sirf offset + Kafka output ko atomic karta hai. <b>Payment gateway / email / third-party API roll back nahi hote.</b> Wahan idempotency hi ek matra hal hai.' },
    { front: 'Industry ka default pattern?', back: '<code>at-least-once delivery + idempotent processing = "effectively exactly once"</code>. Aasan, tez, aur Kafka ke bahar bhi chalta hai.' },
    { front: 'Duplicate ko harmless kaise banate hain?', back: '<b>Idempotency key</b> par kaam karo: <code>INSERT ... ON DUPLICATE KEY UPDATE</code> (no-op), dedup table, ya downstream API ko <code>Idempotency-Key</code> header do. Duplicate rokne ki koshish mat karo — use <b>bekaar</b> bana do.' },
  ],
},

/* =====================================================================
   8. MESSAGE SHAPE & SERIALIZATION
   ===================================================================== */
{
  id: 'serialization',
  icon: '📦',
  title: 'Message ka Shape — "producer ne ek field hata di aur 40 consumers gir gaye"',
  badge: 'Schema Ka Sipahi',
  objectives: [
    'Samjho ki JSON blob bhejna kab aur kyun phatt jaata hai',
    'Event design ke niyam jaano (event vs command, fat vs thin)',
    'Schema evolution ka basic idea samjho',
    'Headers ka sahi istemaal jaano',
  ],
  lesson: [
    `<h3>😰 Dard: "Ek field rename ki, poora platform gir gaya"</h3>
     <p>Ek team ne <code>user-events</code> topic par plain JSON bhejna shuru kiya.
     Tez tha, aasan tha, koi tooling nahi chahiye thi. 18 mahine mein us topic ke
     <b>40 consumers</b> ban gaye.</p>
     <p>Ek din kisi ne cleanup ke naam par field rename ki:</p>
     <pre>// pehle
{ "userId": 123, "email": "a@b.com", "signupDate": "2026-01-01" }

// baad mein — "naam behtar hai"
{ "user_id": 123, "email": "a@b.com", "created_at": "2026-01-01" }</pre>
     <div class="war-story"><b>⚡ 40 mein se 31 consumers turant crash ho gaye</b> —
     <code>TypeError: Cannot read property 'toString' of undefined</code>.
     Baaki 9 aur bhi bure the: woh crash <b>nahi</b> hue, unhone chup-chaap
     <code>userId = undefined</code> likh diya. Analytics 6 din tak galat rahi
     aur kisi ko pata nahi chala.<br><br>
     Aur rollback bhi kaam nahi aaya — kyunki naye format ke lakhon messages
     topic mein pade the aur unhe padhna hi tha.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Sab consumers ko ek saath deploy kar denge"</b><br>
     40 consumers, 12 alag teams, alag release cycles. Coordination namumkin hai.
     Aur jo purane messages topic mein pade hain unka kya?</p>

     <p><b>⚠️ Better: "JSON mein version field daal do"</b><br>
     <code>{ "version": 2, ... }</code> — behtar hai! Par ab har consumer ko
     <b>khud</b> har version handle karna padega, aur kuch bhi compile-time par
     check nahi hota. Koi galti se v3 bhej de toh pata production mein hi chalega.</p>

     <p><b>✅ Real: Schema + compatibility rules</b><br>
     Avro/Protobuf + Schema Registry (poora level Track 3 mein). Par uska
     <b>asli idea</b> abhi seekh lo, kyunki woh JSON par bhi laagu hota hai:</p>
     <pre>✅ BACKWARD-COMPATIBLE badlaav (safe):
   • naya OPTIONAL field add karo (default value ke saath)
   • field ko optional banao

❌ BREAKING badlaav (kabhi mat karo):
   • field ka naam badalna       ← 31 consumers marey
   • field hatana                 (jo use kar rahe hain woh marenge)
   • field ka type badalna        (string → int)
   • enum se value hatana

🔄 Naam badalna hi ho toh — EXPAND-CONTRACT:
   1. naya field ADD karo, dono bhejo (purana + naya)
   2. saare consumers ko naye field par shift hone do (hafte/mahine)
   3. jab koi purana use na kare, TAB purana hatao</pre>`,

    `<h3>⚖️ Event design ke niyam</h3>
     <p><b>1. Event bhejo, command nahi.</b></p>
     <pre>❌ command:  { "action": "sendEmail", "to": "..." }
   → producer bata raha hai ki kya KARNA hai
   → coupling wapas aa gaya

✅ event:    { "type": "OrderPlaced", "orderId": 88, ... }
   → producer bata raha hai ki kya HUA hai
   → consumers khud tay karein ki kya karna hai
   → kal naya consumer aaye toh producer nahi badlega</pre>
     <p><b>2. Fat vs thin events — trade-off samjho.</b></p>
     <pre>🥬 THIN:  { "orderId": 88 }
   → consumer ko DB se baaki data lena padega (N+1 calls!)
   → aur woh data ab BADAL chuka ho sakta hai (event ka waqt nahi milega)

🍔 FAT:   { "orderId": 88, "items": [...], "total": 4990, "user": {...} }
   → self-contained, koi lookup nahi, ITIHAAS sach mein preserve hota hai
   → par message bada, aur schema badalna zyada mushkil

👉 Aam salah: FAT ki taraf jhuko. Us waqt ka data event mein hona
   asli fayda hai — "order us waqt 4990 ka tha" hamesha sach rahega.</pre>
     <p><b>3. Headers ka sahi istemaal.</b> Kafka messages par <b>headers</b> hote hain
     (HTTP jaise). Metadata unme daalo, payload mein nahi:</p>
     <pre>headers: { 'schema-version': '2',
           'correlation-id': 'req-8821',   ← tracing ke liye
           'source-service': 'checkout-api',
           'retry-count': '3' }            ← retry/DLQ mein bahut kaam aata hai</pre>
     <p>Fayda: consumer <b>payload parse kiye bina</b> routing/filtering ka faisla le sakta hai.</p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Safe badlaav ya breaking?',
      instructions: 'Topic par 40 consumers hain. Har badlaav ko sahi dabbe mein daalo.',
      bins: [
        { id: 'safe', label: '✅ Safe (backward-compatible)' },
        { id: 'breaking', label: '💥 Breaking (consumers marenge)' },
      ],
      items: [
        { text: 'Naya optional field add karna (default ke saath)', bin: 'safe',
          explain: 'Purane consumers use ignore kar denge, naye use padhenge. Yahi sabse aam aur safe badlaav hai.' },
        { text: 'Field ka naam userId se user_id karna', bin: 'breaking',
          explain: 'Purane consumers ko userId milega hi nahi → undefined → crash ya (aur bura) chup-chaap galat data.' },
        { text: 'Ek field hatana jo koi use nahi karta', bin: 'safe',
          explain: 'Agar SACH MEIN koi use nahi karta toh safe hai — par pehle yeh confirm karna hi asli kaam hai. Consumer count aur logs check karo.' },
        { text: 'amount ko string se number karna', bin: 'breaking',
          explain: 'Type change hamesha breaking hai. "499" aur 499 alag hain — comparisons aur arithmetic sab galat ho jaayenge.' },
        { text: 'Enum mein nayi value add karna (PENDING, SHIPPED → + RETURNED)', bin: 'breaking',
          explain: 'Yeh chupa hua breaking change hai! Purane consumers ka switch/case RETURNED ko handle nahi karta — ya crash, ya chup-chaap galat branch. Isliye hamesha default case rakho.' },
        { text: 'Optional field ko required banana', bin: 'breaking',
          explain: 'Purane producers woh field bhej hi nahi rahe → naye consumers unke messages reject karenge.' },
        { text: 'Naya event TYPE add karna (naya topic ya naya type value)', bin: 'safe',
          explain: 'Purane consumers unhe filter kar denge (agar unka code unknown types ko theek se ignore karta hai — isliye "unknown = skip" hamesha likhо).' },
        { text: 'Nested object ko flat karna', bin: 'breaking',
          explain: 'user.name se userName karna path badal deta hai — purane consumers ko undefined milega.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Event design',
      questions: [
        {
          q: 'Event bhejna command bhejne se behtar kyun hai?',
          choices: [
            'Event chhote hote hain',
            'Command mein producer bata raha hai ki kya KARNA hai — matlab usse pata hona chahiye ki consumers kaun hain aur kya karte hain. Event batata hai ki kya HUA — consumers khud tay karte hain. Kal naya consumer aaye toh producer bilkul nahi badlega.',
            'Event tez process hote hain',
            'Command ko Kafka support nahi karta',
          ],
          answer: 1,
          explain: 'Yeh decoupling ka asli imtihaan hai. "sendEmail" command bhejne ka matlab hai ki producer ko email service ke baare mein pata hai — wahi coupling jisse bhaagne ke liye humne queue lagayi thi.',
        },
        {
          q: 'Field rename karna hi hai (userId → user_id). Kaise karoge bina kisi ko todhe?',
          choices: [
            'Ek hi deploy mein sab badal do',
            'Expand-contract: pehle NAYA field add karo aur DONO bhejo → saare consumers ko naye par shift hone do → jab koi purana use na kare TAB purana hatao. Yeh hafton/mahinon ka kaam hai, ek deploy ka nahi.',
            'Naya topic bana do',
            'Consumers ko bol do ki dono handle karein',
          ],
          answer: 1,
          explain: 'Expand-contract har schema migration ka jawab hai — DB migrations mein bhi wahi pattern hai. Asli baat yeh hai ki purana field hatane se pehle SABOOT chahiye ki koi use nahi kar raha.',
        },
        {
          q: 'Correlation ID kahan rakhoge — payload mein ya headers mein?',
          choices: [
            'Payload mein — sab ek jagah rahega',
            'Headers mein — kyunki yeh METADATA hai, business data nahi. Consumer/proxy bina poora payload parse kiye tracing aur routing kar sakte hain, aur yeh business schema ko ganda nahi karta.',
            'Dono jagah',
            'Kahin nahi, log mein likh do',
          ],
          answer: 1,
          explain: 'Headers metadata ke liye bane hain: correlation-id, schema-version, source-service, retry-count. Payload sirf business event ka data ho. Isse schema saaf rehta hai aur tooling aasan.',
        },
      ],
    },
    {
      type: 'fill',
      title: '📄 Achha event likho',
      instructions: 'Ek achha order event banao — event (command nahi), fat, aur headers ke saath.',
      template: "await producer.send({\n  topic: 'order-events',\n  messages: [{\n    key: order.id,\n    headers: {\n      'schema-{0}': '2',\n      'correlation-id': ctx.requestId,\n    },\n    value: JSON.stringify({\n      type: 'Order{1}',        // kya HUA (command nahi)\n      orderId: order.id,\n      occurredAt: new Date().toISOString(),\n      items: order.items,       // {2} event — sab andar\n      total: order.total,\n    }),\n  }],\n});",
      blanks: [
        { answer: 'version', explain: 'schema-version header se consumer bina payload parse kiye version dekh sakta hai.' },
        { answer: 'Placed', explain: 'Past tense — "OrderPlaced" batata hai ki kya HUA. "SendEmail" hota toh woh command hota aur coupling wapas aa jaata.' },
        { answer: 'fat', explain: 'Items aur total andar hone se consumer ko DB lookup nahi karna padta, aur us WAQT ka data hamesha ke liye preserve ho jaata hai.' },
      ],
      tokens: ['version', 'Placed', 'fat', 'thin', 'SendEmail', 'id', 'type', 'Create'],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Topic par 40 consumers hain. Tumhe ek field ka naam badalna hai. Kaise karoge?"',
        choices: [
          'Sabko email karke ek din fix kar dunga',
          'Expand-contract mein: (1) naya field add karo, dono likho — yeh backward-compatible hai; (2) consumers ko dheere-dheere naye par migrate hone do, metrics se track karo ki purana field kaun padh raha hai; (3) jab zero consumers rahen tab purana field hatao. Poora process hafton ka hai, aur beech mein kabhi bhi rollback safe rehta hai.',
          'Naya topic v2 banaunga aur sabko shift karunga',
          'Consumers ko defensive coding karne bolunga',
        ],
        answer: 1,
        explain: 'Interviewer dekhna chahta hai ki tum distributed migration ko ek EVENT nahi, PROCESS maante ho. "Sab ek saath deploy" wala jawab batata hai ki tumne kabhi 40-consumer topic nahi sambhala.',
      },
      {
        q: '🧑‍💼 "Fat events ya thin events — kya chunoge aur kyun?"',
        choices: [
          'Hamesha thin — bandwidth bachta hai',
          'Hamesha fat — data zyada hota hai',
          'Aam taur par fat ki taraf jhukunga: consumers ko N+1 lookups nahi karne padte, aur — sabse zaroori — event us WAQT ka sach preserve karta hai ("order tab 4990 ka tha"). Thin event ke saath consumer jab lookup karega tab data badal chuka hoga. Thin tab chunta hoon jab payload bahut bada ho (images/documents) — tab reference bhejo, data nahi.',
          'Koi farak nahi padta',
        ],
        answer: 2,
        explain: 'Asli insight yeh hai ki thin events sirf bandwidth nahi bachate — woh TIME-TRAVEL ki kshamta cheen lete hain. Event log ka poora point hi yeh hai ki woh us waqt ka sach batata hai.',
      },
    ],
  },
  success: [
    'Main safe aur breaking schema changes mein farak kar sakta hoon',
    'Main expand-contract se field rename kar sakta hoon',
    'Main event aur command mein farak samjha sakta hoon',
    'Main jaanta hoon ki metadata headers mein jaata hai, payload mein nahi',
  ],
  homework: `<p>Breaking change khud dekho:</p>
    <pre># v1 messages bhejo
echo '{"userId":1,"email":"a@b.com"}' | kafka-console-producer.sh \\
  --topic evolve-test --bootstrap-server localhost:9092

# ek "purana" consumer chalao jo userId padhta hai
kafka-console-consumer.sh --topic evolve-test --from-beginning \\
  --bootstrap-server localhost:9092 | \\
  node -e "require('readline').createInterface({input:process.stdin})
    .on('line', l => { const m = JSON.parse(l);
      console.log('user:', m.userId.toString()); })"

# ab v2 bhejo — rename kiya hua
echo '{"user_id":1,"email":"a@b.com"}' | kafka-console-producer.sh \\
  --topic evolve-test --bootstrap-server localhost:9092</pre>
    <p>Consumer <code>TypeError: Cannot read properties of undefined</code> se marega —
    bilkul waise hi jaise woh 31 consumers marey the. Ab expand-contract try karo:
    dono field bhejo aur dekho ki purana consumer bina kisi badlaav ke chalta rehta hai.</p>`,
  cards: [
    { front: 'Kaunse schema changes SAFE hain?', back: 'Naya <b>optional</b> field (default ke saath), field ko optional banana, naya event type. Yeh purane consumers ko nahi todte.' },
    { front: 'Kaunse changes BREAKING hain?', back: 'Field <b>rename</b>, field <b>hatana</b>, <b>type</b> badalna, optional ko required banana, aur (chupa hua) <b>enum mein nayi value</b> add karna — purane consumers ka switch/case use handle nahi karta.' },
    { front: 'Field rename kaise karte hain bina kisi ko todhe?', back: '<b>Expand-contract:</b> 1) naya field add karo, <b>dono</b> likho 2) consumers ko migrate hone do (hafte) 3) jab koi purana na padhe tab purana hatao. Beech mein rollback hamesha safe.' },
    { front: 'Event vs Command — farak?', back: '<b>Command</b> = "sendEmail" (kya KARNA hai) → producer ko consumers ka pata hona chahiye = coupling.<br><b>Event</b> = "OrderPlaced" (kya HUA) → consumers khud tay karte hain. Kal naya consumer aaye toh producer nahi badlega.' },
    { front: 'Fat events ki asli taakat kya hai?', back: 'Sirf N+1 lookup bachana nahi — <b>us waqt ka sach preserve karna</b>. "Order tab 4990 ka tha" hamesha sach rahega. Thin event ke saath consumer jab lookup karega, data badal chuka hoga.' },
  ],
},

/* =====================================================================
   9. BOSS: PEHLA PIPELINE
   ===================================================================== */
{
  id: 'boss-pipeline',
  icon: '🏆',
  title: 'BOSS: Pehla Pipeline — sab kuch ek saath',
  badge: 'Sandesh Pandit',
  objectives: [
    'Ek poora order pipeline khud design aur chalao',
    'Sahi partition count, key aur acks chuno — aur kyun, woh bhi batao',
    'Lag ka nidaan karo aur theek karo',
    'Track 1 ki har cheez ek jagah use karo',
  ],
  lesson: [
    `<h3>🏆 Final imtihaan — QuickCart ka pipeline</h3>
     <p>Tum QuickCart (ek e-commerce startup) ke pehle platform engineer ho.
     Abhi checkout seedha 4 services ko sync call karta hai aur sale ke din
     har baar gir jaata hai.</p>
     <p><b>Requirements jo product team ne diye:</b></p>
     <pre>📈 Peak traffic:        2,000 orders/sec (sale ke din)
⚙️ Ek consumer:         ~250 orders/sec kha sakta hai
🔑 Ordering:            ek order ke events ORDER MEIN hone chahiye
                        (created → paid → shipped)
🛡️ Durability:          ek bhi order kho nahi sakta (paisa hai!)
👥 Consumers:           billing, warehouse, analytics — teeno ko
                        POORA data chahiye, alag-alag raftaar par
🔥 Broker failure:      ek broker gire toh bhi sab chale</pre>
     <p>Ab sochо — Track 1 ki har cheez yahan lagegi:</p>
     <ul>
       <li><b>Kitni partitions?</b> (level 3 ka formula)</li>
       <li><b>Kaunsi key?</b> (level 6 — ordering + cardinality)</li>
       <li><b>Kaunsa acks?</b> (level 4 — paisa hai, toh?)</li>
       <li><b>Teeno consumers ko poora data kaise?</b> (level 5 — groups)</li>
       <li><b>RF kitna?</b> (broker failure jhelna hai)</li>
     </ul>`,
  ],
  stages: [
    {
      type: 'order',
      title: '🔢 Pipeline design ka kram',
      instructions: 'Pipeline banane ke faisle sahi tarteeb mein lagao — har kadam agle ko tay karta hai.',
      steps: [
        { text: '📋 Requirements likho: throughput, ordering, durability, consumers',
          explain: 'Pehle yeh. Har technical faisla inhi se nikalta hai. Requirements ke bina partition count bas ek andaaza hai.' },
        { text: '🔑 Partition KEY chuno — kis cheez ka order chahiye? (orderId)',
          explain: 'Key pehle chunte hain kyunki woh batati hai ki ordering kis level par milegi. Yeh partition count se bhi zyada bunyaadi faisla hai.' },
        { text: '🗂️ Partition COUNT nikalo — peak ÷ per-consumer, phir headroom',
          explain: '2000 ÷ 250 = 8 minimum. Headroom ke saath 12-16. Yaad rakho: badhana ordering todta hai, ghatana namumkin hai.' },
        { text: '🛡️ Durability set karo — RF=3, acks=all, min.insync.replicas=2',
          explain: 'Teeno saath mein hi kaam karte hain. RF akela ya acks akela kaafi nahi.' },
        { text: '👥 Har consumer ke liye ALAG group.id do (billing/warehouse/analytics)',
          explain: 'Alag groups = sabko poori copy, apni raftaar par. Isi liye teeno ko alag-alag scale bhi kar sakte ho.' },
        { text: '📉 Lag par monitoring aur alert lagao',
          explain: 'Aakhir mein — par isse chhodo mat. Lag hi woh ek metric hai jo batata hai ki pipeline healthy hai ya nahi.' },
      ],
    },
    {
      type: 'terminal',
      title: '🖥️ FINAL MISSION: QuickCart pipeline banao aur chalao',
      instructions: 'Yeh boss fight hai. Poora pipeline khud banao — koi hint mile bina try karo pehle.',
      goal: `<b>QuickCart ka order pipeline banao:</b><br><br>
        1️⃣ <code>order-events</code> topic: <b>≥8 partitions</b>, <b>RF=3</b>,
        <code>min.insync.replicas=2</code><br>
        2️⃣ Teen consumer groups: <code>billing</code>, <code>warehouse</code>, <code>analytics</code><br>
        &nbsp;&nbsp;&nbsp;(<code>sim consumer add &lt;group&gt;</code> se members do — har group subscribe ho jaayega)<br>
        3️⃣ <b>orderId ko key bana kar</b> messages bhejo (<code>--key order-{i}</code>)
        aur confirm karo ki saare partitions use ho rahe hain<br>
        4️⃣ Ek broker maar do aur dekho ki produce phir bhi chalta hai (RF ka fayda)<br>
        5️⃣ Aakhir mein <b>teeno groups ka lag 0</b> karo`,
      cluster: { brokers: 3 },
      tasks: [
        { text: 'order-events topic (≥8 partitions, RF=3, min.insync=2)',
          check: s => s.hasTopic('order-events') && s.topics['order-events'].partitions.length >= 8 &&
                      s.topics['order-events'].rf === 3 &&
                      String(s.topics['order-events'].config['min.insync.replicas']) === '2' },
        { text: 'teen consumer groups banao', check: s => ['billing', 'warehouse', 'analytics'].every(g => s.groups[g]) },
        { text: 'key ke saath messages bhejo (sab partitions par)',
          check: s => s.hasTopic('order-events') && s.partitionCounts('order-events').every(c => c > 0) },
        { text: 'ek broker maaro (aur produce chalta rahe)', check: s => s.brokers.some(b => !b.alive) },
        { text: 'teeno groups ka lag 0 karo',
          check: s => ['billing', 'warehouse', 'analytics'].every(g =>
            s.groups[g] && s.groups[g].committed['order-events'] && s.lag(g, 'order-events') === 0) },
      ],
      success: s => s.hasTopic('order-events') &&
                    s.topics['order-events'].partitions.length >= 8 &&
                    s.topics['order-events'].rf === 3 &&
                    String(s.topics['order-events'].config['min.insync.replicas']) === '2' &&
                    ['billing', 'warehouse', 'analytics'].every(g =>
                      s.groups[g] && s.groups[g].committed['order-events'] && s.lag(g, 'order-events') === 0) &&
                    s.partitionCounts('order-events').every(c => c > 0) &&
                    s.brokers.some(b => !b.alive),
      successText: `🏆 <b>Tumne apna pehla poora Kafka pipeline bana liya!</b><br><br>
        Dekho tumne kya-kya kiya:<br>
        🗂️ <b>8+ partitions</b> — 2000÷250 ke hisaab se, headroom ke saath<br>
        🔑 <b>orderId key</b> — har order ke events ordered, par load barabar bata<br>
        🛡️ <b>RF=3 + min.insync=2</b> — ek broker gaya, pipeline chalti rahi<br>
        👥 <b>3 alag groups</b> — teeno ko poora data, apni-apni raftaar par<br>
        📉 <b>Lag 0</b> — sab caught up<br><br>
        Aur sabse zaroori: tum har faisle ka <b>KYUN</b> bata sakte ho. Yahi
        interview mein poocha jaata hai.<br><br>
        <b>Ab asli duniya ki taiyaari:</b> Track 2 mein yeh sab TOOTEGA —
        rebalance ka toofan, poison messages, retry storms, backpressure aur
        3AM ka pager. Wahi jagah hai jahan tum senior bante ho. 🔥`,
      hints: [
        'kafka-topics --create --topic order-events --partitions 8 --replication-factor 3 --config min.insync.replicas=2',
        'sim consumer add billing 2   (isse group ban jaayega — phir warehouse, analytics)',
        'Agar group ko topic subscribe nahi hua: sim consumer add ke baad kafka-console-consumer --topic order-events --group billing --from-beginning chalao',
        'kafka-console-producer --topic order-events --count 400 --key order-{i}',
        'sim broker kill 2   phir   sim process billing --records 9999  (teeno groups ke liye)',
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Boss quiz — sab kuch',
      bonus: true,
      questions: [
        {
          q: '2,000 orders/sec, ek consumer 250/sec. Kitni partitions?',
          choices: ['4', 'Kam se kam 8 (2000÷250), par growth headroom ke liye 12-16 behtar — kyunki badhana ordering todta hai aur ghatana namumkin hai', '2000', '3 — default'],
          answer: 1,
          explain: 'Formula: peak ÷ per-consumer. Phir upar ki taraf galti karo, kyunki partition count badalna sasta nahi hai.',
        },
        {
          q: 'Teeno teams (billing, warehouse, analytics) ko poora data chahiye. Kaise?',
          choices: [
            'Teen alag topics banao',
            'Teen alag CONSUMER GROUPS — ek hi topic, teen group.id. Sabko poora data, apni raftaar par, aur producer ko kuch pata bhi nahi chalega.',
            'Ek group mein teeno ko daalo',
            'Topic ko teen baar replicate karo',
          ],
          answer: 1,
          explain: 'Ek group mein daalte toh woh messages BAANT lete — har team ko sirf 1/3 data milta. Alag groups = fanout.',
        },
        {
          q: 'Ek order ke events (created → paid → shipped) ordered chahiye. Key kya rakhoge?',
          choices: ['eventType', 'orderId — same order ke saare events same partition par jaayenge (murmur2 hash), matlab ordered; aur orderIds lakhon hain toh load bhi barabar bantega', 'timestamp', 'koi key nahi'],
          answer: 1,
          explain: 'Dono shartein poori: ordering (jis cheez ka order chahiye wahi key) aur high cardinality (load barabar). eventType hota toh sirf 3 partitions use hotin.',
        },
        {
          q: 'Orders ke liye durability settings?',
          choices: [
            'acks=1, RF=1',
            'acks=all + RF=3 + min.insync.replicas=2 — teeno saath mein. Sirf acks=all kaafi nahi kyunki ISR ghat kar 1 ho sakta hai.',
            'acks=0 taaki tez rahe',
            'Sirf RF=3 kaafi hai',
          ],
          answer: 1,
          explain: 'Paisa hai — durability par samjhauta nahi. Aur yaad rakho ki teeno settings mil kar hi guarantee banati hain.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 FINAL: "Ek e-commerce order pipeline design karo. 5 minute mein batao."',
        choices: [
          'Ek topic banaunga aur consumers laga dunga',
          'Pehle requirements poochunga: peak throughput, ordering ki zaroorat, durability, kitne consumers. Phir: key = orderId (ordering + cardinality), partitions = peak÷per-consumer + headroom, acks=all + RF=3 + min.insync=2 (paisa hai), har team ka alag consumer group, fat events (us waqt ka sach preserve ho), aur lag par alerting. Aur main yeh bhi bataunga ki isse kya NAYI problem aayegi — duplicates, jinke liye idempotency chahiye.',
          'Kafka Streams use karunga',
          'RabbitMQ behtar rahega',
        ],
        answer: 1,
        explain: 'Yeh poora Track 1 ek jawab mein hai. Dhyan do ki achha jawab (1) requirements se shuru hota hai, (2) har faisle ka kyun batata hai, aur (3) apne hi design ki nayi problems khud batata hai. Teesri baat hi senior ki pehchaan hai.',
      },
    ],
  },
  success: [
    'Maine poora order pipeline design aur chalaya',
    'Main partition count, key aur acks ka chunav justify kar sakta hoon',
    'Main jaanta hoon ki fanout ke liye alag consumer groups chahiye',
    'Main broker failure ke doraan pipeline chalti rakh sakta hoon',
    'Main apne design ki NAYI problems khud bata sakta hoon',
  ],
  homework: `<p>Poora pipeline asli Kafka par banao:</p>
    <pre>kafka-topics.sh --create --topic order-events \\
  --partitions 12 --replication-factor 1 \\
  --config min.insync.replicas=1 \\
  --bootstrap-server localhost:9092

# teen consumers, TEEN alag terminals mein
kafka-console-consumer.sh --topic order-events --group billing   --bootstrap-server localhost:9092
kafka-console-consumer.sh --topic order-events --group warehouse --bootstrap-server localhost:9092
kafka-console-consumer.sh --topic order-events --group analytics --bootstrap-server localhost:9092

# ab key ke saath bhejo — teeno ko HAR message milega
printf "o1:created\\no1:paid\\no2:created\\n" | \\
  kafka-console-producer.sh --topic order-events \\
  --property parse.key=true --property key.separator=: \\
  --bootstrap-server localhost:9092</pre>
    <p><b>Asli imtihaan:</b> ek group mein doosra consumer chalao (same group.id).
    Ab messages un dono mein <b>bant</b> jaayenge. Phir teesra group banao —
    usse <b>sab kuch</b> milega. Yahi Kafka ka poora model ek experiment mein.</p>`,
  cards: [
    { front: 'Pipeline design ka pehla kadam kya hai?', back: '<b>Requirements</b>: peak throughput, ordering ki zaroorat, durability, kitne aur kaunse consumers. Har technical faisla (partitions, key, acks) inhi se nikalta hai.' },
    { front: 'Pipeline design ka kram?', back: '1) Requirements 2) <b>Key</b> chuno 3) <b>Partition count</b> nikalo 4) <b>Durability</b> (RF + acks + min.insync) 5) Har consumer ka alag <b>group.id</b> 6) <b>Lag alerting</b>' },
    { front: 'Interview mein senior ki pehchaan kya hai?', back: 'Woh apne hi design ki <b>NAYI problems</b> khud batata hai — "queue lagane se duplicates aayenge, isliye idempotency chahiye". Junior sirf fayde ginata hai.' },
    { front: 'Durability ke teen settings?', back: '<code>acks=all</code> + <code>replication.factor=3</code> + <code>min.insync.replicas=2</code>. <b>Teeno saath mein</b> — koi ek akela guarantee nahi deta.' },
    { front: 'Ek topic, teen teams, sabko poora data — kaise?', back: 'Teen alag <b>consumer groups</b> (alag group.id). Ek hi group mein daalte toh data <b>bant</b> jaata aur har team ko sirf 1/3 milta.' },
  ],
},

]);
