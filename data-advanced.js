/* =====================================================================
   Kafka Gurukul — 🚀 TRACK 3: Stream Guru Path
   Senior engineer ka shastra — 8 levels.

   Lesson ka 3-card niyam (EXTENDING.md section 2):
     Card 1 — 😰 DARD · Card 2 — 🪜 SEEDHI · Card 3 — ⚖️ NAYI MUSIBAT
   ===================================================================== */

KQ.registerLevels('advanced', [

/* =====================================================================
   1. EXACTLY-ONCE & TRANSACTIONS
   ===================================================================== */
{
  id: 'eos-transactions',
  icon: '🎯',
  title: 'Exactly-Once & Transactions — accounting 2 rupaye off tha',
  badge: 'EOS Acharya',
  objectives: [
    'Samjho ki Kafka ka exactly-once (EOS) SIRF Kafka→Kafka ke andar kaam karta hai',
    'Idempotent producer aur transactional producer ka farak jaano',
    'read_committed vs read_uncommitted consumer samjho',
    'Jaano ki EOS ki keemat kya hai aur kab use karna chahiye',
  ],
  lesson: [
    `<h3>😰 Dard: "Daily reconciliation har roz 2-3 rupaye off tha"</h3>
     <p>Ek payments team ka ek stream processor tha jo <code>transactions</code> topic
     se padhta, har transaction ka fee calculate karta, aur do jagah likhta:
     <code>fees</code> topic par aur consumer offset commit. At-least-once tha.</p>
     <pre>Processor ka kaam:
   1. transaction padho (offset N)
   2. fee = amount × 0.02 calculate karo
   3. fees topic par likho
   4. offset N commit karo

Kabhi kabhi step 3 aur 4 ke BEECH crash:
   → fee LIKH di gayi (step 3)
   → par offset commit NAHI hua (step 4)
   → restart → wahi transaction dobara → fee DOBARA likhi
   → ek transaction ki fee do baar count hui</pre>
     <div class="war-story"><b>⚡ Har roz 2-3 rupaye ka mismatch.</b> Chhota lagta hai,
     par accounting mein koi bhi mismatch matlab audit fail. Ghante barbaad hote the
     "kaunsi fee duplicate hui" dhoondhne mein. Root cause: fee likhna aur offset commit
     do ALAG operations the — beech mein crash duplicate banata tha. Yahi woh jagah hai
     jahan Kafka ka exactly-once SACH MEIN kaam aata hai.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Pehle offset commit karo, phir fee likho"</b><br>
     Ab ulta problem — offset commit ho gaya par fee likhne se pehle crash → fee
     KHO gayi. Duplicate ki jagah data loss. Kram badalne se problem badal gayi, gayi nahi.</p>

     <p><b>⚠️ Better: "Idempotent producer ON kar do"</b><br>
     <code>enable.idempotence=true</code> producer ke retries se bante duplicates rokta
     hai (PID + sequence number — Track 1 mein dekha). Par yeh <b>fee-likhna + offset-commit</b>
     ko atomic nahi karta. Woh do alag cheezein hain; idempotent producer sirf ek
     producer ke apne retries sambhaalta hai. Yeh zaroori hai par kaafi nahi.</p>

     <p><b>✅ Real: Transactional producer — fee aur offset EK transaction mein</b></p>
     <pre>producer.initTransactions()   // transactional.id ke saath

while (true) {
  records = consumer.poll()
  producer.beginTransaction()
    for (r of records) producer.send('fees', calcFee(r))
    // 👇 offset commit bhi USI transaction ka hissa!
    producer.sendOffsetsToTransaction(offsets, consumerGroupId)
  producer.commitTransaction()   // fee + offset DONO atomic
}

Ab crash kahin bhi ho:
   commit se pehle → transaction abort → fee bhi nahi, offset bhi nahi
                     → restart par transaction DOBARA (safe)
   commit ke baad  → fee bhi, offset bhi → dobara nahi hoga
→ har transaction ki fee THEEK ek baar ✅</pre>
     <p>Consumer side par: <code>isolation.level=read_committed</code> — taaki consumer
     sirf COMMITTED transactions padhe, aborted ya in-progress nahi.</p>`,

    `<h3>⚖️ Nayi musibat: EOS ki asli SEEMA</h3>
     <p>Yeh sabse zaroori baat hai jo Track 1 mein bhi aayi thi, ab poori tarah:</p>
     <pre>Kafka ka EOS transaction SIRF yeh atomic kar sakta hai:
   ✅ Kafka topics par writes (fees topic)
   ✅ consumer offset commit (__consumer_offsets, jo khud ek topic hai)

Woh yeh atomic NAHI kar sakta:
   ❌ payment gateway ko charge karna
   ❌ email/SMS bhejna
   ❌ external DB mein likhna (jo Kafka nahi hai)
   ❌ koi bhi cheez jo Kafka ke BAHAR hai

Kyun? Kyunki transaction Kafka ke commit protocol par khadi hai.
Payment gateway us protocol ka hissa nahi — woh roll back nahi hoga.</pre>
     <p><b>Toh EOS kab use karo?</b> Jab tumhara kaam poori tarah <b>Kafka→Kafka</b> ho:
     stream processing (aggregate, join, transform), ek topic se padho doosre par likho.
     Kafka Streams internally yahi use karta hai (agla level).</p>
     <p><b>Aur jab bahari duniya ho?</b> Wahan idempotency hi ek matra hal hai
     (Track 1). Aur agar tumhe DB write aur Kafka publish dono atomic chahiye —
     toh transactional outbox pattern (agla level). EOS ka ek jagah hai, par woh
     har jagah ka jawab nahi.</p>`,
  ],
  stages: [
    {
      type: 'order',
      title: '🔢 Transactional processor ka sahi kram',
      instructions: 'Exactly-once processing (Kafka→Kafka) ka sahi kram lagao.',
      steps: [
        { text: '🎬 producer.initTransactions() — transactional.id ke saath ek baar setup',
          explain: 'transactional.id har processor instance ka sthir naam. Isse Kafka "zombie" purane instances ko pehchan ke fence kar deta hai (do instances ek saath likh nahi sakte).' },
        { text: '📥 consumer.poll() — records ka batch lo',
          explain: 'read_committed consumer sirf committed data padhta hai. Batch process karne ka intezaar.' },
        { text: '🎬 producer.beginTransaction() — transaction shuru',
          explain: 'Ab jo bhi send aur offset-commit hoga woh is transaction ka hissa hai — sab ya kuch nahi.' },
        { text: '📤 producer.send() — output topic par results likho',
          explain: 'Fees/aggregates output topic par. Yeh abhi "uncommitted" hai — read_committed consumers ise abhi nahi dekhenge.' },
        { text: '💾 producer.sendOffsetsToTransaction() — input offset ko transaction mein daalo',
          explain: 'Yahi jaadu hai — offset commit ALAG operation nahi, usi transaction ka hissa. Isliye output aur offset atomic ho jaate hain.' },
        { text: '✅ producer.commitTransaction() — sab ek saath commit',
          explain: 'Ab output aur offset DONO ek saath visible/committed. Crash yahan se pehle hua toh sab abort — na output, na offset. Exactly-once.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 EOS ke sawal',
      questions: [
        {
          q: 'Idempotent producer aur transactional producer mein farak?',
          choices: [
            'Dono same hain',
            'Idempotent producer (enable.idempotence=true) ek PRODUCER ke apne retries se bante duplicates rokta hai (PID + sequence). Transactional producer usse aage — kai topics par writes AUR consumer offset commit ko EK atomic transaction bana deta hai (sab ya kuch nahi). EOS ke liye dono chahiye, aur transactional producer idempotence apne aap ON kar deta hai.',
            'Transactional purana hai',
            'Idempotent sirf consumer par hota hai',
          ],
          answer: 1,
          explain: 'Idempotent = ek producer ke duplicate writes rokna. Transactional = multiple writes + offset ko atomic karna. Transactional producer idempotence ke UPAR banta hai.',
        },
        {
          q: 'Tumhara consumer Kafka se padh kar ek EXTERNAL payment gateway ko charge karta hai. EOS bachayega?',
          choices: [
            'Haan, transactions sab sambhaal lenge',
            'Nahi. Kafka ka EOS transaction sirf Kafka ke andar (output topic + offset) atomic karta hai. Payment gateway Kafka ke commit protocol ka hissa nahi — woh roll back nahi hoga. Yahan idempotency chahiye (gateway ko idempotency-key do), EOS nahi.',
            'Haan agar acks=all ho',
            'Haan agar gateway bhi Kafka use kare',
          ],
          answer: 1,
          explain: 'EOS ki sabse zaroori seema. Jaise hi tum Kafka ke bahar side-effect karte ho (payment, email, external DB), EOS ki guarantee wahin khatam. Woh sirf Kafka→Kafka ke liye hai.',
        },
        {
          q: 'read_committed isolation level kya karta hai?',
          choices: [
            'Consumer ko tez karta hai',
            'Consumer ko sirf COMMITTED transactions ke messages dikhata hai — aborted ya abhi in-progress transactions ke messages nahi. Iske bina consumer un messages ko bhi padh leta jo baad mein abort ho gaye (read_uncommitted). EOS ke liye consumer par yeh zaroori hai.',
            'Messages ko encrypt karta hai',
            'Duplicate messages hata deta hai',
          ],
          answer: 1,
          explain: 'read_committed EOS ka consumer-side hissa hai. Producer transactions bana raha hai, par agar consumer read_uncommitted hai toh woh aborted transactions ke messages bhi padh lega — guarantee toot gayi.',
        },
      ],
    },
    {
      type: 'match',
      title: '🔗 Kaunsa tool, kaunsi zaroorat',
      instructions: 'Har scenario ko sahi consistency tool se jodo.',
      pairs: [
        { left: 'Ek topic se padho, aggregate karke doosre topic par likho', right: '🎯 Kafka Transactions (EOS)',
          explain: 'Poori tarah Kafka→Kafka — yahi woh ek jagah hai jahan EOS perfect kaam karta hai.' },
        { left: 'Kafka se padh kar external payment gateway charge karo', right: '🔑 Idempotency key',
          explain: 'Bahari side-effect — EOS bekaar. Gateway ko idempotency-key do taaki duplicate charge na ho.' },
        { left: 'DB commit aur Kafka publish DONO atomic chahiye', right: '📤 Transactional Outbox',
          explain: 'Do alag systems (DB + Kafka) ko atomic karne ka pattern — agla level. EOS yeh nahi kar sakta.' },
        { left: 'Producer ke retries se duplicate na banein', right: '♻️ Idempotent producer',
          explain: 'enable.idempotence=true — ek producer ke apne retries sambhaalta hai. Basic building block.' },
        { left: 'Consumer aborted transaction ke messages na padhe', right: '👁️ read_committed',
          explain: 'Consumer-side isolation — sirf committed transactions dikhte hain.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 EOS ki keemat',
      bonus: true,
      questions: [
        {
          q: 'EOS use karne ki kya keemat hai?',
          choices: [
            'Koi keemat nahi, hamesha ON rakho',
            'Latency aur throughput — transactions coordination overhead laate hain (transaction markers, two-phase commit style protocol), aur read_committed consumers ko transaction commit ka intezaar karna padta hai. Isliye EOS tab use karo jab correctness sach mein chahiye (accounting, financial aggregates), har jagah nahi.',
            'Zyada disk',
            'Zyada partitions',
          ],
          answer: 1,
          explain: 'EOS free nahi hai. Har transaction ke markers, coordination, aur consumer-side buffering latency badhate hain. Jahan at-least-once + idempotency kaafi hai wahan woh sasta aur simple hai.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Kafka exactly-once deta hai — sach ya jhooth? Detail mein."',
        choices: [
          'Sach, enable.idempotence se',
          'Adhoora sach. Kafka→Kafka ke ANDAR haan — transactional producer output writes aur consumer offset commit ko ek atomic transaction bana deta hai, aur read_committed consumer sirf committed data padhta hai. Isse stream processing (padho-transform-likho) mein exactly-once milta hai. PAR jaise hi koi external side-effect ho — payment, email, external DB — woh transaction ka hissa nahi ban sakta (woh roll back nahi hoga), toh wahan EOS nahi, idempotency chahiye. Aur EOS ki latency/throughput keemat hoti hai, toh main use sirf tab lagaunga jab correctness (jaise financial aggregates) zaroori ho.',
          'Jhooth, aisa kuch nahi hota',
          'Sirf Kafka Streams mein',
        ],
        answer: 1,
        explain: 'Yeh senior-level jawab hai. Woh (1) EOS ki seema (Kafka→Kafka), (2) mechanism (transactional producer + read_committed), (3) external limitation, aur (4) keemat — sab batata hai. "Haan Kafka EOS deta hai" bolna production mein double-charge bug likhwata hai.',
      },
      {
        terminal: {
          title: '🖥️ Interview: EOS pipeline ke topics banao',
          goal: `🧑‍💼 Interviewer: "Ek exactly-once stream processor ke liye topics chahiye:
            input <code>transactions</code> (6 partitions), output <code>fees</code>
            (6 partitions). Dono RF=3, aur dono par <code>min.insync.replicas=2</code>
            (kyunki EOS ke liye durable writes chahiye). Phir input mein kuch data daal do."`,
          cluster: { brokers: 3 },
          tasks: [
            { text: 'transactions topic (6 part, RF=3, minISR=2)',
              check: s => s.hasTopic('transactions') && s.topics.transactions.partitions.length >= 6 &&
                          s.topics.transactions.rf === 3 && String(s.topics.transactions.config['min.insync.replicas']) === '2' },
            { text: 'fees topic (6 part, RF=3, minISR=2)',
              check: s => s.hasTopic('fees') && s.topics.fees.partitions.length >= 6 &&
                          s.topics.fees.rf === 3 && String(s.topics.fees.config['min.insync.replicas']) === '2' },
            { text: 'transactions mein data daalo',
              check: s => s.hasTopic('transactions') && s.partitionCounts('transactions').reduce((a, c) => a + c, 0) > 0 },
          ],
          success: s => s.hasTopic('transactions') && s.topics.transactions.rf === 3 &&
                        String(s.topics.transactions.config['min.insync.replicas']) === '2' &&
                        s.hasTopic('fees') && s.topics.fees.rf === 3 &&
                        String(s.topics.fees.config['min.insync.replicas']) === '2' &&
                        s.partitionCounts('transactions').reduce((a, c) => a + c, 0) > 0,
          successText: `Sahi. Dhyan do ki EOS ke liye min.insync.replicas=2 kyun zaroori hai —
            transactional writes bhi ISR par depend karte hain. Agar durability guarantee
            na ho toh "exactly once" ka koi matlab nahi (message hi kho gaya toh exactly
            kya once hua?).<br><br>
            Interview bonus: bolo ki processor mein main <code>transactional.id</code>
            set karunga (har instance ka sthir), <code>isolation.level=read_committed</code>
            consumer par, aur <code>sendOffsetsToTransaction</code> se offset ko transaction
            mein daalunga.`,
          hints: [
            'kafka-topics --create --topic transactions --partitions 6 --replication-factor 3 --config min.insync.replicas=2',
            'kafka-topics --create --topic fees --partitions 6 --replication-factor 3 --config min.insync.replicas=2',
            'kafka-console-producer --topic transactions --count 30 --key txn-{i}',
          ],
        },
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki EOS sirf Kafka→Kafka ke andar kaam karta hai',
    'Main idempotent aur transactional producer mein farak kar sakta hoon',
    'Main read_committed ka role samjha sakta hoon',
    'Main EOS ki keemat aur sahi use-case bata sakta hoon',
  ],
  homework: `<p>Transactional producer khud likho (kafkajs):</p>
    <pre>const producer = kafka.producer({
  transactionalId: 'fee-processor-1',    // sthir id — zombie fencing
  maxInFlightRequests: 1,
  idempotent: true,
});
await producer.connect();

const consumer = kafka.consumer({ groupId: 'fee-proc' });
// consumer read_committed by default in kafkajs transactions

await consumer.run({
  eachBatch: async ({ batch }) => {
    const txn = await producer.transaction();
    try {
      for (const msg of batch.messages) {
        const fee = calcFee(JSON.parse(msg.value));
        await txn.send({ topic: 'fees', messages: [{ value: JSON.stringify(fee) }] });
      }
      // offset ko TRANSACTION mein daalo
      await txn.sendOffsets({
        consumerGroupId: 'fee-proc',
        topics: [{ topic: batch.topic, partitions: [{
          partition: batch.partition,
          offset: (Number(batch.lastOffset()) + 1).toString(),
        }]}],
      });
      await txn.commit();          // fee + offset DONO atomic
    } catch (e) {
      await txn.abort();           // sab wapas — na fee, na offset
    }
  },
});</pre>
    <p><b>Asli experiment:</b> commit se pehle jaan-boojh ke crash karao (throw).
    Restart karo. Dekho ki fee DUPLICATE nahi hui — kyunki aborted transaction ka
    output read_committed consumers ne dekha hi nahi, aur offset bhi commit nahi hua tha.</p>`,
  cards: [
    { front: 'Kafka EOS ki asli seema?', back: 'Sirf <b>Kafka→Kafka</b>: output topic writes + consumer offset commit atomic ho sakte hain. External side-effects (payment, email, DB) transaction ka hissa NAHI ban sakte — woh roll back nahi honge. Wahan idempotency chahiye.' },
    { front: 'Idempotent vs transactional producer?', back: '<b>Idempotent</b> (enable.idempotence): ek producer ke apne retries se duplicate rokta hai (PID+sequence).<br><b>Transactional</b>: kai writes + offset commit ko ek atomic transaction banata hai. Transactional idempotence ke UPAR banta hai.' },
    { front: 'sendOffsetsToTransaction kya karta hai?', back: 'Consumer offset commit ko producer ke transaction ka <b>hissa</b> bana deta hai — isliye output writes aur offset ATOMIC ho jaate hain. Yahi exactly-once processing ka core hai.' },
    { front: 'read_committed isolation level?', back: 'Consumer sirf <b>COMMITTED</b> transactions ke messages padhta hai — aborted/in-progress nahi. EOS ka consumer-side hissa. Iske bina consumer aborted transaction ke messages bhi padh leta.' },
    { front: 'EOS kab use karna chahiye?', back: 'Jab kaam poori tarah <b>Kafka→Kafka</b> ho (stream processing: padho-transform-likho) AUR correctness zaroori ho (financial aggregates). Latency/throughput ki keemat hoti hai, toh har jagah nahi.' },
  ],
},

/* =====================================================================
   2. TRANSACTIONAL OUTBOX & CDC
   ===================================================================== */
{
  id: 'outbox-cdc',
  icon: '📤',
  title: 'Transactional Outbox & CDC — DB commit hua, Kafka publish fail',
  badge: 'Outbox Ustaad',
  objectives: [
    'Dual-write problem ko pehchano (DB + Kafka dono likhna)',
    'Transactional Outbox pattern seekho',
    'Change Data Capture (CDC) aur Debezium ka idea samjho',
    'Jaano ki outbox aur CDC mein kab kaunsa chunte hain',
  ],
  lesson: [
    `<h3>😰 Dard: "Order DB mein bana, par kabhi ship nahi hua"</h3>
     <p>Ek order service order banati thi. Do kaam karne the: order ko DB mein save karna,
     aur <code>order-created</code> event Kafka par publish karna (taaki warehouse
     use ship kare). Code aisa tha:</p>
     <pre>await db.insert(order);                    // step 1: DB mein save
await kafka.publish('order-created', order); // step 2: Kafka par publish</pre>
     <p>Yeh "dual write" hai — do alag systems mein likhna. Aur yeh CHUP-CHAAP tootta hai:</p>
     <pre>Case A: DB save hua, phir Kafka publish se PEHLE crash
   → order DB mein hai (customer ko dikh raha hai)
   → par event kabhi publish nahi hua
   → warehouse ko pata hi nahi → order KABHI ship nahi hua 👻

Case B: (ulta karo) Kafka publish hua, phir DB save se pehle crash
   → warehouse ne ship kar diya
   → par order DB mein hai hi nahi
   → customer ko koi record nahi, par maal chala gaya 📦💸</pre>
     <div class="war-story"><b>⚡ "Ghost orders" — DB mein the par kabhi ship nahi hue.</b>
     Hafte mein 15-20 aise orders. Customer support tickets, refunds, manual investigation.
     Root cause: do alag systems mein likhna kabhi bhi atomic nahi ho sakta bina ek trick ke.
     Do database transactions hain — beech mein crash dono ko sync se bahar kar deta hai.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Retry karo agar Kafka publish fail ho"</b><br>
     Retry logic khud crash kar sakta hai (bijli chali gayi, pod mara). Aur agar DB save
     ke baad process hi mar gaya toh retry karega kaun? Retry dual-write ki jadd ko theek
     nahi karta — woh sirf failure window chhota karta hai.</p>

     <p><b>⚠️ Better: "Distributed transaction (2-phase commit) use karo"</b><br>
     DB aur Kafka ko ek 2PC transaction mein baandho. Theory mein sahi, practice mein
     dukh: 2PC dheema hai, complex hai, coordinator failure par blocking ho jaata hai,
     aur Kafka + zyadatar DBs iske liye achhe se bane hi nahi hain. Industry ne 2PC ko
     lagbhag chhod diya hai.</p>

     <p><b>✅ Real: Transactional Outbox — Kafka ko DB transaction ka hissa bana do</b><br>
     Trick: Kafka par SEEDHA mat likho. Ek "outbox" TABLE mein likho — usi DB transaction
     mein jisme order likhte ho. Phir ek alag process outbox se Kafka par bhejta hai:</p>
     <pre>-- ek hi DB transaction (atomic!)
BEGIN;
  INSERT INTO orders (...) VALUES (...);
  INSERT INTO outbox (topic, payload)
    VALUES ('order-created', '{...}');   -- event bhi DB mein
COMMIT;
-- ab ya DONO hue ya KOI nahi — same transaction

-- alag "relay" process:
--   outbox table se padho → Kafka par publish → outbox row delete/mark
--   agar yeh crash ho toh restart par phir se bhejega (at-least-once)
--   → downstream idempotent hai toh koi problem nahi</pre>
     <p>Ab order aur event ATOMIC hain (ek DB transaction). Event kabhi kho nahi sakta
     — woh DB mein durable hai jab tak relay use publish na kare.</p>`,

    `<h3>⚖️ Outbox vs CDC, aur nayi musibat</h3>
     <p>Outbox relay ko implement karne ke do tareeqe hain:</p>
     <pre>1️⃣ POLLING relay (simple)
   ek process har X ms outbox table SELECT karta hai,
   naye rows Kafka par bhejta hai, phir delete/mark karta hai
   👍 simple  👎 polling overhead, thodi latency

2️⃣ CDC — Change Data Capture (Debezium)
   database ke TRANSACTION LOG (MySQL binlog / Postgres WAL) ko
   padho — jo bhi commit hua woh log mein hai
   Debezium usse padh ke Kafka par bhej deta hai
   👍 koi polling nahi, real-time, DB par load kam
   👎 setup complex, DB-specific</pre>
     <p><b>CDC ka ek bada bonus:</b> tumhe outbox table ki bhi zaroorat nahi —
     Debezium seedha <code>orders</code> table ke changes ko Kafka par stream kar sakta hai.
     "Jo bhi DB mein commit hua woh apne aap Kafka par." Yeh purani legacy systems ko
     bina code badle event-driven banane ka sabse aam tareeqa hai.</p>
     <p><b>Nayi musibat:</b> outbox/CDC dono <b>at-least-once</b> hain (relay crash par
     dobara bhej sakta hai), aur CDC mein ordering DB ke commit order par depend karti hai.
     Toh downstream consumers ko phir bhi <b>idempotent</b> hona padega (Track 2 yaad hai).
     Outbox dual-write ko theek karta hai, par duplicates ko nahi — woh alag layer hai.</p>`,
  ],
  stages: [
    {
      type: 'order',
      title: '🔢 Transactional Outbox ka flow',
      instructions: 'Ghost-order bug se bachne ka sahi kram lagao.',
      steps: [
        { text: '🎬 DB transaction BEGIN',
          explain: 'Sab kuch ek atomic transaction mein hoga — yahi outbox ki poori taakat hai.' },
        { text: '📝 orders table mein order INSERT karo',
          explain: 'Business data. Yeh customer ko dikhega.' },
        { text: '📮 outbox table mein event INSERT karo (usi transaction mein)',
          explain: 'Event ko Kafka par nahi, DB mein likho — order ke SAATH. Ab dono ek transaction mein hain.' },
        { text: '✅ DB transaction COMMIT — order aur event DONO atomic',
          explain: 'Ya dono hue ya koi nahi. Crash yahan se pehle → dono rollback. Yahi dual-write problem ka hal hai.' },
        { text: '🔄 Relay/CDC: outbox (ya DB log) se event padho',
          explain: 'Ek alag process. Polling relay outbox table padhta hai; CDC (Debezium) DB ke transaction log se.' },
        { text: '📤 Event Kafka par publish karo, phir outbox row delete/mark',
          explain: 'Relay crash ho toh restart par phir bhejega (at-least-once) — isliye downstream idempotent hona chahiye. Par event kabhi KHOTA nahi.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Dual-write ke sawal',
      questions: [
        {
          q: 'Dual-write problem kya hai?',
          choices: [
            'Ek row do baar likhna',
            'Do ALAG systems (jaise DB aur Kafka) mein likhna jo atomic nahi ho sakta — beech mein crash ek ko likh deta hai aur doosre ko nahi, jisse woh sync se bahar ho jaate hain (order DB mein hai par event Kafka par nahi, ya ulta).',
            'Do consumers ka ek message padhna',
            'Do partitions par likhna',
          ],
          answer: 1,
          explain: 'Dual-write distributed systems ki sabse aam chup-chaap galti hai. "await db.save(); await kafka.publish();" bilkul saaf dikhta hai par beech ka crash ghost records banata hai.',
        },
        {
          q: 'Transactional Outbox pattern dual-write ko kaise theek karta hai?',
          choices: [
            'Kafka ko tez karke',
            'Kafka par seedha likhne ke bajaye, event ko ek OUTBOX TABLE mein likho — usi DB transaction mein jisme business data likhte ho. Ab dono ek atomic DB transaction mein hain (ya dono ya koi nahi). Ek alag relay process outbox se Kafka par bhejta hai. Event kabhi kho nahi sakta kyunki woh DB mein durable hai.',
            'Do baar publish karke',
            '2-phase commit se',
          ],
          answer: 1,
          explain: 'Outbox ka core insight: do alag systems ko atomic nahi kar sakte, toh dono writes ko EK system (DB) mein le aao, phir usse Kafka par relay karo. Ab atomicity ek hi DB transaction se aa jaati hai.',
        },
        {
          q: 'CDC (Change Data Capture) outbox se kaise alag hai?',
          choices: [
            'CDC dheema hai',
            'CDC database ke TRANSACTION LOG (binlog/WAL) ko seedha padhta hai (jaise Debezium) — jo bhi commit hua woh log mein hai. Isse outbox table ki bhi zaroorat nahi rehti (Debezium seedha table changes stream kar sakta hai), koi polling overhead nahi, aur real-time hai. Trade-off: setup complex aur DB-specific hai.',
            'CDC Kafka ke bina kaam nahi karta',
            'CDC sirf reads capture karta hai',
          ],
          answer: 1,
          explain: 'CDC transaction log se padhta hai — DB ka source of truth. Yeh legacy systems ko bina code badle event-driven banane ka sabse popular tareeqa hai. Outbox explicit table use karta hai; CDC implicit DB log.',
        },
      ],
    },
    {
      type: 'fill',
      title: '📄 Outbox transaction likho',
      instructions: 'Ghost-order bug se bachne wala atomic write. Dono INSERT ek transaction mein.',
      template: "-- ❌ dual write (ghost orders):\n--   await db.insert(order)\n--   await kafka.publish(event)   ← beech mein crash = ghost\n\n-- ✅ transactional outbox:\n{0};\n  INSERT INTO orders (id, amount) VALUES (?, ?);\n  INSERT INTO {1} (topic, payload)\n    VALUES ('order-created', ?);\n{2};\n\n-- alag relay process outbox se Kafka par bhejta hai\n-- (at-least-once → downstream {3} hona chahiye)",
      blanks: [
        { answer: 'BEGIN', explain: 'Transaction shuru — dono INSERT isi ke andar honge, isliye atomic.' },
        { answer: 'outbox', explain: 'Event ko outbox table mein likho, Kafka par seedha nahi. Yahi trick hai.' },
        { answer: 'COMMIT', explain: 'Dono INSERT ek saath commit — ya dono ya koi nahi. Dual-write khatam.' },
        { answer: 'idempotent', explain: 'Relay at-least-once hai (crash par dobara bhej sakta hai), toh downstream ko duplicates sambhaalne aane chahiye.' },
      ],
      tokens: ['BEGIN', 'outbox', 'COMMIT', 'idempotent', 'SELECT', 'kafka', 'ROLLBACK', 'ordered'],
    },
    {
      type: 'quiz',
      title: '🧠 Outbox ki nayi musibat',
      bonus: true,
      questions: [
        {
          q: 'Outbox/CDC lagane ke baad kya downstream duplicates se safe hai?',
          choices: [
            'Haan, outbox exactly-once deta hai',
            'Nahi. Outbox/CDC dono AT-LEAST-ONCE hain — relay crash par ek event dobara publish ho sakta hai. Outbox DUAL-WRITE problem ko theek karta hai (event kabhi khoyega nahi), par duplicates ko nahi. Downstream consumers ko phir bhi idempotent hona padega (Track 2). Yeh do alag problems hain.',
            'Haan agar CDC ho',
            'Haan, DB transaction duplicate rokta hai',
          ],
          answer: 1,
          explain: 'Yeh aam galatfehmi hai. Outbox durability deta hai (event khota nahi), exactly-once nahi. Relay ke retries se duplicate aa sakte hain. Idempotency alag layer hai jo abhi bhi chahiye.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Tumhe order DB mein save karna hai AUR order-created event Kafka par bhejna hai. Kaise ensure karoge ki dono hon ya koi na ho?"',
        choices: [
          'await db.save() phir await kafka.publish()',
          'Yeh dual-write problem hai — do alag systems atomic nahi ho sakte, beech ka crash ghost orders banata hai. Main Transactional Outbox use karunga: order aur event dono ko EK DB transaction mein likhunga (event ko ek outbox table mein), taaki woh atomic hon. Phir ek alag relay process (ya Debezium CDC jo DB ke transaction log se padhta hai) outbox se Kafka par publish karega. Yeh at-least-once hai, toh downstream consumers idempotent honge. 2-phase commit avoid karunga — woh dheema aur blocking hota hai.',
          'Transaction dono ke around laga dunga',
          'Retry logic add kar dunga',
        ],
        answer: 1,
        explain: 'Yeh classic senior sawal hai. Achha jawab (1) dual-write ko naam se pehchanta hai, (2) outbox pattern deta hai, (3) CDC ka option batata hai, (4) 2PC ko reject karta hai, aur (5) idempotency ki zaroorat yaad rakhta hai. "await db then await kafka" wala jawab ghost orders likhwata hai.',
      },
      {
        q: '🧑‍💼 "Ek purani monolith DB hai jise tum event-driven banana chahte ho par code nahi badal sakte. Kya karoge?"',
        choices: [
          'Poora rewrite karna padega',
          'CDC (Change Data Capture) — Debezium jaisa tool DB ke transaction log (MySQL binlog / Postgres WAL) ko padhta hai aur har committed change ko Kafka par stream kar deta hai, bina application code chhue. Legacy tables ke changes seedhe events ban jaate hain. Yeh strangler-fig migration ka sabse popular tareeqa hai — naye consumers Kafka se sunte hain jabki purana system waise ka waisa chalta rehta hai.',
          'Application mein Kafka publish add karna padega',
          'DB ko replace karna padega',
        ],
        answer: 1,
        explain: 'CDC ka sabse strong use-case: legacy systems ko event-driven banana bina unhe chhue. Debezium DB log se padhta hai, toh application ko pata bhi nahi chalta. Yeh interview mein CDC ki asli value dikhata hai.',
      },
    ],
  },
  success: [
    'Main dual-write problem ko pehchan aur samjha sakta hoon',
    'Main Transactional Outbox pattern implement kar sakta hoon',
    'Main CDC (Debezium) aur uska use-case bata sakta hoon',
    'Main jaanta hoon ki outbox at-least-once hai, isliye idempotency abhi bhi chahiye',
  ],
  homework: `<p>Outbox pattern khud banao:</p>
    <pre>-- outbox table
CREATE TABLE outbox (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  topic VARCHAR(100) NOT NULL,
  payload JSON NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  published BOOLEAN DEFAULT FALSE
);

-- order banate waqt (application code):
-- BEGIN;
--   INSERT INTO orders ...;
--   INSERT INTO outbox (topic, payload)
--     VALUES ('order-created', '{"orderId":123}');
-- COMMIT;

-- relay process (alag se chalao):
-- SELECT * FROM outbox WHERE published = FALSE ORDER BY id;
-- har row ke liye: kafka.publish(row.topic, row.payload)
-- phir: UPDATE outbox SET published = TRUE WHERE id = ?;</pre>
    <p><b>Asli experiment:</b> order INSERT aur outbox INSERT ke beech
    <code>throw new Error()</code> daalo. Dekho ki DONO rollback ho jaate hain
    (na order, na event) — atomic. Phir relay process ko outbox INSERT ke baad
    par Kafka publish se pehle maaro. Dekho ki restart par woh event phir bhejta
    hai (at-least-once) — isi liye downstream idempotent chahiye.</p>`,
  cards: [
    { front: 'Dual-write problem kya hai?', back: 'Do ALAG systems (DB + Kafka) mein likhna jo atomic nahi ho sakta. Beech ka crash ek ko likh deta hai, doosre ko nahi → "ghost orders" (DB mein hai par event nahi, ya ulta). <code>await db.save(); await kafka.publish();</code> saaf dikhta hai par yahi bug hai.' },
    { front: 'Transactional Outbox pattern?', back: 'Kafka par seedha likhne ke bajaye event ko ek <b>outbox table</b> mein likho — usi DB transaction mein jisme business data. Ab dono atomic. Alag relay process outbox se Kafka par bhejta hai. Event kabhi khota nahi (DB mein durable).' },
    { front: 'CDC (Change Data Capture) kya hai?', back: 'DB ke <b>transaction log</b> (binlog/WAL) ko padhna (jaise Debezium) — jo bhi commit hua woh log mein hai, use Kafka par stream karo. Legacy systems ko bina code badle event-driven banane ka sabse popular tareeqa. Outbox table ki bhi zaroorat nahi.' },
    { front: 'Outbox exactly-once deta hai?', back: 'Nahi — <b>at-least-once</b>. Relay crash par event dobara publish ho sakta hai. Outbox <b>dual-write</b> ko theek karta hai (event khota nahi), duplicates ko nahi. Downstream ko phir bhi <b>idempotent</b> hona chahiye.' },
    { front: '2-phase commit (2PC) kyun avoid karte hain?', back: 'Dheema, complex, coordinator failure par blocking, aur Kafka + zyadatar DBs iske liye ache se bane nahi. Industry ne ise lagbhag chhod diya — outbox/CDC saral aur reliable hain.' },
  ],
},

/* =====================================================================
   3. TIME, WINDOWS & LATE DATA
   ===================================================================== */
{
  id: 'time-windows',
  icon: '⏰',
  title: 'Time, Windows & Late Data — mobile app ne 3 ghante purane events bheje',
  badge: 'Time Tapasvi',
  objectives: [
    'Event-time aur processing-time ka farak samjho',
    'Windowing (tumbling/sliding/session) ka basic idea jaano',
    'Late data ka problem aur watermark/grace-period ka hal seekho',
    'Jaano ki "correct" aur "timely" ke beech trade-off kaise lete hain',
  ],
  lesson: [
    `<h3>😰 Dard: "Subah ka sales report raat 2 baje badal gaya"</h3>
     <p>Ek retail app har ghante ka sales aggregate banati thi (kis ghante mein kitni
     bikri). Ek stream processor events ko unke <b>aane ke waqt</b> (processing-time)
     ke hisaab se ghante mein daalta tha.</p>
     <pre>Problem: mobile app OFFLINE bhi kaam karta hai
   subah 9 baje: user ne offline purchase kiya (metro mein, no signal)
   dopahar 12 baje: phone online aaya → event ab bheja gaya

Processor ne use 12 baje ke ghante mein count kiya
   → 9 baje ka sales report: galat (kam)
   → 12 baje ka sales report: galat (zyada)

Aur raat ko batch reconciliation ne asli (event-time) aggregate banaya
   → subah ke report se alag → "report raat ko badal gaya"</pre>
     <div class="war-story"><b>⚡ Business team ka bharosa uth gaya.</b> "Report subah kuch
     dikhata hai, raat ko kuch aur — kaunsa sach hai?" Root cause: processor ne EVENT KAB
     HUA (event-time) ke bajaye EVENT KAB AAYA (processing-time) use kiya. Mobile, IoT,
     ya kisi bhi system mein jahan events der se aa sakte hain, yeh farak sab kuch badal
     deta hai.</div>`,

    `<h3>🪜 Do time, aur windowing</h3>
     <p><b>Pehle do "time" ka farak samjho:</b></p>
     <pre>⏱️ EVENT-TIME:     event ASAL mein kab hua (mobile ne 9 baje record kiya)
                  → payload mein timestamp
📥 PROCESSING-TIME: event processor tak kab AAYA (12 baje)
                  → jab consumer ne use dekha

Sahi analytics ke liye lagbhag hamesha EVENT-TIME chahiye —
"kab hua" matter karta hai, "kab humne dekha" nahi.</pre>
     <p><b>Windowing — stream ko time ke tukdon mein baantna:</b></p>
     <pre>🪟 TUMBLING: fixed, non-overlapping (har ghanta alag)
   [9-10) [10-11) [11-12)   ← "har ghante ki sales"

🎞️ SLIDING/HOPPING: overlapping (har 5 min, pichle 1 ghante ka)
   → "moving average" jaisa

⏸️ SESSION: activity ke gaps se define (user active raha jab tak
   30 min ka gap na aaye) → "user session length"</pre>
     <p>Event-time windowing ke saath, 9 baje ka offline purchase — chahe 12 baje aaye —
     <b>9-10 window mein</b> jaayega (uske event-time ke hisaab se). Report sahi.</p>`,

    `<h3>⚖️ Nayi musibat: late data — window kab band karo?</h3>
     <p>Event-time use karne se ek gehri problem aati hai: agar 9 baje ka event 12 baje
     aata hai, toh humne 9-10 window ka result 10 baje "final" declare kar diya tha —
     galat. Toh window kab "band" karein?</p>
     <pre>Agar window turant band karo (10:00 par):
   → late events (jaise woh 12 baje wala) MISS ho jaate hain
   → result timely par galat

Agar hamesha khula rakho:
   → koi result kabhi final nahi
   → memory infinite (har window ka state rakhna padega)

Hal: WATERMARK + GRACE PERIOD
   Watermark: "main maan raha hoon ki is time tak ke saare events aa chuke"
   Grace period: watermark ke baad bhi thodi der late events accept karo

   9-10 window: 10:00 par close, par 10:15 tak (grace) late events le lo
   10:15 ke baad aaya 9 baje ka event? → "too late" → alag handle
      (drop karo, ya ek "late" side-output mein daalo, ya update bhejo)</pre>
     <p><b>Yeh ek fundamental trade-off hai jise Google ne "Dataflow model" mein
     formalize kiya:</b></p>
     <pre>⚡ TIMELY (jaldi result do)  ⟷  ✅ CORRECT (saare late events samet lo)

   chhota grace period → tez result, kuch late events miss
   bada grace period   → sahi result, par zyada intezaar + memory

Yeh business faisla hai: fraud detection ko TIMELY chahiye
(thodi galti chalegi); billing ko CORRECT chahiye (intezaar theek hai).</pre>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Event-time ya Processing-time?',
      instructions: 'Har use-case ke liye sahi time semantic. Sawal: kya der se aaya event apni asli time par count hona chahiye?',
      bins: [
        { id: 'event', label: '⏱️ Event-time (kab hua)' },
        { id: 'processing', label: '📥 Processing-time (kab aaya)' },
      ],
      items: [
        { text: 'Hourly sales report (mobile app, offline support)', bin: 'event',
          explain: 'Offline purchase apne asli ghante mein count hona chahiye. Processing-time report ko galat kar dega — yahi woh incident tha.' },
        { text: 'System monitoring: "abhi kitne requests/sec aa rahe hain"', bin: 'processing',
          explain: 'Yeh "abhi" ke baare mein hai — jab events aa rahe hain. Real-time load dekhne ke liye processing-time sahi hai.' },
        { text: 'IoT sensor readings ka daily average (network unreliable)', bin: 'event',
          explain: 'Sensor ne jab reading li woh matter karta hai. Network delay se aaya reading apne asli time par count hona chahiye.' },
        { text: 'Live dashboard: "pichle 1 minute mein kitne errors"', bin: 'processing',
          explain: 'Operational "abhi kya ho raha hai" — processing-time theek hai. Thodi late data se koi bada farak nahi padta yahan.' },
        { text: 'Billing: kis mahine ka usage kitna', bin: 'event',
          explain: 'Usage jab HUA us mahine mein count hona chahiye. Late-arriving usage ko sahi billing period mein daalna zaroori hai (paisa hai).' },
        { text: 'Fraud detection: rapid transactions in short window', bin: 'event',
          explain: 'Transactions ka ASLI timing pattern matter karta hai (2 transactions 1 second ke andar = shak). Processing-time network jitter se pattern ko galat kar dega.' },
        { text: 'Rate limiting: "is user ne abhi kitni calls ki"', bin: 'processing',
          explain: 'Rate limit "abhi" enforce hota hai jab request aati hai. Processing-time hi natural hai.' },
        { text: 'A/B test: kis variant ne kitne conversions diye', bin: 'event',
          explain: 'Conversion jab HUA woh matter karta hai (kaunsa variant dikha tha us waqt). Late conversions apne asli time par attribute hone chahiye.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Time & windows ke sawal',
      questions: [
        {
          q: 'Event-time aur processing-time mein farak?',
          choices: [
            'Koi farak nahi',
            'Event-time = event ASAL mein kab hua (payload ka timestamp — jaise mobile ne 9 baje record kiya). Processing-time = processor tak kab AAYA (jaise 12 baje jab phone online hua). Der se aane wale events mein yeh bahut alag ho sakte hain, aur analytics ke liye lagbhag hamesha event-time chahiye.',
            'Event-time hamesha baad mein hota hai',
            'Processing-time zyada accurate hai',
          ],
          answer: 1,
          explain: 'Yeh streaming ka sabse fundamental distinction hai. "Kab hua" (event-time) vs "kab dekha" (processing-time). Mobile/IoT/kisi bhi delay-able source mein farak dramatic hota hai.',
        },
        {
          q: 'Watermark kya hota hai?',
          choices: [
            'Ek security feature',
            'Ek marker jo kehta hai "main maan raha hoon ki is event-time tak ke saare events aa chuke hain" — isse system decide karta hai ki kaunsi windows ab "complete" maani ja sakti hain aur result emit kiya ja sakta hai. Grace period watermark ke baad thodi der late events ko abhi bhi accept karta hai.',
            'Ek encryption key',
            'Message ka checksum',
          ],
          answer: 1,
          explain: 'Watermark event-time processing ka core mechanism hai. Bina iske system ko pata hi nahi chalega ki window "poori" kab hui — ya toh jaldi close karke late events miss karega, ya hamesha khula rakh ke infinite memory use karega.',
        },
        {
          q: 'Grace period bada rakhne ka trade-off?',
          choices: [
            'Koi trade-off nahi, hamesha bada rakho',
            'Bada grace period = zyada late events samet loge (CORRECT result), par result ke liye zyada intezaar karna padega (kam TIMELY) aur zyada window state memory mein rakhni padegi. Chhota grace = tez result par kuch late events miss. Yeh timely-vs-correct ka business trade-off hai (fraud = timely, billing = correct).',
            'Bada grace tez hota hai',
            'Grace period sirf memory badhata hai',
          ],
          answer: 1,
          explain: 'Timely vs correct fundamental streaming trade-off hai. Bada grace correctness ke liye timeliness aur memory ki keemat leta hai. Use-case decide karta hai: fraud detection thodi galti ke saath tez chahiye, billing intezaar ke saath sahi.',
        },
      ],
    },
    {
      type: 'order',
      title: '🔢 Late event ka safar (event-time windowing)',
      instructions: 'Ek 9-baje ka event jo 12 baje aaya — uska sahi safar lagao.',
      steps: [
        { text: '📱 9:00 — mobile app offline purchase record karta hai (event-time = 9:00 payload mein)',
          explain: 'Event ka asli waqt payload mein embed hota hai. Yahi source of truth hai, na ki jab woh aaya.' },
        { text: '📵 9:00-12:00 — phone offline, event phone par pada hai',
          explain: 'Network nahi hai, toh event abhi bheja nahi ja sakta. Yeh 3 ghante ki "lateness" hai.' },
        { text: '📥 12:00 — phone online, event ab Kafka tak aaya (processing-time = 12:00)',
          explain: 'Processing-time 12:00 hai, par event-time 9:00. Processor ko event-time use karna hai.' },
        { text: '🪟 Processor event-time (9:00) dekhta hai → 9-10 window mein daalta hai',
          explain: 'Processing-time (12:00) ko ignore karke event-time (9:00) se window chunta hai. Yahi sahi analytics deta hai.' },
        { text: '⏰ Agar 9-10 window abhi grace period ke andar hai → aggregate update ho jaata hai',
          explain: 'Grace period ke andar aaya toh window ka result update hota hai (9-10 sales badhti hai). Report sahi rehta hai.' },
        { text: '🚨 Agar grace period nikal gaya → "too late" → side-output ya drop',
          explain: 'Bahut late aaya (grace ke baad) toh use alag handle karo — drop karo, ya ek "late events" topic mein daalo taaki investigate ho sake, ya ek correction/update bhejo.' },
      ],
    },
    {
      type: 'match',
      title: '🔗 Kaunsa window, kaunsa use-case',
      instructions: 'Har window type ko uske natural use-case se jodo.',
      pairs: [
        { left: 'Har ghante ki total sales (fixed, non-overlap)', right: '🪟 Tumbling window',
          explain: 'Fixed size, non-overlapping. Har event exactly ek window mein. "Har ghante ka total" ke liye perfect.' },
        { left: 'Har 1 min update: pichle 5 min ka moving average', right: '🎞️ Sliding/hopping window',
          explain: 'Overlapping windows. Ek event kai windows mein ho sakta hai. Moving averages, trends ke liye.' },
        { left: 'User ki activity session (30 min gap = naya session)', right: '⏸️ Session window',
          explain: 'Activity gaps se define hoti hai, fixed time se nahi. User engagement, web sessions ke liye.' },
        { left: 'Bina window ke, har event par running total', right: '♾️ Global / no window',
          explain: 'Kabhi kabhi tumhe window nahi chahiye — bas ek running aggregate (jaise all-time counter). KTable jaisa.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Tumhara hourly sales report subah aur raat mein alag dikhta hai. Kya galat hai?"',
        choices: [
          'Report mein bug hai',
          'Processor shayad PROCESSING-TIME use kar raha hai (event kab aaya) EVENT-TIME (event kab hua) ke bajaye. Mobile/offline events der se aate hain — 9 baje ka offline purchase 12 baje aaya toh processing-time use karne par woh 12 baje ke ghante mein count hua, aur raat ki reconciliation ne event-time se sahi banaya, isliye farak. Fix: event-time windowing use karo (payload ka timestamp), watermark + grace period ke saath late events sambhaalo. Aur decide karo ki grace ke baad aaye events ko drop karna hai, side-output karna hai, ya correction bhejna hai.',
          'Database sync issue hai',
          'Do reports alag systems se aate hain',
        ],
        answer: 1,
        explain: 'Yeh classic event-time bug hai. Achha jawab (1) processing-time vs event-time pehchanta hai, (2) fix (event-time windowing) deta hai, (3) late data ko watermark/grace se sambhaalta hai. Yeh streaming maturity dikhata hai.',
      },
      {
        q: '🧑‍💼 "Late-arriving data ko kaise handle karoge? Window kab band karoge?"',
        choices: [
          'Window turant band kar do jab time paar ho',
          'Turant band karna late events miss karega; hamesha khula rakhna infinite memory. Main watermark + grace period use karunga: watermark batata hai ki kis event-time tak ke events aa chuke maane jaayein, aur grace period uske baad thodi der aur late events accept karta hai. Grace ki lambai timely-vs-correct trade-off hai — fraud detection ke liye chhota (tez, thodi galti OK), billing ke liye bada (sahi, intezaar OK). Grace ke baad aaye events ko main drop nahi karunga — ek side-output topic mein bhejunga taaki pata rahe kitne aur kyun late hue.',
          'Sab events ko 24 ghante rakho',
          'Late events ko ignore kar do',
        ],
        answer: 1,
        explain: 'Yeh gehra streaming sawal hai. Achha jawab watermark + grace ka mechanism, timely-vs-correct trade-off, aur late events ko drop na karke side-output karna — teeno batata hai. Side-output wali baat operational maturity dikhati hai.',
      },
    ],
  },
  success: [
    'Main event-time aur processing-time mein farak kar sakta hoon',
    'Main tumbling/sliding/session windows ke use-cases bata sakta hoon',
    'Main watermark aur grace period se late data sambhaal sakta hoon',
    'Main timely-vs-correct trade-off business terms mein samjha sakta hoon',
  ],
  homework: `<p>Event-time windowing ka concept code mein (Kafka Streams style pseudocode):</p>
    <pre>// event-time extractor — payload se timestamp lo
class EventTimeExtractor {
  extract(record) {
    return JSON.parse(record.value).eventTimestamp;  // NOT record.timestamp
  }
}

// tumbling window with grace period
stream
  .selectKey((k, v) => v.userId)
  .windowedBy(
    TimeWindows.ofSizeAndGrace(
      Duration.ofHours(1),      // 1-hour tumbling window
      Duration.ofMinutes(15)    // 15-min grace for late events
    )
  )
  .count()
  .toStream()
  .to('hourly-sales');</pre>
    <p><b>Asli experiment (soch ke):</b> ek 9:00 event-time wala message banao par
    use 12:00 par bhejo (processing-time). Processing-time windowing mein woh 12-baje
    ke bucket mein jaayega; event-time windowing mein 9-baje ke. Yeh farak samajhna hi
    streaming ka aadha gyaan hai. Flink aur Kafka Streams dono event-time first-class
    support karte hain.</p>`,
  cards: [
    { front: 'Event-time vs processing-time?', back: '<b>Event-time</b> = event asal mein kab hua (payload timestamp — mobile ne 9 baje record kiya).<br><b>Processing-time</b> = processor tak kab aaya (12 baje online hua). Der se aane wale events mein yeh bahut alag. Analytics ke liye lagbhag hamesha event-time.' },
    { front: 'Teen window types?', back: '<b>Tumbling</b>: fixed, non-overlapping (har ghante ka total).<br><b>Sliding/hopping</b>: overlapping (moving average).<br><b>Session</b>: activity gaps se define (user session, 30-min gap = naya).' },
    { front: 'Watermark kya hai?', back: 'Ek marker: "main maan raha hoon ki is event-time tak ke saare events aa chuke". Isse system decide karta hai ki kaunsi windows "complete" hain aur result emit kiya ja sakta hai. Bina iske window kabhi close hi nahi hoti.' },
    { front: 'Late data ka hal?', back: '<b>Watermark + grace period</b>. Window watermark par "close" hoti hai par grace period uske baad thodi der late events accept karta hai. Grace ke baad aaye events → drop, side-output topic, ya correction. Turant close = miss; hamesha khula = infinite memory.' },
    { front: 'Timely vs correct trade-off?', back: 'Chhota grace period = tez result par kuch late events miss (TIMELY). Bada grace = sahi result par zyada intezaar + memory (CORRECT). Business decide karta hai: fraud detection = timely, billing = correct.' },
  ],
},

]);

/* ===================== Levels 4-6: Streams, Schema Registry, Security ===================== */
KQ.registerLevels('advanced', [

/* =====================================================================
   4. STREAM PROCESSING (Kafka Streams / ksqlDB)
   ===================================================================== */
{
  id: 'stream-processing',
  icon: '🌊',
  title: 'Stream Processing — KTable ka state 45 min mein restore hua',
  badge: 'Stream Sadhak',
  objectives: [
    'KStream aur KTable ka farak samjho (event stream vs latest-state table)',
    'Stateful operations (aggregations, joins) aur unki keemat jaano',
    'Changelog topics aur state restore ka dard samjho',
    'Jaano ki kab Kafka Streams use karo aur kab simple consumer',
  ],
  lesson: [
    `<h3>😰 Dard: "Ek pod restart hua, 45 minute tak stale data serve hua"</h3>
     <p>Ek team ne Kafka Streams se ek real-time leaderboard banaya — har user ka
     current score ek KTable mein maintained tha (crore users). Yeh state consumer ke
     local disk par (RocksDB) rehta tha.</p>
     <pre>Ek pod restart hua (routine deploy). Us pod ke paas jo
partitions the, unka poora KTable state local disk se GAYA
(naya pod, khaali disk).

Kafka Streams ne state ko CHANGELOG TOPIC se restore karna shuru kiya:
   → changelog topic mein crore updates the
   → poora topic replay karke KTable rebuild karna pada
   → 45 MINUTE laga

Us 45 minute mein us pod ke users ka leaderboard:
   → ya toh error, ya STALE (purana score)</pre>
     <div class="war-story"><b>⚡ State restore ka dard.</b> Kafka Streams ka power yahi
     hai ki woh local state rakhta hai (fast reads, no external DB), par uski keemat yeh
     hai ki state kho jaane par use changelog se rebuild karna padta hai — aur woh dheema
     ho sakta hai. Yeh Kafka Streams ka sabse aam production surprise hai.</div>`,

    `<h3>🪜 KStream vs KTable — do tarike se data dekhna</h3>
     <p>Kafka Streams do abstractions deta hai, aur inhe samajhna poore paradigm ki chaabi hai:</p>
     <pre>📜 KStream — har event ek alag record (event stream)
   ("user-1 ne 10 points kamaye", "user-1 ne 5 points kamaye")
   → har message ek NAYA fact hai, purane ko replace nahi karta
   → jaise: transactions, clicks, sensor readings

📊 KTable — har key ka LATEST value (changelog / state)
   ("user-1 ka score ab 15 hai")
   → naya message purani value ko REPLACE karta hai (upsert)
   → jaise: current score, current profile, current stock
   → yeh compacted topic ka natural roop hai (pichla level!)

Rishta:
   KStream ko aggregate karo → KTable ("stream of events" → "current state")
   KTable ka changelog dekho → KStream ("current state" → "stream of updates")</pre>
     <p><b>Stateful operations</b> — jahan asli power hai:</p>
     <pre>• aggregations: count, sum, custom (har key ka running total)
• joins: KStream ⋈ KTable (event ko current state se enrich karo)
         KStream ⋈ KStream (do streams ko window ke andar match karo)
• windowing: pichla level — time buckets mein aggregate

Yeh sab STATE maangte hain (kahin toh running total rakhna padega),
aur woh state local (RocksDB) + changelog topic mein rehta hai.</pre>`,

    `<h3>⚖️ Nayi musibat: state ki keemat, aur Streams kab NA use karo</h3>
     <p><b>Local state ke teen dard:</b></p>
     <pre>1️⃣ RESTORE time: state kho gaya (pod restart, disk fail) → changelog
   se rebuild → bada state = lamba restore (woh 45 min)
   → hal: standby replicas (num.standby.replicas) — ek aur pod
     par state ki copy warm rakho, failover tez ho

2️⃣ REBALANCE: partition move hui → uska state naye pod par restore
   → yahi rebalance ko Streams mein aur mehnga banata hai
   → hal: static membership + cooperative rebalancing (Track 2!)

3️⃣ DISK: local state ko disk chahiye — crore keys = bahut disk
   → capacity planning ka hissa</pre>
     <p><b>Aur sabse zaroori: Kafka Streams kab NA use karo?</b></p>
     <pre>❌ Agar tumhara kaam STATELESS hai (bas transform/filter/route)
   → simple consumer kaafi hai, Streams ka overhead mat lo

❌ Agar tumhe cross-system joins chahiye (Kafka + external DB)
   → Streams local state model fit nahi baithta

✅ Kafka Streams tab jab: stateful (aggregations, joins, windows),
   Kafka→Kafka, aur tum exactly-once + local fast state chahte ho
   → tab woh ek external stream-processing cluster (Flink/Spark) se
     bhi simple hai — sirf ek library, apni app mein chalti hai</pre>
     <p>ksqlDB isi ke upar SQL layer deta hai — <code>CREATE TABLE … AS SELECT …</code> —
     taaki tum stream processing SQL mein likh sako. Andar wahi KStream/KTable hai.</p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ KStream ya KTable?',
      instructions: 'Har data ko sahi abstraction se jodo. Sawal: naya message purane ko REPLACE karta hai (table) ya ek naya alag fact hai (stream)?',
      bins: [
        { id: 'kstream', label: '📜 KStream (har event ek naya fact)' },
        { id: 'ktable', label: '📊 KTable (har key ka latest value)' },
      ],
      items: [
        { text: 'User ka current account balance', bin: 'ktable',
          explain: 'Latest value matter karta hai — naya balance purane ko replace karta hai. Classic KTable (upsert).' },
        { text: 'Har purchase transaction', bin: 'kstream',
          explain: 'Har transaction ek alag fact hai — purani ko replace nahi karta. Event stream. Inhe aggregate karke KTable (total) bana sakte ho.' },
        { text: 'Product ka current price', bin: 'ktable',
          explain: 'Current price ek state hai — naya price purane ko replace karta hai. KTable.' },
        { text: 'Website clicks', bin: 'kstream',
          explain: 'Har click ek alag event hai. Stream. Window ke andar count karke trends nikaal sakte ho.' },
        { text: 'IoT device ka current online/offline status', bin: 'ktable',
          explain: 'Current status ek state hai (online ya offline). Naya status purane ko replace karta hai. KTable.' },
        { text: 'Sensor temperature readings', bin: 'kstream',
          explain: 'Har reading ek alag data point hai (itihaas). Stream. Average nikaalne ke liye aggregate.' },
        { text: 'User ka current shipping address', bin: 'ktable',
          explain: 'Latest address matter karta hai. KTable — naya address purane ko replace.' },
        { text: 'Order status changes (created→paid→shipped)', bin: 'kstream',
          explain: 'Yeh thoda tricky hai — status CHANGES ek stream hain (har change ek event), par "current status" ek KTable hai. Changes ki stream ko aggregate karke current-status KTable banate hain.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Stream processing ke sawal',
      questions: [
        {
          q: 'KStream aur KTable mein core farak?',
          choices: [
            'KTable tez hai',
            'KStream mein har message ek NAYA alag fact hai (purane ko replace nahi karta) — jaise transactions, clicks. KTable mein har key ka LATEST value hota hai, naya message purane ko upsert (replace) karta hai — jaise current balance, current profile. KTable ek compacted changelog ka natural roop hai.',
            'KStream purana hai',
            'KTable sirf reads ke liye hai',
          ],
          answer: 1,
          explain: 'Yeh Kafka Streams ka core mental model hai. Stream = event log (har fact matter karta hai). Table = current state (sirf latest matter karta hai). Dono ek doosre mein convert ho sakte hain (aggregate / changelog).',
        },
        {
          q: 'Kafka Streams ka pod restart hua aur 45 min tak stale data mila. Kyun?',
          choices: [
            'Kafka down tha',
            'Local state (KTable/aggregations RocksDB mein) pod ke saath gaya. Kafka Streams use CHANGELOG TOPIC se replay karke rebuild karta hai — bada state = lamba restore. Hal: standby replicas (num.standby.replicas) se ek warm copy doosre pod par rakho taaki failover tez ho, aur static membership + cooperative rebalancing se rebalance kam karo.',
            'Consumers kam the',
            'Partitions badhane padenge',
          ],
          answer: 1,
          explain: 'State restore Kafka Streams ka sabse aam production surprise hai. Local state fast reads deta hai par kho jaane par changelog se rebuild dheema hota hai. Standby replicas iska standard ilaaj hain.',
        },
        {
          q: 'Kab Kafka Streams NA use karke simple consumer use karoge?',
          choices: [
            'Kabhi nahi, Streams hamesha better hai',
            'Jab kaam STATELESS ho — bas transform, filter, ya route karna. Simple consumer kaafi hai aur Streams ka state/changelog/restore overhead nahi lena chahiye. Streams tab use karo jab stateful ho (aggregations, joins, windows) aur Kafka→Kafka.',
            'Jab data zyada ho',
            'Jab partitions zyada hon',
          ],
          answer: 1,
          explain: 'Kafka Streams ka overhead (local state, changelog topics, restore time, disk) sirf tab justify hota hai jab tumhe stateful processing chahiye. Stateless kaam ke liye woh over-engineering hai.',
        },
      ],
    },
    {
      type: 'match',
      title: '🔗 Stateful operation aur uski state',
      instructions: 'Har operation ko usse jodo jो woh karta hai.',
      pairs: [
        { left: 'KStream → aggregate (count/sum)', right: '📊 KStream ko KTable banata hai',
          explain: 'Events ki stream ko aggregate karo → har key ka running total (KTable). "Stream of purchases" → "total per user".' },
        { left: 'KStream ⋈ KTable join', right: '🔗 Event ko current state se enrich',
          explain: 'Har event ko uske key ke current state se jodo. "Order event" ⋈ "user profile KTable" = enriched order.' },
        { left: 'KStream ⋈ KStream join', right: '🪟 Window ke andar do streams match',
          explain: 'Do event streams ko time window ke andar match karo. "Clicks" ⋈ "purchases" within 10 min = attribution.' },
        { left: 'KTable ka changelog', right: '📜 KTable ko KStream banata hai',
          explain: 'Har state change ek event ban jaata hai. "Current balance table" → "stream of balance updates".' },
        { left: 'Local state kho gaya', right: '🔄 Changelog topic se restore',
          explain: 'State RocksDB mein tha, kho gaya → changelog topic replay karke rebuild. Bada state = lamba restore.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "KStream aur KTable mein kya farak hai? Ek example do."',
        choices: [
          'KTable database hai, KStream nahi',
          'KStream har message ko ek naya alag fact maanta hai (jaise "user-1 ne 10 points kamaye", "user-1 ne 5 points kamaye" — dono alag events); KTable har key ka latest value rakhta hai, naya message purane ko replace karta hai (jaise "user-1 ka current score = 15"). KStream ko aggregate karke KTable banate hain (events → current state), aur KTable ka changelog KStream deta hai (state → updates ki stream). KTable ek compacted topic ka natural roop hai. Example: purchases = KStream, current balance = KTable.',
          'Dono same hain, naam alag hai',
          'KStream sirf reads, KTable sirf writes',
        ],
        answer: 1,
        explain: 'Yeh Kafka Streams ka sabse fundamental sawal hai. Achha jawab dono ko example ke saath define karta hai AUR unke beech ka rishta (aggregate/changelog) batata hai. Yeh dikhata hai ki tum "stream-table duality" samajhte ho.',
      },
      {
        q: '🧑‍💼 "Kafka Streams stateful app deploy karte waqt tumhe kya dhyan rakhna hoga?"',
        choices: [
          'Kuch khaas nahi',
          'Sabse bada: STATE RESTORE. Local state (RocksDB) rebalance ya pod restart par changelog se rebuild hota hai, jo bada state ke saath dheema (minutes) ho sakta hai. Isliye main: (1) num.standby.replicas set karunga taaki warm copies failover tez karein, (2) static membership + cooperative rebalancing (Track 2) se rebalance kam karunga, (3) disk capacity plan karunga (crore keys = bahut local disk), (4) changelog topics ka retention/compaction dekhunga. Aur agar kaam stateless hota toh Streams use hi na karta — simple consumer kaafi.',
          'Sirf zyada memory chahiye',
          'Bas partitions badha do',
        ],
        answer: 1,
        explain: 'Yeh operational maturity ka sawal hai. Achha jawab state restore ko primary concern pehchanta hai aur standby replicas + rebalancing tuning + disk planning deta hai. "Stateless ho toh Streams mat use karo" wali baat bonus wisdom hai.',
      },
    ],
  },
  success: [
    'Main KStream aur KTable mein farak kar sakta hoon (event stream vs latest state)',
    'Main stateful operations (aggregations, joins) aur unki state samjha sakta hoon',
    'Main state restore ka dard aur uska hal (standby replicas) bata sakta hoon',
    'Main jaanta hoon ki kab Streams use karo aur kab simple consumer',
  ],
  homework: `<p>Kafka Streams ka mental model code mein (Java/pseudocode):</p>
    <pre>StreamsBuilder builder = new StreamsBuilder();

// KStream — har purchase ek event
KStream<String, Purchase> purchases =
    builder.stream("purchases");

// aggregate karke KTable banao — har user ka total
KTable<String, Long> userTotals = purchases
    .groupByKey()
    .aggregate(
        () -> 0L,
        (userId, purchase, total) -> total + purchase.amount,
        Materialized.as("user-totals-store")   // ← local state + changelog
    );

// KTable ko wapas stream (changelog) mein bhej do
userTotals.toStream().to("user-totals");</pre>
    <p><b>Asli experiment (soch ke ya asli cluster par):</b> yeh app chalao, kuch
    purchases bhejo, dekho ki user-totals update hote hain. Phir app restart karo —
    dekho ki woh changelog topic (user-totals-store-changelog) se state restore karta
    hai. Chhota state = tez; imagine karo crore users ke saath. Standby replicas kaise
    madad karti woh soch ke dekho.</p>`,
  cards: [
    { front: 'KStream vs KTable?', back: '<b>KStream</b>: har message ek naya alag fact (transactions, clicks) — purane ko replace nahi karta.<br><b>KTable</b>: har key ka latest value (current balance, profile) — naya message upsert karta hai. KTable = compacted changelog ka natural roop.' },
    { front: 'Stream-table duality?', back: 'KStream ko <b>aggregate</b> karo → KTable (events → current state). KTable ka <b>changelog</b> dekho → KStream (state → updates ki stream). Dono ek doosre mein convert ho sakte hain.' },
    { front: 'Kafka Streams state restore ka dard?', back: 'Local state (RocksDB) pod restart/rebalance par kho jaata hai → <b>changelog topic</b> se rebuild → bada state = minutes ka restore (stale data). Hal: <code>num.standby.replicas</code> (warm copies) + static membership + cooperative rebalancing.' },
    { front: 'Kab Kafka Streams NA use karein?', back: 'Jab kaam <b>stateless</b> ho (bas transform/filter/route) — simple consumer kaafi hai, Streams ka state/changelog/restore overhead mat lo. Streams sirf stateful (aggregations, joins, windows) + Kafka→Kafka ke liye.' },
    { front: 'ksqlDB kya hai?', back: 'Kafka Streams ke upar ek <b>SQL layer</b> — <code>CREATE TABLE … AS SELECT …</code> se stream processing SQL mein likho. Andar wahi KStream/KTable model chalta hai.' },
  ],
},

/* =====================================================================
   5. SCHEMA REGISTRY & EVOLUTION
   ===================================================================== */
{
  id: 'schema-registry',
  icon: '📜',
  title: 'Schema Registry — producer ne field badli, 40 consumers gire',
  badge: 'Schema Shastri',
  objectives: [
    'Samjho ki Schema Registry kaise breaking changes rokta hai',
    'BACKWARD / FORWARD / FULL compatibility modes seekho',
    'Avro/Protobuf ke schema-id approach ka fayda jaano',
    'Jaano ki compatibility mode kaise chunte hain',
  ],
  lesson: [
    `<h3>😰 Dard: "Ek field rename ne production ka aadha platform gira diya"</h3>
     <p>Track 1 mein tumne yeh dekha tha — plain JSON ke saath ek producer ne field
     rename ki aur 40 consumers gir gaye. Ab dekho ki iska SYSTEMATIC hal kya hai.</p>
     <p>Us waqt problem yeh thi ki <b>kisi ne producer ko roka nahi</b>. JSON mein koi
     schema enforcement nahi tha — producer kuch bhi bhej sakta tha, aur consumers ko
     production mein hi pata chalta tha.</p>
     <pre>Bina Schema Registry:
   producer → koi bhi shape bhej sakta hai → Kafka → consumer crash
   (galti PRODUCTION mein pakdi jaati hai, deploy ke baad)

Chahiye: producer ke DEPLOY ke waqt hi rok — "yeh change
   purane consumers ko todega, isliye allow nahi"</pre>
     <div class="war-story"><b>⚡ Yeh problem itni aam hai</b> ki iska ek poora system
     hai: Schema Registry. Woh ek central jagah hai jahan har topic ka schema register
     hota hai, aur producer ko naya schema tabhi accept hota hai jab woh compatibility
     rules paas kare. Galti CI/deploy time par pakdi jaati hai, na ki 3AM production mein.</div>`,

    `<h3>🪜 Schema Registry kaise kaam karta hai</h3>
     <pre>1. Har topic ka ek SCHEMA registered hota hai (Avro/Protobuf/JSON-Schema)
   Registry us schema ko ek ID deta hai (jaise schema-id 42)

2. Producer message bhejte waqt:
   → schema ke against message VALIDATE karo
   → payload mein poora schema nahi, sirf 5-byte schema-ID daalo
   → (isliye messages chhote rehte hain — schema ek baar registered)

3. Consumer message padhte waqt:
   → schema-ID se registry se schema fetch karo (cached)
   → us schema se message deserialize karo

4. Producer NAYA schema register karne ki koshish kare:
   → registry COMPATIBILITY check karta hai purane schema se
   → agar breaking hai → REJECT (producer deploy fail)
   → agar compatible hai → accept, naya version</pre>
     <p><b>Yahi asli jaadu hai:</b> breaking change producer ke deploy/CI time par
     REJECT ho jaata hai. Woh kabhi production tak pahunchta hi nahi. Us 40-consumer
     incident ka schema-registry version: producer ki deploy pipeline fail ho jaati,
     developer ko turant pata chalta, aur woh field rename ko expand-contract mein karta.</p>`,

    `<h3>⚖️ Compatibility modes — kaunsa kab</h3>
     <p>Registry check karta hai "naya schema purane se compatible hai?" — par
     "compatible" ka matlab kaunsa? Yeh compatibility MODE se tay hota hai:</p>
     <pre>🔙 BACKWARD (sabse aam, default)
   naya schema PURANE data ko padh sakta hai
   → CONSUMERS ko pehle upgrade karo, phir producers
   → allow: field HATANA, optional field ADD karna
   → "naye consumer purane messages padh saken"

🔜 FORWARD
   purana schema NAYE data ko padh sakta hai
   → PRODUCERS ko pehle upgrade karo, phir consumers
   → allow: field ADD karna, optional field HATANA
   → "purane consumer naye messages padh saken"

🔄 FULL (backward + forward dono)
   dono taraf compatible — sabse safe, sabse restrictive
   → allow: sirf optional fields add/remove
   → jab tum producer/consumer ka upgrade order control nahi kar sakte

🚫 NONE — koi check nahi (mat use karo, yeh "no registry" jaisa)</pre>
     <p><b>Nayi musibat — mode ka matlab upgrade order hai:</b> compatibility mode sirf
     "kya allow hai" nahi batata — woh yeh bhi batata hai ki tumhe <b>kis kram mein</b>
     deploy karna hai. BACKWARD ka matlab consumers pehle; FORWARD ka matlab producers
     pehle. Galat kram mein deploy karoge toh compatible schema bhi break karega.
     Zyadatar teams BACKWARD use karti hain (consumers pehle upgrade karna natural hai),
     par agar tum sab kuch ek saath control nahi kar sakte toh FULL sabse safe hai.</p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Yeh schema change kis mode mein allowed hai?',
      instructions: 'Har change ko dekho: naya schema purana data padh sakta hai (backward), purana schema naya data padh sakta hai (forward), ya dono/koi nahi.',
      bins: [
        { id: 'backward', label: '🔙 BACKWARD-safe (naya schema purana data padhe)' },
        { id: 'forward', label: '🔜 FORWARD-safe (purana schema naya data padhe)' },
        { id: 'breaking', label: '🚫 BREAKING (dono taraf todega)' },
      ],
      items: [
        { text: 'Optional field ADD karna (default value ke saath)', bin: 'backward',
          explain: 'Backward-safe: naya schema (jisme field hai) purana data (bina field ke) padh sakta hai — default use hota hai. (Yeh actually FULL-safe bhi hai.) Backward mein classic allowed change.' },
        { text: 'Ek field HATANA', bin: 'backward',
          explain: 'Backward-safe: naya schema (bina field ke) purana data (jisme field tha) padh sakta hai — extra field ignore. Par FORWARD-breaking hai (purana schema naye data mein woh field expect karega).' },
        { text: 'Required field ADD karna (bina default)', bin: 'breaking',
          explain: 'Dono taraf todega: purana data mein woh field nahi (backward fail), aur purana schema use nahi bhejega (forward fail). Isliye hamesha default do ya optional banao.' },
        { text: 'Field ka naam badalna (userId → user_id)', bin: 'breaking',
          explain: 'Rename = ek field hatana + ek naya add karna, dono breaking. Yahi woh 40-consumer incident tha. Expand-contract se karo (Track 1).' },
        { text: 'Field ka type badalna (int → string)', bin: 'breaking',
          explain: 'Type change dono taraf todega — data deserialize hi nahi hoga. Kabhi bhi safe nahi.' },
        { text: 'Optional field HATANA', bin: 'forward',
          explain: 'Forward-safe: purana schema (jisme optional field tha) naya data (bina field ke) padh sakta hai — optional tha toh missing OK. Backward-breaking ho sakta hai agar naya consumer us field ko expect kare.' },
        { text: 'Naya optional field with default (dono taraf works)', bin: 'backward',
          explain: 'Yeh FULL-compatible hai (dono taraf safe), par backward category mein bhi aata hai. Default value hone se yeh sabse safe change hai. (Note: yeh aur pehla item dono default-wale hain — yahi sabse common safe pattern.)' },
        { text: 'Enum mein nayi value add karna', bin: 'breaking',
          explain: 'Chupa hua breaking! Purane consumers ka code nayi enum value handle nahi karta (unknown value par crash ya galat branch). Avro mein yeh forward-incompatible hai. Hamesha default/unknown case rakho.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Schema Registry ke sawal',
      questions: [
        {
          q: 'Schema Registry ka sabse bada fayda kya hai?',
          choices: [
            'Messages chhote ho jaate hain',
            'Breaking schema changes PRODUCER ke deploy/CI time par REJECT ho jaate hain — woh production tak pahunchte hi nahi. Bina registry ke galat schema production mein consumers ko crash karke pakdi jaati hai; registry ke saath producer ki deploy pipeline pehle hi fail ho jaati hai. (Bonus: schema-id use karne se messages chhote bhi rehte hain, par asli value early rejection hai.)',
            'Encryption deta hai',
            'Consumers tez ho jaate hain',
          ],
          answer: 1,
          explain: 'Registry ka core value "shift left" hai — galti ko production se hata ke CI/deploy time par le aana. Woh 40-consumer incident registry ke saath ek failed deploy ban jaata, na ki 3AM outage.',
        },
        {
          q: 'BACKWARD compatibility mode ka matlab deploy order ke liye kya hai?',
          choices: [
            'Koi order nahi, kabhi bhi deploy karo',
            'CONSUMERS ko pehle upgrade karo, phir producers. Kyunki BACKWARD ka matlab hai "naya schema purana data padh sakta hai" — toh naye consumers (naya schema) abhi bhi aa rahe purane messages padh sakte hain. Agar producer pehle upgrade karoge toh purane consumers naya data nahi padh paayenge.',
            'Producers ko pehle upgrade karo',
            'Sab ek saath deploy karo',
          ],
          answer: 1,
          explain: 'Compatibility mode sirf "kya allow" nahi, "kis kram mein deploy" bhi batata hai. BACKWARD = consumers pehle. FORWARD = producers pehle. Yeh galat samajhne se compatible schema bhi break karta hai.',
        },
        {
          q: 'Tum producer aur consumer ka upgrade order control nahi kar sakte. Kaunsa mode?',
          choices: [
            'NONE — flexibility ke liye',
            'FULL — kyunki woh backward AUR forward dono compatibility maangta hai, matlab naye/purane schema kisi bhi kram mein deploy ho sakte hain bina break kiye. Trade-off: yeh sabse restrictive hai (sirf optional fields add/remove allowed), par jab deploy order guaranteed nahi ho toh sabse safe.',
            'BACKWARD hamesha',
            'FORWARD hamesha',
          ],
          answer: 1,
          explain: 'FULL mode order-independence deta hai — iski keemat hai ki tum sirf optional fields hi add/remove kar sakte ho. Jab tum deploy order control nahi kar sakte (jaise alag teams, alag release cycles), FULL sabse safe hai.',
        },
      ],
    },
    {
      type: 'order',
      title: '🔢 Schema-based message ka safar',
      instructions: 'Ek message ka producer se consumer tak schema-registry ke saath safar lagao.',
      steps: [
        { text: '📝 Producer schema register karta hai (ya woh pehle se registered hai) → schema-ID milta hai',
          explain: 'Har topic ka schema registry mein register hota hai aur use ek ID milti hai (jaise 42). Yeh ek baar hota hai.' },
        { text: '✅ Naya schema? Registry compatibility check karta hai — breaking hai toh REJECT',
          explain: 'Yahi asli suraksha hai. Agar naya schema purane consumers ko todega (compatibility mode ke hisaab se), producer ka deploy fail ho jaata hai — production se pehle.' },
        { text: '📤 Producer message ko schema se validate karta hai, phir schema-ID + payload bhejta hai',
          explain: 'Payload mein poora schema nahi — sirf 5-byte schema-ID. Isliye messages chhote rehte hain aur schema ek jagah maintain hota hai.' },
        { text: '📥 Consumer message padhta hai — usme se schema-ID nikaalta hai',
          explain: 'Message ke shuru mein schema-ID hai. Consumer use dekh ke jaanta hai ki kaunse schema se deserialize karna hai.' },
        { text: '🔍 Consumer registry se us schema-ID ka schema fetch karta hai (cached)',
          explain: 'Pehli baar registry se fetch, phir cache. Isliye har message par network call nahi — sirf naye schema-IDs par.' },
        { text: '🎯 Consumer us schema se message deserialize karta hai — type-safe object',
          explain: 'Ab consumer ke paas ek sahi typed object hai. Koi "field undefined" surprise nahi, kyunki schema guarantee deta hai.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Producer ne field rename ki aur consumers gir gaye. Iska systematic hal kya hai?"',
        choices: [
          'Sabko email karke coordinate karo',
          'Schema Registry (Avro/Protobuf ke saath). Har topic ka schema register hota hai, aur producer naya schema tab hi register kar sakta hai jab woh compatibility check paas kare — toh breaking change (jaise rename) producer ke DEPLOY/CI time par reject ho jaata hai, production mein 3AM ko nahi. Compatibility mode (aam taur par BACKWARD) yeh bhi tay karta hai ki deploy kis kram mein karna hai (BACKWARD = consumers pehle). Rename khud karna ho toh expand-contract se: naya field add karo (compatible), consumers migrate karo, phir purana hatao.',
          'JSON ko validate karne wala middleware',
          'Consumers ko defensive banao',
        ],
        answer: 1,
        explain: 'Yeh Track 1 ke serialization level ka systematic answer hai. Schema Registry galti ko "shift left" karta hai — production se CI time par. Achha jawab registry + compatibility mode + expand-contract sab jodta hai.',
      },
      {
        q: '🧑‍💼 "BACKWARD, FORWARD aur FULL compatibility mein kya farak hai?"',
        choices: [
          'Sab same hain, naam alag',
          'BACKWARD: naya schema purana data padh sakta hai → consumers pehle upgrade → field hatana/optional add allowed. FORWARD: purana schema naya data padh sakta hai → producers pehle upgrade → field add/optional hatana allowed. FULL: dono → koi bhi upgrade order → sirf optional fields add/remove. Main aam taur par BACKWARD use karunga (consumers pehle upgrade karna natural hai), par jab producer/consumer ka deploy order control nahi kar sakta toh FULL sabse safe hai.',
          'BACKWARD purana hai',
          'FULL sirf reads ke liye',
        ],
        answer: 1,
        explain: 'Yeh detail-level sawal check karta hai ki tum compatibility ka matlab sach mein samajhte ho — sirf "kya allowed" nahi, balki "deploy order kya" bhi. Yeh distinction hi senior answer banati hai.',
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki Schema Registry breaking changes ko deploy-time par kaise rokta hai',
    'Main BACKWARD/FORWARD/FULL compatibility modes ka farak bata sakta hoon',
    'Main jaanta hoon ki compatibility mode deploy order bhi tay karta hai',
    'Main schema-id approach ka fayda samjha sakta hoon',
  ],
  homework: `<p>Schema evolution ke rules dekho (Avro example):</p>
    <pre>// v1 schema (Avro)
{
  "type": "record", "name": "Order",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "amount", "type": "double" }
  ]
}

// v2 — BACKWARD-compatible change (optional field with default)
{
  "type": "record", "name": "Order",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "currency", "type": "string", "default": "INR" }  // ← safe
  ]
}

// registry check (Confluent):
// curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \\
//   --data '{"schema": "..."}' \\
//   http://localhost:8081/compatibility/subjects/orders-value/versions/latest
// → {"is_compatible": true}   ya   false</pre>
    <p><b>Asli experiment:</b> v2 mein <code>currency</code> ko bina default ke required
    banao — compatibility check <code>false</code> dega (backward-breaking). Phir default
    daalo — <code>true</code>. Yeh dekhna ki registry deploy-time par kaise galti pakadta
    hai, us 40-consumer incident ka asli antidote hai.</p>`,
  cards: [
    { front: 'Schema Registry ka sabse bada fayda?', back: 'Breaking schema changes <b>producer ke deploy/CI time par REJECT</b> ho jaate hain — production tak pahunchte hi nahi. Bina registry galti 3AM production mein pakdi jaati hai; registry ke saath failed deploy ban jaati hai. Galti ko "shift left".' },
    { front: 'BACKWARD compatibility + deploy order?', back: '"Naya schema purana data padh sakta hai" → <b>CONSUMERS pehle upgrade</b>, phir producers. Allow: field hatana, optional field add. Sabse common default.' },
    { front: 'FORWARD compatibility + deploy order?', back: '"Purana schema naya data padh sakta hai" → <b>PRODUCERS pehle upgrade</b>, phir consumers. Allow: field add, optional field hatana.' },
    { front: 'FULL compatibility kab use karein?', back: 'Jab tum producer/consumer ka <b>upgrade order control nahi kar sakte</b> (alag teams/release cycles). Backward + forward dono — koi bhi kram safe. Keemat: sabse restrictive (sirf optional fields add/remove).' },
    { front: 'Schema-id approach ka fayda?', back: 'Payload mein poora schema nahi, sirf <b>5-byte schema-ID</b> jaata hai (messages chhote rehte hain). Consumer ID se registry se schema fetch karke (cached) deserialize karta hai. Schema ek jagah maintain, har message par nahi.' },
  ],
},

/* =====================================================================
   6. SECURITY & MULTI-TENANCY
   ===================================================================== */
{
  id: 'security-quotas',
  icon: '🔐',
  title: 'Security & Quotas — ek team ne pura cluster kha liya',
  badge: 'Suraksha Sadhak',
  objectives: [
    'Kafka ke teen security layers samjho: encryption, authentication, authorization',
    'SASL aur mTLS ka basic idea jaano',
    'ACLs se topic-level access control seekho',
    'Quotas se ek tenant ko cluster monopolize karne se rokna jaano',
  ],
  lesson: [
    `<h3>😰 Dard: "Ek batch job ne poore cluster ko 20 minute ke liye bekaar kar diya"</h3>
     <p>Ek company ka shared Kafka cluster tha jise 30 teams use karti thin. Ek team ne
     ek backfill job chalaya jo <b>50 lakh messages ek saath</b> ek topic mein daal raha
     tha, full speed.</p>
     <pre>Us ek job ne:
   → cluster ka poora network bandwidth kha liya
   → broker CPU 100% (compression, replication)
   → baaki 29 teams ke producers/consumers DHEEME ho gaye
   → kai teams ka lag spike, kuch ke timeouts

Ek team ke resource-hog ne SAB ko affect kiya —
kyunki cluster mein koi QUOTA nahi tha. Har client jitna
chaahe utna maang sakta tha.</pre>
     <div class="war-story"><b>⚡ "Noisy neighbour" problem.</b> Shared cluster mein ek
     tenant doosron ko bhookha maar sakta hai. Aur security ka doosra pehlu: uss cluster
     mein koi authentication bhi nahi tha — koi bhi service kisi bhi topic ko padh/likh
     sakti thi. Ek misconfigured consumer galti se payment topic padh sakta tha. Shared
     infrastructure security aur quotas ke bina ek time-bomb hai.</div>`,

    `<h3>🪜 Kafka security ke teen layers</h3>
     <p>Security teen alag sawaalon ka jawab hai — teeno chahiye:</p>
     <pre>1️⃣ ENCRYPTION (data ko chhupao) — "beech mein koi padh na le"
   TLS/SSL — network par data encrypted
   → in-transit encryption; man-in-the-middle se bachav

2️⃣ AUTHENTICATION (tum kaun ho?) — "sirf jaane-pehchaane clients"
   SASL (username/password, Kerberos, OAuth) YA mTLS (client certs)
   → har client ko apni pehchan deni padti hai
   → bina iske koi bhi connect kar sakta hai (woh open cluster)

3️⃣ AUTHORIZATION (tum kya kar sakte ho?) — "sirf apne topics"
   ACLs (Access Control Lists)
   → "team-A sirf team-A.* topics padh/likh sakti hai"
   → payment topic sirf payment-service likh sakti hai
   → bina iske authenticated client bhi SAB kuch access kar sakta hai</pre>
     <p><b>Yeh teeno alag hain aur teeno zaroori:</b> encryption ke bina data taar par
     nanga hai; authentication ke bina koi bhi ghus sakta hai; authorization ke bina
     ghusa hua banda sab kuch kar sakta hai. "Authenticated" ≠ "authorized" —
     tum jaan-pehchaan sakte ho par phir bhi payment topic access nahi karna chahiye.</p>`,

    `<h3>⚖️ ACLs aur Quotas — multi-tenancy ka asli kaam</h3>
     <p><b>ACLs — kaun kya kar sakta hai:</b></p>
     <pre>Principal (kaun)  →  Operation (kya)  →  Resource (kis par)
"User:payment-svc"  →  WRITE           →  Topic:payments
"User:analytics"    →  READ            →  Topic:orders (aur DENY payments)
"User:team-A-*"     →  READ,WRITE      →  Topic:team-A.*

Rule: LEAST PRIVILEGE — har service ko sirf utna access jitna
zaroori. Analytics ko payments topic padhne ki zaroorat nahi? → deny.</pre>
     <p><b>Quotas — "noisy neighbour" ka ilaaj:</b></p>
     <pre>Ab woh backfill wala incident quotas se rukega:

produce/consume rate quota:
   "User:team-A produce max 10 MB/s"  → usse zyada bhejo toh throttle
request rate quota:
   "User:team-B max 20% broker CPU time"

→ ek tenant apni limit se zyada maange toh Kafka use THROTTLE karta hai
   (usko dheema karta hai, error nahi deta — woh bas slow ho jaata hai)
→ baaki tenants surakshit rehte hain
→ shared cluster fair rehta hai</pre>
     <p><b>Nayi musibat — security ki keemat aur balance:</b> yeh sab free nahi hai.
     TLS encryption CPU leti hai (zero-copy sendfile ka fayda TLS ke saath kam ho jaata
     hai — kyunki data ab encrypt karne ke liye user-space mein aana padta hai). SASL/ACLs
     operational overhead (credentials, cert rotation, ACL management). Isliye teams
     aksar balance leti hain: internal trusted network par TLS optional, par
     authentication + ACLs + quotas lagbhag hamesha zaroori shared clusters mein.
     Aur ek galti mat karna: security ko "baad mein add karenge" mat chhodna — retrofit
     karna 10× mushkil hai jab 100 clients already connect ho chuke hon.</p>`,
  ],
  stages: [
    {
      type: 'match',
      title: '🔗 Security layer aur uska sawal',
      instructions: 'Har security concern ko sahi layer/tool se jodo.',
      pairs: [
        { left: '"Network par koi data padh na le"', right: '🔒 TLS/SSL encryption',
          explain: 'In-transit encryption. Data taar par encrypted, man-in-the-middle bekaar.' },
        { left: '"Sirf jaane-pehchaane clients connect karein"', right: '🪪 SASL / mTLS (authentication)',
          explain: 'Har client apni pehchan deta hai (password/Kerberos/OAuth ya client cert). Bina iske cluster open hai.' },
        { left: '"Analytics service payment topic na padh sake"', right: '📋 ACLs (authorization)',
          explain: 'Least-privilege access control. Authenticated hone se access nahi milta — ACL alag se deta hai.' },
        { left: '"Ek team pura cluster na kha jaaye"', right: '🚦 Quotas',
          explain: 'Produce/consume rate aur request-rate limits. Noisy neighbour ko throttle karke baaki tenants ko bachata hai.' },
        { left: '"Payment topic sirf payment-service likhe"', right: '📋 ACL (WRITE on Topic:payments)',
          explain: 'Specific principal → operation → resource. WRITE permission sirf payment-service ko.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Security ke sawal',
      questions: [
        {
          q: 'Authentication aur authorization mein farak?',
          choices: [
            'Dono same hain',
            'Authentication = "tum KAUN ho?" (client apni pehchan deta hai — SASL/mTLS). Authorization = "tum KYA kar sakte ho?" (ACLs — kaun sa topic padh/likh sakte ho). Ek authenticated client jo galat topic access kare woh authentication paas karega par authorization fail — dono alag layers hain aur dono chahiye.',
            'Authorization pehle aata hai',
            'Authentication sirf encryption hai',
          ],
          answer: 1,
          explain: 'Yeh core security distinction hai. "Main jaanta hoon tum kaun ho" (auth-n) ≠ "tumhe yeh karne ki ijaazat hai" (auth-z). Ek known user ko bhi payment topic access nahi karna chahiye agar uska kaam nahi.',
        },
        {
          q: 'Ek team ka batch job pura cluster dheema kar deta hai. Kaise rokoge?',
          choices: [
            'Aur brokers add karo',
            'Quotas set karo — us principal (team/user) par produce/consume rate aur request-rate limits. Jab woh apni limit se zyada maange toh Kafka use THROTTLE karta hai (dheema karta hai, error nahi), jisse baaki tenants surakshit rehte hain. Yeh "noisy neighbour" problem ka standard multi-tenancy ilaaj hai.',
            'Us team ko alag cluster do',
            'Job ko raat mein chalane bolo',
          ],
          answer: 1,
          explain: 'Quotas shared cluster fairness ka core mechanism hain. Woh ek tenant ko throttle karte hain (block nahi), taaki woh apni limit mein rahe aur doosron ko bhookha na maare. Alag cluster dena mahenga aur aksar unnecessary hai.',
        },
        {
          q: 'ACLs set karte waqt kaunsa principle follow karoge?',
          choices: [
            'Sabko sab access do, simplicity ke liye',
            'LEAST PRIVILEGE — har service ko sirf utna access jitna uske kaam ke liye zaroori. Analytics ko orders READ chahiye par payments nahi? → payments par deny. Payment-service ko payments WRITE chahiye par orders nahi. Isse ek compromised/misconfigured service ka blast radius chhota rehta hai.',
            'Sirf admins ko access do',
            'ACLs ki zaroorat nahi agar authentication ho',
          ],
          answer: 1,
          explain: 'Least privilege security ka foundational principle hai. Har extra permission ek extra risk hai. Aur yeh dikhata hai ki authentication kaafi nahi — authenticated services ko bhi minimal authorization deni chahiye.',
        },
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Yeh security concern kis layer se hal hota hai?',
      instructions: 'Har problem ko sahi security mechanism se jodo.',
      bins: [
        { id: 'encrypt', label: '🔒 Encryption (TLS)' },
        { id: 'authn', label: '🪪 Authentication (SASL/mTLS)' },
        { id: 'authz', label: '📋 Authorization (ACLs)' },
        { id: 'quota', label: '🚦 Quotas' },
      ],
      items: [
        { text: 'Kisi ne network tap karke messages padh liye', bin: 'encrypt',
          explain: 'In-transit data plain tha. TLS encryption isse rokta hai — taar par data encrypted.' },
        { text: 'Ek unknown service cluster se connect ho gayi', bin: 'authn',
          explain: 'Koi authentication nahi tha. SASL/mTLS har client se pehchan maangta hai — anonymous connect nahi.' },
        { text: 'Analytics service galti se payment topic padh rahi thi', bin: 'authz',
          explain: 'Authenticated thi par authorized nahi honi chahiye thi. ACL se payments par uska READ deny karo.' },
        { text: 'Ek team ka job network bandwidth kha gaya', bin: 'quota',
          explain: 'Noisy neighbour. Produce-rate quota us team ko throttle karke baaki ko bachata hai.' },
        { text: 'Compromised service ko sab topics ka access mil gaya', bin: 'authz',
          explain: 'Least-privilege ACLs se blast radius chhota hota — us service ko sirf apne topics ka access hota.' },
        { text: 'Password plain-text mein network par ja raha tha', bin: 'encrypt',
          explain: 'TLS ke bina credentials bhi plain jaate hain. Encryption authentication ke saath saath zaroori hai.' },
        { text: 'Ek buggy consumer ne 90% broker CPU le liya', bin: 'quota',
          explain: 'Request-rate quota broker CPU time ko cap karti hai — ek client 20% se zyada nahi le sakta.' },
        { text: 'Naya developer galti se prod topic delete kar raha tha', bin: 'authz',
          explain: 'DELETE operation ACL se restrict hoti hai. Sirf admin principals ko topic DELETE ka access.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Ek shared multi-tenant Kafka cluster secure aur fair kaise banaoge?"',
        choices: [
          'Har team ko alag cluster do',
          'Teen security layers + quotas. (1) Encryption — TLS taaki network par data safe. (2) Authentication — SASL ya mTLS taaki sirf known clients connect karein. (3) Authorization — ACLs with LEAST PRIVILEGE taaki har service sirf apne topics access kare (analytics ko payments padhne ki zaroorat nahi → deny). (4) Quotas — produce/consume aur request-rate limits taaki ek "noisy neighbour" team pura cluster na kha jaaye (throttle, block nahi). Aur main security ko din ek se lagaunga, "baad mein" nahi — 100 clients ke baad retrofit karna 10× mushkil hai.',
          'Bas firewall laga do',
          'Authentication kaafi hai',
        ],
        answer: 1,
        explain: 'Yeh comprehensive multi-tenancy jawab hai. Achha jawab teeno security layers ko unke alag roles ke saath, plus quotas for fairness, plus "security day one" wisdom deta hai. "Alag cluster" aksar mehnga aur unnecessary hai.',
      },
      {
        q: '🧑‍💼 "Kafka mein TLS encryption ki koi performance keemat hai?"',
        choices: [
          'Nahi, bilkul free hai',
          'Haan. Kafka ki ek badi speed-trick ZERO-COPY (sendfile) hai — disk se network seedha, bina user-space copy. Par TLS ke saath data ko encrypt karne ke liye user-space mein aana padta hai, toh zero-copy ka fayda kam ho jaata hai aur CPU usage badhta hai. Isliye kuch teams internal trusted networks par TLS optional rakhti hain (par authentication + ACLs phir bhi zaroori). Yeh security vs performance ka soch-samajh kar liya gaya trade-off hai.',
          'Sirf latency badhti hai',
          'TLS Kafka mein support nahi hai',
        ],
        answer: 1,
        explain: 'Yeh gehra sawal hai jo Track 1 (zero-copy) se connect karta hai. Achha jawab dikhata hai ki tum Kafka ke internals samajhte ho — TLS zero-copy ko todta hai. Yeh senior-level detail hai jo rote learning se nahi aati.',
      },
    ],
  },
  success: [
    'Main Kafka ke teen security layers (encryption/authn/authz) alag kar sakta hoon',
    'Main authentication aur authorization ka farak samjha sakta hoon',
    'Main ACLs ko least-privilege ke saath design kar sakta hoon',
    'Main quotas se noisy-neighbour problem hal kar sakta hoon',
  ],
  homework: `<p>Security config dekho (server + client):</p>
    <pre># broker: SASL_SSL listener
listeners=SASL_SSL://:9093
security.inter.broker.protocol=SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-256

# ACL: analytics sirf orders READ kare, payments NAHI
kafka-acls.sh --bootstrap-server localhost:9093 \\
  --add --allow-principal User:analytics \\
  --operation READ --topic orders --group analytics-grp

# (payments par koi allow ACL nahi → default deny)

# QUOTA: team-A ko 10 MB/s produce tak limit karo
kafka-configs.sh --bootstrap-server localhost:9093 \\
  --alter --add-config 'producer_byte_rate=10485760' \\
  --entity-type users --entity-name team-A</pre>
    <p><b>Asli experiment (soch ke):</b> ACL lagane ke baad analytics service se payments
    topic padhne ki koshish karo — <code>TopicAuthorizationException</code> milega. Yahi
    least-privilege in action hai. Phir quota laga ke team-A se 50 MB/s bhejne ki koshish
    karo — Kafka use 10 MB/s par throttle karega (error nahi, bas dheema).</p>`,
  cards: [
    { front: 'Kafka security ke teen layers?', back: '1) <b>Encryption</b> (TLS) — network par data chhupao.<br>2) <b>Authentication</b> (SASL/mTLS) — "tum kaun ho".<br>3) <b>Authorization</b> (ACLs) — "tum kya kar sakte ho". Teeno alag, teeno zaroori.' },
    { front: 'Authentication vs authorization?', back: '<b>Authentication</b> = "tum KAUN ho?" (client pehchan deta hai — SASL/mTLS).<br><b>Authorization</b> = "tum KYA kar sakte ho?" (ACLs — kaunsa topic). Authenticated client bhi galat topic access nahi karna chahiye — dono alag.' },
    { front: 'ACL design ka principle?', back: '<b>Least privilege</b> — har service ko sirf utna access jitna zaroori. Analytics ko orders READ chahiye par payments nahi → deny. Isse compromised/misconfigured service ka blast radius chhota rehta hai.' },
    { front: 'Quotas kya rokte hain?', back: '<b>"Noisy neighbour"</b> — ek tenant pura cluster kha jaaye. Produce/consume rate + request-rate limits. Limit paar par Kafka THROTTLE karta hai (dheema, error nahi), baaki tenants safe. Shared cluster fairness ka core.' },
    { front: 'TLS ki performance keemat?', back: 'TLS Kafka ke <b>zero-copy (sendfile)</b> ko todta hai — encrypt karne ke liye data user-space mein aana padta hai, CPU badhta hai. Isliye kuch teams internal networks par TLS optional rakhti hain (par authentication + ACLs + quotas phir bhi zaroori).' },
  ],
},

]);

/* ===================== Levels 7-8: Multi-DC DR, Boss ===================== */
KQ.registerLevels('advanced', [

/* =====================================================================
   7. MULTI-DC, MIRRORMAKER & DR
   ===================================================================== */
{
  id: 'multi-dc-dr',
  icon: '🌍',
  title: 'Multi-DC & DR — region gira, offsets match nahi hue',
  badge: 'DR Rishi',
  objectives: [
    'Samjho ki ek region/cluster failure se DR kyun mushkil hai',
    'MirrorMaker 2 se cross-cluster replication seekho',
    'Offset translation ka problem aur active-active ki ordering samjho',
    'RPO aur RTO ko business terms mein define karo',
  ],
  lesson: [
    `<h3>😰 Dard: "DR region par failover kiya, consumers ne sab kuch dobara process kiya"</h3>
     <p>Ek company ka primary Kafka cluster us-east mein tha, aur unhone us-west mein ek
     DR (disaster recovery) cluster banaya — MirrorMaker se sab kuch replicate hota tha.
     "Region gire toh us-west par failover kar denge."</p>
     <p>Ek din us-east sach mein gira. Team ne consumers ko us-west par point kiya. Aur:</p>
     <pre>Consumer us-east par offset 8,45,231 tak process kar chuka tha.
Woh us-west par offset 8,45,231 se shuru hua.

PAR us-west par offset 8,45,231 ek BILKUL ALAG message tha!

Kyun? MirrorMaker messages ko replicate karta hai, par har cluster
apne offsets INDEPENDENTLY assign karta hai. us-east ka message #5
us-west par offset #3 ho sakta hai (agar replication kabhi kabhi
alag order/timing mein hui). Offsets clusters ke beech match NAHI karte.

Natija: consumers ne ya toh messages SKIP kiye (data loss) ya
DOBARA process kiye (duplicates) — dono asli production mein hua.</pre>
     <div class="war-story"><b>⚡ Offset translation ka dard.</b> DR ka sabse bada jaal
     yeh hai ki log-model mein "kahan tak process kiya" ek offset number hai, aur woh
     number clusters ke beech portable nahi hai. Failover ke waqt consumer ko yeh nahi
     pata ki naye cluster par kahan se shuru karna hai. Isi ko MirrorMaker 2 ka "offset
     translation" feature hal karta hai — par usse jaan-boojh ke setup karna padta hai.</div>`,

    `<h3>🪜 Cross-cluster replication aur DR patterns</h3>
     <p><b>MirrorMaker 2 (MM2)</b> Kafka ka standard cross-cluster replication tool hai:</p>
     <pre>us-east (primary)  ──MM2──▶  us-west (DR)
   topics replicate hote hain (prefix ke saath: us-east.orders)
   consumer group offsets bhi replicate + TRANSLATE hote hain
   → offset translation: "us-east par offset 8,45,231 =
     us-west par offset X" ka mapping MM2 maintain karta hai
   → failover par consumer sahi jagah se shuru kar sakta hai</pre>
     <p><b>Do main DR patterns:</b></p>
     <pre>🔵 ACTIVE-PASSIVE (aam DR)
   primary par sab kuch, DR sirf standby (replicate hota rehta)
   region gire → DR ko active banao (failover)
   👍 simple, koi conflict nahi
   👎 DR resources idle (paisa), failover manual/scripted

🟢 ACTIVE-ACTIVE (dono live)
   dono regions active, dono par produce/consume
   ek region gire → doosra pehle se chal raha hai (tez failover)
   👍 no idle resources, fast failover
   👎 CONFLICT: same key dono regions par → ordering kaise?
      duplicate detection cross-region? bahut complex</pre>
     <p>Active-active mein ek fundamental problem hai: agar ek user ke events dono regions
     par ja sakte hain, toh us user ki global ordering namumkin ho jaati hai (do alag
     logs, do alag offset spaces). Isliye active-active aksar "region-pinned" hota hai —
     ek user hamesha ek region par, taaki uski ordering ek jagah rahe.</p>`,

    `<h3>⚖️ RPO aur RTO — DR ki asli bhasha</h3>
     <p>DR ke faisle do numbers par tikte hain, aur yeh interview mein bahut poochhe jaate hain:</p>
     <pre>📉 RPO — Recovery Point Objective
   "kitna DATA kho sakte ho?" (time mein)
   RPO = 5 min → failover par pichle 5 min ka data kho sakta hai
   kyunki replication asynchronous hai — DR hamesha thoda peeche

   RPO = 0 chahiye? → synchronous replication (har write DR par bhi
   confirm ho tab ACK) → par yeh latency bahut badha deta hai aur
   cross-region mein aksar impractical

⏱️ RTO — Recovery Time Objective
   "kitni DER mein wapas online?" (failover ka time)
   RTO = 15 min → gire se 15 min mein DR par chalu ho jaana
   active-passive: RTO zyada (failover setup); active-active: RTO kam</pre>
     <p><b>Nayi musibat — RPO/RTO free nahi hain:</b> chhota RPO (kam data loss)
     synchronous ya frequent replication maangta hai = zyada latency + cost. Chhota RTO
     (tez recovery) active-active ya automated failover maangta hai = zyada complexity +
     cost. Yeh business decision hai: payments ko RPO ~0 chahiye (paisa khona namumkin),
     analytics RPO 1 ghanta jhel sakta hai. Aur sabse zaroori — <b>DR ko regularly TEST
     karo</b>. Ek untested DR plan sirf ek dua hai. Woh offset-translation wala incident
     isliye hua kyunki failover kabhi test hi nahi kiya gaya tha — production mein pehli
     baar chalaya, aur woh toota.</p>`,
  ],
  stages: [
    {
      type: 'quiz',
      title: '🧠 Multi-DC ke sawal',
      questions: [
        {
          q: 'DR cluster par failover kiya aur consumers ne messages skip/duplicate kiye. Kyun?',
          choices: [
            'DR cluster mein bug tha',
            'Offsets clusters ke beech match nahi karte — MirrorMaker messages replicate karta hai par har cluster apne offsets independently assign karta hai. us-east ka offset 8,45,231 us-west par ek alag message ho sakta hai. Consumer ko pata nahi ki naye cluster par kahan se shuru kare. Iska hal MirrorMaker 2 ka OFFSET TRANSLATION hai, jise jaan-boojh ke setup karna padta hai.',
            'Consumers kam the',
            'Replication dheemi thi',
          ],
          answer: 1,
          explain: 'Offset translation DR ka sabse bada jaal hai. Log-model mein progress ek offset number hai, aur woh number cross-cluster portable nahi. MM2 ka offset translation isi ko hal karta hai — par woh automatic nahi, setup karna padta hai.',
        },
        {
          q: 'RPO aur RTO mein farak?',
          choices: [
            'Dono same hain',
            'RPO (Recovery Point Objective) = "kitna DATA kho sakte ho" (time mein) — async replication ki wajah se DR thoda peeche rehta hai, toh failover par utna data kho sakta hai. RTO (Recovery Time Objective) = "kitni DER mein wapas online" — failover ka time. Chhota RPO = zyada replication (latency/cost); chhota RTO = active-active/automation (complexity/cost).',
            'RPO time hai, RTO data hai',
            'Dono sirf backups ke liye hain',
          ],
          answer: 1,
          explain: 'RPO = data loss tolerance, RTO = downtime tolerance. Dono business decisions hain jinki keemat hai. Payments: RPO ~0. Analytics: RPO 1hr OK. Interview mein yeh distinction bahut poochha jaata hai.',
        },
        {
          q: 'Active-active multi-region mein sabse badi challenge kya hai?',
          choices: [
            'Zyada paisa lagta hai',
            'Conflict aur ordering — agar ek key ke events dono regions par ja sakte hain, toh us key ki global ordering namumkin ho jaati hai (do alag logs, do offset spaces) aur cross-region duplicate detection bahut complex. Isliye active-active aksar "region-pinned" hota hai: ek user/key hamesha ek region par, taaki uski ordering ek jagah rahe.',
            'Latency badh jaati hai',
            'Consumers zyada chahiye',
          ],
          answer: 1,
          explain: 'Active-active fast failover deta hai par conflict resolution ka fundamental problem laata hai. Region-pinning (ek entity ek region) yeh solve karta hai par woh "sach mein active-active" ko thoda kam kar deta hai. Yeh distributed systems ka core trade-off hai.',
        },
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Active-Passive ya Active-Active?',
      instructions: 'Har requirement ke liye sahi DR pattern chuno.',
      bins: [
        { id: 'passive', label: '🔵 Active-Passive (simple, DR standby)' },
        { id: 'active', label: '🟢 Active-Active (dono live, tez failover)' },
      ],
      items: [
        { text: 'Simple DR, cost kam, failover kabhi-kabhi', bin: 'passive',
          explain: 'Active-passive simple hai — DR standby rehta hai, gire toh failover. Setup aasan, conflict nahi. Zyadatar teams yahi se shuru karti hain.' },
        { text: 'Global app, users har region ke paas se low latency chahiye', bin: 'active',
          explain: 'Active-active se har region local users ko serve karta hai (kam latency). Par region-pinning se ordering sambhaalni padegi.' },
        { text: 'RTO < 1 minute chahiye (lagbhag zero downtime)', bin: 'active',
          explain: 'Active-active mein doosra region pehle se chal raha hai, toh failover lagbhag instant. Active-passive mein failover setup mein minutes lagte hain.' },
        { text: 'Team chhoti, DR complexity afford nahi kar sakti', bin: 'passive',
          explain: 'Active-active bahut operational complexity laata hai (conflict, offset sync, region-pinning). Chhoti team ke liye active-passive practical hai.' },
        { text: 'Regulatory: data ek region mein hi rehna chahiye', bin: 'passive',
          explain: 'Agar data sovereignty rules hain toh active-active (jahan data dono regions par jaata hai) problematic ho sakta hai. Active-passive with controlled replication behtar.' },
        { text: 'Massive scale, idle DR resources ka waste nahi chahiye', bin: 'active',
          explain: 'Active-active mein dono regions kaam karte hain — koi idle standby nahi. Bade scale par yeh cost-efficient hai (agar complexity handle kar sako).' },
      ],
    },
    {
      type: 'order',
      title: '🔢 DR failover ka sahi kram',
      instructions: 'Ek region failure par active-passive failover ka sahi kram lagao.',
      steps: [
        { text: '🔍 Detect: primary region down hai (monitoring/health checks confirm)',
          explain: 'Pehle pakka karo ki primary sach mein down hai, ek transient glitch nahi. False failover apne aap ek outage hai.' },
        { text: '⏸️ Primary par writes rokо (agar reachable ho) — split-brain se bacho',
          explain: 'Agar primary partially reachable hai toh dono regions par writes ho sakti hain (split-brain). Primary ko fence karo.' },
        { text: '📊 DR par offset translation check karo — consumers kahan se shuru honge',
          explain: 'MM2 ka offset translation batata hai ki DR par consumers ki sahi shuruaati position kya hai. Yahi woh step tha jo us incident mein miss hua.' },
        { text: '🔀 Consumers ko DR cluster par point karo (translated offsets se)',
          explain: 'Ab consumers translated offsets se shuru hote hain — na skip, na duplicate. Yahi offset translation ka poora point hai.' },
        { text: '📤 Producers ko DR cluster par point karo',
          explain: 'Naye writes ab DR par jaate hain. DR ab effectively primary ban gaya.' },
        { text: '✅ Verify + monitor: lag, throughput, aur data integrity check karo',
          explain: 'Failover ke baad confirm karo ki sab theek chal raha hai. Aur baad mein: primary wapas aane par failback plan (jo utna hi careful hona chahiye).' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Multi-region DR setup karte waqt sabse bada gotcha kya hai?"',
        choices: [
          'Bandwidth cost',
          'OFFSET TRANSLATION. Offsets clusters ke beech match nahi karte — har cluster apne offsets independently assign karta hai, toh primary ka offset N DR par ek alag message hota hai. Failover par consumer ko pata hi nahi ki DR par kahan se shuru kare, jisse skip (data loss) ya duplicate hota hai. MirrorMaker 2 ka offset translation isse hal karta hai par use explicitly setup karna padta hai. Aur sabse zaroori — DR ko REGULARLY TEST karo, kyunki untested failover pehli baar production mein toot-ta hai. Main RPO/RTO bhi business ke saath define karunga.',
          'Latency',
          'Zyada partitions chahiye',
        ],
        answer: 1,
        explain: 'Offset translation DR ka signature gotcha hai jo log-model se aata hai. Achha jawab (1) offset mismatch ka mechanism, (2) MM2 offset translation ka hal, (3) DR testing ki zaroorat, (4) RPO/RTO — sab batata hai. Yeh Track 1 ke offset concept ka advanced payoff hai.',
      },
      {
        q: '🧑‍💼 "Tumhare CEO poochte hain: region gire toh kitna data khoyenge aur kitni der down rahenge? Kaise answer doge?"',
        choices: [
          'Bilkul data nahi khoyenge, zero downtime',
          'Yeh RPO aur RTO ka sawal hai. RPO (kitna data): humari replication asynchronous hai toh DR ~X second/minute peeche rehta hai — failover par utna data risk mein hai. RPO ~0 chahiye toh synchronous replication chahiye, jo latency bahut badha dega aur cross-region mein aksar impractical. RTO (kitni der): active-passive setup mein failover ~Y minute leta hai (detect + fence + offset-translate + repoint); active-active mein woh lagbhag instant hai par complexity zyada. Main inhe measure karunga (DR test se) aur business ke saath decide karunga ki kaunsa data-class kya RPO/RTO deserve karta hai.',
          'Kuch nahi keh sakte, depends karta hai',
          'Backup se restore kar lenge',
        ],
        answer: 1,
        explain: 'Yeh business-communication sawal hai. Achha jawab RPO/RTO ko concrete terms mein explain karta hai, unki keemat batata hai, aur DR testing se measure karne ki baat karta hai. "Zero downtime, zero loss" bolna naive hai — sab kuch ki keemat hoti hai.',
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki offsets cross-cluster kyun match nahi karte',
    'Main MirrorMaker 2 aur offset translation ka role bata sakta hoon',
    'Main active-passive aur active-active ka trade-off samjha sakta hoon',
    'Main RPO aur RTO ko business terms mein define kar sakta hoon',
  ],
  homework: `<p>MirrorMaker 2 config dekho (do clusters ke beech):</p>
    <pre># mm2.properties
clusters = primary, dr
primary.bootstrap.servers = us-east-kafka:9092
dr.bootstrap.servers = us-west-kafka:9092

# primary se dr ko replicate karo
primary->dr.enabled = true
primary->dr.topics = orders, payments, .*

# offset translation ON — DR par consumer offsets sync karo
primary->dr.sync.group.offsets.enabled = true
primary->dr.emit.checkpoints.enabled = true

# DR par topic prefix: primary.orders, primary.payments</pre>
    <p><b>Asli experiment (soch ke):</b> primary par ek consumer group offset 1000 tak
    le jao. MM2 ke offset translation ke saath dekho ki DR par uska corresponding offset
    kya hai (aksar alag number). Bina translation ke, DR par "offset 1000" se shuru karna
    galat message se shuru karna hota. Yahi woh gotcha hai jo DR ko test kiye bina pakda
    nahi jaata. RPO measure karo: primary aur DR ke beech kitna lag hai — wahi tumhara
    potential data loss window hai.</p>`,
  cards: [
    { front: 'DR mein offsets cross-cluster match kyun nahi karte?', back: 'Har cluster apne offsets <b>independently</b> assign karta hai. MirrorMaker messages replicate karta hai par primary ka offset N DR par ek ALAG message ho sakta hai. Failover par consumer ko pata nahi kahan se shuru kare → skip/duplicate.' },
    { front: 'Offset translation kya hal karta hai?', back: 'MirrorMaker 2 ka feature jo "primary offset N = DR offset X" ka mapping maintain karta hai, taaki failover par consumer DR par SAHI jagah se shuru kare (na skip, na duplicate). Explicitly setup karna padta hai (<code>sync.group.offsets.enabled</code>).' },
    { front: 'RPO vs RTO?', back: '<b>RPO</b> (Recovery Point Objective) = "kitna DATA kho sakte ho" (async replication se DR peeche rehta hai).<br><b>RTO</b> (Recovery Time Objective) = "kitni DER mein wapas online". Chhota RPO = zyada replication cost; chhota RTO = active-active complexity.' },
    { front: 'Active-passive vs active-active DR?', back: '<b>Active-passive</b>: DR standby, gire toh failover. Simple, no conflict, par RTO zyada + idle resources.<br><b>Active-active</b>: dono live, tez failover, no idle — par conflict/ordering complex (aksar region-pinning se hal).' },
    { front: 'Active-active ki sabse badi challenge?', back: '<b>Conflict aur ordering</b> — ek key ke events dono regions par gaye toh global ordering namumkin (do logs, do offset spaces). Hal: <b>region-pinning</b> — ek user/key hamesha ek region par.' },
  ],
},

/* =====================================================================
   8. BOSS: EK DIN KA PLATFORM OWNER
   ===================================================================== */
{
  id: 'boss-platform',
  icon: '🏆',
  title: 'BOSS: Ek Din Ka Platform Owner — cluster teri zimmedari',
  badge: 'Stream Guru',
  objectives: [
    'Ek poore Kafka platform ki health, cost aur reliability sambhaalo',
    'Schema break, lag SLO, EOS pipeline aur DR — sab ek jagah',
    'Platform-level trade-offs (durability/cost/complexity) balance karo',
    'Track 3 ke saare concepts ek jagah use karo',
  ],
  lesson: [
    `<h3>🏆 Final imtihaan — aaj tum platform owner ho</h3>
     <p>Tum ek company ke Kafka platform ke owner ban gaye ho — 40 teams, 800 topics,
     ek din ka shift. Aaj kai cheezein tumhari mez par aayengi, aur har ek Track 3 ka
     ek concept test karegi. Yeh incident-firefighting nahi hai (woh Track 2 tha) —
     yeh <b>platform-level decision making</b> hai.</p>
     <pre>Aaj ke faisle:
   1. Ek team ka schema change deploy block ho raha hai — kyun aur kya karein?
   2. Ek EOS pipeline dheemi hai — EOS sach mein chahiye ya at-least-once kaafi?
   3. Ek naye "ghost order" bug ki report — dual write?
   4. DR test kabhi nahi hua — RPO/RTO kya hai?
   5. Ek team pura cluster slow kar rahi hai — kya karein?</pre>
     <p>Yeh sab tumne Track 3 mein seekha. Ab ek platform owner ki tarah sochо —
     har faisle mein correctness, cost, aur complexity ka balance.</p>`,
  ],
  stages: [
    {
      type: 'quiz',
      title: '🧠 Platform owner ke faisle',
      questions: [
        {
          q: '📋 Team-A ka deploy fail ho raha hai: "schema incompatible". Unhone orders schema mein ek required field add ki. Tumhari salah?',
          choices: [
            'Compatibility check off kar do taaki deploy ho jaaye',
            'Yeh Schema Registry sahi kaam kar raha hai — required field add karna backward-breaking hai (purane consumers ko woh field nahi milegi, aur naye messages purane data se incompatible). Field ko OPTIONAL banao (default value ke saath) — tab woh compatible hai. Yeh galti ko production se pehle pakadna hi registry ka poora point hai.',
            'Naya topic bana do',
            'Team-A ko alag cluster do',
          ],
          answer: 1,
          explain: 'Platform owner ka kaam registry ko bypass karna nahi, teams ko sahi pattern sikhana hai. Required→optional (with default) yeh fix karta hai. Compatibility check off karna woh 40-consumer incident ko wapas laana hai.',
        },
        {
          q: '🎯 Ek team EOS (transactions) use kar rahi hai par pipeline dheemi hai. Woh Kafka se padh kar external DB likhte hain. Tumhari salah?',
          choices: [
            'EOS ka config tune karo',
            'EOS yahan galat tool hai — woh sirf Kafka→Kafka atomicity deta hai, par yeh log External DB likh rahe hain (jo transaction ka hissa nahi ban sakta). Woh EOS ka overhead le rahe hain bina uska fayda paaye. Salah: at-least-once + idempotent DB writes (unique constraint / conditional update) use karo — sasta, tez, aur yahan zyada correct.',
            'External DB ko Kafka se replace karo',
            'EOS hamesha behtar hai, rehne do',
          ],
          answer: 1,
          explain: 'EOS ki seema pehchanna platform owner ki key skill hai. External side-effects par EOS bekaar hai — woh sirf latency add karta hai. Idempotency sahi (aur sasta) hal hai.',
        },
        {
          q: '👻 Report: orders DB mein ban rahe hain par kabhi ship nahi ho rahe (koi Kafka event nahi). Root cause aur fix?',
          choices: [
            'Kafka down hai',
            'Dual-write problem — service DB save aur Kafka publish alag-alag kar rahi hai, beech ka crash ghost orders banata hai. Fix: Transactional Outbox (event ko usi DB transaction mein outbox table mein likho, phir relay/CDC se Kafka par bhejo) — ya Debezium CDC seedha DB log se. At-least-once hoga toh downstream idempotent chahiye.',
            'Consumers slow hain',
            'Partitions badhao',
          ],
          answer: 1,
          explain: 'Ghost orders = dual-write ka signature. Outbox/CDC iska systematic hal hai. Platform owner ko yeh pattern teams ko provide karna chahiye (shared library ya CDC infrastructure).',
        },
        {
          q: '🌍 CEO poochte hain: "region gire toh?" DR cluster hai par failover kabhi test nahi hua. Sabse bada risk?',
          choices: [
            'Bandwidth',
            'Offset translation — failover par consumers offsets mismatch se skip/duplicate kar sakte hain kyunki offsets cross-cluster match nahi karte. Untested DR = pehli baar production mein toot-ta hai. Salah: MirrorMaker 2 offset translation setup karo AUR ek DR drill chalao (game-day), phir RPO/RTO measure karke CEO ko concrete number do.',
            'DR cluster ki cost',
            'Zyada replication',
          ],
          answer: 1,
          explain: 'Untested DR ek dua hai, plan nahi. Offset translation woh gotcha hai jo sirf testing se pakda jaata hai. Platform owner ka kaam DR ko regularly test (game-day drills) karwana hai.',
        },
        {
          q: '🚦 Ek team ka backfill job pure cluster ko slow kar raha hai (30 teams affected). Turant kya?',
          choices: [
            'Us team ka job maar do',
            'Turant: us principal par produce-rate QUOTA lagao — woh throttle ho jaayega (dheema, error nahi) aur baaki 29 teams recover ho jaayengi. Yeh un-affected teams ko bachata hai bina backfill ko poori tarah roke. Long-term: shared cluster par default quotas set karo taaki yeh dobara na ho. Yeh noisy-neighbour ka standard multi-tenancy fix hai.',
            'Aur brokers add karo',
            'Cluster restart karo',
          ],
          answer: 1,
          explain: 'Quotas noisy-neighbour ka turant aur graceful fix hain — throttle, block nahi. Platform owner ko default quotas proactively set karne chahiye. Job maarna aakhri option hai (woh team ka kaam rok deta hai).',
        },
      ],
    },
    {
      type: 'terminal',
      title: '🖥️ FINAL MISSION: platform setup karo',
      instructions: 'Ek naye team ke liye poora production-ready topic set banao — EOS-ready, durable, DR-taggable.',
      goal: `<b>Naye "payments-platform" team ke liye topics banao:</b><br><br>
        1️⃣ <code>payments</code> — 12 partitions, RF=3, <code>min.insync.replicas=2</code>
        (durable, EOS-ready)<br>
        2️⃣ <code>payments.DLQ</code> — 1 partition, RF=3 (dead letters)<br>
        3️⃣ <code>payment-state</code> — 12 partitions, RF=3, <code>cleanup.policy=compact</code>
        (KTable-style current state)<br>
        4️⃣ Sab describe karke verify karo, aur payments mein kuch data daalo`,
      cluster: { brokers: 3 },
      tasks: [
        { text: 'payments (12 part, RF=3, minISR=2)',
          check: s => s.hasTopic('payments') && s.topics.payments.partitions.length >= 12 &&
                      s.topics.payments.rf === 3 && String(s.topics.payments.config['min.insync.replicas']) === '2' },
        { text: 'payments.DLQ (RF=3)',
          check: s => s.hasTopic('payments.DLQ') && s.topics['payments.DLQ'].rf === 3 },
        { text: 'payment-state (compact)',
          check: s => s.hasTopic('payment-state') && String(s.topics['payment-state'].config['cleanup.policy']).includes('compact') },
        { text: 'sab describe karo', check: s => s.history.some(h => /kafka-topics.*--describe/.test(h)) },
        { text: 'payments mein data daalo', check: s => s.hasTopic('payments') && s.partitionCounts('payments').reduce((a, c) => a + c, 0) > 0 },
      ],
      success: s => s.hasTopic('payments') && s.topics.payments.partitions.length >= 12 &&
                    s.topics.payments.rf === 3 && String(s.topics.payments.config['min.insync.replicas']) === '2' &&
                    s.hasTopic('payments.DLQ') && s.topics['payments.DLQ'].rf === 3 &&
                    s.hasTopic('payment-state') && String(s.topics['payment-state'].config['cleanup.policy']).includes('compact') &&
                    s.history.some(h => /kafka-topics.*--describe/.test(h)) &&
                    s.partitionCounts('payments').reduce((a, c) => a + c, 0) > 0,
      successText: `🏆 <b>Tumne ek poora production-ready platform set banaya!</b> Dekho har
        faisla Track 3 se aaya:<br><br>
        🛡️ <b>payments RF=3 + minISR=2</b> — durable, aur EOS transactions ke liye zaroori
        (level 1: ISR ke bina "exactly once" ka matlab nahi)<br>
        💀 <b>payments.DLQ</b> — poison messages ke liye (Track 2), 1 partition kaafi<br>
        🗜️ <b>payment-state compact</b> — har payment ka current state, KTable-style
        (level 4: stream-table duality; level: retention/compaction)<br><br>
        Aur ek platform owner ke roop mein tum yeh bhi ensure karoge: schema registry
        (level 5) is team ke topics par, quotas (level 6) taaki woh noisy-neighbour na
        banein, aur MM2 offset translation (level 7) agar DR chahiye.<br><br>
        <b>Yahi Stream Guru hai</b> — ek engineer jo sirf ek pipeline nahi, poora
        platform sambhaal sakta hai, har faisle mein correctness/cost/complexity balance
        karke. 🌊`,
      hints: [
        'kafka-topics --create --topic payments --partitions 12 --replication-factor 3 --config min.insync.replicas=2',
        'kafka-topics --create --topic payments.DLQ --partitions 1 --replication-factor 3',
        'kafka-topics --create --topic payment-state --partitions 12 --replication-factor 3 --config cleanup.policy=compact',
        'kafka-topics --describe    phir    kafka-console-producer --topic payments --count 30 --key pay-{i}',
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 FINAL: "Tum ek Kafka platform ke owner ho. Ek team ek naya event-driven service bana rahi hai. Unhe kya guidance doge?"',
        choices: [
          'Bas ek topic bana ke consume karo',
          'Main poore lifecycle par guidance dunga: (1) Topic design — sahi partition count (throughput+parallelism), RF=3 + min.insync=2 durability ke liye, aur delete vs compact policy (event history vs current state). (2) Schema — Schema Registry with BACKWARD compatibility taaki breaking changes deploy-time par ruk jaayein. (3) Reliability — DLQ for poison messages, idempotent consumers for at-least-once, retry with jitter + circuit breaker. (4) Consistency — agar DB+Kafka dono likhna hai toh transactional outbox (dual-write se bacho); EOS sirf Kafka→Kafka par. (5) Platform hygiene — quotas taaki noisy-neighbour na banein, lag monitoring/alerting, aur DR plan agar critical hai. Har faisle mein correctness/cost/complexity balance.',
          'Kafka Streams use karne bol dunga',
          'Documentation padhne bhej dunga',
        ],
        answer: 1,
        explain: 'Yeh poore course ka capstone jawab hai — Track 1 (topic design), Track 2 (DLQ, idempotency, retry, breaker), Track 3 (schema registry, outbox, EOS, quotas, DR) sab ek jagah. Ek platform owner ki tarah sochna, ek single-service developer ki tarah nahi. Yahi Stream Guru banata hai.',
      },
    ],
  },
  success: [
    'Maine platform-level faisle liye (schema, EOS, outbox, DR, quotas)',
    'Main jaanta hoon ki kaunsa tool kaunsi problem ke liye hai',
    'Main correctness, cost aur complexity ka balance le sakta hoon',
    'Maine Track 3 ke saare concepts ek jagah use kiye',
  ],
  homework: `<p>Ek "platform checklist" banao apne kisi bhi service ke liye:</p>
    <pre>Topic design:
  [ ] Partition count = peak throughput ÷ per-consumer + headroom
  [ ] RF=3, min.insync.replicas=2 (agar durable chahiye)
  [ ] cleanup.policy: delete (events) ya compact (current state)
  [ ] retention.ms + retention.bytes DONO set

Schema:
  [ ] Schema Registry registered
  [ ] Compatibility mode (BACKWARD default)

Reliability:
  [ ] DLQ topic + monitoring/alert
  [ ] Idempotent consumers (unique constraint / conditional update)
  [ ] Retry with jitter + maxRetries
  [ ] Circuit breaker for external calls

Consistency:
  [ ] Dual-write? → transactional outbox / CDC
  [ ] EOS sirf agar Kafka→Kafka aur correctness critical

Platform hygiene:
  [ ] Quotas set (noisy-neighbour se bachav)
  [ ] Lag monitoring + alert (seconds mein, derivative par)
  [ ] DR plan + RPO/RTO defined + TESTED</pre>
    <p><b>Asli kaam:</b> yeh checklist apne kisi asli (ya kaalpanik) service par chalao.
    Har item par socho ki tumne kyun woh choice ki. Yeh checklist tumhare poore Kafka
    Gurukul safar ka nichod hai — Sandesh Shishya se Stream Guru tak.</p>`,
  cards: [
    { front: 'Platform owner: schema deploy block ho raha hai (required field add). Fix?', back: 'Registry sahi kaam kar raha hai — required field add backward-breaking hai. Field ko <b>OPTIONAL + default</b> banao. Compatibility check off karna woh 40-consumer incident wapas laana hai.' },
    { front: 'EOS pipeline dheemi hai par woh external DB likhte hain. Salah?', back: 'EOS galat tool — woh sirf Kafka→Kafka atomicity deta hai. External DB writes transaction ka hissa nahi. <b>at-least-once + idempotent DB writes</b> use karo — sasta, tez, yahan zyada correct.' },
    { front: 'Ghost orders (DB mein hain, Kafka event nahi). Root cause?', back: '<b>Dual-write</b> — DB save aur Kafka publish alag, beech ka crash. Fix: <b>Transactional Outbox</b> (event usi DB transaction mein) ya <b>CDC</b> (Debezium). Downstream idempotent chahiye.' },
    { front: 'Untested DR ka sabse bada risk?', back: '<b>Offset translation</b> — failover par offsets mismatch se skip/duplicate. Untested DR pehli baar production mein toot-ta hai. Fix: MM2 offset translation + DR game-day drill + RPO/RTO measure.' },
    { front: 'Noisy-neighbour team pura cluster slow kar rahi hai. Turant fix?', back: 'Us principal par <b>produce-rate quota</b> — woh throttle (dheema, error nahi), baaki teams recover. Long-term: default quotas proactively set karo. Job maarna aakhri option.' },
  ],
},

]);

/* =====================================================================
   Kafka Gurukul — Track 3 ke naye levels (append-only)
   9. KRaft & Cluster Ops      10. Kafka Connect
   ===================================================================== */

KQ.registerLevels('advanced', [

/* =====================================================================
   9. KRAFT & CLUSTER OPERATIONS
   ===================================================================== */
{
  id: 'kraft-ops',
  icon: '🗳️',
  title: 'KRaft & Cluster Ops — "topic ban hi nahi raha, par traffic theek hai"',
  badge: 'Quorum Rakshak',
  objectives: [
    'Samjho ki KRaft ne ZooKeeper ki jagah kyun li aur usse kya badla',
    'Controller quorum ka math jaano — 3 voters, 5 voters, kitne failures jhel sakte ho',
    'METADATA plane aur DATA plane ka farak pehchano — 3AM par yehi bachata hai',
    'Naye brokers add karke partition reassignment khud chalao',
  ],
  lesson: [
    `<h3>😰 Dard: "Deploy atak gaya, par traffic bilkul theek chal raha tha"</h3>
     <p>Ek team ne apne 12-broker cluster mein 3 naye brokers add kiye. CI pipeline
     ne naya topic banane ki koshish ki — aur atak gaya:</p>
     <pre>$ kafka-topics --create --topic payments-v2 --partitions 12
Error: org.apache.kafka.common.errors.TimeoutException:
       Call(callName=createTopics) timed out after 60000 ms

$ kafka-topics --list
(chalta hai, list aa jaati hai)

Dashboard: throughput 84,000 msg/s ✅
           consumer lag 0 ✅
           error rate 0 ✅</pre>
     <p>On-call engineer confuse: "cluster down hai ya nahi?" Dashboards keh rahe the
     sab theek hai. Par koi bhi admin operation — topic create, config change,
     partition add — 60 second baad timeout ho raha tha.</p>
     <div class="war-story"><b>⚡ Asli wajah:</b> 3 controllers mein se 2 ek hi rack par
     the, aur us rack ka switch mar gaya. Quorum <b>1/3</b> par aa gaya — majority 2 chahiye thi.
     KRaft ne metadata writes rok di. Par brokers ke paas metadata ki apni copy thi,
     toh <b>produce/consume bilkul chalta raha</b>. Cluster "aadha zinda" tha, aur
     yahi confusion 40 minute kha gaya.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Brokers restart kar do"</b><br>
     Sabse mehnga faisla. Brokers restart karoge toh unhe metadata dobara chahiye —
     aur metadata dene wala controller quorum toh hai hi nahi. Ab jo cheez chal rahi thi
     (data plane) woh bhi mar jaayegi. <b>Aadha zinda cluster ko poora mat maar do.</b></p>

     <p><b>⚠️ Better: "ZooKeeper wapas le aao"</b><br>
     Ho hi nahi sakta — Kafka 4.0 se ZooKeeper support <b>hata diya gaya hai</b>.
     Aur ZK ke saath bhi yahi problem thi: ZK ka bhi apna quorum tha. Farak sirf itna hai
     ki ab woh quorum Kafka ke andar hai, bahar nahi.</p>

     <p><b>✅ Real: Quorum ko pehchano, phir usse theek karo</b></p>
     <pre>$ kafka-metadata-quorum --describe --status
LeaderId:        -1              ← koi leader nahi = quorum toota
CurrentVoters:   [1,2,3]
LiveVoters:      [3]             ← 1/3, majority 2 chahiye</pre>
     <p>Jawab: mare hue controllers wapas lao. Ek bhi aa gaya toh 2/3 = majority = quorum
     wapas, aur admin commands turant chalne lagengi.</p>
     <p>Aur permanent fix: controllers ko <b>alag-alag racks/AZs</b> mein rakho.
     3 voters ek hi rack par rakhna quorum ko us rack ka bandhak bana dena hai.</p>`,

    `<h3>⚖️ Nayi musibat: naye brokers khud kaam nahi bantate</h3>
     <p>Quorum theek ho gaya, 3 naye brokers cluster mein aa gaye. Team ne socha kaam
     khatam. Ek hafte baad purane 12 brokers ka disk 90% par tha aur naye 3 khaali baithe the.</p>
     <p><b>Kafka naye brokers par partitions apne aap nahi le jaata.</b> Naya broker sirf
     <i>naye</i> topics ke liye candidate banta hai. Purani partitions wahin rehti hain
     jahan thi — hamesha ke liye.</p>
     <pre>Manually reassign karna padta hai:
  kafka-reassign-partitions --topic orders --broker-list 1..15 --execute
  kafka-reassign-partitions --topic orders --verify</pre>
     <p>Aur yeh sasta nahi hai — partition move karna matlab uska <b>poora log network par
     copy</b> karna. 500GB partition = 500GB traffic, jo tumhare asli traffic se hi
     bandwidth cheenta hai. Isiliye hamesha <code>--throttle</code> ke saath, aur
     hamesha off-peak. Yahi wajah hai ki log Cruise Control jaise tools lagate hain.</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: quorum todo aur dekho kya kya rukta hai',
      instructions: 'Yeh wahi 3AM wala confusion hai. Khud karke dekho ki kaunsi cheez rukti hai aur kaunsi chalti rehti hai.',
      goal: `Cluster mein 3 controllers hain (voters 9091, 9092, 9093).<br><br>
        1. <code>kafka-metadata-quorum --describe --status</code> — abhi ka haal dekho<br>
        2. <b>Ek</b> controller maaro — quorum abhi bhi zinda hona chahiye<br>
        3. <b>Doosra</b> bhi maaro — ab quorum TOOT jaayega<br>
        4. Topic banane ki koshish karo → fail hoga<br>
        5. Par <code>orders</code> par produce karke dekho → <b>chalega</b><br>
        6. Ek controller wapas start karo — quorum wapas`,
      cluster: {
        brokers: 3,
        controllers: 3,
        topics: { orders: { partitions: 3, rf: 3 } },
      },
      tasks: [
        { text: 'quorum ka status dekho', check: s => s.history.some(h => /kafka-metadata(-quorum)?/.test(h)) },
        { text: '2 controllers maar ke quorum todo', check: s => s.controllers.filter(c => !c.alive).length >= 2 },
        { text: 'topic create karke dekho — TimeoutException aana chahiye',
          check: s => s.history.some(h => /kafka-topics.*--create/.test(h)) && s.controllers.filter(c => !c.alive).length >= 2 },
        { text: 'quorum toota hone par bhi produce karke dikhao',
          check: s => s.partitionCounts('orders').reduce((a, c) => a + c, 0) > 0 },
        { text: 'controller wapas start karke quorum bahaal karo', check: s => s.hasQuorum() && s.controllers.some(c => !c.alive) === false || s.hasQuorum() },
      ],
      success: s => s.controllers.filter(c => !c.alive).length <= 1 && s.hasQuorum() &&
        s.partitionCounts('orders').reduce((a, c) => a + c, 0) > 0 &&
        s.history.some(h => /sim controller kill/.test(h)),
      successText: `Bas yahi ek cheez yaad rakh lo toh 3AM par 40 minute bach jaayenge:<br><br>
        <b>Quorum toota = METADATA plane band, DATA plane chalu.</b><br><br>
        Topic nahi ban raha par traffic theek hai? Sabse pehle
        <code>kafka-metadata-quorum --describe --status</code> chalao.
        Brokers ko <b>bilkul mat</b> chhedo — woh apni aakhri maloom metadata se kaam
        chala rahe hain, aur restart karte hi woh bhi chali jaayegi.`,
      hints: [
        'kafka-metadata-quorum --describe --status',
        'sim controller kill 9091   phir   sim controller kill 9092',
        'Ab topic banane ki koshish karo (fail hona hi chahiye): kafka-topics --create --topic test --partitions 1 --replication-factor 1',
        'Par yeh phir bhi chalega — data plane zinda hai: kafka-console-producer --topic orders --count 20 --key user-1',
        'sim controller start 9091   — quorum wapas 2/3',
      ],
    },

    {
      type: 'bins',
      title: 'Metadata plane ya Data plane?',
      instructions: 'Quorum toot gaya hai. Har operation ko sahi jagah rakho — kya rukega, kya chalega?',
      bins: [
        { id: 'stops', label: '🛑 Ruk jaayega (metadata plane)' },
        { id: 'works', label: '✅ Chalta rahega (data plane)' },
      ],
      items: [
        { text: 'kafka-topics --create --topic new-orders', bin: 'stops', explain: 'Topic banana metadata log par likhna hai — quorum ke bina TimeoutException.' },
        { text: 'Producer orders topic par messages bhej raha hai', bin: 'works', explain: 'Brokers ke paas partition leadership ki apni copy hai. Produce chalta rahega.' },
        { text: 'Consumer group offsets commit kar raha hai', bin: 'works', explain: 'Offsets __consumer_offsets topic mein jaate hain — woh normal data hai, metadata nahi.' },
        { text: 'kafka-configs --alter se retention.ms badalna', bin: 'stops', explain: 'Config change metadata hai. Quorum ke bina nahi hoga.' },
        { text: 'kafka-topics --describe se topic dekhna', bin: 'works', explain: 'Yeh sirf PADHNA hai — broker apni cached metadata se jawab de deta hai. Likhna ruka hai, padhna nahi.' },
        { text: 'Partition reassignment shuru karna', bin: 'stops', explain: 'Replica assignment metadata hai. Quorum chahiye.' },
        { text: 'Consumer group rebalance (naya consumer join)', bin: 'works', explain: 'Group coordination broker par hota hai, controller par nahi. Rebalance chalta rahega.' },
        { text: 'Naya ACL add karna', bin: 'stops', explain: 'KRaft mein ACLs bhi metadata log mein rehte hain — quorum ke bina nahi.' },
        { text: 'Ek broker ka mar jaana aur leader election hona', bin: 'stops', explain: 'Chalaaki wala sawaal! Leader election controller karta hai — quorum ke bina naya leader chun hi nahi sakta. Us partition ka traffic ruk jaayega. Isiliye quorum lost + broker failure ek saath = asli outage.' },
      ],
    },

    {
      type: 'quiz',
      title: 'Quorum ka math',
      questions: [
        {
          q: '5 controllers wale KRaft cluster mein kitne controller failures jhel sakte ho?',
          choices: ['5', '4', '2', '3'],
          answer: 2,
          explain: 'Majority = floor(5/2) + 1 = <b>3</b>. Toh 3 zinda chahiye, yaani <b>2</b> mar sakte hain. Formula yaad rakho: n voters, majority floor(n/2)+1, tolerance floor((n-1)/2). 3 voters → 1 tolerate, 5 → 2, 7 → 3.',
        },
        {
          q: 'Even number ke voters (jaise 4) kyun bekaar hain?',
          choices: [
            'Kyunki Kafka even number allow nahi karta',
            '4 voters ki majority 3 hai — yaani 1 hi failure jhel sakte ho, bilkul 3 voters jitna. Ek extra machine ka kharcha bina kisi extra tolerance ke',
            'Kyunki even number mein tie ho jaata hai aur cluster hang ho jaata hai',
            'Kyunki leader election nahi ho paata',
          ],
          answer: 1,
          explain: '3 voters: majority 2, tolerance 1. 4 voters: majority 3, tolerance 1. <b>Wahi tolerance, ek extra machine.</b> Isiliye quorum hamesha odd rakhte hain — 3, 5, ya 7. (7 se aage jaana bhi bekaar hai: har metadata write ko 4 machines par replicate karna latency badhata hai.)',
        },
        {
          q: 'KRaft mein controller failover ZooKeeper se tez kyun hai?',
          choices: [
            'Kyunki KRaft kam metadata rakhta hai',
            'Kyunki controllers zyada powerful machines par chalte hain',
            'Kyunki metadata khud ek replicated LOG hai — har controller ke paas already poori copy hai, toh naye leader ko sirf chunna padta hai, ZK se sab kuch dobara padhna nahi padta',
            'Kyunki KRaft mein leader election hoti hi nahi',
          ],
          answer: 2,
          explain: 'ZK ke daur mein controller failover par naye controller ko poora cluster metadata ZooKeeper se padhna padta tha — bade clusters par isme <b>minute</b> lagte the aur us dauran koi leader election nahi hoti thi. KRaft mein metadata ek Raft log hai jo saare controllers already follow kar rahe hain. Failover = naya leader chuno, bas. Seconds.',
        },
        {
          q: 'Naye brokers cluster mein add kiye. Purane topics ki partitions ka kya hota hai?',
          choices: [
            'Kuch nahi — woh wahin rehti hain jahan thi. Naye brokers sirf NAYE topics/partitions ke liye candidate bante hain',
            'Kafka apne aap sab rebalance kar deta hai',
            'Purane brokers automatically drain ho jaate hain',
            'Partitions round-robin dobara bant jaati hain',
          ],
          answer: 0,
          explain: 'Yeh sabse mehngi galatfehmi hai. Kafka <b>kabhi</b> apne aap partitions move nahi karta. Naya broker add karke bhool jaoge toh woh hafton khaali baitha rahega jabki purane brokers ka disk bharta rahega. Reassignment <b>manual</b> hai (ya Cruise Control jaise tool se).',
        },
        {
          q: 'Partition reassignment mehngi kyun hai?',
          choices: [
            'Kyunki usme cluster restart hota hai',
            'Kyunki us dauran produce band ho jaata hai',
            'Kyunki partition ka POORA log naye broker par network se copy hota hai — 500GB partition = 500GB traffic, jo asli traffic se bandwidth cheenta hai',
            'Kyunki consumer offsets reset ho jaate hain',
          ],
          answer: 2,
          explain: 'Reassignment ke dauran produce/consume chalta rehta hai — par background mein log copy ho raha hota hai. Bina <code>--throttle</code> ke yeh replication traffic saara network kha leta hai, ISR shrink hone lagti hai, aur tumne apne aap ko ek incident de diya. Hamesha throttle + off-peak.',
        },
      ],
    },

    {
      type: 'terminal',
      title: '🖥️ Mission: naye brokers ko kaam par lagao',
      instructions: 'Cluster mein 5 brokers hain par orders topic sirf pehle 3 par baitha hai. Usse phailao.',
      goal: `<code>orders</code> topic ke replicas sirf brokers 1,2,3 par hain —
        brokers 4 aur 5 khaali baithe hain.<br><br>
        1. <code>kafka-topics --describe --topic orders</code> se abhi ka assignment dekho<br>
        2. Reassignment <b>execute</b> karo taaki saare 5 brokers use hon<br>
        3. Phir <b>verify</b> karke usse poora karo<br>
        4. Dobara describe karke confirm karo ki 4 aur 5 ab replicas mein hain`,
      cluster: {
        brokers: 5,
        controllers: 3,
        topics: {
          orders: {
            partitions: 6, rf: 3,
            messages: Array.from({ length: 300 }, (_, i) => ({ key: 'order-' + i, value: 'payload' })),
          },
        },
      },
      tasks: [
        { text: 'abhi ka replica assignment dekho', check: s => s.history.some(h => /kafka-topics.*--describe/.test(h)) },
        { text: 'reassignment execute karo (broker 4 aur 5 shaamil karo)',
          check: s => s.history.some(h => /kafka-reassign-partitions.*--execute/.test(h)) },
        { text: 'reassignment verify karke poora karo',
          check: s => s.history.some(h => /kafka-reassign-partitions.*--verify/.test(h)) },
        { text: 'broker 4 aur 5 dono ab replicas mein hain',
          check: s => {
            const all = s.topics.orders ? s.topics.orders.partitions.flatMap(p => p.replicas) : [];
            return all.includes(4) && all.includes(5);
          } },
      ],
      success: s => {
        const all = s.topics.orders ? s.topics.orders.partitions.flatMap(p => p.replicas) : [];
        return all.includes(4) && all.includes(5) &&
          s.history.some(h => /kafka-reassign-partitions.*--verify/.test(h));
      },
      successText: `Ho gaya. Dhyaan do ki output ne bataya <b>kitne records network par copy hue</b> —
        yahi cheez production mein ghante leti hai.<br><br>
        Asli duniya mein tum yeh chalate:<br>
        <code>--throttle 50000000</code> (50 MB/s cap) taaki replication tumhare
        asli traffic ko na khaye, aur reassignment ke baad throttle hataana
        <b>mat bhoolna</b> — warna normal replication bhi capped reh jaayegi
        (yeh ek classic production galti hai).`,
      hints: [
        'kafka-topics --describe --topic orders',
        'kafka-reassign-partitions --topic orders --broker-list 1,2,3,4,5 --execute',
        'kafka-reassign-partitions --topic orders --verify',
      ],
    },

    {
      type: 'incident',
      title: '🚨 3AM: "cluster down hai ya nahi?"',
      pager: `<b>PAGER 03:22</b> — Deploy pipeline fail: <code>createTopics timed out after 60000ms</code>.
        Par Grafana par throughput 84k msg/s, consumer lag 0, error rate 0. Kuch samajh nahi aa raha.`,
      start: 'n1',
      nodes: {
        n1: {
          sitrep: `$ kafka-topics --create --topic payments-v2 --partitions 12 --replication-factor 3
Error: org.apache.kafka.common.errors.TimeoutException:
       Call(callName=createTopics) timed out after 60000 ms

$ kafka-topics --list
orders
payments
inventory
(list theek chal rahi hai)

Grafana: throughput 84,000 msg/s ✅   lag 0 ✅   errors 0 ✅`,
          text: 'Admin command timeout, par traffic bilkul theek. Pehla kadam?',
          choices: [
            { t: 'Controller quorum ka status check karo', good: true, next: 'n2',
              explain: 'Bilkul sahi. "Metadata writes fail, data plane theek" ka matlab lagbhag hamesha controller quorum hota hai. Yeh ek hi command 30 second mein jawab de deti hai.' },
            { t: 'Saare brokers rolling restart kar do', good: false,
              explain: '☠️ Sabse khatarnak faisla. Brokers restart par metadata controller se maangte hain — aur controller quorum hai hi nahi. Jo aadha cluster chal raha tha, woh bhi mar jaayega. Ek aadhi outage ko poori outage bana dena.' },
            { t: 'Timeout 60s se 300s kar do aur retry karo', good: false,
              explain: 'Quorum nahi hai — 300 second mein bhi nahi banega. Tum sirf failure ko dheema kar rahe ho, aur is beech asli wajah nahi dhoondh rahe.' },
            { t: 'Grafana theek dikha raha hai, ticket subah dekhenge', good: false,
              explain: 'Khatarnak. Abhi cluster ek hi failure door hai poori outage se — agar koi broker mar gaya toh leader election bhi nahi ho paayegi (woh bhi controller ka kaam hai). Aadha zinda cluster raat bhar chhodna jua hai.' },
          ],
        },
        n2: {
          sitrep: `$ kafka-metadata-quorum --describe --status
ClusterId:              prod-cluster-1
LeaderId:               -1              ← koi leader nahi
LeaderEpoch:            47
CurrentVoters:          [1,2,3]
LiveVoters:             [3]             ← sirf ek zinda
QuorumMajority:         2 of 3

$ ping controller-1  →  100% packet loss
$ ping controller-2  →  100% packet loss
$ ping controller-3  →  ok

(infra team: rack B ka top-of-rack switch 03:18 par mar gaya)`,
          text: 'Quorum 1/3 par hai, majority 2 chahiye. Controllers 1 aur 2 rack B par the. Ab?',
          choices: [
            { t: 'Infra ko escalate karo aur EK controller wapas laao — majority ke liye 2 kaafi hai', good: true, next: 'n3',
              explain: 'Sahi aur sabse tez raasta. Tumhe teenon nahi chahiye — sirf ek aur. 2/3 = majority = quorum wapas, aur admin commands turant chalne lagengi.' },
            { t: 'Quorum ko 1 voter par reconfigure kar do', good: false,
              explain: 'Yeh unclean/forced recovery hai — bahut khatarnak. Agar purane controllers wapas aa gaye toh split-brain metadata ban sakta hai. Yeh sirf tab hota hai jab machines HAMESHA ke liye jaa chuki hon, aur documented disaster procedure ke saath.' },
            { t: 'Do naye controllers khade karo', good: false,
              explain: 'Naye controllers ko voter set mein add karna khud ek metadata operation hai — jo quorum ke bina hoga hi nahi. Chicken-and-egg. Purane wapas laana hi raasta hai.' },
            { t: 'Traffic doosre region par bhej do', good: false,
              explain: 'Traffic toh theek chal raha hai! Tum ek kaam karti cheez ko hila rahe ho aur jo tooti hai usse nahi chhu rahe. Failover ka apna risk hai, bina kisi faayde ke.' },
          ],
        },
        n3: {
          sitrep: `03:41 — infra ne rack B ka switch bahaal kiya, controller-2 wapas aaya

$ kafka-metadata-quorum --describe --status
LeaderId:               2
LiveVoters:             [2,3]
QuorumMajority:         2 of 3
✅ Quorum healthy

$ kafka-topics --create --topic payments-v2 --partitions 12 --replication-factor 3
Created topic payments-v2.       ← turant chal gaya`,
          text: 'Quorum wapas, deploy nikal gaya. Incident khatam — par kya likhoge follow-up mein?',
          choices: [
            { t: 'Controllers ko alag racks/AZs mein phailao, aur "metadata write failure" par alag alert banao', good: true, next: 'end',
              explain: 'Dono asli fixes. 3 voters ek rack par rakhna quorum ko us rack ka bandhak banata hai. Aur throughput dashboards ne is outage ko bilkul nahi pakda — metadata plane ke liye alag signal chahiye.' },
            { t: 'Controllers ki sankhya 3 se 7 kar do', good: false,
              explain: 'Voters badhane se kuch nahi milega agar woh phir bhi ek hi rack par hain — wahi switch sabko le jaayega. Aur zyada voters matlab har metadata write ko zyada machines par replicate karna = dheemi admin operations. Placement theek karo, ginti nahi.' },
            { t: 'Kuch nahi — problem infra ki thi, hamari nahi', good: false,
              explain: 'Switch fail hona toh hoga hi. Sawaal yeh hai ki ek switch tumhara poora metadata plane kyun le gaya. Woh design choice tumhari thi.' },
            { t: 'Alert ko "throughput drop" par bana do', good: false,
              explain: 'Throughput gira hi nahi tha — poore incident mein 84k msg/s chal raha tha. Yeh alert kabhi nahi bajta. Yahi is incident ka sabak hai: data plane theek dikhta hai jabki metadata plane mar chuka hota hai.' },
          ],
        },
        end: {
          end: true,
          text: '✅ 19 minute mein resolve. Ek bhi message nahi khoya, ek bhi consumer affect nahi hua.',
          summary: `<b>Kya seekha:</b><br><br>
            1️⃣ <b>Metadata plane aur data plane alag hain.</b> Quorum toota = admin commands band,
            par produce/consume chalu. "Traffic theek hai" ka matlab "cluster theek hai" nahi hota.<br><br>
            2️⃣ <b>Quorum lost par brokers ko mat chhedo.</b> Woh apni cached metadata se chal rahe hain.
            Restart karoge toh woh bhi chali jaayegi — aadhi outage poori ban jaayegi.<br><br>
            3️⃣ <b>Sirf majority chahiye, sab nahi.</b> 3 voters mein se 2 = quorum. Ek controller
            wapas laana kaafi tha.<br><br>
            4️⃣ <b>Controllers ko alag failure domains mein rakho.</b> 3 voters ek rack par =
            ek switch tumhara poora metadata plane le jaayega.<br><br>
            5️⃣ <b>Metadata plane ka apna alert chahiye.</b> Throughput dashboards ne is 19-minute
            outage ko bilkul nahi dekha. <code>LeaderId == -1</code> par page karo.`,
        },
      },
    },

    {
      type: 'order',
      title: 'Naye brokers add karne ka sahi kram',
      instructions: 'Cluster mein capacity badhani hai. Steps sahi kram mein lagao.',
      steps: [
        { text: 'Naye brokers ko unique node.id ke saath start karo, controllers ke same cluster.id par',
          explain: 'Broker cluster mein tabhi aata hai jab uska cluster.id match kare. Galat id = woh join hi nahi karega.' },
        { text: 'Confirm karo ki woh cluster mein dikh rahe hain (kafka-metadata / describe se)',
          explain: 'Pehle yeh pakka karo ki broker sach mein register hua hai — warna reassignment plan usse use hi nahi karega.' },
        { text: 'Reassignment plan banao aur --execute karo, --throttle ke saath',
          explain: 'Throttle ke bina replication traffic saara network kha lega, ISR shrink hogi, aur tumne khud ko ek incident de diya.' },
        { text: '--verify se progress dekho jab tak sab complete na ho jaye',
          explain: 'Bade partitions ghante lete hain. Verify tak karte raho — beech mein chhod dene se cluster aadhe-adhoore assignment par baith jaata hai.' },
        { text: 'Throttle HATAO',
          explain: 'Sabse zyada bhoola jaane wala step. Throttle chhod doge toh normal replication bhi capped rahegi, aur mahine baad koi ISR lag debug kar raha hoga.' },
        { text: 'Disk usage aur leader distribution dono verify karo',
          explain: 'Replicas barabar hone ka matlab LEADERS barabar hona nahi hai. Leader skew se kuch brokers par traffic zyada rehta hai — preferred leader election chalani pad sakti hai.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "KRaft mein controller quorum toot jaye toh cluster ka kya hota hai?"',
        choices: [
          'Poora cluster down ho jaata hai',
          'Kuch nahi hota, Kafka ko controller chahiye hi nahi',
          'METADATA plane rukh jaata hai — topic create/delete, config change, partition reassignment, ACL, aur leader election sab fail. Par DATA plane chalta rehta hai: brokers apni cached metadata se produce/consume serve karte rehte hain aur offsets bhi commit hote hain. Isliye dashboards "sab theek" dikhate hain jabki har admin command timeout ho rahi hoti hai. Khatra yeh hai ki ab koi broker mar gaya toh naya leader chun hi nahi sakta — toh ek aur failure poori outage bana degi.',
          'Sirf naye consumers join nahi kar paate',
        ],
        answer: 2,
        explain: 'Yeh senior-level jawab hai. Woh (1) metadata vs data plane ka farak, (2) exactly kya rukta hai, (3) monitoring ka blind spot, aur (4) chhupa hua risk — sab batata hai. Sirf "cluster down ho jaata hai" bolna galat hai aur 3AM par mehnga padta hai.',
      },
      {
        q: '🧑‍💼 "Tumhare paas 3 controllers hain. 5 kar dein toh kya faayda?"',
        choices: [
          'Tolerance 1 se 2 ho jaayegi — 5 voters ki majority 3 hai. Faayda: ek saath do controller failures (ya ek poora AZ) jhel loge. Keemat: har metadata write ab 3 machines par replicate hoga, toh admin operations thodi dheemi. Main 5 tab chunta hoon jab 3 AZs mein phailana ho ya cluster bahut bada ho; chhote cluster par 3 hi theek hai. Aur 4 kabhi nahi — uski tolerance bhi 1 hi hai, bas ek machine ka kharcha zyada.',
          '5 se cluster tez ho jaayega',
          'Koi farak nahi padta',
          '5 se zyada partitions bana sakte ho',
        ],
        answer: 0,
        explain: 'Achha jawab formula (majority = floor(n/2)+1), trade-off (latency), aur even-number wali trap — teenon cover karta hai. Bonus: yeh bolna ki controllers ko alag failure domains mein rakhna ginti se zyada zaroori hai.',
      },
      {
        terminal: {
          title: '🖥️ Interview: cluster expand karke dikhao',
          goal: `🧑‍💼 Interviewer: "5 brokers hain. Ek <code>events</code> topic banao —
            8 partitions, RF=3. Usme thoda data daalo. Phir mujhe dikhao ki tum
            uske replicas ko <b>saare 5 brokers</b> par kaise phailaoge."`,
          cluster: { brokers: 5, controllers: 3 },
          tasks: [
            { text: 'events topic banao (8 partitions, RF=3)',
              check: s => s.hasTopic('events') && s.topics.events.partitions.length >= 8 && s.topics.events.rf === 3 },
            { text: 'usme data daalo',
              check: s => s.hasTopic('events') && s.partitionCounts('events').reduce((a, c) => a + c, 0) > 0 },
            { text: 'reassignment execute + verify karo',
              check: s => s.history.some(h => /kafka-reassign-partitions.*--execute/.test(h)) &&
                          s.history.some(h => /kafka-reassign-partitions.*--verify/.test(h)) },
            { text: 'saare 5 brokers replicas mein aa gaye',
              check: s => {
                if (!s.topics.events) return false;
                const all = new Set(s.topics.events.partitions.flatMap(p => p.replicas));
                return [1, 2, 3, 4, 5].every(id => all.has(id));
              } },
          ],
          success: s => {
            if (!s.topics.events) return false;
            const all = new Set(s.topics.events.partitions.flatMap(p => p.replicas));
            return [1, 2, 3, 4, 5].every(id => all.has(id)) &&
              s.partitionCounts('events').reduce((a, c) => a + c, 0) > 0 &&
              s.history.some(h => /--verify/.test(h));
          },
          successText: `Sahi. Interview mein yeh zaroor bolna:<br><br>
            • <b>--throttle</b> ke bina kabhi mat chalana — replication traffic asli traffic kha jaata hai<br>
            • baad mein <b>throttle hataana</b> mat bhoolna<br>
            • replicas barabar hone ka matlab <b>leaders</b> barabar hona nahi — preferred leader election alag se dekhni padti hai<br>
            • bade clusters par yeh kaam <b>Cruise Control</b> jaise tool par chhod dete hain`,
          hints: [
            'kafka-topics --create --topic events --partitions 8 --replication-factor 3',
            'kafka-console-producer --topic events --count 100 --key evt-{i}',
            'kafka-reassign-partitions --topic events --broker-list 1,2,3,4,5 --execute',
            'kafka-reassign-partitions --topic events --verify',
          ],
        },
      },
    ],
  },
  success: [
    'Main bata sakta hoon ki KRaft ne ZooKeeper ki jagah kyun li',
    'Main quorum ka math (majority, tolerance, odd voters) samjha sakta hoon',
    'Main metadata plane aur data plane ka farak pehchan sakta hoon',
    'Main partition reassignment chala sakta hoon aur uski keemat jaanta hoon',
  ],
  homework: `<p>Apne cluster par yeh teen commands chalao aur output samjho:</p>
    <pre># 1. Quorum ka haal — yeh command yaad kar lo, 3AM par kaam aayegi
kafka-metadata-quorum.sh --bootstrap-server localhost:9092 \\
    --describe --status

# 2. Har voter kitna peeche hai
kafka-metadata-quorum.sh --bootstrap-server localhost:9092 \\
    --describe --replication

# 3. Reassignment — hamesha throttle ke saath
cat > plan.json &lt;&lt;'EOF'
{"topics":[{"topic":"orders"}],"version":1}
EOF

kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \\
    --topics-to-move-json-file plan.json \\
    --broker-list "1,2,3,4,5" --generate     # plan banao

kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \\
    --reassignment-json-file new.json --execute --throttle 50000000

kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \\
    --reassignment-json-file new.json --verify   # throttle yahi hataata hai</pre>
    <p><b>Alert banao:</b> <code>LeaderId == -1</code> ya
    <code>kafka.controller:type=KafkaController,name=ActiveControllerCount != 1</code>
    par page karo. Yeh woh signal hai jo throughput dashboards kabhi nahi denge.</p>`,
  cards: [
    { front: 'KRaft ne ZooKeeper se kya behtar kiya?', back: 'Metadata khud ek <b>Raft log</b> ban gaya jise saare controllers follow karte hain. Controller failover ab sirf naya leader chunna hai (seconds) — pehle poora metadata ZK se dobara padhna padta tha (minutes). Aur ek alag system ka ops bhi khatam.' },
    { front: 'Quorum ka formula?', back: 'Majority = <code>floor(n/2) + 1</code>. Tolerance = <code>floor((n-1)/2)</code>.<br>3 voters → 1 failure. 5 → 2. 7 → 3.<br><b>Even number bekaar hai</b>: 4 voters ki tolerance bhi 1 hi hai.' },
    { front: 'Quorum toota — kya rukta hai, kya chalta hai?', back: '<b>Rukta hai (metadata):</b> topic create/delete, config change, reassignment, ACL, <b>leader election</b>.<br><b>Chalta hai (data):</b> produce, consume, offset commit, rebalance — brokers cached metadata se.' },
    { front: 'Quorum lost par sabse badi galti?', back: '<b>Brokers restart karna.</b> Woh cached metadata se chal rahe hain; restart par woh controller se metadata maangenge jo hai hi nahi. Aadhi outage poori ban jaayegi.' },
    { front: 'Naye brokers add kiye — partitions ka kya?', back: 'Kuch nahi. Kafka <b>kabhi apne aap</b> partitions move nahi karta. Naye brokers sirf naye topics ke liye candidate bante hain. Reassignment manual hai (ya Cruise Control se).' },
    { front: 'Reassignment ki keemat?', back: 'Partition ka <b>poora log network par copy</b> hota hai. 500GB partition = 500GB traffic. Hamesha <code>--throttle</code> ke saath aur off-peak — aur baad mein <b>throttle hataana mat bhoolna</b>.' },
  ],
},

/* =====================================================================
   10. KAFKA CONNECT
   ===================================================================== */
{
  id: 'kafka-connect',
  icon: '🔌',
  title: 'Kafka Connect — "ek record ne poori pipeline 6 ghante rok di"',
  badge: 'Connect Karigar',
  objectives: [
    'Jaano ki Connect kab likho aur kab apna consumer likhna behtar hai',
    'Samjho ki sink connector andar se ek CONSUMER GROUP hi hota hai',
    'tasks.max ki asli seema pehchano — partitions se zyada parallelism hota hi nahi',
    'Poison pill ko errors.tolerance + DLQ se handle karo',
  ],
  lesson: [
    `<h3>😰 Dard: "Analytics 6 ghante purana tha, aur kisi ko pata nahi chala"</h3>
     <p>Ek team ne <code>orders</code> topic se S3 par data likhne ke liye Kafka Connect
     ka S3 sink lagaya. Sab theek chala — teen mahine. Phir ek subah analytics team ne
     poocha ki kal raat ka data kahan hai.</p>
     <pre>$ curl localhost:8083/connectors/s3-sink/status
{
  "name": "s3-sink",
  "connector": { "state": "RUNNING" },      ← "RUNNING" 🙂
  "tasks": [
    { "id": 0, "state": "FAILED",           ← par task FAILED
      "trace": "org.apache.kafka.connect.errors.DataException:
                Converting byte[] to Kafka Connect data failed" },
    { "id": 1, "state": "RUNNING" },
    { "id": 2, "state": "RUNNING" }
  ]
}</pre>
     <p>Ek upstream service ne galti se ek malformed message bheja tha. Us partition ka
     task usse deserialize nahi kar paaya, exception phenka, aur <b>mar gaya</b>.</p>
     <div class="war-story"><b>⚡ Do cheezein milkar 6 ghante kha gayin.</b>
     Ek — connector ka overall state <b>RUNNING</b> hi dikhta raha, kyunki 3 mein se 2 tasks
     chal rahe the; alert connector state par tha, task state par nahi.
     Do — <b>Connect fail hue task ko apne aap restart nahi karta</b>. Woh wahin pada raha.
     Us ek partition ka lag chadhta gaya jabki baaki do partitions theek chalte rahe,
     toh total throughput bhi "thoda kam" hi lagta raha.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Task restart kar do"</b><br>
     <code>POST /connectors/s3-sink/tasks/0/restart</code> — task uthta hai, wahi record
     phir padhta hai, phir mar jaata hai. <b>Poison pill loop.</b> Tumne 30 second khareede.</p>

     <p><b>❌ Aur bura: "Offset aage khiska do"</b><br>
     Kaam kar jaayega, par tumne chup-chaap kuch records phenk diye aur kisi ko pata bhi
     nahi chalega ki kitne aur kaunse. Audit mein yeh sabse mehnga jawab hai.</p>

     <p><b>⚠️ Better: "errors.tolerance = all"</b><br>
     Ab kharab record skip ho jaata hai aur pipeline chalti rehti hai. Par bina DLQ ke
     yeh <b>data ko chup-chaap gayab</b> kar deta hai — pehle wale se bhi khatarnak,
     kyunki ab koi alert bhi nahi bajega.</p>

     <p><b>✅ Real: errors.tolerance = all + DLQ topic</b></p>
     <pre>errors.tolerance                       = all
errors.deadletterqueue.topic.name      = s3-sink-dlq
errors.deadletterqueue.context.headers.enable = true    ← kyun fail hua, headers mein
errors.log.enable                      = true</pre>
     <p>Ab kharab record DLQ topic mein chala jaata hai (poore context ke saath: original
     topic, partition, offset, aur exception), pipeline chalti rehti hai, aur tum DLQ ki
     depth par alert laga sakte ho. Kuch nahi khota, kuch nahi rukta.</p>`,

    `<h3>⚖️ Nayi musibat: "tasks.max 20 kar diya, phir bhi tez nahi hua"</h3>
     <p>Team ne socha throughput badhane ke liye <code>tasks.max</code> 3 se 20 kar do.
     Kuch nahi badla. 17 tasks bilkul khaali baithe rahe.</p>
     <p><b>Kyunki sink connector andar se ek consumer group hi hai.</b> Uske tasks group
     ke members hain. Aur wahi purana niyam lagta hai jo Track 1 mein seekha tha:</p>
     <pre>topic ki partitions = 3
sink tasks          = 20
                    → 3 tasks ko kaam mila
                    → 17 tasks IDLE

Parallelism ki asli limit hamesha PARTITIONS hai.</pre>
     <p>Iska proof khud dekh sakte ho: <code>kafka-consumer-groups --list</code> chalao
     aur wahan <code>connect-s3-sink</code> naam ka group milega — <b>tumhara sink connector</b>.
     Uspar <code>--describe</code> chalao aur normal consumer lag table dikh jaayega.</p>
     <p>Isliye Connect ka throughput badhane ka raasta tasks nahi, <b>partitions</b> hai.
     (Source connectors ka parallelism alag hota hai — woh source system par depend karta hai,
     jaise JDBC source mein kitni tables ya kitne partitioned queries.)</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: pehli Connect pipeline banao',
      instructions: 'Ek source connector se topic bharo, phir sink se usse padho. Beech mein ek SMT lagao.',
      goal: `Ek poori pipeline banani hai: DB → Kafka → S3.<br><br>
        1. <code>connect plugins</code> se dekho kya available hai<br>
        2. Ek <b>source</b> connector banao: <code>JdbcSource</code> → topic <code>orders</code><br>
        &nbsp;&nbsp;&nbsp;saath mein <code>--transform MaskField</code> (card numbers chhupao)<br>
        3. Usse <code>connect poll</code> se chalao — records topic mein aane chahiye<br>
        4. Phir ek <b>sink</b> connector banao: <code>S3Sink</code> usi topic par<br>
        5. <code>kafka-consumer-groups --list</code> chalao — sink dikhega!`,
      cluster: { brokers: 3, controllers: 3 },
      tasks: [
        { text: 'available plugins dekho', check: s => s.history.some(h => /connect\s+plugins?/.test(h)) },
        { text: 'JdbcSource connector banao (MaskField SMT ke saath)',
          check: s => Object.values(s.connectors).some(c => c.type === 'source' && c.transforms.includes('MaskField')) },
        { text: 'source ko chalao — orders topic mein data aaya',
          check: s => s.hasTopic('orders') && s.partitionCounts('orders').reduce((a, c) => a + c, 0) > 0 },
        { text: 'S3Sink connector banao',
          check: s => Object.values(s.connectors).some(c => c.type === 'sink' && c.cls === 'S3Sink') },
        { text: 'sink ka consumer group dhoondo (kafka-consumer-groups --list)',
          check: s => s.history.some(h => /kafka-consumer-groups.*--list/.test(h)) &&
                      Object.keys(s.groups).some(g => g.startsWith('connect-')) },
      ],
      success: s =>
        Object.values(s.connectors).some(c => c.type === 'source' && c.transforms.includes('MaskField')) &&
        Object.values(s.connectors).some(c => c.type === 'sink') &&
        s.partitionCounts('orders').reduce((a, c) => a + c, 0) > 0 &&
        Object.keys(s.groups).some(g => g.startsWith('connect-')),
      successText: `Dhyaan do ki <code>dump orders 0</code> chalao toh card numbers
        <code>****MASKED****</code> dikhenge — SMT ne record ko <b>Kafka mein likhne se pehle</b>
        badla.<br><br>
        Aur sabse zaroori baat: <code>kafka-consumer-groups --list</code> mein
        <code>connect-...</code> group dikha. <b>Sink connector koi jaadu nahi hai — woh
        ek consumer group hai.</b> Uspar <code>--describe</code> chalao aur wahi purana
        lag table milega. Yaani jo debugging tum consumers ke liye jaante ho, woh seedha
        Connect par bhi lagti hai.`,
      hints: [
        'connect plugins',
        'connect create --name jdbc-src --class JdbcSource --topic orders --tasks 2 --transform MaskField',
        'connect poll jdbc-src --records 60',
        'connect create --name s3-sink --class S3Sink --topic orders --tasks 2',
        'kafka-consumer-groups --list',
      ],
    },

    {
      type: 'terminal',
      title: '🖥️ Mission: poison pill se pipeline bachao',
      instructions: 'Yeh wahi 6-ghante wala scenario hai. Pehle usse todo, phir sahi tareeke se theek karo.',
      goal: `<code>payments</code> topic mein ek kharab record chhupa hai.<br><br>
        1. Ek sink connector banao <b>bina</b> error handling ke — <code>ElasticsearchSink</code><br>
        2. <code>connect poll</code> chalao — ek task FAILED hoga<br>
        3. <code>connect status</code> dekho: connector "DEGRADED" par <b>lag atka hua</b><br>
        4. Ab usse <b>delete</b> karo aur naya banao —
        <code>--errors-tolerance all --dlq payments-dlq</code> ke saath<br>
        5. Poll karo — ab sab nikal jaana chahiye aur kharab record DLQ mein<br>
        6. <code>dump payments-dlq 0</code> se DLQ ke andar jhaanko`,
      cluster: {
        brokers: 3,
        controllers: 3,
        topics: {
          payments: {
            partitions: 3, rf: 3,
            messages: [
              ...Array.from({ length: 40 }, (_, i) => ({ key: 'pay-' + i, value: 'amount=' + (100 + i) })),
              { key: 'bad-record', value: 'CORRUPT_PAYLOAD_v1' },
              ...Array.from({ length: 35 }, (_, i) => ({ key: 'pay-x' + i, value: 'amount=' + (50 + i) })),
            ],
          },
        },
      },
      tasks: [
        { text: 'bina DLQ wala sink banao aur usse fail karao',
          check: s => s.history.some(h => /connect\s+poll/.test(h)) &&
                      (Object.values(s.connectors).some(c => c.taskStates.includes('FAILED')) ||
                       s.history.some(h => /connect\s+(delete|create)/.test(h))) },
        { text: 'connect status se FAILED task dekho',
          check: s => s.history.some(h => /connect\s+status/.test(h)) },
        { text: 'errors.tolerance=all + DLQ ke saath naya connector banao',
          check: s => Object.values(s.connectors).some(c => c.errorsTolerance === 'all' && c.dlq) },
        { text: 'poll karo — kharab record DLQ mein jaana chahiye',
          check: s => Object.values(s.connectors).some(c => c.dlqCount > 0) },
        { text: 'DLQ topic ke andar dekho', check: s => s.history.some(h => /^dump\s+\S*dlq/.test(h.trim())) },
      ],
      success: s => {
        const good = Object.values(s.connectors).find(c => c.errorsTolerance === 'all' && c.dlq);
        return !!good && good.dlqCount > 0 && s.totalLag('connect-' + good.name) === 0;
      },
      successText: `Yahi woh farak hai jo 6 ghante bachata hai.<br><br>
        Pehle: <b>ek</b> record ne <b>poori partition</b> rok di, connector "RUNNING" dikhta raha,
        aur lag chup-chaap chadhta gaya.<br><br>
        Ab: kharab record DLQ mein hai (poore context ke saath — original topic, partition,
        offset, exception headers mein), baaki sab nikal gaya, aur lag 0 hai.<br><br>
        <b>Production checklist:</b> har sink connector par
        <code>errors.tolerance=all</code> + <code>DLQ topic</code> +
        <code>context.headers.enable=true</code>, aur alert <b>DLQ ki depth</b> par —
        connector state par nahi. Aur yaad rakho: <code>errors.tolerance=all</code>
        <b>bina DLQ ke</b> sabse khatarnak setting hai, kyunki data chup-chaap gayab hota hai.`,
      hints: [
        'connect create --name es --class ElasticsearchSink --topic payments --tasks 3',
        'Ab poll karo, ek task FAILED hoga — yehi point hai: connect poll es --records 100',
        'connect status es',
        'connect delete es',
        'connect create --name es2 --class ElasticsearchSink --topic payments --tasks 3 --errors-tolerance all --dlq payments-dlq',
        'connect poll es2 --records 100',
        'dump payments-dlq 0',
      ],
    },

    {
      type: 'quiz',
      title: 'Connect ki asli baatein',
      questions: [
        {
          q: 'tasks.max = 20 rakha par topic mein 4 partitions hain. Sink connector ke kitne tasks kaam karenge?',
          choices: ['20', '4', '10', '1'],
          answer: 1,
          explain: 'Sink connector andar se ek <b>consumer group</b> hai, aur uske tasks group ke members. Wahi purana niyam: members > partitions toh extra members IDLE. 4 tasks kaam karenge, 16 khaali baithenge. <b>Throughput badhane ke liye tasks nahi, partitions badhao.</b>',
        },
        {
          q: 'Sink connector ka task FAILED ho gaya. Connect kya karta hai?',
          choices: [
            'Task ko apne aap restart karta hai',
            'Poore connector ko restart karta hai',
            'Kuch nahi — task pada rehta hai aur us partition ka lag chadhta rehta hai. Restart tumhe manually karna padta hai',
            'Kharab record skip karke aage badh jaata hai',
          ],
          answer: 2,
          explain: 'Yeh sabse mehngi galatfehmi hai. Connect fail hue task ko <b>apne aap restart nahi karta</b>. Aur agar 3 mein se 1 task mara hai, toh connector ka overall state phir bhi <b>RUNNING</b> dikhta hai. Isliye alert hamesha <b>task-level state</b> par lagao, connector state par nahi.',
        },
        {
          q: 'errors.tolerance = all set hai par koi DLQ topic nahi. Kya hoga?',
          choices: [
            'Connector fail ho jaayega',
            'Kharab records CHUP-CHAAP gir jaayenge — koi error, koi alert, koi record nahi. Yeh sabse khatarnak setting hai',
            'Records automatically retry honge',
            'Records original topic mein wapas chale jaayenge',
          ],
          answer: 1,
          explain: 'Silent data loss. Pipeline "healthy" dikhti rahegi, lag 0 rahega, aur mahine baad koi poochhega ki 400 orders kahan gaye. <code>errors.tolerance=all</code> hamesha DLQ ke saath. Aur DLQ ki <b>depth par alert</b> lagao — warna DLQ bhi ek aisi jagah ban jaata hai jahan data ja ke chup-chaap marta hai.',
        },
        {
          q: 'SMT (Single Message Transform) se kya NAHI ho sakta?',
          choices: [
            'Field mask karna',
            'Timestamp field add karna',
            'Do topics ke records ko JOIN karna ya aggregation nikalna',
            'Kuch records filter karna',
          ],
          answer: 2,
          explain: 'SMT <b>per-record aur stateless</b> hai — ek record andar, ek (ya zero) record bahar. Join, aggregation, windowing — inke liye state chahiye, toh Kafka Streams ya Flink chahiye. SMT ko "ETL" samajhna Connect ka sabse aam durupyog hai.',
        },
        {
          q: 'Kab Connect ki jagah apna consumer likhna behtar hai?',
          choices: [
            'Hamesha — Connect kabhi use nahi karna chahiye',
            'Jab standard system (S3, JDBC, Elasticsearch) se jodna ho',
            'Jab tumhe complex business logic, multi-step transformations, ya external API calls with custom retry chahiye — Connect standard systems ke liye hai, application logic ke liye nahi',
            'Jab data bahut zyada ho',
          ],
          answer: 2,
          explain: 'Connect ka sweet spot hai <b>bina code likhe</b> standard systems se jodna — config file, bas. Jaise hi tum SMT chain mein business logic thoosne lago, ya custom retry/enrichment chahiye, tum Connect ko woh bana rahe ho jo woh nahi hai. Tab apna consumer ya Streams app saaf rehta hai.',
        },
      ],
    },

    {
      type: 'bins',
      title: 'Connect, Streams, ya apna consumer?',
      instructions: 'Har kaam ko sahi auzaar se jodo.',
      bins: [
        { id: 'connect', label: '🔌 Kafka Connect' },
        { id: 'streams', label: '🌊 Streams / Flink' },
        { id: 'custom', label: '✍️ Apna consumer' },
      ],
      items: [
        { text: 'Postgres table ke changes Kafka mein laane hain', bin: 'connect', explain: 'Debezium jaisa CDC source connector — bilkul yahi kaam ke liye bana hai. Khud likhoge toh WAL parsing se ladoge.' },
        { text: 'Topic ka data har ghante S3 par parquet mein likhna hai', bin: 'connect', explain: 'S3 sink connector: partitioning, file rotation, formats — sab config se. Ise khud likhna hafton ka kaam hai.' },
        { text: '5-minute windows mein per-user spend ka total nikalna hai', bin: 'streams', explain: 'Windowed aggregation ko state store chahiye. SMT stateless hai, Connect yeh kar hi nahi sakta.' },
        { text: 'Orders stream ko users table se join karna hai', bin: 'streams', explain: 'Stream-table join = state. Kafka Streams ka KTable, ya Flink.' },
        { text: 'Har order par ek third-party API call jisme custom retry aur circuit breaker chahiye', bin: 'custom', explain: 'Business logic + custom failure handling. Ise SMT mein thoosna Connect ka durupyog hai.' },
        { text: 'Elasticsearch mein records index karne hain', bin: 'connect', explain: 'Elasticsearch sink connector — bulk API, retries, mapping, sab handled.' },
        { text: 'Fraud detection jisme pichle 30 din ka user pattern chahiye', bin: 'streams', explain: 'Bada state + time-based logic. Yeh stream processing engine ka kaam hai.' },
        { text: 'Ek legacy SOAP service ko call karna hai jiska apna auth flow hai', bin: 'custom', explain: 'Koi standard connector nahi hai aur logic app-specific hai. Apna consumer sabse saaf raasta hai.' },
      ],
    },

    {
      type: 'match',
      title: 'Connect ke knobs',
      instructions: 'Har setting ko uske asli asar se jodo.',
      pairs: [
        { left: 'tasks.max', right: 'Kitne parallel tasks — par sink par partitions se zyada bekaar', explain: 'Sink connector consumer group hai, toh partitions hi asli limit hain. Extra tasks idle baithte hain.' },
        { left: 'errors.tolerance = all', right: 'Kharab records par task marne mat do — par DLQ ke bina data chup-chaap gayab', explain: 'Hamesha DLQ ke saath. Akela yeh setting silent data loss hai.' },
        { left: 'errors.deadletterqueue.topic.name', right: 'Kharab records yahan jaate hain, poore context ke saath', explain: 'context.headers.enable=true bhi lagao — original topic/partition/offset aur exception headers mein aa jaate hain.' },
        { left: 'MaskField SMT', right: 'Sensitive field ko Kafka mein likhne se PEHLE chhupa do', explain: 'PII masking source par karna sabse saaf hai — phir woh topic ke har consumer ke liye pehle se safe hai.' },
        { left: 'Filter SMT', right: 'Kuch records ko poori tarah gira do (jaise heartbeats)', explain: 'Stateless per-record filter. Volume kam karne ka sasta tareeka.' },
        { left: 'connect-<name> consumer group', right: 'Har sink connector ka chhupa hua consumer group — normal lag tools isi par chalte hain', explain: 'Yahi Connect debugging ka sabse bada shortcut hai: kafka-consumer-groups --describe se seedha lag dekh lo.' },
      ],
    },

    {
      type: 'incident',
      title: '🚨 "Connector RUNNING hai par data 6 ghante purana hai"',
      pager: `<b>PAGER 09:05</b> — Analytics team: "S3 par kal raat 02:00 ke baad ka data hai hi nahi."
        Connect UI par connector <b>RUNNING</b> dikha raha hai. Koi alert nahi baja.`,
      start: 'n1',
      nodes: {
        n1: {
          sitrep: `$ curl -s localhost:8083/connectors/s3-sink/status | jq
{
  "name": "s3-sink",
  "connector": { "state": "RUNNING", "worker_id": "10.0.3.4:8083" },
  "tasks": [ ... ]
}

Connect UI: 🟢 RUNNING
Alert: koi nahi baja`,
          text: 'Connector RUNNING dikha raha hai par data ruka hua hai. Pehla kadam?',
          choices: [
            { t: 'Task-level states dekho — connector state jhooth bol sakta hai', good: true, next: 'n2',
              explain: 'Bilkul. Connector ka overall state RUNNING rehta hai chahe uske kuch tasks mar gaye hon. Asli sach tasks[] array mein hota hai.' },
            { t: 'Connector restart kar do', good: false,
              explain: 'Bina wajah jaane restart karna sabse aam aur sabse bekaar step hai. Agar koi poison record hai toh task uthega, wahi record padhega, aur phir mar jaayega. Aur ab tumne evidence bhi kho diya.' },
            { t: 'S3 bucket permissions check karo', good: false,
              explain: 'Ho sakta hai, par yeh andaza hai. Status endpoint 5 second mein exact exception de dega — pehle usse padho, phir theory banao.' },
            { t: 'tasks.max badha do taaki tez ho jaye', good: false,
              explain: 'Data ruka hua hai, dheema nahi. Aur partitions se zyada tasks kaam hi nahi karenge. Yeh galat problem ka ilaaj hai.' },
          ],
        },
        n2: {
          sitrep: `$ curl -s localhost:8083/connectors/s3-sink/status | jq '.tasks'
[
  { "id": 0, "state": "FAILED",
    "trace": "org.apache.kafka.connect.errors.DataException:
              Converting byte[] to Kafka Connect data failed due to
              serialization error: ... at offset 8847291, partition 2" },
  { "id": 1, "state": "RUNNING" },
  { "id": 2, "state": "RUNNING" }
]

$ kafka-consumer-groups --describe --group connect-s3-sink
TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders  0          9,912,004       9,912,004       0
orders  1          9,908,771       9,908,772       1
orders  2          8,847,291       9,904,552       1,057,261   ← atka hua`,
          text: 'Task 0 partition 2 par ek kharab record par mara hai. 10 lakh ka lag. Ab?',
          choices: [
            { t: 'errors.tolerance=all + DLQ topic ke saath connector update karo, phir restart', good: true, next: 'n3',
              explain: 'Sahi ilaaj. Kharab record DLQ mein chala jaayega (poore context ke saath), pipeline aage badhegi, aur tumhare paas woh record baad mein dekhne ke liye bacha rahega. Kuch nahi khoya.' },
            { t: 'Bas task 0 restart kar do', good: false,
              explain: 'Poison pill loop. Task uthega, offset 8847291 padhega, wahi exception, phir FAILED. Tumne 30 second khareede aur ek aur alert generate kiya.' },
            { t: 'Offset ko kharab record ke aage khiska do', good: false,
              explain: 'Chalega, par tumne chup-chaap ek record phenk diya — aur tumhe pata bhi nahi ki usme kya tha. Payments data mein yeh audit finding ban jaata hai. DLQ isi liye hai.' },
            { t: 'Poora topic offset latest par reset kar do', good: false,
              explain: '☠️ 10 lakh records chhod dena. Yeh sabse mehnga faisla hai — aur root cause abhi bhi wahin hai, agla kharab record phir sab rok dega.' },
          ],
        },
        n3: {
          sitrep: `Connector update + restart:
  errors.tolerance                              = all
  errors.deadletterqueue.topic.name             = s3-sink-dlq
  errors.deadletterqueue.context.headers.enable = true

$ kafka-consumer-groups --describe --group connect-s3-sink
orders  2   9,901,004   9,905,110   4,106     ← tezi se ghat raha hai

$ kafka-console-consumer --topic s3-sink-dlq --from-beginning --property print.headers=true
__connect.errors.topic=orders
__connect.errors.partition=2
__connect.errors.offset=8847291
__connect.errors.exception.message=Unknown magic byte!
  {"order_id":"ORD-88472", ...}`,
          text: 'Lag khatam ho raha hai, kharab record DLQ mein hai. Ab follow-up mein kya?',
          choices: [
            { t: 'Alert task-level state aur DLQ depth dono par lagao, aur upstream se poocho ki "Unknown magic byte" kahan se aaya', good: true, next: 'end',
              explain: 'Teenon zaroori hain. Task-level alert isliye ki connector state jhooth bolta hai. DLQ depth isliye ki DLQ bhi ek silent grave ban sakta hai. Aur "Unknown magic byte" ka matlab hai kisi producer ne Schema Registry ke bina raw JSON bheja — woh asli bug hai.' },
            { t: 'Bas errors.tolerance=all rehne do, kaam ho gaya', good: false,
              explain: 'Aadha kaam. Ab pipeline nahi rukegi par kharab records DLQ mein jamte rahenge aur koi nahi dekhega. Aur upstream ka bug abhi bhi zinda hai.' },
            { t: 'DLQ topic delete kar do, jagah bachegi', good: false,
              explain: 'DLQ hi woh evidence hai jisse tum root cause dhoondhoge aur records replay karoge. Usse delete karna apna hi safety net kaat dena hai.' },
            { t: 'Connector ko har ghante auto-restart karne wala cron laga do', good: false,
              explain: 'Classic band-aid. Poison record par yeh har ghante restart-fail-restart karta rahega, alerts ka shor banayega, aur asli problem chhupa dega.' },
          ],
        },
        end: {
          end: true,
          text: '✅ Lag 12 minute mein 0. Kharab record DLQ mein safe. Upstream ka bug bhi mil gaya.',
          summary: `<b>Kya seekha:</b><br><br>
            1️⃣ <b>Connector ka state jhooth bolta hai.</b> 3 mein se 1 task mar jaye toh bhi
            connector "RUNNING" dikhta hai. Hamesha <b>task-level</b> states dekho aur unhi par alert lagao.<br><br>
            2️⃣ <b>Connect fail hue task ko apne aap restart nahi karta.</b> Woh wahin pada rehta hai
            jab tak koi haath na lagaye.<br><br>
            3️⃣ <b>Sink connector ek consumer group hai.</b> <code>kafka-consumer-groups --describe
            --group connect-&lt;name&gt;</code> ne 5 second mein bata diya ki kaunsi partition atki hai.<br><br>
            4️⃣ <b>errors.tolerance=all bina DLQ ke = silent data loss.</b> Dono hamesha saath.<br><br>
            5️⃣ <b>DLQ ki depth par bhi alert lagao</b> — warna DLQ khud ek aisi jagah ban jaata hai
            jahan data ja ke chup-chaap marta hai.`,
        },
      },
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Sink connector ka throughput kam hai. tasks.max badha dein?"',
        choices: [
          'Haan, tasks.max hamesha badhana chahiye',
          'Pehle dekhunga ki topic mein kitni partitions hain — sink connector andar se ek consumer group hai, toh tasks partitions se zyada ho hi nahi sakte. Agar partitions 4 hain toh tasks.max 20 karne se 16 tasks idle baithenge. Asli lever PARTITIONS hai. Saath hi main task-level states check karunga (koi FAILED task chup-chaap lag toh nahi bana raha), aur consumer group ka lag per-partition dekhunga — aksar ek hi partition atki hoti hai.',
          'Nahi, tasks.max se kabhi farak nahi padta',
          'Connector delete karke naya banana chahiye',
        ],
        answer: 1,
        explain: 'Achha jawab teen cheezein karta hai: (1) Connect ko consumer group ki tarah samajhta hai, (2) asli bottleneck (partitions) pehchanta hai, (3) pehle <b>naapta</b> hai — per-partition lag aur task states — andaza nahi lagata.',
      },
      {
        q: '🧑‍💼 "Connect pipeline mein data quality kaise handle karoge?"',
        choices: [
          'errors.tolerance=all laga dunga',
          'Try-catch laga dunga',
          'Teen layers: (1) Schema Registry + compatibility rules taaki kharab data topic mein aaye hi na, (2) sink par errors.tolerance=all + DLQ topic with context headers, taaki ek record poori pipeline na roke, (3) alerting DLQ ki depth par aur TASK-level state par — connector state par nahi, kyunki woh ek task mar jaane par bhi RUNNING dikhata hai. Aur DLQ ko replay karne ka tareeka pehle se ready rakhunga.',
          'Manually monitor karunga',
        ],
        answer: 2,
        explain: 'Yeh production-grade jawab hai: prevention (schema), containment (DLQ), detection (task-level + DLQ depth alerts), aur recovery (replay). "errors.tolerance=all" akela bolna silent data loss hai — aur interviewer wahi sunna chahta hai ki tum DLQ ka zikr karte ho ya nahi.',
      },
      {
        terminal: {
          title: '🖥️ Interview: production-ready pipeline banao',
          goal: `🧑‍💼 Interviewer: "Ek <code>events</code> topic hai (6 partitions).
            Uspar ek <b>production-ready</b> S3 sink lagao — jaisa tum asli mein deploy karte.
            Phir usse chala ke dikhao ki lag 0 ho gaya."`,
          cluster: {
            brokers: 3, controllers: 3,
            topics: {
              events: {
                partitions: 6, rf: 3,
                messages: [
                  ...Array.from({ length: 50 }, (_, i) => ({ key: 'evt-' + i, value: 'type=click' })),
                  { key: 'broken', value: 'CORRUPT_PAYLOAD' },
                  ...Array.from({ length: 40 }, (_, i) => ({ key: 'evt-b' + i, value: 'type=view' })),
                ],
              },
            },
          },
          tasks: [
            { text: 'S3Sink connector banao — errors.tolerance=all aur ek DLQ topic ke saath',
              check: s => Object.values(s.connectors).some(c => c.type === 'sink' && c.errorsTolerance === 'all' && c.dlq) },
            { text: 'tasks partitions se zyada mat maango (6 tak theek hai)',
              check: s => Object.values(s.connectors).some(c => c.type === 'sink' && c.tasks > 1 && c.tasks <= 6) },
            { text: 'connector chalao — kharab record DLQ mein jaana chahiye',
              check: s => Object.values(s.connectors).some(c => c.dlqCount > 0) },
            { text: 'lag 0 karo',
              check: s => Object.values(s.connectors).some(c => c.type === 'sink' && s.totalLag('connect-' + c.name) === 0 && c.consumed > 0) },
          ],
          success: s => {
            const c = Object.values(s.connectors).find(x => x.type === 'sink' && x.errorsTolerance === 'all' && x.dlq);
            return !!c && c.dlqCount > 0 && c.tasks <= 6 && s.totalLag('connect-' + c.name) === 0;
          },
          successText: `Sahi. Interview mein yeh points zaroor bolna:<br><br>
            • <b>errors.tolerance=all + DLQ</b> — dono, kabhi akela pehla nahi<br>
            • <b>context.headers.enable=true</b> — DLQ record mein original topic/partition/offset aur exception<br>
            • <b>tasks.max ≤ partitions</b> — sink ek consumer group hai<br>
            • alert <b>task-level state</b> par aur <b>DLQ depth</b> par — connector state par nahi<br>
            • upstream par <b>Schema Registry</b> taaki kharab data pehle hi ruk jaye`,
          hints: [
            'connect create --name s3 --class S3Sink --topic events --tasks 6 --errors-tolerance all --dlq events-dlq',
            'connect poll s3 --records 200',
            'connect status s3',
          ],
        },
      },
    ],
  },
  success: [
    'Main bata sakta hoon ki sink connector ek consumer group hi hota hai',
    'Main tasks.max ki asli seema (partitions) samjha sakta hoon',
    'Main poison pill ko errors.tolerance + DLQ se handle kar sakta hoon',
    'Main decide kar sakta hoon ki kab Connect, kab Streams, kab apna consumer',
  ],
  homework: `<p>Ek production-ready sink connector ka poora config likho:</p>
    <pre>{
  "name": "s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "topics": "orders",
    "tasks.max": "6",                         // partitions se zyada mat maango

    "s3.bucket.name": "my-datalake",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "flush.size": "10000",

    // --- data quality: teenon zaroori hain ---
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "s3-sink-dlq",
    "errors.deadletterqueue.topic.replication.factor": "3",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    // --- SMT chain ---
    "transforms": "mask,ts",
    "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
    "transforms.mask.fields": "card_number,ssn",
    "transforms.ts.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.ts.timestamp.field": "ingested_at"
  }
}</pre>
    <p><b>Asli experiment:</b> ek jaan-boojh ke kharab record produce karo
    (<code>echo "not-json" | kafka-console-producer --topic orders</code>).
    Phir DLQ topic ko <code>--property print.headers=true</code> ke saath padho —
    tumhe <code>__connect.errors.topic</code>, <code>__connect.errors.partition</code>,
    <code>__connect.errors.offset</code> aur exception message dikhenge.
    Yahi headers DLQ ko replay karne laayak banate hain.</p>
    <p><b>Alert likho:</b> <code>connect_connector_task_status{state="failed"} > 0</code>
    aur DLQ topic ke production rate par. Connector-level state par alert <b>mat</b> lagao —
    woh task marne par bhi RUNNING dikhata hai.</p>`,
  cards: [
    { front: 'Sink connector andar se kya hai?', back: 'Ek <b>consumer group</b> — naam <code>connect-&lt;connector-name&gt;</code>. Isliye <code>kafka-consumer-groups --describe</code> seedha uspar chalta hai, aur partitions hi uske parallelism ki limit hain.' },
    { front: 'tasks.max ki asli limit?', back: 'Sink par <b>partitions</b>. tasks.max=20 par 4 partitions = 4 tasks kaam karenge, 16 idle. Throughput badhane ka raasta partitions hai, tasks nahi.' },
    { front: 'Task FAILED ho gaya — Connect kya karta hai?', back: '<b>Kuch nahi.</b> Task pada rehta hai, us partition ka lag chadhta rehta hai, aur connector ka overall state phir bhi <b>RUNNING</b> dikhta hai. Alert hamesha task-level par.' },
    { front: 'Poison pill ka sahi ilaaj?', back: '<code>errors.tolerance=all</code> + <code>errors.deadletterqueue.topic.name</code> + <code>context.headers.enable=true</code>. Teenon saath. Sirf pehla = <b>silent data loss</b>.' },
    { front: 'SMT se kya nahi ho sakta?', back: 'Join, aggregation, windowing — kuch bhi jisme <b>state</b> chahiye. SMT per-record aur stateless hai. Uske liye Kafka Streams ya Flink.' },
    { front: 'Connect kab NAHI use karna?', back: 'Jab complex business logic ho, multi-step transformation, ya external API calls with custom retry. Connect <b>standard systems</b> ko jodne ke liye hai, application logic ke liye nahi.' },
  ],
},

]);
