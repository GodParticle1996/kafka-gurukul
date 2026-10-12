/* =====================================================================
   Kafka Gurukul — 🔥 TRACK 2: Vishwas Sadhak Path (Reliability)
   Yahan asli production rehta hai.

   Yeh track app ka DIL hai. Jitne bhi "system design" wale hathiyaar hain —
   DLQ, exponential backoff + jitter, circuit breaker, back-pressure —
   sab yahin hain, aur har ek ek ASLI problem se shuru hota hai.

   Lesson ka 3-card niyam (EXTENDING.md section 2):
     Card 1 — 😰 DARD · Card 2 — 🪜 SEEDHI · Card 3 — ⚖️ NAYI MUSIBAT
   ===================================================================== */

KQ.registerLevels('intermediate', [

/* =====================================================================
   1. CONSUMER LAG — pehla SLI
   ===================================================================== */
{
  id: 'consumer-lag',
  icon: '📉',
  title: 'Consumer Lag — woh number jo sabse pehle dekhna hai',
  badge: 'Lag Lakshak',
  objectives: [
    'Lag ko sahi tareeqe se naapo — messages mein nahi, WAQT mein',
    'Per-partition lag dekhna seekho (total lag jhooth bolta hai)',
    'Jaano ki lag ke NUMBER se zyada uska DERIVATIVE zaroori hai',
    'Samjho ki lag = 0 bhi kab jhooth hota hai',
  ],
  lesson: [
    `<h3>😰 Dard: "Dashboard poora green tha"</h3>
     <p>Ek e-commerce company ka order pipeline. Monitoring mein sab kuch tha —
     CPU, memory, disk, pod restarts, error rate. Sab green.</p>
     <p>Support team ne ticket kholi: <b>customers ko order confirmation 40 minute
     baad mil raha hai.</b></p>
     <pre>$ kafka-consumer-groups --describe --group order-workers

TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders    0          8,891,204       8,891,251       47
orders    1          8,887,109       8,887,166       57
orders    2          6,204,881       8,890,772       2,685,891   🔥
orders    3          8,890,455       8,890,502       47</pre>
     <div class="war-story"><b>⚡ Ek partition 26 lakh messages peeche thi</b> —
     poore teen din se. Kisi ko pata nahi chala kyunki <b>lag par koi alert hi
     nahi tha</b>. Team ne infra ke saare metrics monitor kiye the, par woh EK
     metric nahi jo batata hai ki "kaam ho raha hai ya nahi".<br><br>
     Aur dhyan do — CPU low tha, memory theek thi, koi crash nahi. Us partition ka
     consumer <b>zinda</b> tha, bas ek poison message par baar-baar atak raha tha.</div>`,

    `<h3>🪜 Lag ko sahi tarah naapna</h3>
     <p><b>❌ Naive: "Lag > 10,000 par alert kar do"</b><br>
     Absolute number bekaar hai. Ek topic par 10,000 lag = 2 second peeche
     (bilkul theek). Doosre par 10,000 lag = 4 ghante peeche (aag lagi hui hai).
     Aur deploy ke waqt lag har baar spike karta hai → itne false alerts ki
     log alert hi mute kar denge.</p>

     <p><b>⚠️ Better: "Total lag dekho"</b><br>
     Total lag <b>skew ko chhupa deta hai</b>. Upar wale example mein 4 partitions
     ka total lag 26 lakh tha — par woh 4 partitions par barabar nahi tha,
     ek hi par tha. Total dekh ke tum "consumers badha do" karoge, aur kuch
     nahi hoga (ek partition = ek consumer, yaad hai?).</p>

     <p><b>✅ Real: Teen cheezein saath dekho</b></p>
     <pre>1️⃣ LAG IN TIME  (sabse zaroori)
   lag_seconds = lag ÷ consumption_rate
   "2,685,891 ÷ 1,100 msg/s = 40 minute peeche"
   → yeh number business ko samajh aata hai, "26 lakh" nahi

2️⃣ PER-PARTITION lag
   Ek partition par lag = skew ya poison message
   Sab par barabar lag  = sach mein capacity kam hai

3️⃣ LAG KA DERIVATIVE  (badh raha hai ya ghat raha?)
   lag badh raha hai       → consumer producer se dheema hai, HAMESHA badhega
   lag zyada par ghat raha → recovery chal rahi hai, shaant raho
   Alert derivative par karo, absolute number par nahi.</pre>`,

    `<h3>⚖️ Nayi musibat: lag = 0 bhi jhooth bol sakta hai</h3>
     <p>Lag alert laga liya, ab safe? Nahi. Yeh teen cases dekho:</p>
     <pre>🫥 Case 1: Consumer MAR gaya, producer bhi ruk gaya
   → LEO nahi badh raha, committed offset wahin hai
   → LAG = 0  ✅ green  … par kuch chal hi nahi raha
   → Isliye lag ke SAATH throughput bhi monitor karo

🫥 Case 2: Consumer commit kar raha hai par kaam nahi
   → try/catch mein exception nigal liya, offset commit ho gaya
   → LAG = 0  ✅ green  … par messages CHUP-CHAAP gum
   → Isliye processing errors alag se count karo

🫥 Case 3: Group hi gayab ho gaya
   → consumer band, retention ne offsets bhi kha liye
   → group hi list mein nahi → lag "0" ya "-"
   → Isliye group ka EXIST karna bhi check karo</pre>
     <p><b>Isliye asli monitoring teen metrics ki jodi hai:</b></p>
     <pre>📉 lag_seconds        — kitna peeche ho (badh raha ya ghat raha)
⚡ throughput         — kaam ho bhi raha hai ya nahi
❌ processing_errors  — jo "ho gaya" bola woh sach mein hua?</pre>
     <p>Teeno saath. Koi ek akela jhooth bol sakta hai.</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: 40 minute wala lag dhoondo',
      instructions: 'Yeh wahi asli scenario hai. Total lag mat dekho — gehrai mein jao.',
      goal: `<code>orders</code> topic par lag hai. Support keh raha hai ki
        confirmations bahut der se ja rahe hain.<br><br>
        1. Group <code>order-workers</code> ko describe karo<br>
        2. Pata lagao ki lag <b>saari partitions par barabar hai ya ek par</b><br>
        3. Lag ko <b>0 tak</b> le aao (<code>sim process</code> se sach mein consume karo)`,
      cluster: {
        brokers: 3,
        topics: {
          orders: {
            partitions: 4, rf: 3,
            messages: [
              ...Array.from({ length: 60 }, (_, i) => ({ key: 'a' + i, value: 'ok', partition: 0 })),
              ...Array.from({ length: 55 }, (_, i) => ({ key: 'b' + i, value: 'ok', partition: 1 })),
              ...Array.from({ length: 1400 }, (_, i) => ({ key: 'c' + i, value: 'ok', partition: 2 })),
              ...Array.from({ length: 58 }, (_, i) => ({ key: 'd' + i, value: 'ok', partition: 3 })),
            ],
          },
        },
        groups: {
          'order-workers': { members: 4, committed: { orders: [55, 51, 20, 54] } },
        },
      },
      tasks: [
        { text: 'group describe karo', check: s => s.history.some(h => /consumer-groups.*--describe/.test(h)) },
        { text: 'per-partition lag dekho', check: s => s.history.some(h => /sim lag|consumer-groups.*--describe/.test(h)) },
        { text: 'lag 0 tak le aao', check: s => s.lag('order-workers', 'orders') === 0 },
      ],
      success: s => s.lag('order-workers', 'orders') === 0 &&
                    s.history.some(h => /consumer-groups.*--describe/.test(h)),
      successText: `Dhyan do ki tumne kya dekha: <b>p0, p1, p3 par lag sirf ~5 tha,
        par p2 par 1,380.</b><br><br>
        Agar tum sirf <b>total lag (1,395)</b> dekhte, toh natija nikalte "consumers
        badha do". Aur kuch nahi hota — kyunki ek partition ko ek hi consumer padh
        sakta hai. Asli sawal yeh hai: <b>"p2 par aisa kya alag hai?"</b><br><br>
        Asli duniya mein jawab in mein se ek hota hai:<br>
        🔥 <b>Hot key</b> — sab kuch ek key par ja raha hai (Track 1: Keys & Ordering)<br>
        ☠️ <b>Poison message</b> — ek message par consumer atak gaya (aage: DLQ level)<br>
        💀 <b>Us partition ka consumer mar gaya</b> par group se leave nahi kiya`,
      hints: [
        'kafka-consumer-groups --describe --group order-workers',
        'Har PARTITION ka LAG column alag-alag dekho — ek number baaki se bahut bada hai',
        'sim process order-workers --records 9999',
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Lag ka nidaan',
      instructions: 'Har lag-pattern ko uske asli karan se jodo. Sochо: lag kahan hai aur kya woh badh raha hai?',
      bins: [
        { id: 'capacity', label: '⚙️ Capacity kam hai (consumers/speed badhao)' },
        { id: 'skew', label: '🔥 Skew ya poison (ek partition ka masla)' },
        { id: 'fake', label: '🫥 Lag jhooth bol raha hai' },
      ],
      items: [
        { text: 'Saari 12 partitions par lag barabar badh raha hai', bin: 'capacity',
          explain: 'Sab par barabar = producer rate consumer capacity se zyada hai. Yahi ek case hai jahan consumers/processing-speed badhana sach mein kaam karta hai.' },
        { text: '11 partitions par lag 0, ek par 20 lakh', bin: 'skew',
          explain: 'Ek partition ka masla hai — hot key, poison message, ya us partition ka consumer atka hua. Consumers badhane se kuch nahi hoga.' },
        { text: 'Lag 0 hai par throughput bhi 0 hai', bin: 'fake',
          explain: 'Producer bhi ruk gaya hai. LEO nahi badh raha, isliye lag "0" dikh raha hai — par pipeline murda hai. Isliye lag ke saath throughput hamesha dekho.' },
        { text: 'Lag 0 hai par error logs mein hazaaron exceptions', bin: 'fake',
          explain: 'Consumer exception nigal ke offset commit kar raha hai. "Ho gaya" bola par kaam nahi hua — yeh sabse khatarnaak data loss hai kyunki dashboard green rehta hai.' },
        { text: 'Deploy ke waqt lag spike hua, 2 minute baad 0', bin: 'fake',
          explain: 'Yeh normal hai! Rebalance ke doraan processing rukti hai. Isi liye absolute lag par alert bekaar hai — derivative aur duration dekho.' },
        { text: 'Lag lagataar badh raha hai, saare consumers 100% CPU par', bin: 'capacity',
          explain: 'Consumers apni poori taakat laga rahe hain par kaafi nahi. Ya toh zyada consumers (agar partitions bachi hain) ya processing tez karo.' },
        { text: 'Ek partition ka lag ghatta hai phir wahi jagah wapas aa jaata hai', bin: 'skew',
          explain: 'Classic poison message! Consumer use process karne ki koshish karta hai, fail hota hai, restart hota hai, wahi offset se shuru karta hai — infinite loop. Ilaaj DLQ hai.' },
        { text: 'Group hi --list mein nahi dikh raha', bin: 'fake',
          explain: 'Group gayab = koi consumer chal hi nahi raha (aur shayad offsets bhi expire ho gaye). Lag "0" ya "-" dikhega. Isliye group ka exist karna bhi monitor karo.' },
      ],
    },
    {
      type: 'lab',
      title: '🎛️ Lab: spike ke baad kitni der mein recover?',
      instructions: 'Lag ka number nahi — uska DHALAAN dekho. Chart mein lag ka upar jaana aur wapas neeche aana dono dekho.',
      scenario: `Normal traffic <b>500 msg/s</b> hai. Par <b>20s se 50s tak flash sale</b>
        chalegi aur traffic <b>6× (3,000 msg/s)</b> ho jaayega. Sale ke baad traffic
        wapas normal.<br><br>
        Abhi tumhare paas <b>2 consumers</b> hain jo har message par 20ms lete hain
        — yaani capacity sirf <b>100 msg/s</b>.`,
      goal: `Aisa configure karo ki spike ke baad pipeline <b>catch up kar le</b>
        (run ke ant mein lag lagbhag 0) aur <b>peak lag 20,000 se kam</b> rahe.<br><br>
        Dhyan do: tumhe spike ke DORAAN lag zero rakhne ki zaroorat NAHI hai —
        queue ka kaam hi yahi hai ki woh spike ko <b>rok ke rakhe</b>. Bas usse
        <b>baad mein khatam</b> hona chahiye.`,
      duration: 120,
      preset: { producerRate: 500, consumers: 2, processMs: 20, failRate: 0,
                retry: 'none', dlq: false, prefetch: 500,
                downstreamCapacity: 8000, spike: [20, 50, 6] },
      knobs: ['consumers', 'processMs'],
      charts: ['lag', 'throughput'],
      success: r => r.caughtUp && r.peakLag < 20000,
      verdict: r => {
        const cap = Math.round(r.cfg.consumers * (1000 / r.cfg.processMs));
        const peakDemand = r.cfg.producerRate * (r.cfg.spike ? r.cfg.spike[2] : 1);
        const burn = cap - r.cfg.producerRate;
        return `📐 <b>Capacity ${cap} msg/s</b> · normal demand ${r.cfg.producerRate} · spike demand ${peakDemand}<br>
          ${cap < r.cfg.producerRate
            ? '❌ Capacity <b>normal</b> traffic se bhi kam hai — lag kabhi khatam nahi hoga, chahe kitna intezaar karo.'
            : cap < peakDemand
              ? `✅ Capacity normal se zyada hai, isliye spike ke baad lag <b>burn down</b> hoga.
                 Burn-down rate = ${cap} − ${r.cfg.producerRate} = <b>${burn} msg/s</b>.
                 Peak lag ${r.peakLag.toLocaleString()} tha, toh recovery mein ~${Math.round(r.peakLag / Math.max(1, burn))}s lagenge.`
              : `✅ Capacity spike se bhi zyada hai — lag lagbhag bana hi nahi. Par socho: kya itni capacity 24×7 chalane ka kharcha wajib hai?`}`;
      },
      successText: `Yeh sabse zaroori mental model hai jo tumne abhi banaya:<br><br>
        📈 <b>Spike ke doraan lag ka badhna BURA NAHI hai.</b> Queue ka poora kaam hi
        yeh hai — usse "load leveling" kehte hain. Woh spike ko <b>rok ke rakhti hai</b>
        taaki tumhara system peak ke hisaab se nahi, AUSAT ke hisaab se bana ho.<br><br>
        📉 <b>Asli sawal hai: burn-down rate.</b><br>
        <code>burn_down = capacity − normal_rate</code><br>
        Agar yeh number <b>positive</b> hai toh lag khatam ho jaayega (bas waqt lagega).
        Agar <b>zero ya negative</b> hai toh lag <b>kabhi</b> khatam nahi hoga —
        chahe kitna intezaar kar lo.<br><br>
        Isi liye alert "lag > 10000" par nahi, <b>"lag 15 minute se lagataar badh raha hai"</b>
        par lagana chahiye.`,
      hints: [
        'Capacity = consumers × (1000 ÷ processMs). Abhi 2 × 50 = 100 msg/s hai, aur normal traffic hi 500 hai.',
        'Pehle capacity ko NORMAL traffic (500) se upar le jao — warna lag kabhi burn down nahi hoga.',
        'processMs 5 aur consumers 8 → 1,600 msg/s. Normal se 3× upar, toh spike ka backlog jaldi khatam hoga.',
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Lag ke sawal',
      questions: [
        {
          q: 'Lag 500,000 hai. Alert bhejein ya nahi?',
          choices: [
            'Haan, 500k bahut zyada hai',
            'Nahi, 500k kuch bhi nahi hai',
            'Number akela kuch nahi batata. Poocho: (a) yeh kitne SECOND ka lag hai (lag ÷ throughput), (b) badh raha hai ya ghat raha hai. 500k lag at 50k msg/s = 10 second peeche aur ghat raha hai → bilkul theek. Wahi 500k at 100 msg/s aur badhta hua = 83 minute peeche aur bigadta ja raha hai → aag.',
            'Consumers double kar do phir dekho',
          ],
          answer: 2,
          explain: 'Yahi lag ko samajhne ka pura saar hai. Absolute number ka koi matlab nahi — WAQT (lag ÷ rate) aur DISHA (derivative) matlab rakhte hain.',
        },
        {
          q: 'Lag 0 hai aur dashboard green hai. Kya pipeline healthy hai?',
          choices: [
            'Haan bilkul',
            'Zaroori nahi. Lag 0 ho sakta hai kyunki (a) producer bhi ruk gaya (kuch chal hi nahi raha), ya (b) consumer exception nigal ke offset commit kar raha hai (messages chup-chaap gum). Isliye lag ke saath THROUGHPUT aur PROCESSING ERRORS bhi dekhne chahiye.',
            'Haan, lag 0 ka matlab hi healthy hai',
            'Nahi, lag 0 hamesha bug hota hai',
          ],
          answer: 1,
          explain: 'Lag ek RELATIVE metric hai — woh consumer ki tulna producer se karta hai. Dono ruk jaayein toh woh khush dikhta hai. Isi liye monitoring hamesha teen metrics ki jodi hoti hai.',
          context: 'lag:        0        ✅\nthroughput: 0 msg/s  ⚠️  ← 3 ghante se\nerrors:     0\n\n(consumer pod 3 ghante pehle crash ho gaya tha,\n producer bhi usi deploy mein band ho gaya)',
        },
        {
          q: '11 partitions par lag 0, ek par 20 lakh. Pehla kadam?',
          choices: [
            'Consumers 11 se 22 kar do',
            'Us partition ke consumer ke logs dekho aur pata lagao ki woh atka kyun hai — kyunki ek partition ko ek hi consumer padh sakta hai, toh consumers badhane se us partition ko koi madad nahi milegi. Aam karan: poison message ya hot key.',
            'Partition delete kar do',
            'Offsets ko latest par reset kar do',
          ],
          answer: 1,
          explain: 'Skew ka ilaaj kabhi bhi "aur consumers" nahi hota. Ek partition ki parallelism hamesha 1 hai. Aur offsets reset karna (option 4) 20 lakh messages ko chup-chaap chhodna hai — kabhi kabhi sahi hai, par yeh ek DATA LOSS ka faisla hai, use jaan-boojh ke lena chahiye.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Tum apne Kafka consumers par kaunsa alert lagaoge?"',
        choices: [
          'Lag > 10,000',
          'CPU aur memory',
          'Lag ko SECONDS mein (lag ÷ consumption rate), aur alert us par ki woh N minute se lagataar badh raha hai — absolute number par nahi, kyunki deploy/rebalance par lag hamesha spike karta hai aur woh normal hai. Saath mein throughput > 0 aur processing error rate bhi, kyunki lag 0 apne aap mein jhooth bol sakta hai.',
          'Consumer pod restarts',
        ],
        answer: 2,
        explain: 'Yeh jawab teen cheezein dikhata hai: (1) tum lag ko business terms mein naapte ho, (2) tumhe pata hai ki absolute threshold false alerts deta hai, (3) tum jaante ho ki ek metric akela jhooth bol sakta hai. Teesri baat hi sabse senior signal hai.',
      },
      {
        terminal: {
          title: '🖥️ Interview: 3 minute mein nidaan karo',
          goal: `🧑‍💼 Interviewer: "Production hai. Customers complain kar rahe hain.
            Tumhare paas terminal hai. Mujhe batao ki problem kya hai — aur phir theek karo.
            <br><br>Main dekhna chahta hoon ki tum <b>kis kram mein</b> commands chalate ho."`,
          cluster: {
            brokers: 3,
            topics: {
              payments: {
                partitions: 3, rf: 3,
                messages: [
                  ...Array.from({ length: 40 }, (_, i) => ({ key: 'p' + i, value: 'x', partition: 0 })),
                  ...Array.from({ length: 900 }, (_, i) => ({ key: 'q' + i, value: 'x', partition: 1 })),
                  ...Array.from({ length: 42 }, (_, i) => ({ key: 'r' + i, value: 'x', partition: 2 })),
                ],
              },
            },
            groups: { 'pay-workers': { members: 3, committed: { payments: [38, 12, 40] } } },
          },
          tasks: [
            { text: 'group ki halat dekho', check: s => s.history.some(h => /consumer-groups.*--describe/.test(h)) },
            { text: 'lag 0 karo', check: s => s.lag('pay-workers', 'payments') === 0 },
          ],
          success: s => s.lag('pay-workers', 'payments') === 0 &&
                        s.history.some(h => /consumer-groups.*--describe/.test(h)),
          successText: `Sahi kram tha: <b>describe → per-partition lag dekho → pehchano ki
            yeh skew hai → theek karo.</b><br><br>
            Interview mein yahan bonus point milta hai agar tum bolo:
            <b>"main pehle yeh check karunga ki lag badh raha hai ya ghat raha —
            kyunki agar ghat raha hai toh recovery pehle se chal rahi hai aur mujhe
            kuch nahi karna chahiye."</b> Panic mein galat action lena aadhe outages
            ko lamba kar deta hai.`,
          hints: [
            'kafka-consumer-groups --describe --group pay-workers',
            'Ek partition ka LAG baaki se bahut bada hai — yeh skew hai, capacity ki problem nahi',
            'sim process pay-workers --records 9999',
          ],
        },
      },
    ],
  },
  success: [
    'Main lag ko seconds mein naap sakta hoon (lag ÷ throughput)',
    'Main per-partition lag dekhta hoon, sirf total nahi',
    'Main jaanta hoon ki alert derivative par lagta hai, absolute number par nahi',
    'Main teen tarike bata sakta hoon jisme lag = 0 jhooth bolta hai',
  ],
  homework: `<p>Apne cluster par lag ka behaviour dekho:</p>
    <pre># ek consumer chalao aur usse dheema karo
kafka-console-consumer.sh --topic orders --group lag-test \\
  --bootstrap-server localhost:9092 | while read l; do sleep 1; echo "$l"; done

# doosre terminal mein tez producer
for i in $(seq 1 1000); do echo "msg-$i"; done | \\
  kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092

# teesre terminal mein lag ko live dekho
watch -n 1 'kafka-consumer-groups.sh --describe --group lag-test \\
  --bootstrap-server localhost:9092'</pre>
    <p><b>Asli sabak:</b> lag ko badhte hue dekho. Phir consumer band karo aur
    dekho ki lag <b>ruk jaata hai</b> (badhta nahi) jab producer bhi ruk jaaye —
    yahi woh "lag 0 jhooth bolta hai" wala case hai jo tumne abhi padha.</p>`,
  cards: [
    { front: 'Lag ko kis unit mein naapna chahiye?', back: '<b>Seconds mein</b>, messages mein nahi. <code>lag_seconds = lag ÷ consumption_rate</code>. "26 lakh" se koi faisla nahi hota; "40 minute peeche" se hota hai.' },
    { front: 'Lag par alert kis cheez par lagate hain?', back: 'Absolute number par <b>nahi</b> (deploy par har baar spike hota hai) — <b>derivative</b> par: "lag N minute se lagataar badh raha hai". Direction matters, number nahi.' },
    { front: 'Total lag kyun jhooth bolta hai?', back: 'Woh <b>skew chhupa deta hai</b>. 26 lakh total lag ek partition par ho sakta hai — us case mein consumers badhane se kuch nahi hoga (ek partition = ek consumer).' },
    { front: 'Lag = 0 kab jhooth hota hai?', back: '1) Producer bhi ruk gaya (kuch chal hi nahi raha)<br>2) Consumer exception nigal ke commit kar raha hai (chup-chaap data loss)<br>3) Group hi gayab ho gaya<br>Isliye lag + <b>throughput</b> + <b>error rate</b> teeno saath dekho.' },
    { front: 'Burn-down rate ka formula?', back: '<code>burn_down = capacity − normal_rate</code>. Positive hai toh spike ka lag khatam ho jaayega (waqt lagega). Zero ya negative hai toh lag <b>kabhi</b> khatam nahi hoga.' },
  ],
},

/* =====================================================================
   2. REBALANCE KA TOOFAN
   ===================================================================== */
{
  id: 'rebalance',
  icon: '🔄',
  title: 'Rebalance ka Toofan — deploy karte hi sab ruk gaya',
  badge: 'Rebalance Rakshak',
  objectives: [
    'Samjho ki rebalance ke doraan POORA group kyun ruk jaata hai',
    'Jaano ki rolling deploy se rebalance-storm kaise banta hai',
    'cooperative-sticky aur static membership ka fayda aur keemat dono samjho',
    'Jaano ki max.poll.interval.ms se infinite rebalance loop kaise banta hai',
  ],
  lesson: [
    `<h3>😰 Dard: "Har deploy par 6 minute ka blackout"</h3>
     <p>Ek team ke paas 20 consumer pods the. Rolling deploy mein Kubernetes ek-ek
     karke pod replace karta tha — har pod ~18 second leta tha.</p>
     <p>Har pod ke jaane par <b>rebalance</b> hota tha. Har pod ke aane par
     <b>phir rebalance</b>. Aur purane (eager) rebalance protocol mein rebalance ka
     matlab hai — <b>saare consumers apni saari partitions chhod dete hain aur
     intezaar karte hain</b>.</p>
     <pre>Rolling deploy: 20 pods × 2 rebalance = 40 rebalances
Har rebalance ≈ 9 second stop-the-world

  pod-1 gaya  → 🛑 saare 20 consumers ruke (9s)
  pod-1 aaya  → 🛑 saare 20 consumers ruke (9s)
  pod-2 gaya  → 🛑 …
  … × 20

Total processing rukna: ~6 MINUTE
Deploy ke doraan lag: 0 → 4,20,000</pre>
     <div class="war-story"><b>⚡ Aur asli mazak yeh tha:</b> team ne socha
     "deploy dheere karte hain, ek-ek pod ko 60 second do" — isse deploy 20 minute
     ka ho gaya aur <b>rebalance ki sankhya bilkul utni hi rahi</b>. Problem
     raftaar ki thi hi nahi, <b>rebalance ki ginti</b> ki thi.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Deploy dheere karo"</b><br>
     Rebalance ki ginti wahi rahi, bas dard lamba ho gaya. Aur ab deploy window
     itni badi ho gayi ki uske andar doosri problems aane lagin.</p>

     <p><b>⚠️ Better: "session.timeout.ms badha do (10s → 60s)"</b><br>
     Isse pod restart ke doraan group ko lagta hai ki consumer abhi zinda hai,
     toh rebalance nahi hota. Kaam karta hai! <b>Par</b> ab jab consumer SACH MEIN
     mar jaayega, tab uski partitions <b>60 second</b> tak koi nahi padhega.
     Tumne deploy ka dard kam kiya aur <b>crash ka dard badha diya</b>.</p>

     <p><b>✅ Real: Do settings jo yeh problem khatam kar deti hain</b></p>
     <pre>1️⃣ partition.assignment.strategy = CooperativeStickyAssignor

   EAGER (purana):       sab apni partitions CHHOD dete hain,
                         phir naya assignment → sab ruke
   COOPERATIVE (naya):   sirf woh partitions move hoti hain jinhe
                         move karna ZAROORI hai; baaki consumers
                         bina ruke kaam karte rehte hain
   → 20 consumers mein se 19 chalte rehte hain ✅

2️⃣ group.instance.id = "worker-3"   (STATIC MEMBERSHIP)

   Har pod ko ek STHIR pehchan do (StatefulSet ka ordinal).
   Ab pod restart hone par woh WAHI id le kar wapas aata hai
   → group samajhta hai "yeh wahi banda hai, naya nahi"
   → session.timeout ke andar wapas aa gaya toh REBALANCE HI NAHI HOTA
   → rolling deploy mein 40 rebalance → lagbhag 0</pre>`,

    `<h3>⚖️ Nayi musibat + ek chupa hua qatil</h3>
     <p><b>Static membership ki keemat:</b> ab jab pod SACH MEIN mar jaata hai
     (crash, node failure), toh group <code>session.timeout.ms</code> tak intezaar
     karta hai us "wapas aane wale" pod ka. Us dauran uski partitions <b>koi nahi
     padhta</b>. Toh yeh seedha sauda hai:</p>
     <pre>session.timeout chhota → deploy par zyada rebalance, crash par tez recovery
session.timeout bada   → deploy par kam rebalance,   crash par dheemi recovery
(static membership ke saath 45s–120s aam hai)</pre>

     <h4>☠️ Aur ab woh chupa hua qatil: max.poll.interval.ms</h4>
     <p>Yeh rebalance ka sabse aam karan hai aur log ise pakad nahi paate:</p>
     <pre>consumer.poll() → 500 messages mile
har message par 2 second ka kaam
→ agla poll() 1000 second baad

max.poll.interval.ms default = 300000 (5 min)
→ 300s paar → broker: "yeh consumer mar gaya" → REBALANCE
→ uska batch kisi aur ko mila → woh bhi 1000s lega → REBALANCE
→ 🔁 INFINITE REBALANCE LOOP — kuch bhi process nahi hota</pre>
     <p>Iska ilaaj <b>timeout badhana nahi</b> hai (woh sirf loop dheema karta hai
     aur crash-detection kharab kar deta hai). Ilaaj hai <b>batch chhota karo</b>:</p>
     <pre>max.poll.records: 500 → 50     ← poll ke beech ka waqt 10× kam
processing ko tez ya parallel karo
lamba kaam ho toh: message ko dusre topic par daal ke aage badho</pre>
     <p><b>Ek aur baat jo yaad rakhni hai:</b> <code>session.timeout.ms</code> aur
     <code>max.poll.interval.ms</code> alag cheezein hain. Heartbeat ek
     <b>background thread</b> bhejta hai (isliye consumer busy ho tab bhi woh
     chalta rehta hai), par <code>poll()</code> tumhara <b>apna loop</b> hai.
     Isi liye consumer "zinda" dikh sakta hai aur phir bhi poll-timeout par
     nikaal diya jaata hai.</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: rebalance ki generation ginti dekho',
      instructions: 'Har rebalance group ki "generation" badhata hai. Usse gin ke dekho ki deploy kitna mehnga hai.',
      goal: `Group <code>billing</code> mein 4 consumers hain.<br><br>
        1. Group describe karo — abhi <b>Generation</b> kya hai?<br>
        2. Ek rolling deploy ki nakal karo: ek consumer hatao, ek add karo — kai baar<br>
        3. Phir se describe karo — generation kitni badh gayi?<br>
        4. Har generation = ek stop-the-world pause`,
      cluster: {
        brokers: 3,
        topics: { invoices: { partitions: 8, rf: 3 } },
        groups: { billing: { members: 4, committed: { invoices: [0, 0, 0, 0, 0, 0, 0, 0] } } },
      },
      tasks: [
        { text: 'shuruaati generation dekho', check: s => s.history.some(h => /consumer-groups.*--describe|sim assignment/.test(h)) },
        { text: 'rolling deploy ki nakal karo (6+ add/remove)',
          check: s => s.history.filter(h => /sim consumer(s)? (add|remove)/.test(h)).length >= 6 },
        { text: 'generation 12 se upar le jao', check: s => !!s.groups.billing && s.groups.billing.generation >= 12 },
      ],
      success: s => !!s.groups.billing && s.groups.billing.generation >= 12 &&
                    s.history.filter(h => /sim consumer(s)? (add|remove)/.test(h)).length >= 6,
      successText: `Har generation number ek <b>rebalance</b> hai, aur har rebalance
        eager protocol mein <b>poore group ka pause</b> hai.<br><br>
        Ab hisaab lagao: 20 pods ka rolling deploy = 40 rebalance.
        Har ek 9 second = <b>6 minute</b> jisme kuch process nahi hua.<br><br>
        Isi liye do settings itni zaroori hain:<br>
        🤝 <b>CooperativeStickyAssignor</b> — pause sirf un consumers par jinki
        partitions sach mein move ho rahi hain, baaki chalte rehte hain<br>
        🆔 <b>group.instance.id</b> (static membership) — pod restart par
        rebalance <b>hota hi nahi</b>, agar woh session.timeout ke andar wapas aa jaaye`,
      hints: [
        'kafka-consumer-groups --describe --group billing  — sabse neeche Generation dikhta hai',
        'sim consumer remove billing 1   phir   sim consumer add billing 1',
        'Isse 6+ baar karo. Har command generation badhati hai.',
      ],
    },
    {
      type: 'order',
      title: '🔢 Rebalance ke doraan kya hota hai (eager protocol)',
      instructions: 'Sahi kram lagao — yahin se pata chalega ki "stop-the-world" ka matlab kya hai.',
      steps: [
        { text: '🚪 Ek consumer group chhodta hai (ya uska heartbeat band ho jaata hai)',
          explain: 'Trigger: pod restart, crash, network issue, ya max.poll.interval paar ho jaana.' },
        { text: '📢 Group coordinator baaki sabko REBALANCE ka signal bhejta hai',
          explain: 'Coordinator ek broker hai jo group ka hisaab rakhta hai. Woh agle heartbeat ke jawab mein sabko batata hai.' },
        { text: '🛑 SAARE consumers apni SAARI partitions chhod dete hain',
          explain: 'Yahi "stop-the-world" hai. Eager protocol mein un 19 consumers ko bhi rukna padta hai jinka kuch badla hi nahi. Cooperative protocol yahi ek kadam badalta hai.' },
        { text: '💾 Sab apne offsets commit karte hain (taaki kaam dobara na ho)',
          explain: 'Agar yeh na ho toh naya owner purane offset se shuru karega = duplicate processing.' },
        { text: '🤝 Sab dobara join karte hain, leader naya assignment banata hai',
          explain: 'Group leader (pehla member) assignment strategy chala kar partitions baantta hai. Generation number badh jaata hai.' },
        { text: '▶️ Sab apni nayi partitions se kaam shuru karte hain',
          explain: 'Ab jaake processing wapas chalti hai. Poore process mein group ka throughput ZERO tha.' },
      ],
    },
    {
      type: 'incident',
      title: '🚨 3AM: infinite rebalance loop',
      pager: `<b>PAGER 03:14</b> — <code>orders-consumer</code> group ka lag 8,00,000 aur badh raha hai.
        Consumers "healthy" hain, koi crash nahi, CPU 30%. Par throughput lagbhag <b>0</b> hai.`,
      start: 'n1',
      nodes: {
        n1: {
          sitrep: `$ kafka-consumer-groups --describe --group orders-consumer

Warning: Consumer group 'orders-consumer' is rebalancing.
(30 second baad phir chalaya — phir wahi warning)
(1 minute baad phir — PHIR wahi warning)`,
          text: 'Group lagataar rebalance kar raha hai. Pehla kadam?',
          choices: [
            { t: 'Consumer logs dekho — rebalance ka KARAN dhoondo', good: true, next: 'n2',
              explain: 'Bilkul sahi. Rebalance ek LAKSHAN hai, bimari nahi. Logs batayenge ki trigger kya hai — poll timeout, session timeout, ya sach mein crash.' },
            { t: 'Saare consumers restart kar do', good: false,
              explain: 'Restart se aur zyada rebalance honge — tum aag mein ghee daal rahe ho. Aur karan pata nahi chalega, toh 10 minute mein wahi haal hoga.' },
            { t: 'Consumers double kar do taaki lag jaldi khatam ho', good: false,
              explain: 'Naye consumers join karenge = aur rebalance. Aur agar processing ho hi nahi rahi toh consumers badhane se kya faayda?' },
            { t: 'Offsets latest par reset kar do', good: false,
              explain: '8 lakh orders chup-chaap chhod dena — 3AM ka sabse mehnga faisla. Aur root cause abhi bhi wahin hai, 10 minute baad phir wahi lag.' },
          ],
        },
        n2: {
          sitrep: `$ kubectl logs orders-consumer-7 | tail

WARN [Consumer clientId=c-7 groupId=orders-consumer]
  This member will leave the group because consumer poll timeout has expired.
  This means the time between subsequent calls to poll() was longer than the
  configured max.poll.interval.ms (300000).

INFO  Revoking previously assigned partitions [orders-0, orders-1, orders-2]
INFO  (Re-)joining group
… yahi block har ~5 minute par dohra raha hai`,
          text: 'Karan mil gaya: poll() ke beech 5 minute se zyada lag raha hai. Ab kya?',
          choices: [
            { t: 'max.poll.records ghatao (500 → 50) — batch chhota karo', good: true, next: 'n3',
              explain: 'Yahi asli ilaaj hai. Consumer ek poll mein 500 messages le raha tha aur har ek par kuch second laga raha tha. Batch 10× chhota = poll ke beech ka waqt 10× kam = timeout ke andar.' },
            { t: 'max.poll.interval.ms 5 min se 60 min kar do', good: false,
              explain: 'Yeh loop ko dheema karta hai, khatam nahi. Aur ab jab consumer SACH MEIN marega toh 60 minute tak uski partitions koi nahi padhega. Symptom chhupaya, bimari nahi.' },
            { t: 'session.timeout.ms badha do', good: false,
              explain: 'Galat knob. session.timeout heartbeat ke liye hai (background thread), aur heartbeat toh theek chal raha hai. Yahan problem POLL ke beech ka waqt hai — woh max.poll.interval.ms hai.' },
            { t: 'Aur partitions add kar do', good: false,
              explain: 'Har consumer ko kam partitions milengi, par har MESSAGE ka processing time wahi rahega — poll ke beech ka waqt utna hi rahega. Aur ordering alag se toot jaayegi.' },
          ],
        },
        n3: {
          sitrep: `max.poll.records: 500 → 50 karke deploy kiya

$ kafka-consumer-groups --describe --group orders-consumer
GROUP            TOPIC   PARTITION  LAG      CONSUMER-ID
orders-consumer  orders  0          198,441  c-1
orders-consumer  orders  1          201,882  c-2
…
Total lag: 812,004   State: Stable   Generation: 1,847

throughput: 4,200 msg/s ✅ (pehle 0 tha)
incoming:   1,200 msg/s`,
          text: 'Rebalance ruk gaya aur processing chalu ho gayi. Lag 8 lakh hai. Ab?',
          choices: [
            { t: 'Kuch mat karo — burn-down rate nikalo, ETA batao, monitor karo', good: true, next: 'end',
              explain: 'Sahi. Throughput 4,200/s, incoming 1,200/s → burn-down 3,000/s → 8 lakh lag ~4.5 minute mein khatam. Recovery chal rahi hai; ab koi naya action sirf risk hai.' },
            { t: 'Consumers double kar do taaki aur tez ho', good: false,
              explain: 'Abhi abhi group stable hua hai. Naye consumers = naya rebalance = phir se pause. Recovery chal rahi ho toh usse chhedо mat.' },
            { t: 'Offsets reset karke bacha hua lag chhod do', good: false,
              explain: 'Recovery apne aap ho rahi hai aur 5 minute mein khatam hogi. 8 lakh orders phenkne ki koi wajah hi nahi hai.' },
          ],
        },
        end: {
          end: true,
          text: '✅ Lag 5 minute mein 0 ho gaya. Ek bhi order nahi khoya.',
          summary: `<b>Kya seekha:</b><br><br>
            1️⃣ <b>Rebalance ek LAKSHAN hai, bimari nahi.</b> "Group is rebalancing" dekhte hi
            logs mein KARAN dhoondo — poll timeout, session timeout, ya asli crash.<br><br>
            2️⃣ <b>max.poll.interval.ms ka ilaaj usse badhana NAHI hai.</b> Woh loop dheema
            karta hai aur crash-recovery kharab karta hai. Asli ilaaj:
            <code>max.poll.records</code> ghatao ya processing tez karo.<br><br>
            3️⃣ <b>Recovery chal rahi ho toh haath mat lagao.</b> 3AM ki aadhi outages
            "kuch aur karke dekhte hain" se lambi hoti hain. Burn-down rate nikalo,
            ETA batao, monitor karo.<br><br>
            4️⃣ <b>Sthayi ilaaj:</b> <code>CooperativeStickyAssignor</code> +
            <code>group.instance.id</code> lagao, taaki deploy par yeh dobara na ho.`,
        },
      },
    },
    {
      type: 'quiz',
      title: '🧠 Rebalance ke sawal',
      questions: [
        {
          q: 'Eager aur Cooperative rebalancing mein farak?',
          choices: [
            'Cooperative tez hai',
            'Eager mein SAARE consumers apni SAARI partitions chhod dete hain (poora group rukta hai), Cooperative mein sirf woh partitions move hoti hain jinhe move karna zaroori hai — baaki consumers bina ruke kaam karte rehte hain',
            'Cooperative zyada memory leta hai',
            'Eager sirf purane Kafka mein hai',
          ],
          answer: 1,
          explain: 'Yeh farak deploy ke doraan 6 minute blackout aur lagbhag zero impact ka farak hai. Modern setup mein CooperativeStickyAssignor default hona chahiye.',
        },
        {
          q: 'group.instance.id (static membership) set karne ka kya fayda hai?',
          choices: [
            'Consumers tez ho jaate hain',
            'Pod ko sthir pehchan mil jaati hai — restart par woh WAHI id le kar wapas aata hai, toh agar session.timeout ke andar wapas aa gaya toh rebalance HOTA HI NAHI. Rolling deploy mein 40 rebalance lagbhag 0 ho jaate hain.',
            'Har consumer ko fixed partition milti hai hamesha ke liye',
            'Offsets kabhi expire nahi hote',
          ],
          answer: 1,
          explain: 'Iski keemat bhi jaano: asli crash par group session.timeout tak intezaar karega, aur us dauran uski partitions koi nahi padhega. Deploy ka dard kam, crash ka dard thoda zyada.',
        },
        {
          q: 'Consumer har 5 minute par rebalance kar raha hai. max.poll.interval.ms 5 min hai. Kya karoge?',
          choices: [
            'max.poll.interval.ms 60 minute kar do',
            'max.poll.records ghatao (500 → 50) ya processing tez/parallel karo — taaki poll ke beech ka waqt timeout ke andar aa jaaye. Timeout badhana sirf loop dheema karta hai aur crash-detection kharab kar deta hai.',
            'session.timeout.ms badhao',
            'Consumers add karo',
          ],
          answer: 1,
          explain: 'Yeh classic "symptom vs bimari" sawal hai. Timeout badhana lagta hai ki kaam kar gaya (loop dheema ho gaya) par asli problem — ek batch bahut lamba le raha hai — wahin hai, aur ab crash detection bhi 60 minute dheemi hai.',
          context: 'WARN This member will leave the group because consumer poll\n     timeout has expired. This means the time between subsequent\n     calls to poll() was longer than the configured\n     max.poll.interval.ms (300000).',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Har deploy par tumhara consumer lag spike karta hai. Kya karoge?"',
        choices: [
          'Deploy raat mein karunga',
          'Do cheezein: (1) CooperativeStickyAssignor — taaki rebalance par sirf zaroori partitions move hon aur baaki consumers chalte rahen; (2) group.instance.id se static membership — taaki pod restart par rebalance ho hi na, agar woh session.timeout ke andar wapas aa jaaye. Aur session.timeout main soch-samajh kar chunuunga, kyunki bada timeout crash-recovery dheemi kar deta hai.',
          'Consumers kam kar dunga',
          'Deploy ke waqt producer band kar dunga',
        ],
        answer: 1,
        explain: 'Achha jawab dono settings deta hai AUR unki keemat bhi batata hai. Sirf "cooperative sticky laga do" bolna aadha jawab hai — interviewer dekhna chahta hai ki tum trade-off jaante ho.',
      },
      {
        q: '🧑‍💼 "Rebalance ke doraan tumhare messages ka kya hota hai?"',
        choices: [
          'Woh kho jaate hain',
          'Kuch nahi hota — woh log mein safe pade rehte hain. Rebalance sirf yeh badalta hai ki kaunsa CONSUMER kaunsi partition padhega. Par do cheezein zaroor hoti hain: (a) us dauran processing rukti hai (lag badhta hai), aur (b) agar offsets commit hone se pehle partition revoke ho gayi toh naya owner un messages ko DOBARA process karega — yaani duplicates.',
          'Woh DLQ mein chale jaate hain',
          'Producer ko error milta hai',
        ],
        answer: 1,
        explain: 'Yeh dikhata hai ki tum log-model samajhte ho (data safe hai) aur uske practical asar bhi (pause + duplicates). Duplicate wali baat hi asli senior detail hai — isi liye at-least-once ke saath idempotency chahiye.',
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki eager rebalance mein poora group kyun rukta hai',
    'Main rolling deploy ke rebalance-storm ka hisaab laga sakta hoon',
    'Main cooperative-sticky aur static membership ka fayda aur keemat dono bata sakta hoon',
    'Main max.poll.interval.ms wale infinite rebalance loop ko pehchan aur theek kar sakta hoon',
  ],
  homework: `<p>Rebalance ko live dekho:</p>
    <pre># Terminal 1 — consumer chalao
kafka-console-consumer.sh --topic orders --group rb-test \\
  --bootstrap-server localhost:9092

# Terminal 2 — doosra consumer, SAME group
kafka-console-consumer.sh --topic orders --group rb-test \\
  --bootstrap-server localhost:9092
# → Terminal 1 mein rebalance ka log dikhega

# Terminal 3 — generation badhte hue dekho
watch -n 1 'kafka-consumer-groups.sh --describe --group rb-test \\
  --bootstrap-server localhost:9092 --state'</pre>
    <p><b>Asli experiment:</b> consumers ko baar-baar Ctrl+C karke restart karo aur
    <code>--state</code> se generation number gino. Phir kafkajs mein
    cooperative-sticky assigner laga ke wahi karo — farak khud dekho.</p>`,
  cards: [
    { front: 'Rebalance ke doraan group ka throughput kya hota hai?', back: 'Eager protocol mein <b>ZERO</b> — saare consumers apni saari partitions chhod dete hain aur naye assignment ka intezaar karte hain. Isliye 20-pod rolling deploy = 40 rebalance = minuton ka blackout.' },
    { front: 'CooperativeStickyAssignor kya badalta hai?', back: 'Rebalance par <b>sirf woh partitions move hoti hain jinhe move karna zaroori hai</b>. Baaki consumers bina ruke kaam karte rehte hain. Eager mein sab rukte the.' },
    { front: 'group.instance.id (static membership) ka fayda aur keemat?', back: '<b>Fayda:</b> pod restart par rebalance hota hi nahi (agar session.timeout ke andar wapas aaye) — deploy lagbhag muft.<br><b>Keemat:</b> asli crash par group session.timeout tak intezaar karta hai, us dauran uski partitions koi nahi padhta.' },
    { front: 'Infinite rebalance loop ka sabse aam karan?', back: '<code>max.poll.interval.ms</code> paar hona — ek poll ka batch process karne mein 5 min se zyada lag raha hai. <b>Ilaaj timeout badhana NAHI hai</b> (woh crash-detection kharab karta hai) — <code>max.poll.records</code> ghatao ya processing tez karo.' },
    { front: 'session.timeout.ms aur max.poll.interval.ms mein farak?', back: '<b>session.timeout</b> = heartbeat ke liye (background thread bhejta hai) — "process zinda hai?"<br><b>max.poll.interval</b> = do poll() calls ke beech ka max waqt — "kaam atka toh nahi?"<br>Consumer busy ho toh heartbeat theek chalta hai aur phir bhi poll-timeout par nikaal diya jaata hai.' },
  ],
},

/* =====================================================================
   3. RETRIES & IDEMPOTENCY
   ===================================================================== */
{
  id: 'retries-idempotency',
  icon: '🔁',
  title: 'Retries & Idempotency — retry ne stock −47 kar diya',
  badge: 'Idempotency Ustaad',
  objectives: [
    'Jaano ki kaunsi errors retry karne laayak hain aur kaunsi nahi',
    'Idempotent operation design karna seekho',
    'Samjho ki in-memory dedup kyun jhooth bolta hai',
    'Jaano ki dedup store ka TTL kaise chunte hain',
  ],
  lesson: [
    `<h3>😰 Dard: "Inventory mein stock −47 dikha raha hai"</h3>
     <p>Ek warehouse consumer <code>order-placed</code> events padhta tha aur
     stock kam karta tha:</p>
     <pre>// consumer ka code
await db.query(
  'UPDATE inventory SET stock = stock - ? WHERE sku = ?',
  [order.qty, order.sku]
);</pre>
     <p>Ek raat DB thoda dheema hua. Query 30 second le rahi thi. Consumer ka
     <code>max.poll.interval.ms</code> paar ho gaya → rebalance → naye consumer ne
     wahi batch dobara uthaya → <b>wahi UPDATE dobara chala</b>.</p>
     <pre>Asli order: sku=TSHIRT-M, qty=1

  attempt 1:  stock = 12 − 1 = 11   ✅ (DB mein ho gaya)
  (commit se pehle rebalance 💥)
  attempt 2:  stock = 11 − 1 = 10   ← wahi order, dobara
  attempt 3:  stock = 10 − 1 =  9   ← teesri baar

3 ghante mein 200+ orders ke saath yahi hua
→ inventory: TSHIRT-M stock = −47</pre>
     <div class="war-story"><b>⚡ Asli nuksan stock ka number nahi tha.</b> Negative
     stock ki wajah se "out of stock" logic chalu ho gaya aur poore din woh product
     website par <b>"sold out"</b> dikha — jabki godaam mein 200 pieces the.
     Ek din ki bikri gayi.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Retries band kar do"</b><br>
     Ab jo message ek baar fail hua woh <b>hamesha ke liye gaya</b>. Duplicate
     ki jagah <b>data loss</b> le liya — ilaaj bimari se bura. Aur retry sirf
     tumhare code se nahi aate; rebalance aur crash se bhi aate hain, jinhe tum
     "band" nahi kar sakte.</p>

     <p><b>⚠️ Better: "Consumer mein ek Set rakh lo processed ids ka"</b></p>
     <pre>const seen = new Set();
if (seen.has(order.id)) return;    // duplicate — chhod do
await processOrder(order);
seen.add(order.id);</pre>
     <p>Yeh <b>teen tarah se</b> toot jaata hai:<br>
     1. Consumer restart → <code>Set</code> khaali → sab duplicate wapas<br>
     2. 5 consumer instances → har ek ka apna <code>Set</code> → cross-instance duplicate<br>
     3. Set kabhi khaali nahi hota → <b>memory leak</b> → OOM</p>

     <p><b>✅ Real: Operation ko hi idempotent bana do</b><br>
     Sabse achhi idempotency woh hai jisme <b>koi extra store hi na chahiye</b> —
     operation khud aisi ho ki dobara chalne se kuch na bigde:</p>
     <pre>❌ NAHI (har call se badalta hai):
   UPDATE inventory SET stock = stock - 1 WHERE sku = ?

✅ HAAN — option A: unique constraint (DB hi rok dega)
   INSERT INTO stock_moves (order_id, sku, qty) VALUES (?, ?, ?);
   -- order_id par UNIQUE index → doosri baar 1062 duplicate key
   -- stock ab moves ka SUM hai, isliye hamesha sahi

✅ HAAN — option B: conditional update (state machine)
   UPDATE orders SET status = 'SHIPPED'
     WHERE id = ? AND status = 'PAID';
   -- doosri baar 0 rows affected — kuch nahi hua

✅ HAAN — option C: idempotency key downstream ko do
   POST /charge  Idempotency-Key: payment-8821
   -- gateway khud duplicate rok lega aur PURANA result wapas dega</pre>`,

    `<h3>⚖️ Nayi musibat: har error retry ke laayak nahi</h3>
     <p>Idempotency laga li, ab andhadhund retry kar sakte ho? Nahi. Errors do tarah ke hote hain:</p>
     <pre>♻️ TRANSIENT (retry karo — apne aap theek ho sakti hai)
   • network timeout, connection reset
   • HTTP 503 / 429 (rate limited)
   • DB deadlock, lock timeout
   • "service temporarily unavailable"

☠️ PERMANENT (retry BEKAAR hai — chahe 1000 baar karo)
   • HTTP 400 — payload hi galat hai
   • JSON parse error / schema mismatch
   • HTTP 404 — cheez hai hi nahi
   • business rule fail — "account band hai"
   • HTTP 401/403 — credentials galat hain</pre>
     <p><b>Permanent error ko retry karna sabse mehnga bug hai</b>, kyunki woh
     kabhi safal nahi hoga — woh bas partition ko <b>hamesha ke liye block</b> kar
     dega. Ek galat JSON poore partition ko rok sakta hai. Isi ko
     <b>poison message</b> kehte hain — aur uska ilaaj aane wale DLQ level mein hai. 💀</p>
     <p><b>Aur dedup store ka TTL:</b> agar tum dedup table rakhte ho toh usse
     hamesha ke liye nahi rakh sakte. Rule: <b>TTL ≥ maximum retry window</b>.
     Agar retries 6 ghante tak chal sakti hain toh dedup entry kam se kam 6 ghante
     (asal mein 24-48 ghante) rakho, warna deri se aaya duplicate "naya" lagega.</p>`,
  ],
  stages: [
    {
      type: 'bins',
      title: '🗂️ Retry karein ya nahi?',
      instructions: 'Ek hi sawal: kya yeh error apne aap theek ho sakti hai?',
      bins: [
        { id: 'retry', label: '♻️ Transient — retry karo' },
        { id: 'dlq', label: '☠️ Permanent — DLQ bhejo, retry bekaar hai' },
      ],
      items: [
        { text: 'HTTP 503 Service Unavailable', bin: 'retry',
          explain: 'Downstream abhi busy hai. Thodi der baad shayad theek ho jaaye. Classic transient — par backoff ke saath retry karo, warna usse aur maroge.' },
        { text: 'HTTP 400 Bad Request — payload invalid', bin: 'dlq',
          explain: 'Payload galat hai aur woh apne aap theek nahi hoga. 1000 baar bhejo, 1000 baar 400 milega. Yeh partition block karne ka pakka nuskha hai.' },
        { text: 'Connection timeout', bin: 'retry',
          explain: 'Network glitch. Agli koshish mein aksar chal jaata hai. Par dhyan: agar operation idempotent nahi hai toh timeout khatarnaak hai — pata nahi kaam hua ya nahi.' },
        { text: 'JSON parse error — message corrupt hai', bin: 'dlq',
          explain: 'Bytes hi kharab hain. Retry se woh theek nahi honge. Ise DLQ mein bhejo aur producer side investigate karo.' },
        { text: 'DB deadlock detected', bin: 'retry',
          explain: 'Deadlock ka standard ilaaj hi retry hai — DB khud yahi salah deta hai. Do transactions takra gaye; dobara koshish mein aksar nahi takrayenge.' },
        { text: 'HTTP 429 Too Many Requests', bin: 'retry',
          explain: 'Rate limit hai — par ZAROOR backoff ke saath (aur Retry-After header maano). Bina backoff ke retry karoge toh rate limit se kabhi nikal hi nahi paoge.' },
        { text: 'Business rule: "account suspended hai"', bin: 'dlq',
          explain: 'Yeh error nahi, ek FAISLA hai. Retry se account unsuspend nahi hoga. Ise DLQ ya kisi manual review queue mein bhejo.' },
        { text: 'HTTP 401 Unauthorized', bin: 'dlq',
          explain: 'Credentials galat ya expire hain. Retry se woh theek nahi honge — deploy/config fix chahiye. (Apwaad: agar token auto-refresh hota hai toh ek baar retry theek hai.)' },
      ],
    },
    {
      type: 'order',
      title: '🔢 Idempotent consumer ka sahi kram',
      instructions: 'Payment worker ko is tarah likho ki duplicate se paisa dobara na kate.',
      steps: [
        { text: '📥 poll() — message lo aur uski IDEMPOTENCY KEY nikalo (paymentId)',
          explain: 'Key message ke ANDAR honi chahiye, generate nahi karni. Agar tum key khud banaoge (jaise naya UUID) toh har retry par nayi key banegi aur dedup kaam hi nahi karega.' },
        { text: '🔍 Dedup store se poocho: yeh key pehle process ho chuki?',
          explain: 'DB table ya Redis — par PERSISTENT hona chahiye. In-memory Set restart par khaali ho jaata hai aur multi-instance mein toh kaam hi nahi karta.' },
        { text: '⏭️ Agar mil gayi → chhod do (par offset commit KARO)',
          explain: 'Duplicate ko chup-chaap skip karo. Offset commit karna zaroori hai warna woh message hamesha ke liye atka rahega.' },
        { text: '⚙️ Naya hai → kaam karo (downstream ko bhi idempotency-key do)',
          explain: 'Do layer ki suraksha: tumhara check, aur downstream ka apna check. Agar tumhare check aur kaam ke beech crash ho toh downstream bacha lega.' },
        { text: '💾 Kaam ka natija AUR dedup entry — EK HI DB transaction mein',
          explain: 'Yeh sabse zaroori kadam hai. Alag-alag commit karoge toh beech mein crash par ya kaam ho jaayega bina dedup entry ke (duplicate aayega), ya ulta (message chup-chaap gum).' },
        { text: '✅ Kafka offset commit karo',
          explain: 'Sabse aakhir mein. Yahan crash hua toh message dobara aayega — par ab dedup entry maujood hai, toh woh chup-chaap skip ho jaayega. Poori tarah safe.' },
      ],
    },
    {
      type: 'fill',
      title: '📄 Idempotent stock update likho',
      instructions: 'Woh −47 wala bug theek karo. Aise likho ki dobara chalne se kuch na bigde.',
      template: "-- ❌ purana (har baar ghatata hai)\n-- UPDATE inventory SET stock = stock - 1 WHERE sku = ?\n\n-- ✅ naya: har move ek ROW hai, order_id par {0}\nINSERT INTO stock_moves (order_id, sku, qty)\nVALUES (?, ?, ?);\n-- dobara chala? → error {1} (duplicate key) → chup-chaap skip\n\n-- stock ab hamesha derive hota hai, store nahi hota\nSELECT SUM(qty) FROM stock_moves WHERE sku = ?;\n\n-- ya option B: {2} update (state machine)\nUPDATE orders SET status = 'SHIPPED'\n  WHERE id = ? AND status = '{3}';\n-- dobara chala? → 0 rows affected → kuch nahi hua",
      blanks: [
        { answer: 'UNIQUE', explain: 'UNIQUE index hi asli guarantee deta hai — application ka check race condition mein fail ho sakta hai, DB constraint nahi.' },
        { answer: '1062', explain: 'MySQL ka duplicate-key error code. Ise pakdo aur SUCCESS maan lo — kaam pehle se ho chuka hai.' },
        { answer: 'conditional', explain: 'WHERE clause mein expected state daal do — yahi use idempotent banata hai.' },
        { answer: 'PAID', explain: 'Sirf tab SHIPPED karo jab abhi PAID ho. Doosri baar status already SHIPPED hai, toh WHERE match nahi karega aur 0 rows badlenge.' },
      ],
      tokens: ['UNIQUE', '1062', 'conditional', 'PAID', 'INDEX', '1146', 'blind', 'SHIPPED'],
    },
    {
      type: 'quiz',
      title: '🧠 Idempotency ke jaal',
      questions: [
        {
          q: 'Consumer mein `const seen = new Set()` se dedup karna kyun kaafi nahi?',
          choices: [
            'Set dheema hai',
            'Teen wajah: (1) consumer restart par Set khaali ho jaata hai, (2) 5 instances hain toh har ek ka apna Set hai — cross-instance duplicate nahi rukega, (3) Set kabhi khaali nahi hota toh memory leak ho jaayega',
            'Set mein strings nahi rakh sakte',
            'Kafka Set support nahi karta',
          ],
          answer: 1,
          explain: 'In-memory dedup dev machine par bilkul kaam karta dikhta hai aur production mein chup-chaap fail hota hai. Dedup state hamesha SHARED aur PERSISTENT honi chahiye — DB ya Redis.',
        },
        {
          q: 'Sabse achhi idempotency kaunsi hai?',
          choices: [
            'Ek bada dedup table jisme har message id ho',
            'Redis mein SETNX',
            'Woh jisme ALAG se dedup store chahiye hi nahi — operation khud aisi ho ki dobara chalne se kuch na bigde (unique constraint wala INSERT, ya conditional UPDATE jo state check kare)',
            'Har message ko do baar bhejo aur compare karo',
          ],
          answer: 2,
          explain: 'Dedup store ek aur cheez hai jo gir sakti hai, sync se bahar ja sakti hai, aur jiski TTL sambhalni padti hai. Agar operation khud idempotent ho toh yeh saara tamjhaam hi nahi chahiye.',
        },
        {
          q: 'Dedup table ka TTL kaise chunoge?',
          choices: [
            '1 minute — memory bachti hai',
            'Hamesha ke liye rakho',
            'TTL ≥ maximum retry window. Agar retries 6 ghante tak chal sakti hain toh dedup entry kam se kam utni der (aam taur par 24-48 ghante) rehni chahiye — warna deri se aaya duplicate "naya" lagega aur dobara process ho jaayega.',
            'Message size ke hisaab se',
          ],
          answer: 2,
          explain: 'Yeh woh detail hai jo interview mein senior ko alag karti hai. Bahut chhota TTL dedup ko CHUP-CHAAP bekaar kar deta hai; anant TTL table ko infinite badha deta hai. Rule: retry window se bada rakho.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Tumhara consumer at-least-once hai. Duplicate se kaise bachoge?"',
        choices: [
          'Exactly-once ON kar dunga',
          'Duplicates rokne ki koshish nahi karunga — unhe HARMLESS banaunga. Pehle dekhunga ki operation khud idempotent ban sakti hai (unique constraint par INSERT, ya conditional UPDATE jo current state check kare) — yeh sabse achha hai kyunki koi extra store nahi chahiye. Agar nahi ban sakti toh persistent dedup store (DB/Redis) rakhunga, uska TTL retry window se bada, aur dedup entry + kaam ko EK transaction mein commit karunga.',
          'In-memory Set rakhunga',
          'Consumers ek hi rakhunga',
        ],
        answer: 1,
        explain: 'Teen cheezein sahi hain is jawab mein: (1) framing — duplicate rokna nahi, harmless banana; (2) natural idempotency ko dedup store se upar rakhna; (3) kaam aur dedup entry ka ek hi transaction mein commit hona. Teesri baat hi asli production wisdom hai.',
      },
      {
        q: '🧑‍💼 "Kya tum HTTP 400 error par retry karoge?"',
        choices: [
          'Haan, 3 baar toh karna hi chahiye',
          'Nahi. 400 ka matlab hai payload hi galat hai — woh apne aap theek nahi hoga, chahe 1000 baar bhejo. Ise seedha DLQ bhejo. Retry sirf TRANSIENT errors par (503, 429, timeout, deadlock). Permanent error ko retry karna partition ko hamesha ke liye block kar deta hai — yahi poison message hai.',
          'Haan, agar backoff ho toh',
          'Payload par depend karta hai',
        ],
        answer: 1,
        explain: 'Yeh sabse aam production galti hai: retry ko blanket policy bana dena. Retry ka poora matlab hi yeh hai ki "shayad agli baar chal jaaye" — agar woh mumkin hi nahi hai toh retry sirf nuksan hai.',
      },
    ],
  },
  success: [
    'Main transient aur permanent errors mein farak kar sakta hoon',
    'Main idempotent operation design kar sakta hoon (unique constraint / conditional update)',
    'Main jaanta hoon ki in-memory dedup kyun jhooth bolta hai',
    'Main dedup store ka TTL sahi chun sakta hoon (≥ retry window)',
  ],
  homework: `<p>Wahi −47 bug apne DB par banao aur theek karo:</p>
    <pre>-- ❌ galat tareeqa
CREATE TABLE inventory (sku VARCHAR(20) PRIMARY KEY, stock INT);
INSERT INTO inventory VALUES ('TSHIRT-M', 12);
UPDATE inventory SET stock = stock - 1 WHERE sku = 'TSHIRT-M';
-- yeh 3 baar chalao → stock 9 (jabki ek hi order tha)

-- ✅ sahi tareeqa
CREATE TABLE stock_moves (
  order_id VARCHAR(40) NOT NULL,
  sku      VARCHAR(20) NOT NULL,
  qty      INT NOT NULL,
  UNIQUE KEY uk_order (order_id)      -- ← yahi sab kuch hai
);
INSERT INTO stock_moves VALUES ('order-1','TSHIRT-M',-1);
-- yeh 3 baar chalao → doosri aur teesri baar ERROR 1062 ✅

SELECT 12 + COALESCE(SUM(qty),0) FROM stock_moves WHERE sku='TSHIRT-M';</pre>
    <p>Apne consumer mein <code>1062</code> ko pakad ke <b>success</b> maano —
    kyunki kaam pehle se ho chuka hai.</p>`,
  cards: [
    { front: 'Transient vs Permanent error?', back: '<b>Transient</b> (retry karo): timeout, 503, 429, DB deadlock — apne aap theek ho sakti hai.<br><b>Permanent</b> (DLQ bhejo): 400, JSON parse error, 404, 401, business rule fail — retry se KABHI nahi chalega, bas partition block hoga.' },
    { front: 'In-memory Set se dedup kyun fail hota hai?', back: '1) Restart par Set khaali<br>2) Multi-instance — har ek ka apna Set<br>3) Kabhi khaali nahi hota → memory leak<br>Dedup state hamesha <b>shared + persistent</b> honi chahiye.' },
    { front: 'Sabse achhi idempotency kaunsi hai?', back: 'Woh jisme <b>dedup store chahiye hi nahi</b> — operation khud idempotent ho: <code>INSERT</code> with UNIQUE constraint, ya <code>UPDATE … WHERE status = \'PAID\'</code> (conditional). Dedup store khud ek aur cheez hai jo gir sakti hai.' },
    { front: 'Kaam aur dedup entry ko alag-alag commit karne se kya hota hai?', back: 'Beech mein crash par ya kaam ho jaayega bina dedup entry ke (<b>duplicate wapas aayega</b>), ya dedup entry ban jaayegi bina kaam ke (<b>message chup-chaap gum</b>). Dono ek hi transaction mein commit karo.' },
    { front: 'Dedup store ka TTL kitna?', back: '<code>TTL ≥ maximum retry window</code>. Retries 6 ghante chal sakti hain toh entry 24-48 ghante rakho. Chhota TTL dedup ko <b>chup-chaap</b> bekaar kar deta hai — deri se aaya duplicate "naya" lagega.' },
  ],
},

]);

/* ===================== Levels 4-7: named system-design concepts ===================== */
KQ.registerLevels('intermediate', [

/* =====================================================================
   4. EXPONENTIAL BACKOFF + JITTER
   ===================================================================== */
{
  id: 'backoff-jitter',
  icon: '⏳',
  title: 'Backoff + Jitter — 10,000 clients ne ek saath retry kiya',
  badge: 'Jitter Ka Gyani',
  objectives: [
    'Samjho ki fixed-interval retry se retry-storm kaise banta hai',
    'Exponential backoff ka basic idea aur uski KAMI dono jaano',
    'Jitter KYUN chahiye — Lab mein retry spikes ko flat hote dekho',
    'full / equal / decorrelated jitter mein farak samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "Downstream 30 second ke liye gira, 8 minute tak nahi utha"</h3>
     <p>Ek payment service ke 10,000 consumer clients the (mobile + web + workers).
     Downstream bank API 30 second ke liye gira. Sab clients ne ek saath fail dekha
     aur ek saath retry kiya — <b>har 1 second par, fixed interval</b>.</p>
     <pre>t=0s    bank API gira → 10,000 requests fail
t=1s    10,000 clients ne EK SAATH retry kiya  💥
t=2s    phir 10,000 ek saath
t=3s    phir 10,000 ek saath
…
Bank API ab RECOVER karna chahta hai, par har second
10,000 ka spike use dobara gira deta hai.

Actual outage: 30 second ki thi
Retry storm ki wajah se: 8 MINUTE tak chali 🔥</pre>
     <div class="war-story"><b>⚡ Yeh ka real naam hai "thundering herd".</b>
     Downstream apne aap theek ho jaata, par sabke ek saath, ek jaise interval par
     retry karne se woh baar-baar girta raha. Retries ne outage ko <b>16× lamba</b>
     kar diya. Yeh AWS ke apne engineers ne 2015 mein likha tha — aur isi se
     "jitter" mainstream mein aaya.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Fixed retry — har 1 second baad dobara"</b><br>
     Yahi upar wali storm ki jadd hai. Sab clients synchronized ho jaate hain aur
     downstream ko ek saath maarte rehte hain. Recovery namumkin.</p>

     <p><b>⚠️ Better: "Exponential backoff — 1s, 2s, 4s, 8s…"</b><br>
     Bada sudhaar! Ab har client apne retries ke beech ka gap badhata hai, toh
     downstream par load kam hota jaata hai:</p>
     <pre>attempt 1 fail → 1s ruko
attempt 2 fail → 2s ruko
attempt 3 fail → 4s ruko
attempt 4 fail → 8s ruko   (cap laga do, jaise max 60s)</pre>
     <p><b>Par ek chhupi hui kami hai:</b> agar 10,000 clients <b>ek saath</b> fail
     hue, toh woh sab <b>ek jaisa exponential schedule</b> follow karenge —
     sab 1s par, phir sab 2s par, phir sab 4s par. Spikes <b>chhote</b> hote hain
     par abhi bhi <b>synchronized</b> hain. Herd patli ho gayi, gayab nahi hui.</p>

     <p><b>✅ Real: Exponential backoff + JITTER (randomness daalo)</b><br>
     Delay ko randomize kar do taaki clients aapas mein <b>bikhar</b> jaayein:</p>
     <pre>Full jitter:    delay = random(0,  base × 2^attempt)
Equal jitter:   delay = (base × 2^attempt)/2 + random(0, that/2)
Decorrelated:   delay = min(cap, random(base, prev_delay × 3))

10,000 clients ab 0-8 second mein RANDOMLY bikhre hue retry karte hain
→ downstream par steady, kam load
→ woh saans le kar recover kar leta hai ✅</pre>`,

    `<h3>⚖️ Kaunsa jitter, aur uska mol</h3>
     <p>Teeno jitter kaam karte hain — farak thoda sa hai:</p>
     <pre>🎲 FULL JITTER  — delay = random(0, base × 2^n)
   sabse zyada bikhraav, downstream par sabse kam peak load
   kami: kabhi kabhi delay bahut chhota (0 ke paas) → thoda faltu retry

⚖️ EQUAL JITTER — aadha fixed + aadha random
   guarantee karta hai ki minimum backoff toh hoga hi
   thoda kam bikhraav par zyada predictable

🔄 DECORRELATED — delay agle ke liye pichle delay par based
   AWS ka favourite — achha balance, backoff tezi se badhta hai

👉 Practical: full jitter zyadatar cases ke liye sabse achha hai.
   AWS ka apna paper yahi kehta hai.</pre>
     <p><b>Nayi musibat — retry ki asli seema:</b> jitter storm ko flat karta hai,
     par woh ek buniyaadi sawal ka jawab nahi deta: <b>kab tak retry karte raho?</b>
     Agar downstream 6 ghante ke liye gira hai, toh koi bhi jitter tumhe nahi bachayega —
     tum bas ek achhe interval par usse maarte rahoge. Do cheezein chahiye:</p>
     <pre>1️⃣ maxRetries — kitni baar? (uske baad DLQ) ← agla level
2️⃣ circuit breaker — jab downstream sach mein down ho,
   toh retry karna hi BAND kar do ← aage aane wala level</pre>
     <p>Yaani: <b>jitter</b> retry ke <b>waqt</b> ko sudharta hai, <b>circuit breaker</b>
     retry ki <b>zaroorat</b> ko khatam karta hai. Dono chahiye.</p>`,
  ],
  stages: [
    {
      type: 'lab',
      title: '🎛️ Lab: thundering herd ko apni aankhon se maaro',
      instructions: 'Yeh iss track ka SABSE zaroori lab hai. Retry policy badlo aur retry-rate chart ka nukeela spike flat hote dekho.',
      scenario: `Payment consumer chal raha hai — 1,200 msg/s. Downstream bank API
        <b>10s se 60s tak POORA down</b> rahega (yeh ek asli outage hai). Sab messages
        us dauran fail honge aur retry hone lagenge.<br><br>
        Abhi retry policy <b>"exponential (bina jitter)"</b> par hai aur maxRetries 10 hai.`,
      goal: `Retry-rate chart mein <b>thundering herd ka spike</b> dekho, phir usse
        FLAT karo. Success tab hai jab <b>peak retry rate 2,500/s se neeche</b> ho
        aur <b>DLQ 5,000 se kam</b> — yaani na to spike ho, na hi messages gum hon.`,
      duration: 150,
      preset: { producerRate: 1200, consumers: 16, processMs: 10, failRate: 0.02,
                retry: 'exponential', baseDelay: 1, maxRetries: 10, dlq: true,
                downstreamCapacity: 900, outage: [10, 60] },
      knobs: ['retry', 'maxRetries'],
      charts: ['lag', 'retryRate', 'throughput'],
      success: r => r.peakRetryRate < 2500 && r.dlq < 5000,
      verdict: r => {
        const ratio = r.steadyRetryRate ? (r.peakRetryRate / r.steadyRetryRate) : r.peakRetryRate;
        if (r.cfg.retry === 'none' || r.cfg.maxRetries === 0)
          return `💀 Koi retry nahi (ya maxRetries=0) — outage ke saare messages seedhe
            DLQ/drop mein gaye (${(r.dlq + r.dropped).toLocaleString()}). Storm toh nahi bana,
            par tumne har transient failure ko permanent maan liya. Yeh over-correction hai.`;
        if (r.cfg.retry === 'exponential' || r.cfg.retry === 'fixed')
          return `🌊 <b>THUNDERING HERD</b> — peak retry rate <b>${r.peakRetryRate.toLocaleString()}/s</b>,
            steady sirf <b>${r.steadyRetryRate.toLocaleString()}/s</b> (${ratio.toFixed(1)}× spike!).
            ${r.cfg.retry === 'fixed'
              ? 'Fixed interval sabse bura hai — sab clients har baar theek ek saath aate hain.'
              : 'Exponential ne backoff toh badhaya, par sab clients ka schedule EK JAISA hai — isliye woh abhi bhi synchronized spike maarte hain.'}
            Jitter lagao — delay ko randomize karo.`;
        return `🍃 <b>Jitter kaam kar gaya!</b> Peak retry rate <b>${r.peakRetryRate.toLocaleString()}/s</b>
          (pehle exponential mein ~5,000+ tha). Retries ab time par bikhre hue hain,
          spike lagbhag gayab. Downstream ko saans mil rahi hai.`;
      },
      successText: `🎉 Tumne abhi woh dekha jo padh ke kabhi samajh nahi aata:<br><br>
        📊 <b>Bina jitter ke:</b> retry-rate chart mein nukeele SPIKES — kyunki
        outage ke saare messages ek saath fail hue aur (exponential ke saath bhi)
        ek jaise schedule par ek saath wapas aaye. Har spike downstream ko dobara
        girata hai.<br><br>
        📊 <b>Jitter ke saath:</b> wahi retries time par BIKHAR gaye. Chart flat.
        Peak load kaafi kam.<br><br>
        <b>Sabse zaroori baat jo yaad rakhni hai:</b> jitter ne throughput nahi badhaya —
        woh consumer capacity se bandha hai. Jitter ne <b>PEAK DOWNSTREAM LOAD</b>
        ghataya, taaki downstream dobara na gire. Availability ka fayda, speed ka nahi.<br><br>
        <b>Aur ek limit dekhi:</b> maxRetries=10 tak DLQ lagbhag khaali raha, par kam
        maxRetries par messages DLQ mein bharne lage — kyunki outage 50 second ka tha
        aur kam retries usse "wait out" nahi kar paaye. Yeh backoff aur DLQ ka
        rishta hai — agla level. 💀`,
      hints: [
        'Retry policy ko "exponential + FULL jitter" par le jao — yeh delay ko 0 se max ke beech randomize karta hai.',
        'Agar DLQ zyada bhar raha hai: maxRetries badhao. Outage 50s ka hai, toh messages ko utni der "wait out" karne ke liye kaafi retries chahiye.',
        'Sweet spot: expJitterFull + maxRetries 10. Spike flat ho jaayega aur DLQ lagbhag khaali rahega.',
      ],
    },
    {
      type: 'order',
      title: '🔢 Retry schedule ka evolution',
      instructions: 'Sabse bure se sabse achhe retry approach tak sahi kram lagao.',
      steps: [
        { text: '🔴 Fixed interval: har 1s baad retry (sab clients ek saath)',
          explain: 'Sabse bura. 10,000 clients har second theek ek saath aate hain. Downstream ka recover karna namumkin.' },
        { text: '🟠 Exponential backoff: 1s, 2s, 4s, 8s (gap badhta hai)',
          explain: 'Behtar — load waqt ke saath kam hota hai. Par sab clients ka schedule EK JAISA hai, toh spikes chhote par abhi bhi synchronized.' },
        { text: '🟡 Exponential + equal jitter: aadha fixed + aadha random',
          explain: 'Ab randomness aa gayi — clients bikharne lage. Minimum backoff guarantee bhi rehta hai.' },
        { text: '🟢 Exponential + full jitter: random(0, base×2^n)',
          explain: 'Sabse zyada bikhraav, downstream par sabse kam peak. AWS ka apna paper isi ko recommend karta hai zyadatar cases ke liye.' },
        { text: '🔵 Full jitter + circuit breaker + maxRetries + DLQ',
          explain: 'Complete solution. Jitter storm flat karta hai, circuit breaker down downstream par retry hi band kar deta hai, maxRetries infinite loop rokta hai, DLQ poison messages ko alag karta hai.' },
      ],
    },
    {
      type: 'fill',
      title: '📄 Jitter wala retry likho',
      instructions: 'Full jitter ke saath exponential backoff — jo AWS recommend karta hai.',
      template: "function retryDelay(attempt) {\n  const base = 1000;              // 1 second\n  const cap  = 60000;             // max 60 second\n\n  // exponential: base × 2^attempt, cap tak\n  const exp = Math.min(cap, base * Math.pow({0}, attempt));\n\n  // ❌ bina jitter: return exp;\n  //    → sab clients EK JAISA schedule → synchronized storm\n\n  // ✅ FULL jitter: 0 se exp ke beech RANDOM\n  return Math.{1}() * exp;\n}\n\n// aur retry ki ek SEEMA rakho:\nif (attempt >= {2}) sendToDLQ(message);   // warna infinite loop",
      blanks: [
        { answer: '2', explain: 'base × 2^attempt = exponential growth: 1s, 2s, 4s, 8s…' },
        { answer: 'random', explain: 'Math.random() 0 se 1 ke beech deta hai — usse exp se guna karke 0 se exp ke beech random delay milta hai. Yahi full jitter hai.' },
        { answer: 'maxRetries', explain: 'Retry ki seema. Iske bina ek permanent-fail message hamesha ke liye retry hota rahega aur partition block karega.' },
      ],
      tokens: ['2', 'random', 'maxRetries', '10', 'floor', 'attempt', 'ceil', 'infinity'],
    },
    {
      type: 'quiz',
      title: '🧠 Jitter ke sawal',
      questions: [
        {
          q: 'Exponential backoff LAGA hai phir bhi retry storm aa raha hai. Kyun?',
          choices: [
            'Backoff kaam nahi karta',
            'Kyunki agar saare clients EK SAATH fail hue, toh woh sab EK JAISA exponential schedule follow karte hain — sab 1s par, phir sab 2s par, phir sab 4s par. Spikes chhote ho gaye par abhi bhi synchronized hain. Jitter (randomness) hi unhe bikherti hai.',
            'Base delay bahut chhota hai',
            'maxRetries bahut zyada hai',
          ],
          answer: 1,
          explain: 'Yahi woh insight hai jo jitter ka poora point hai. Exponential backoff clients ke beech ka SPACING nahi todta — woh sirf gap badhata hai. Bina randomness ke sab sync rehte hain.',
        },
        {
          q: 'Jitter ka asli fayda kya hai?',
          choices: [
            'Retries tez ho jaate hain',
            'Throughput badhta hai',
            'PEAK downstream load ghatta hai — retries time par bikhar jaate hain, isliye downstream par ek saath spike nahi aata aur woh recover kar pata hai. Throughput nahi badhta (woh consumer capacity se bandha hai) — yeh availability ka fayda hai, speed ka nahi.',
            'Messages kam fail hote hain',
          ],
          answer: 2,
          explain: 'Bahut log samajhte hain ki jitter "tez" karta hai. Nahi — woh PEAK ko flat karta hai. Retry storm downstream ko baar-baar girata hai; jitter us baar-baar girne ko rokta hai.',
        },
        {
          q: 'Downstream 6 ghante ke liye down hai. Kya jitter-wala backoff kaafi hai?',
          choices: [
            'Haan, jitter sab sambhal lega',
            'Nahi. Jitter retries ko BIKHERTA hai, par woh downstream ke down hone ka hal nahi hai — tum bas ek achhe interval par 6 ghante tak usse maarte rahoge. Iske liye chahiye: (a) maxRetries + DLQ taaki messages phase jaayein na, aur (b) circuit breaker taaki down downstream par retry karna hi BAND ho jaaye.',
            'Haan agar cap 60s ho',
            'Nahi, retries hamesha band kar do',
          ],
          answer: 1,
          explain: 'Jitter retry ke WAQT ko sudharta hai, retry ki ZAROORAT ko nahi. Jab downstream sach mein down ho toh sabse achha retry woh hai jo hota hi na — yahi circuit breaker ka kaam hai.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Retries mein jitter kyun add karte hain? Exponential backoff kaafi nahi?"',
        choices: [
          'Jitter retries ko tez karta hai',
          'Exponential backoff gap badhata hai par clients ke beech ka SPACING nahi todta — agar sab ek saath fail hue toh sab ek jaise schedule par ek saath retry karte hain, yaani synchronized spikes jo downstream ko baar-baar girate hain (thundering herd). Jitter delay mein randomness daalta hai taaki clients time par bikhar jaayein aur downstream par steady, kam load pade. Fayda peak-load ghatane ka hai, speed ka nahi.',
          'Jitter security ke liye hota hai',
          'Jitter se messages kam fail hote hain',
        ],
        answer: 1,
        explain: 'Yeh classic system-design sawal hai. Achha jawab (1) thundering herd ko naam se pehchanata hai, (2) batata hai ki exponential AKELA kyun kaafi nahi (spacing vs gap), aur (3) samajhta hai ki fayda availability ka hai speed ka nahi.',
      },
      {
        q: '🧑‍💼 "Full, equal aur decorrelated jitter mein kya farak hai? Kaunsa chunoge?"',
        choices: [
          'Sab same hain',
          'Full jitter = random(0, exp) — sabse zyada bikhraav, sabse kam peak load, par kabhi kabhi bahut chhota delay. Equal jitter = aadha fixed + aadha random — minimum backoff guarantee. Decorrelated = agla delay pichle par based, tezi se badhta hai. Zyadatar cases ke liye main FULL jitter chunuunga — AWS ka apna paper yahi recommend karta hai — aur upar ek cap (jaise 30-60s) aur maxRetries lagaunga.',
          'Decorrelated hamesha best hai',
          'Equal jitter kabhi use nahi karte',
        ],
        answer: 1,
        explain: 'Yeh detail-level sawal hai. Interviewer dekhna chahta hai ki tumne sach mein iske baare mein socha hai, na ki bas "jitter laga do" ratt liya. Full jitter + cap + maxRetries ka jawab dikhata hai ki tum poora picture jaante ho.',
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki fixed-interval retry se thundering herd kaise banta hai',
    'Main jaanta hoon ki exponential backoff AKELA kyun kaafi nahi (synchronized spikes)',
    'Main jitter ka asli fayda bata sakta hoon (peak load, throughput nahi)',
    'Main full/equal/decorrelated jitter mein farak kar sakta hoon',
  ],
  homework: `<p>Jitter ko code mein dekho (kafkajs ka apna retry bhi jitter use karta hai):</p>
    <pre>// AWS-style full jitter
function fullJitterDelay(attempt, base = 1000, cap = 30000) {
  const exp = Math.min(cap, base * 2 ** attempt);
  return Math.random() * exp;         // 0 se exp ke beech
}

// isse 1000 baar chala ke "attempt 3" ke delays plot karo:
const delays = Array.from({length: 1000}, () => fullJitterDelay(3));
// exponential (bina jitter) mein sab 8000ms par hote — ek spike
// full jitter mein 0-8000ms mein FAILE hue — flat distribution</pre>
    <p><b>Asli experiment:</b> ek histogram banao "attempt 3" ke 1000 delays ka —
    pehle bina jitter (sab ek jagah), phir full jitter ke saath (bikhre hue).
    Yahi farak downstream 30s ya 8min mein recover karne ka farak hai.</p>`,
  cards: [
    { front: 'Thundering herd kya hai?', back: 'Jab bahut saare clients EK SAATH fail hote hain aur EK SAATH (ek jaise interval par) retry karte hain — woh downstream ko baar-baar girate rehte hain, jisse ek 30-second outage 8 minute ka ho jaata hai.' },
    { front: 'Exponential backoff AKELA kyun kaafi nahi?', back: 'Woh gap badhata hai par clients ke beech ka <b>spacing</b> nahi todta. Sab ek saath fail hue toh sab ek jaise schedule par (1s, 2s, 4s…) ek saath retry karte hain — spikes chhote par abhi bhi <b>synchronized</b>.' },
    { front: 'Jitter ka asli fayda?', back: '<b>Peak downstream load</b> ghatana — retries time par bikhar jaate hain, isliye downstream par ek saath spike nahi aata. Throughput NAHI badhta (woh capacity se bandha hai). Availability ka fayda, speed ka nahi.' },
    { front: 'Full jitter ka formula?', back: '<code>delay = random(0, base × 2^attempt)</code>, cap tak. AWS ka recommended default. Sabse zyada bikhraav, sabse kam peak load.' },
    { front: 'Jitter ke baad bhi kya chahiye?', back: 'Jitter retry ke <b>waqt</b> ko sudharta hai, retry ki <b>zaroorat</b> ko nahi. Chahiye: <b>maxRetries + DLQ</b> (infinite loop rokne ko) aur <b>circuit breaker</b> (down downstream par retry hi band karne ko).' },
  ],
},

/* =====================================================================
   5. DEAD LETTER QUEUE
   ===================================================================== */
{
  id: 'dlq',
  icon: '💀',
  title: 'Dead Letter Queue — ek message ne poora partition 6 ghante rok diya',
  badge: 'DLQ Dwarpal',
  objectives: [
    'Poison message ko pehchano aur samjho ki woh partition kaise block karta hai',
    'DLQ ka pattern seekho — aur retry-topics ka tiered approach',
    'Jaano ki DLQ ka MONITORING kyun sabse zaroori hissa hai',
    'DLQ se replay karne ki strategy samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "Ek corrupt message ne 2 lakh orders rok diye"</h3>
     <p>Ek order consumer chal raha tha. Ek producer ne galti se ek message bheja
     jisme <code>amount</code> field <code>"NaN"</code> thi (ek bug ki wajah se).
     Consumer ne use process karne ki koshish ki:</p>
     <pre>partition 2 ka log:
  offset 45,231:  { order: 8819, amount: 499 }   ✅
  offset 45,232:  { order: 8820, amount: "NaN" }  ☠️ POISON
  offset 45,233:  { order: 8821, amount: 1299 }   ⏳ intezaar
  offset 45,234:  { order: 8822, amount: 250 }    ⏳ intezaar
  … 2 lakh aur messages ⏳ intezaar

consumer:
  offset 45,232 process karo → parse error → retry
  → wahi error → retry → wahi error → retry …
  → INFINITE LOOP, offset 45,232 par ATKA hua</pre>
     <div class="war-story"><b>⚡ Kafka mein offset EK line mein aage badhta hai.</b>
     Consumer offset 45,232 ko chhod ke 45,233 par nahi ja sakta — kyunki tab woh
     poison message hamesha ke liye "un-processed" reh jaata aur commit offset use
     paar nahi kar sakta. Toh woh atka raha. Us partition ke <b>peeche ke 2 lakh
     valid orders 6 ghante tak process nahi hue</b> — sirf ek "NaN" ki wajah se.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Us message ko skip kar do (offset aage badha do)"</b><br>
     Kaam ki jaldi mein team ne offset manually aage kar diya. Partition chal padi...
     par woh poison order <b>hamesha ke liye kho gaya</b> — kisi ne use dekha nahi,
     investigate nahi kiya. Aur agli baar phir yahi hoga. Yeh data loss hai
     jise "fix" ka naam de diya.</p>

     <p><b>⚠️ Better: "try/catch mein error nigal lo aur aage badho"</b></p>
     <pre>try { await process(msg); }
catch (e) { logger.error(e); }   // ← chup-chaap nigal liya
// offset commit ho gaya, aage badh gaye</pre>
     <p>Partition ab kabhi block nahi hogi. Par ab har poison message
     <b>chup-chaap gayab</b> ho jaata hai — sirf ek log line ke saath jo kabhi
     koi nahi dekhta. Tumne partition-block ki jagah <b>silent data loss</b> le liya.</p>

     <p><b>✅ Real: Dead Letter Queue — fail hua message ek ALAG topic mein bhejo</b></p>
     <pre>process(msg) fail hua (retries ke baad bhi)?
  → use "orders.DLQ" topic par bhej do (poore context ke saath)
  → main partition ka offset AAGE badha do
  → main pipeline chalti rahe ✅
  → poison message DLQ mein SURAKSHIT, investigate karne ke liye

DLQ message mein yeh sab daalo:
  • original message (poora)
  • error ka reason aur stack trace
  • kitni baar retry hua
  • original topic/partition/offset
  • timestamp</pre>
     <p>Ab partition block nahi hoti, aur message gum bhi nahi hota. Best of both.</p>`,

    `<h3>⚖️ Nayi musibat: DLQ khud bhar sakta hai (aur retry-topics)</h3>
     <p>DLQ laga liya, ab safe? Sirf tab jab tum use <b>dekh rahe ho</b>:</p>
     <pre>💀 Sabse aam DLQ galti: DLQ par MONITORING na lagana
   → DLQ chup-chaap bharta rehta hai
   → 3 mahine baad kisi ko pata chalta hai ki 50,000
     orders DLQ mein pade hain, kabhi process nahi hue
   → DLQ = "jahan messages marne jaate hain" ban jaata hai

✅ DLQ par ALERT lagao: "DLQ depth > 0" ya "DLQ mein naya message"
   DLQ khali hona chahiye. Usme kuch aana = koi INSAAN dekhe.</pre>

     <h4>🎯 Aur ek behtar pattern: tiered retry topics</h4>
     <p>Har fail turant DLQ nahi bhejna — kuch failures transient hote hain
     (yaad hai, pichla level?). Toh <b>retry topics</b> banao alag-alag delay ke:</p>
     <pre>main topic → fail → retry-5s topic   (5 second baad phir try)
                   → fail → retry-1m topic   (1 minute baad)
                   → fail → retry-10m topic  (10 minute baad)
                   → fail → DLQ (ab yeh sach mein poison hai)

Isse: transient failures retry-topics mein theek ho jaate hain,
      sirf ASLI poison messages DLQ tak pahunchte hain.
Aur main partition kabhi block nahi hoti — fail hote hi
message doosre topic par chala jaata hai.</pre>
     <p><b>Replay strategy:</b> DLQ ke messages ko theek karne ke baad wapas main
     topic par daalna hota hai. Par andhadhund nahi — pehle root cause fix karo
     (warna woh phir DLQ mein aayenge), phir controlled rate par replay karo
     (warna woh khud ek load spike ban jaayenge).</p>`,
  ],
  stages: [
    {
      type: 'incident',
      title: '🚨 3AM: poison message ne partition rok di',
      pager: `<b>PAGER 03:42</b> — <code>orders</code> group ke partition 2 ka lag 2,00,000
        aur badh raha hai. Baaki partitions bilkul theek (lag ~0). Consumer "healthy" hai.`,
      start: 'n1',
      nodes: {
        n1: {
          sitrep: `$ kafka-consumer-groups --describe --group order-workers

TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders  0          891,203         891,210         7
orders  1          887,455         887,461         6
orders  2          45,232          245,109         199,877   🔥
orders  3          890,101         890,108         7`,
          text: 'Sirf partition 2 atki hui hai. Uska current-offset badh bhi nahi raha. Pehla kadam?',
          choices: [
            { t: 'Us consumer ke logs dekho — offset 45,232 par kya ho raha hai', good: true, next: 'n2',
              explain: 'Sahi. Ek partition atki hui aur offset move nahi kar raha = consumer us ek message par phansa hua hai. Logs batayenge ki woh message kyun fail ho raha hai.' },
            { t: 'Consumers double kar do', good: false,
              explain: 'Ek partition ko ek hi consumer padh sakta hai. 100 consumers laga do, partition 2 phir bhi ek hi banda padhega — aur woh usi message par atka rahega.' },
            { t: 'Partition 2 delete kar do', good: false,
              explain: 'Partition delete nahi hoti, aur agar hoti bhi toh usme pade 2 lakh valid orders bhi chale jaate. Yeh problem ko bomb se theek karna hai.' },
            { t: 'Offset 45,232 ko skip karke aage badha do', good: false,
              explain: 'Yeh temporary rahat degi par woh poison order HAMESHA ke liye gum ho jaayega — bina dekhe, bina samjhe. Aur agli poison message par phir yahi 3AM. Pehle root cause dekho.' },
          ],
        },
        n2: {
          sitrep: `$ kubectl logs order-worker-2 | tail

ERROR Processing offset 45232 partition orders-2
  JSON parse: unexpected token in amount field: "NaN"
  at parseOrder (order.js:44)
INFO  Retrying offset 45232 (attempt 8,847) …
ERROR Processing offset 45232 … "NaN"
INFO  Retrying offset 45232 (attempt 8,848) …
… yeh 6 ghante se chal raha hai`,
          text: 'Ek message ka amount "NaN" hai — parse error. Yeh PERMANENT failure hai (retry se theek nahi hoga). Ab?',
          choices: [
            { t: 'Us message ko DLQ topic par bhejo aur offset aage badhao', good: true, next: 'n3',
              explain: 'Bilkul sahi. Yeh permanent failure hai — retry bekaar hai. Message ko DLQ mein bhejo (taaki gum na ho aur investigate ho sake), aur main partition ko aage badha do taaki peeche ke 2 lakh valid orders chal sakein.' },
            { t: 'max.poll.interval.ms badha do', good: false,
              explain: 'Isse rebalance ruk jaayega par consumer abhi bhi usi message par atka rahega. Problem message ki hai, timeout ki nahi.' },
            { t: 'Retries aur badha do (8,847 kaafi nahi)', good: false,
              explain: 'Yeh PERMANENT failure hai — "NaN" 8,848th baar bhi "NaN" hi rahega. Retries badhana matlab partition ko aur zyada der block karna.' },
            { t: 'Producer ko theek karo pehle', good: false,
              explain: 'Producer fix zaroori hai par woh AGE ki poison messages rokega. Yeh wala message toh abhi bhi partition block kiye baitha hai — pehle use hatao.' },
          ],
        },
        n3: {
          sitrep: `Consumer mein DLQ logic add kiya:
  try { process(msg) }
  catch (e) {
    if (isPermanent(e) || attempts > MAX)
      await produce('orders.DLQ', { ...msg, error: e, attempts })
    // offset aage badh gaya
  }

$ kafka-consumer-groups --describe --group order-workers
orders  2   245,109   245,109   0   ✅

$ kafka-console-consumer --topic orders.DLQ --from-beginning
{ order: 8820, amount: "NaN", _error: "parse", _attempts: 8848,
  _origin: "orders-2@45232" }`,
          text: 'Partition chal padi, poison message DLQ mein surakshit hai. Ab akhri kadam?',
          choices: [
            { t: 'DLQ par alert lagao aur producer ka NaN bug theek karo', good: true, next: 'end',
              explain: 'Perfect. DLQ par monitoring = agli baar 6 ghante nahi lagenge, turant pata chalega. Aur producer fix = ab NaN messages aayenge hi nahi. Symptom aur root cause dono.' },
            { t: 'DLQ ke message ko turant main topic par wapas daal do', good: false,
              explain: 'Woh abhi bhi "NaN" hai! Wapas daaloge toh phir wahi parse error, phir DLQ. Pehle theek karo, phir replay.' },
            { t: 'DLQ topic delete kar do, ab kaam ho gaya', good: false,
              explain: 'DLQ delete karna = us poison order ko phenk dena, aur agli baar koi safety net nahi. DLQ ko rakhо aur MONITOR karo.' },
          ],
        },
        end: {
          end: true,
          text: '✅ Partition ne 5 minute mein 2 lakh ka backlog khatam kiya. Poison order DLQ mein safe hai, investigate ho raha hai.',
          summary: `<b>Kya seekha:</b><br><br>
            1️⃣ <b>Ek poison message poore partition ko block kar deta hai</b> —
            kyunki offset ek line mein aage badhta hai, consumer use chhod nahi sakta.
            Ek "NaN" ne 2 lakh valid orders roke.<br><br>
            2️⃣ <b>Skip karna ≠ fix.</b> Offset aage badha dena message ko chup-chaap
            kho deta hai. DLQ use SURAKSHIT rakhta hai aur partition bhi chala deta hai.<br><br>
            3️⃣ <b>DLQ ka sabse zaroori hissa MONITORING hai.</b> Bina alert ke DLQ
            "jahan messages marne jaate hain" ban jaata hai. DLQ khali hona chahiye —
            usme kuch aana = koi insaan dekhe.<br><br>
            4️⃣ <b>Replay se pehle root cause fix karo</b> — warna theek kiya hua message
            bhi phir DLQ mein aayega.`,
        },
      },
    },
    {
      type: 'order',
      title: '🔢 Tiered retry → DLQ ka flow',
      instructions: 'Ek message ka safar sahi kram mein lagao — transient se lekar poison tak.',
      steps: [
        { text: '📥 main topic: message process karo',
          explain: 'Pehli koshish hamesha main topic par. Zyadatar messages yahin safal ho jaate hain.' },
        { text: '⏱️ Fail hua? → retry-5s topic par bhejo (5 second baad process)',
          explain: 'Transient failure (503, timeout) 5 second mein aksar theek ho jaati hai. Main partition block nahi hoti kyunki message doosre topic par chala gaya.' },
        { text: '⏱️ Phir fail? → retry-1m topic (1 minute baad)',
          explain: 'Thoda lamba downstream issue. 1 minute ka gap deta hai recover hone ka. Har tier ka apna consumer hota hai jo apne delay ke baad process karta hai.' },
        { text: '⏱️ Phir fail? → retry-10m topic (10 minute baad)',
          explain: 'Aakhri retry tier. Agar yahan tak aa gaya toh ya toh downstream lambe samay se down hai ya message sach mein problematic hai.' },
        { text: '💀 Ab bhi fail? → DLQ (yeh sach mein poison hai)',
          explain: 'Teen tiers ke baad bhi fail = yeh transient nahi tha. Ab DLQ mein poore context ke saath bhejo aur ALERT karo. Koi insaan ise dekhe.' },
        { text: '🔔 DLQ par alert → insaan investigate kare → root cause fix → controlled replay',
          explain: 'DLQ ka ant nahi, shuruaat hai. Fix karo, phir controlled rate par main topic par wapas daalo (taaki replay khud spike na ban jaaye).' },
      ],
    },
    {
      type: 'lab',
      title: '🎛️ Lab: poison messages ko DLQ se pakdo',
      instructions: 'DLQ ON/OFF karke dekho ki messages kahan jaate hain — gum ho jaate hain ya safe rehte hain.',
      scenario: `Pipeline mein <b>15% messages permanently fail</b> ho rahe hain
        (corrupt payloads — ek producer bug). Yeh transient nahi hai — retry se theek
        nahi honge. Abhi <b>DLQ OFF</b> hai aur retry ON hai.`,
      goal: `Aisa configure karo ki <b>ek bhi message HAMESHA ke liye gum na ho</b>
        (dropped = 0). Sochо: fail hue messages ko kahan jaana chahiye taaki woh
        na partition block karein, na gayab hon?`,
      duration: 90,
      preset: { producerRate: 600, consumers: 10, processMs: 10, failRate: 0.15,
                retry: 'expJitterFull', baseDelay: 1, maxRetries: 3, dlq: false,
                downstreamCapacity: 3000 },
      knobs: ['dlq', 'maxRetries'],
      charts: ['lag', 'throughput'],
      success: r => r.dropped === 0 && r.produced > 0,
      verdict: r => {
        if (r.dropped > 0)
          return `💀 <b>${r.dropped.toLocaleString()} messages HAMESHA ke liye gum</b> —
            DLQ OFF hai, toh maxRetries ke baad fail hua message seedha DROP ho gaya.
            Yeh silent data loss hai. Woh corrupt orders ab kabhi investigate nahi honge.`;
        return `✅ <b>Kuch gum nahi hua.</b> <b>${r.dlq.toLocaleString()}</b> messages
          DLQ mein gaye — woh permanent-fail the (corrupt payloads), toh unhe alag rakhna
          hi sahi hai. Ab yeh <b>surakshit</b> hain aur koi insaan inhe dekh sakta hai.
          Dhyan do: main pipeline ka throughput poora chalu raha — poison messages ne
          use block nahi kiya, kyunki fail hote hi woh DLQ mein chale gaye.`;
      },
      successText: `🎉 Tumne DLQ ka poora point dekh liya:<br><br>
        💀 <b>DLQ OFF:</b> permanent-fail messages retry ke baad seedha DROP —
        chup-chaap, hamesha ke liye gum. Aur agar retry na hota toh woh partition block karte.<br><br>
        💾 <b>DLQ ON:</b> wahi messages ek alag topic mein SURAKSHIT — na partition
        block hui, na data gum hua. Ab koi insaan inhe dekh ke root cause (producer ka
        bug) theek kar sakta hai.<br><br>
        <b>Par yaad rakho — DLQ ka sabse zaroori hissa yeh lab mein nahi dikhta:</b>
        DLQ par MONITORING. Bina alert ke DLQ chup-chaap bharta rehta hai aur 3 mahine
        baad pata chalta hai. DLQ khali hona chahiye — usme kuch aana = koi dekhe.`,
      hints: [
        'DLQ OFF hai — toh maxRetries khatam hone par fail message ke paas jaane ki koi jagah nahi, woh drop ho jaata hai.',
        'DLQ ON karo. Ab fail hue messages ek alag jagah jaayenge, gum nahi honge.',
        'Yeh failures PERMANENT hain (corrupt payloads), toh maxRetries badhane se woh theek nahi honge — bas DLQ mein der se pahunchenge. DLQ ON karna hi asli fix hai.',
      ],
    },
    {
      type: 'quiz',
      title: '🧠 DLQ ke sawal',
      questions: [
        {
          q: 'Ek poison message poore partition ko block kyun kar deta hai?',
          choices: [
            'Kyunki woh bahut bada hota hai',
            'Kyunki Kafka mein offset EK line mein aage badhta hai — consumer poison message ko chhod ke aage nahi ja sakta, warna commit offset use paar nahi kar paayega. Toh woh usi par atka rehta hai, aur peeche ke saare valid messages intezaar karte hain.',
            'Kyunki woh baaki partitions ko bhi rok deta hai',
            'Kyunki broker use reject kar deta hai',
          ],
          answer: 1,
          explain: 'Yeh Kafka ke offset model ka seedha natija hai. Ek queue (jaise SQS) mein tum ek message ko alag se "nack" kar sakte ho, par Kafka mein offset ek hi aage badhta hai — isi liye DLQ pattern itna zaroori hai.',
        },
        {
          q: 'DLQ ka SABSE zaroori hissa kya hai?',
          choices: [
            'DLQ topic ka naam',
            'Uska retention',
            'MONITORING — DLQ par alert lagana. Bina alert ke DLQ chup-chaap bharta rehta hai aur "jahan messages marne jaate hain" ban jaata hai; 3 mahine baad pata chalta hai. DLQ khali hona chahiye, usme kuch aana = koi insaan turant dekhe.',
            'DLQ ka partition count',
          ],
          answer: 2,
          explain: 'Yeh sabse aam DLQ galti hai — DLQ bana dena par monitor na karna. Ek DLQ jise koi nahi dekhta woh data loss se sirf thoda behtar hai (kam se kam messages wahan pade hain, par unpar kaam nahi ho raha).',
        },
        {
          q: 'DLQ ke messages ko theek karne ke baad wapas kaise daaloge?',
          choices: [
            'Turant sab ek saath main topic par',
            'Pehle root cause fix karo (warna theek kiya hua message phir DLQ mein aayega), phir CONTROLLED rate par replay karo (warna hazaaron messages ek saath daalne se woh khud ek load spike ban jaayenge)',
            'DLQ delete kar do',
            'Ek-ek karke haath se',
          ],
          answer: 1,
          explain: 'Do galtiyaan yahan aam hain: (1) root cause fix kiye bina replay karna (messages phir fail honge), aur (2) sab ek saath daalna (replay khud ek thundering herd ban jaata hai). Controlled replay dono se bachata hai.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Ek message baar-baar fail ho raha hai aur poore partition ko block kar raha hai. Kya karoge?"',
        choices: [
          'Offset skip kar dunga',
          'Pehle dekhunga ki failure TRANSIENT hai ya PERMANENT. Transient (503, timeout) hai toh backoff ke saath retry, ya retry-topic mein bhejo. Permanent (parse error, 400) hai toh use DLQ topic par bhejo — poore context ke saath (error, attempts, origin offset) — aur main partition ka offset aage badha do taaki peeche ke valid messages chal sakein. DLQ par alert zaroor lagaunga, aur producer ka root cause bhi theek karunga.',
          'Consumers badha dunga',
          'max.poll.interval.ms badha dunga',
        ],
        answer: 1,
        explain: 'Achha jawab (1) transient vs permanent distinguish karta hai, (2) DLQ ko context ke saath use karta hai, (3) monitoring aur root-cause dono batata hai. "Offset skip" wala jawab silent data loss hai.',
      },
      {
        terminal: {
          title: '🖥️ Interview: DLQ setup karo',
          goal: `🧑‍💼 Interviewer: "Ek order pipeline ke liye DLQ set up karo. Mujhe teen topics
            chahiye: <code>orders</code> (main, 6 partitions), <code>orders.retry</code>
            (retry ke liye, 6 partitions), aur <code>orders.DLQ</code> (dead letters, 1 partition
            kaafi hai kyunki volume kam hoga). Teeno RF=3 rakho.<br><br>
            Phir <code>orders</code> topic mein kuch messages daal ke dikhao ki pipeline ready hai."`,
          cluster: { brokers: 3 },
          tasks: [
            { text: 'orders topic (6 partitions, RF=3)', check: s => s.hasTopic('orders') && s.topics.orders.partitions.length >= 6 && s.topics.orders.rf === 3 },
            { text: 'orders.retry topic', check: s => s.hasTopic('orders.retry') },
            { text: 'orders.DLQ topic', check: s => s.hasTopic('orders.DLQ') },
            { text: 'orders mein messages daalo', check: s => s.hasTopic('orders') && s.partitionCounts('orders').reduce((a, c) => a + c, 0) > 0 },
          ],
          success: s => s.hasTopic('orders') && s.topics.orders.partitions.length >= 6 && s.topics.orders.rf === 3 &&
                        s.hasTopic('orders.retry') && s.hasTopic('orders.DLQ') &&
                        s.partitionCounts('orders').reduce((a, c) => a + c, 0) > 0,
          successText: `Achha DLQ setup! Dhyan do ki <code>orders.DLQ</code> mein sirf 1 partition
            kaafi hai — kyunki agar sab theek chal raha hai toh DLQ mein bahut kam messages
            aane chahiye. Agar DLQ mein zyada volume aa raha hai toh woh khud ek alarm hai.<br><br>
            Bonus interview point: ab main <code>orders.DLQ</code> par ek alert lagaunga
            ("depth > 0") — kyunki ek DLQ jise koi nahi dekhta bekaar hai.`,
          hints: [
            'kafka-topics --create --topic orders --partitions 6 --replication-factor 3',
            'kafka-topics --create --topic orders.retry --partitions 6 --replication-factor 3',
            'kafka-topics --create --topic orders.DLQ --partitions 1 --replication-factor 3',
            'kafka-console-producer --topic orders --count 20 --key order-{i}',
          ],
        },
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki ek poison message poore partition ko kaise block karta hai',
    'Main DLQ pattern implement kar sakta hoon (context ke saath)',
    'Main tiered retry topics ka flow bata sakta hoon',
    'Main jaanta hoon ki DLQ monitoring aur controlled replay kyun zaroori hain',
  ],
  homework: `<p>Poison message aur DLQ khud banao:</p>
    <pre>// consumer with DLQ
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    try {
      const order = JSON.parse(message.value.toString());
      if (isNaN(order.amount)) throw new Error('invalid amount');
      await processOrder(order);
    } catch (e) {
      // permanent failure → DLQ
      await producer.send({
        topic: 'orders.DLQ',
        messages: [{
          key: message.key,
          value: message.value,
          headers: {
            'x-error': e.message,
            'x-origin': \`\${topic}-\${partition}@\${message.offset}\`,
            'x-failed-at': new Date().toISOString(),
          },
        }],
      });
      // offset aage badh jaayega — partition block nahi hogi
    }
  },
});</pre>
    <p><b>Asli experiment:</b> ek valid message, phir ek <code>{"amount":"NaN"}</code>,
    phir do valid bhejo. Bina DLQ ke consumer NaN par atak jaayega. DLQ ke saath
    woh NaN ko DLQ mein bhej ke aage badh jaayega. <code>orders.DLQ</code> consume
    karke dekho ki poison message poore context ke saath wahan hai.</p>`,
  cards: [
    { front: 'Poison message poore partition ko kyun block karta hai?', back: 'Kafka mein offset <b>ek line mein</b> aage badhta hai — consumer poison message ko chhod ke aage nahi ja sakta (warna commit offset use paar nahi karega). Toh woh usi par atka rehta hai aur peeche ke saare valid messages intezaar karte hain.' },
    { front: 'DLQ pattern kya hai?', back: 'Jo message retries ke baad bhi fail ho, use ek <b>alag topic</b> (orders.DLQ) par bhej do — poore context ke saath (error, attempts, origin offset) — aur main partition ka offset aage badha do. Partition block nahi hoti, message gum bhi nahi hota.' },
    { front: 'DLQ ka sabse zaroori hissa?', back: '<b>Monitoring/alerting.</b> Bina alert ke DLQ chup-chaap bharta rehta hai aur "jahan messages marne jaate hain" ban jaata hai. DLQ <b>khali</b> hona chahiye — usme kuch aana = koi insaan turant dekhe.' },
    { front: 'Tiered retry topics kya hain?', back: 'main → retry-5s → retry-1m → retry-10m → DLQ. Transient failures retry-topics mein theek ho jaate hain; sirf ASLI poison messages DLQ tak pahunchte hain. Aur main partition kabhi block nahi hoti.' },
    { front: 'DLQ se replay kaise karte hain?', back: '1) Pehle <b>root cause fix</b> karo (warna theek kiya message phir DLQ mein aayega)<br>2) <b>Controlled rate</b> par replay karo (warna hazaaron messages ek saath = khud ek load spike/thundering herd).' },
  ],
},

]);

/* ===================== Levels 6-7: back-pressure & circuit breaker ===================== */
KQ.registerLevels('intermediate', [

/* =====================================================================
   6. BACK-PRESSURE & FLOW CONTROL
   ===================================================================== */
{
  id: 'backpressure',
  icon: '🌊',
  title: 'Back-pressure — producer 5000/s, consumer 500/s, memory phatt gayi',
  badge: 'Backpressure Baadshah',
  objectives: [
    'Samjho ki fast producer + slow consumer se OOM kaise hota hai',
    'Jaano ki Kafka pull-model se natural back-pressure kaise deta hai',
    'prefetch / max.poll.records se memory control karna seekho',
    'Load shedding aur rate limiting mein farak samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "Consumers har 90 second mein OOM ho ke mar rahe the"</h3>
     <p>Ek analytics consumer events process kar ke ek dheeme ML model ko bhejta tha
     (~40ms per event). Traffic 3,000 events/s tha. Consumer ne "performance ke liye"
     <code>max.poll.records</code> bahut bada rakha — <code>10000</code> — taaki
     "ek saath zyada le sake".</p>
     <pre>Consumer capacity: ~250 events/s (ML model dheema)
Incoming:          3,000 events/s
Har poll:          10,000 records memory mein utha liye

t=0s    poll → 10,000 records RAM mein
        processing shuru (250/s)… 40 second lagenge
t=40s   abhi 10,000 process nahi hue, par memory mein hain
        agla poll → 10,000 AUR
t=80s   RAM mein 20,000+ records → heap full → 💥 OutOfMemory
        pod mara → rebalance → naya pod → phir wahi</pre>
     <div class="war-story"><b>⚡ Consumers ek loop mein mar rahe the.</b> Har OOM
     par rebalance, har rebalance par processing ruki, lag aur badha, aur naya pod
     bhi wahi 10,000 utha ke phir mar gaya. Root cause "performance optimization"
     thi — <code>max.poll.records</code> bada karna. Woh consume karne ki raftaar
     nahi badhata, sirf ek saath zyada memory mein bhar leta hai.</div>`,

    `<h3>🪜 Solutions ki seedhi</h3>
     <p><b>❌ Naive: "Consumer ko zyada memory do (heap 2GB → 8GB)"</b><br>
     Yeh OOM ko thoda TAAL deta hai, rokta nahi. Ab 80,000 records memory mein
     bharne par marega bajaye 20,000 ke. Producer consumer se 12× tez hai —
     koi bhi memory kam padegi. Tumne bimari ko der se aane wala bana diya.</p>

     <p><b>⚠️ Better: "Producer ko dheema karo (rate limit)"</b><br>
     Kabhi kabhi sahi, par aksar tum producer ko control nahi karte (woh 100 alag
     services ho sakti hain), aur traffic ghatana matlab business ghatana. Aur
     Kafka mein producer aur consumer <b>decoupled</b> hain — producer ko consumer
     ki raftaar se baandhna us decoupling ko hi tod dena hai.</p>

     <p><b>✅ Real: Consumer ko utna hi khinchne do jitna woh paccha sakta hai</b><br>
     Kafka ka sabse bada tohfa yahi hai — woh <b>PULL model</b> hai:</p>
     <pre>PUSH model (RabbitMQ default):
   broker consumer ko messages DHAKELTA hai
   consumer dheema? messages uske buffer mein bharte rehte → OOM
   (isliye RabbitMQ mein prefetch/QoS itna zaroori hai)

PULL model (Kafka):
   consumer broker se utna MAANGTA hai jitna woh sambhal sake
   consumer dheema? woh kam maangega → messages LOG mein pade rehte
   (log toh disk par hai, tumhare consumer ki RAM mein nahi!)
   → yahi NATURAL back-pressure hai

Bas ek cheez control karo:
   max.poll.records = 100    (10000 nahi!)
   → har poll sirf 100 records memory mein
   → consumer 100 process karega, TAB agla poll
   → memory bounded, OOM namumkin</pre>`,

    `<h3>⚖️ Nayi musibat: kabhi kabhi peeche rehna kaafi nahi</h3>
     <p>Pull model se consumer OOM nahi hota — par ek nayi problem hai:
     agar consumer <b>hamesha</b> peeche rehta hai (lag lagataar badhta hai),
     toh sirf "chhota batch" kaafi nahi. Tumhe faisla lena padega:</p>
     <pre>🐢 Lag badh raha hai par data ZAROORI hai (orders, payments):
   → consumers/partitions badhao (capacity badhao)
   → yeh asli capacity problem hai, tal nahi sakte

⚡ Lag badh raha hai par data PURANA hone par bekaar hai (live metrics,
   location pings, stock ticks):
   → LOAD SHEDDING karo — purane messages chhod do,
     seedhe latest par jump karo (offset reset to latest)
   → "60 second purana location ab kisi kaam ka nahi"

🚦 Downstream ko bacha na hai (woh tumse dheema hai):
   → RATE LIMITING — consumer khud ko dheema karo
     (pause/resume) taaki downstream na gire
   → yeh back-pressure ko AAGE pass karna hai</pre>
     <p><b>Load shedding vs rate limiting ka farak:</b><br>
     🗑️ <b>Load shedding</b> = kaam <b>chhod</b> do (data phenko, jab purana bekaar ho).<br>
     🚦 <b>Rate limiting</b> = kaam <b>dheema</b> karo (data rakho, par slow — jab downstream bachana ho).<br>
     Dono back-pressure ke jawab hain, par ek data phenkta hai aur doosra sambhaal ke rakhta hai.</p>`,
  ],
  stages: [
    {
      type: 'lab',
      title: '🎛️ Lab: consumer ko OOM se bachao',
      instructions: 'prefetch (max.poll.records) badha ke OOM banao, phir ghata ke theek karo. In-flight memory par dhyan do.',
      scenario: `Fast producer <b>3,000 msg/s</b> bhej raha hai. Consumers dheeme hain
        (40ms per message). Abhi <b>prefetch 5,000</b> par hai — har consumer 5,000
        messages memory mein bhar leta hai. Memory limit 20,000 messages hai.`,
      goal: `Consumer ko OOM se bachao — <b>oomEvents = 0</b> — aur phir bhi kaam
        chalta rahe (<b>completed > 20,000</b>). Sochо: consumer ko ek saath kitne
        messages memory mein rakhne chahiye?`,
      duration: 90,
      preset: { producerRate: 3000, consumers: 8, processMs: 40, failRate: 0,
                retry: 'none', dlq: false, prefetch: 5000, memLimitMsgs: 20000,
                downstreamCapacity: 6000 },
      knobs: ['prefetch', 'consumers'],
      charts: ['lag', 'throughput'],
      success: r => r.oomEvents === 0 && r.completed > 20000,
      verdict: r => {
        if (r.oomEvents > 0)
          return `💥 <b>${r.oomEvents} baar OOM hua</b> — peak in-flight
            ${r.peakInflight.toLocaleString()} messages memory mein the (limit 20,000).
            prefetch × consumers = ${r.cfg.prefetch} × ${r.cfg.consumers} =
            <b>${(r.cfg.prefetch * r.cfg.consumers).toLocaleString()}</b> messages ek saath
            RAM mein aa sakte hain. Har OOM par pod mara aur ~${5}s tak kuch process nahi hua.`;
        return `✅ <b>Koi OOM nahi.</b> Peak in-flight sirf ${r.peakInflight.toLocaleString()}
          messages (limit 20,000 se neeche). prefetch chhota rakhne se consumer ek saath
          utna hi memory mein leta hai jitna paccha sake — baaki messages LOG mein safe
          pade rehte hain (disk par, tumhari RAM mein nahi). Yahi natural back-pressure hai.`;
      },
      successText: `🎉 Tumne Kafka ke pull-model ka poora fayda dekh liya:<br><br>
        💥 <b>Bada prefetch:</b> consumer 5,000 × 8 = 40,000 messages ek saath RAM mein
        khinch leta hai — memory limit paar → OOM → pod mara → rebalance → phir wahi.
        "Performance" ke naam par kiya gaya yeh sabse aam bug hai.<br><br>
        💾 <b>Chhota prefetch:</b> consumer sirf utne messages memory mein leta hai
        jitne woh turant process kar sake. Baaki <b>log mein pade rehte hain</b> —
        aur log disk par hai, tumhari RAM mein nahi. Consumer dheema ho toh woh bas
        kam maangega. <b>OOM namumkin.</b><br><br>
        <b>Yaad rakho:</b> <code>max.poll.records</code> consume karne ki RAFTAAR
        nahi badhata — woh sirf ek saath kitni memory bhari jaayegi woh tay karta hai.
        Chhota rakho. Kafka ka pull-model tumhare liye back-pressure khud sambhal leta hai.`,
      hints: [
        'prefetch × consumers = ek saath memory mein aane wale max messages. Abhi 5000 × 8 = 40,000, aur limit 20,000 hai.',
        'prefetch ko bahut chhota karo — 100 ya 200. Consumer ab sirf itne records ek saath memory mein rakhega.',
        'prefetch 100 se OOM khatam ho jaayega. Consumer dheema hai toh bhi theek — messages log mein safe hain, RAM mein nahi.',
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Back-pressure ka jawab: shed, limit, ya scale?',
      instructions: 'Har situation ke liye sahi jawab chuno. Sochо: data purana hone par bekaar hai? Downstream ko bachana hai? Ya sach mein capacity kam hai?',
      bins: [
        { id: 'shed', label: '🗑️ Load shed (purana data phenko)' },
        { id: 'limit', label: '🚦 Rate limit (consumer dheema karo)' },
        { id: 'scale', label: '📈 Scale (capacity badhao)' },
      ],
      items: [
        { text: 'Live GPS location pings, consumer 5 min peeche', bin: 'shed',
          explain: '5 minute purana location bekaar hai — bas latest chahiye. Purane pings chhod do (offset latest par jump), warna hamesha peeche rehoge.' },
        { text: 'Payment events, lag lagataar badh raha hai', bin: 'scale',
          explain: 'Payment kabhi chhod nahi sakte (load shed nahi), aur dheema karna lag aur badhayega. Yeh asli capacity problem hai — consumers/partitions badhao.' },
        { text: 'Consumer ek dheeme third-party API ko call karta hai jo gir raha hai', bin: 'limit',
          explain: 'Downstream tumse dheema hai — use bachana hai. Consumer ko rate-limit karo (pause/resume) taaki API na gire. Back-pressure ko aage pass karo.' },
        { text: 'Real-time stock price ticks, consumer peeche', bin: 'shed',
          explain: 'Purana stock price bekaar hai — trader ko latest chahiye. Purane ticks chhod ke latest par jump karo.' },
        { text: 'Order pipeline, Black Friday, lag badh raha hai', bin: 'scale',
          explain: 'Orders har haal mein process hone hain. Load shed = orders phenkna (namumkin), rate limit = aur peeche. Sirf capacity badhana hi jawab hai.' },
        { text: 'Consumer ek DB ko likhta hai jiska connection pool bhar raha hai', bin: 'limit',
          explain: 'DB ko bachana hai. Consumer ko dheema karo taaki DB ka pool na phate. Data zaroori hai (phenk nahi sakte), bas dheere daalo.' },
        { text: 'Analytics events (clickstream), consumer peeche par sab chahiye', bin: 'scale',
          explain: 'Analytics ke liye aksar poora data chahiye (sampling na ho toh). Load shed nahi kar sakte. Consumers badhao. Agar sampling OK hai toh shed bhi option hai.' },
        { text: 'Sensor readings jahan sirf latest value matter karti hai', bin: 'shed',
          explain: 'Sirf latest reading chahiye (jaise temperature). Purane readings chhod do — ya behtar, log compaction use karo (agla level) jo purani values apne aap uda deta hai.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Back-pressure ke sawal',
      questions: [
        {
          q: 'Kafka mein fast producer + slow consumer se consumer OOM kab hota hai?',
          choices: [
            'Hamesha — yeh Kafka ki kami hai',
            'Kabhi nahi, agar prefetch/max.poll.records sahi ho — Kafka PULL model hai, consumer utna hi maangta hai jitna sambhal sake, baaki messages log (disk) par pade rehte hain. OOM tab hota hai jab tum max.poll.records bahut bada rakh do aur consumer ek saath zyada memory mein bhar le.',
            'Jab producer 2× tez ho',
            'Jab partitions kam hon',
          ],
          answer: 1,
          explain: 'Yeh Kafka ka pull-model ka core fayda hai. RabbitMQ (push) mein slow consumer par broker messages dhakelta rehta hai — wahan OOM asaan hai. Kafka mein OOM lagbhag hamesha ek config galti (bada prefetch) hoti hai.',
        },
        {
          q: 'max.poll.records ko 500 se 10,000 karne se kya hota hai?',
          choices: [
            'Consumer 20× tez ho jaata hai',
            'Consume karne ki RAFTAAR nahi badhti (woh processing speed se bandhi hai) — bas ek poll mein 10,000 records MEMORY mein aa jaate hain. Agar consumer dheema hai toh yeh sidha OOM ki taraf le jaata hai. Yeh "performance" optimization ulta padta hai.',
            'Lag kam ho jaata hai',
            'Kuch nahi badalta',
          ],
          answer: 1,
          explain: 'Yeh sabse aam "performance" galti hai. Bada batch throughput thoda badha sakta hai agar processing tez ho, par slow consumer ke saath woh sirf memory pressure aur OOM laata hai. Chhota rakho.',
        },
        {
          q: 'Load shedding aur rate limiting mein farak?',
          choices: [
            'Dono same hain',
            'Load shedding = kaam CHHOD do (data phenko) — jab purana data bekaar ho (live location, stock ticks). Rate limiting = kaam DHEEMA karo (data rakho par slow) — jab downstream ko bachana ho. Ek data phenkta hai, doosra sambhaal ke rakhta hai.',
            'Load shedding sirf Kafka mein hai',
            'Rate limiting data phenkta hai',
          ],
          answer: 1,
          explain: 'Yeh do alag tools hain do alag problems ke liye. Load shedding: "yeh data ab bekaar hai, aage badho". Rate limiting: "yeh data zaroori hai par dheere lo taaki downstream na gire".',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Tumhara producer consumer se bahut tez hai. Kafka mein iska kya hota hai?"',
        choices: [
          'Consumer OOM ho jaayega',
          'Kuch bura nahi — Kafka pull-model hai, isliye tez producer ke messages LOG (disk) mein jama hote hain, consumer ki RAM mein nahi. Consumer apni raftaar se pull karta hai; lag badhega par woh crash nahi hoga. Bas main max.poll.records chhota rakhunga taaki har poll bounded memory le. Agar lag lagataar badhta rahe toh woh asli capacity problem hai — tab consumers/partitions badhaunga, ya agar data purana hone par bekaar hai toh load shedding karunga.',
          'Producer ko error milega',
          'Broker crash ho jaayega',
        ],
        answer: 1,
        explain: 'Yeh push vs pull ka core sawal hai. Achha jawab batata hai ki (1) Kafka mein log buffer ka kaam karta hai, (2) OOM ek config galti hai natural nahi, (3) lagataar lag ek alag (capacity) problem hai.',
      },
      {
        q: '🧑‍💼 "Consumer ek dheeme downstream ko call karta hai jo overload ho raha hai. Kaise sambhaloge?"',
        choices: [
          'Consumers double kar dunga',
          'Consumers BADHANA yahan galat hai — woh downstream par aur load daalega. Yeh back-pressure ko aage pass karne ka case hai: consumer ko rate-limit karunga (pause/resume ya semaphore) taaki downstream ki capacity se zyada na maare. Messages log mein safe pade rahenge (lag badhega par woh theek hai). Aur main downstream ke liye ek circuit breaker bhi lagaunga taaki jab woh sach mein gir jaaye toh usse maarna hi band ho.',
          'max.poll.records badha dunga',
          'Producer band kar dunga',
        ],
        answer: 1,
        explain: 'Yeh dikhata hai ki tum back-pressure ko poori chain mein samajhte ho. Aam galti "consumers badha do" hoti hai — par jab bottleneck downstream hai, toh zyada consumers use aur tez maarenge. Rate limiting + circuit breaker sahi jodi hai.',
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki bada max.poll.records se consumer OOM kaise hota hai',
    'Main jaanta hoon ki Kafka pull-model se natural back-pressure kaise deta hai',
    'Main prefetch/max.poll.records se memory bound kar sakta hoon',
    'Main load shedding, rate limiting aur scaling mein sahi chunav kar sakta hoon',
  ],
  homework: `<p>Back-pressure ko dekho — pull model ka fayda:</p>
    <pre>// SLOW consumer, chhota batch
const consumer = kafka.consumer({
  groupId: 'bp-test',
  maxBytesPerPartition: 1024 * 100,   // chhota — bounded memory
});
await consumer.run({
  eachBatchAutoResolve: false,
  eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
    console.log('batch size:', batch.messages.length);  // dekho kitna aaya
    for (const message of batch.messages) {
      await slowWork(message);           // 40ms per message
      resolveOffset(message.offset);
      await heartbeat();                 // rebalance se bacho
    }
  },
});</pre>
    <p><b>Asli experiment:</b> ek tez producer chalao (10,000 messages) aur yeh slow
    consumer. Dekho ki consumer <b>crash nahi hota</b> — woh bas dheere-dheere
    process karta hai aur lag badhta hai. Messages log mein safe hain. Ab
    <code>maxBytesPerPartition</code> bahut bada karo aur RAM usage dekho —
    yahi woh OOM wala farak hai.</p>`,
  cards: [
    { front: 'Kafka mein slow consumer OOM kab hota hai?', back: 'Lagbhag hamesha ek <b>config galti</b> se — <code>max.poll.records</code> bahut bada rakhna. Kafka pull-model hai: consumer utna hi maangta hai jitna sambhal sake, baaki messages <b>log (disk)</b> par pade rehte hain, RAM mein nahi.' },
    { front: 'Push vs Pull model — back-pressure ke liye?', back: '<b>Push</b> (RabbitMQ): broker consumer ko messages dhakelta hai — slow consumer par uska buffer bharta hai → OOM (isliye prefetch/QoS zaroori).<br><b>Pull</b> (Kafka): consumer utna maangta hai jitna paccha sake — natural back-pressure, messages log mein pade rehte hain.' },
    { front: 'max.poll.records bada karne se kya hota hai?', back: 'Consume ki <b>raftaar nahi badhti</b> (woh processing speed se bandhi hai) — bas ek saath zyada records <b>memory mein</b> aa jaate hain. Slow consumer ke saath yeh sidha OOM. Chhota rakho.' },
    { front: 'Load shedding vs rate limiting?', back: '<b>Load shedding</b> = data <b>phenko</b> (jab purana bekaar ho — live location, stock ticks).<br><b>Rate limiting</b> = data rakho par <b>dheema</b> karo (jab downstream ko bachana ho). Ek phenkta hai, doosra sambhaalta hai.' },
    { front: 'Lag lagataar badh raha hai — teen options?', back: '1) Data zaroori (orders) → <b>scale</b> (consumers/partitions badhao)<br>2) Data purana hone par bekaar (location) → <b>load shed</b> (latest par jump)<br>3) Downstream ko bachana hai → <b>rate limit</b> (consumer dheema karo)' },
  ],
},

/* =====================================================================
   7. CIRCUIT BREAKER
   ===================================================================== */
{
  id: 'circuit-breaker',
  icon: '⚡',
  title: 'Circuit Breaker — 2 lakh retries ne downstream ko uthne hi nahi diya',
  badge: 'Breaker Baba',
  objectives: [
    'Samjho ki down downstream par retry karna recovery ko kaise rokta hai',
    'Circuit breaker ke teen states seekho: closed / open / half-open',
    'Jaano ki breaker jitter/retry se alag KYUN aur zaroori hai',
    'Bulkhead pattern ka basic idea samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "Downstream utha, par humne use turant dobara gira diya"</h3>
     <p>Ek notification consumer SMS gateway ko call karta tha. Gateway 2 minute ke
     liye gira. Consumer ke paas achha jitter-wala exponential backoff tha
     (pichla level yaad hai). Toh usne retry kiye — theek se, bikhre hue.</p>
     <p>Par yahan ek gehri problem thi jise backoff hal nahi karta:</p>
     <pre>Gateway down hai. Har message:
   try → 30s timeout → fail → backoff → retry → 30s timeout → fail …

2 lakh messages ka backlog, har ek gateway ko HIT kar raha hai
(chahe bikhre hue), har ek 30 second connection HOLD kar raha hai

Gateway 2 minute baad UTHNA chahta hai —
par uski saari connections humare timeout-wale retries se BHARI hain
→ woh naye (asli) requests le hi nahi paata
→ health check bhi fail → woh "down" hi maana jaata hai
→ humare retries use uthne hi nahi de rahe 🔥</pre>
     <div class="war-story"><b>⚡ Backoff kaafi nahi tha.</b> Jitter ne retries ko
     bikhera zaroor, par woh phir bhi ho rahe the — aur har retry ek marte hue
     downstream par ek aur zakhm tha. 2-minute ka outage 25 minute chala.
     Asli ilaaj yeh tha ki jab downstream sach mein down ho, toh usse maarna hi
     BAND kar do — usse saans lene do. Yahi circuit breaker hai.</div>`,

    `<h3>🪜 Circuit breaker ke teen states</h3>
     <p>Circuit breaker ghar ke electrical breaker jaisa hai — jab bahut current aaye
     (bahut failures), woh "trip" ho jaata hai aur circuit KAAT deta hai. Teen states:</p>
     <pre>🟢 CLOSED (normal)
   requests downstream ko jaate hain
   failures gino — agar failure rate > threshold (jaise 50%)
   → OPEN ho jao

🔴 OPEN (downstream down maana ja raha hai)
   koi request downstream ko NAHI jaata — turant FAIL FAST
   (ya DLQ/fallback), bina timeout ka intezaar kiye
   → downstream ko SAANS milti hai, woh recover kar leta hai
   cooldown ke baad → HALF-OPEN

🟡 HALF-OPEN (test kar rahe hain)
   sirf EK (ya kuch) test request jaane do
   chal gaya? → CLOSED (normal wapas)
   fail hua?  → OPEN (aur cooldown do)</pre>
     <p>Sabse zaroori state <b>OPEN</b> hai: jab downstream down hai, breaker
     requests ko turant fail kar deta hai — <b>bina 30-second timeout ke intezaar</b>,
     aur <b>bina downstream ko chhue</b>. Isse do fayde: (1) downstream ko recover
     karne ka mauka milta hai, (2) tumhare consumers 30s ke timeouts mein phanste
     nahi, toh woh bhi nahi marte.</p>`,

    `<h3>⚖️ Backoff vs breaker, aur bulkhead</h3>
     <p><b>Jitter/backoff aur circuit breaker alag cheezein hain — dono chahiye:</b></p>
     <pre>⏳ BACKOFF + JITTER: retry ke WAQT ko sudharta hai
   "agar retry karna hai, toh kab aur kaise bikhar ke"
   → transient (chhoti) failures ke liye

⚡ CIRCUIT BREAKER: retry ki ZAROORAT ko khatam karta hai
   "downstream sach mein down hai — retry karo hi mat"
   → sustained (lambi) failures ke liye

Dono ka rishta:
   chhoti failure → backoff+jitter se retry (breaker CLOSED rehta hai)
   bahut failures → breaker OPEN → retry hi band → downstream ko saans</pre>

     <h4>🧱 Aur ek saathi pattern: bulkhead</h4>
     <p>Bulkhead jahaaz ke alag-alag water-tight compartments jaise hai — ek mein
     paani bhare toh poora jahaaz na doobe. Software mein:</p>
     <pre>❌ Bina bulkhead: ek shared thread pool (100 threads)
   downstream A dheema hua → uske calls saare 100 threads le lete hain
   → downstream B, C ke calls ko bhi thread nahi milta
   → ek dheeme downstream ne SAB kuch rok diya

✅ Bulkhead: har downstream ka apna alag pool
   A ke liye 30 threads, B ke liye 30, C ke liye 30
   A dheema hua → sirf A ke 30 threads phanse
   → B aur C bilkul theek chalte rahe
   → failure ISOLATE ho gayi</pre>
     <p><b>Poora reliability stack:</b> timeout (har call ki seema) → retry with
     jitter (transient ke liye) → circuit breaker (sustained ke liye) →
     bulkhead (isolation) → DLQ (jo phir bhi fail ho). Har ek ek alag failure
     mode sambhaalta hai.</p>`,
  ],
  stages: [
    {
      type: 'lab',
      title: '🎛️ Lab: circuit breaker se retry-barbaadi roko',
      instructions: 'Breaker OFF rakh ke dekho ki outage ke doraan kitne retries barbaad hote hain, phir ON karke tune karo.',
      scenario: `Consumer ek downstream ko call karta hai jo <b>15s se 55s tak POORA down</b>
        rahega. Achha jitter-wala backoff already ON hai. Downstream capacity (2,000/s)
        consumer se zyada hai — toh normal mein sab theek, sirf outage hi problem hai.
        Abhi <b>circuit breaker OFF</b> hai.`,
      goal: `Circuit breaker ON karke tune karo taaki outage ke doraan
        <b>retry-barbaadi kam ho</b> — total retries <b>15,000 se neeche</b> —
        aur phir bhi <b>completed 75,000 se upar</b> rahe (yaani throughput na gire).`,
      duration: 120,
      preset: { producerRate: 800, consumers: 12, processMs: 10, failRate: 0.02,
                retry: 'expJitterFull', baseDelay: 1, maxRetries: 10, dlq: true,
                downstreamCapacity: 2000, outage: [15, 55],
                breaker: false, breakerThreshold: 0.5, breakerCooldown: 10 },
      knobs: ['breaker', 'breakerThreshold', 'breakerCooldown'],
      charts: ['lag', 'retryRate', 'throughput'],
      success: r => r.breakerOpens > 0 && r.totalRetries < 15000 && r.completed > 75000,
      verdict: r => {
        if (!r.cfg.breaker || r.breakerOpens === 0)
          return `🔴 Breaker OFF (ya kabhi khula hi nahi) — outage ke doraan consumer
            ne <b>${r.totalRetries.toLocaleString()} retries</b> phenke, har ek down
            downstream par ek zakhm. Bekaar mehnat: throughput utna hi raha jitna breaker
            ke saath hota, bas downstream ko aur pit-ta raha.`;
        if (r.completed <= 75000)
          return `⚠️ Breaker khula (${r.breakerOpens} baar) par cooldown <b>bahut lamba</b>
            (${r.cfg.breakerCooldown}s) — retries toh bach gaye (${r.totalRetries.toLocaleString()})
            par breaker outage khatam hone ke baad bhi band raha, isliye
            <b>throughput gir gaya</b> (${r.completed.toLocaleString()} completed). Cooldown ghatao —
            breaker ko jaldi check karna chahiye ki downstream wapas aaya ya nahi.`;
        return `✅ <b>Balance mil gaya!</b> Breaker ${r.breakerOpens} baar khula,
          retries sirf <b>${r.totalRetries.toLocaleString()}</b> (breaker OFF mein
          ~50,000 hote), aur throughput poora <b>${r.completed.toLocaleString()}</b>.
          Breaker ne outage ke doraan retry BAND kiya (downstream ko saans di), aur
          cooldown ke baad half-open probe se check kiya ki woh wapas aaya — phir normal.`;
      },
      successText: `🎉 Tumne circuit breaker ka poora point dekh liya:<br><br>
        🔴 <b>Breaker OFF:</b> outage ke doraan consumer ~50,000 retries phenkta hai —
        har ek marte hue downstream par ek aur zakhm. Woh retries kuch achieve nahi
        karte (throughput wahi rehta hai), bas downstream ko uthne nahi dete.<br><br>
        ⚡ <b>Breaker ON (tuned):</b> failure rate threshold paar karte hi breaker
        OPEN ho jaata hai — retry BAND, fail fast. Downstream ko saans milti hai.
        Cooldown ke baad half-open ek probe bhejta hai; downstream wapas aaya toh
        breaker CLOSED, normal chalu.<br><br>
        <b>Sabse zaroori seekh:</b> tumne dekha ki cooldown <b>bahut lamba</b> karne se
        throughput girta hai (breaker downstream ke wapas aane ke baad bhi band rehta hai).
        Yeh circuit breaker ka asli trade-off hai: jaldi check karo (throughput bachao)
        vs downstream ko zyada saans do. Aur yaad rakho — <b>backoff retry ke waqt ko
        sudharta hai, breaker retry ki zaroorat ko khatam karta hai. Dono chahiye.</b>`,
      hints: [
        'Circuit Breaker ko ON karo. Ab jab failure rate threshold paar karega, breaker khulega aur retry band ho jaayega.',
        'breakerThreshold 0.5 theek hai (50% fail par khulo). breakerCooldown ko medium rakho — 3 se 10 second.',
        'Agar throughput gir raha hai toh cooldown bahut lamba hai — usse ghatao (3-5s) taaki breaker jaldi check kare ki downstream wapas aaya.',
      ],
    },
    {
      type: 'order',
      title: '🔢 Circuit breaker ke states ka safar',
      instructions: 'Downstream ke girne se uthne tak breaker ka safar sahi kram mein lagao.',
      steps: [
        { text: '🟢 CLOSED: requests normal ja rahe hain, failures gine ja rahe hain',
          explain: 'Shuruaati state. Sab kuch theek chal raha hai. Breaker sirf failure rate par nazar rakh raha hai.' },
        { text: '📈 Failure rate threshold (jaise 50%) paar kar gaya',
          explain: 'Downstream girne laga. Ek rolling window mein aadhe se zyada requests fail ho rahe hain. Breaker ko trip karne ka signal.' },
        { text: '🔴 OPEN: breaker trip — ab koi request downstream ko nahi jaata (fail fast)',
          explain: 'Yahi sabse zaroori state hai. Requests turant fail (ya DLQ/fallback), bina timeout ke, bina downstream ko chhue. Downstream ko saans milti hai.' },
        { text: '⏳ Cooldown timer chalta hai (jaise 10 second)',
          explain: 'Breaker downstream ko recover hone ka waqt deta hai. Is dauran koi request nahi jaati. Cooldown ki lambai ek trade-off hai.' },
        { text: '🟡 HALF-OPEN: cooldown khatam — ek TEST request jaane do',
          explain: 'Breaker check karta hai ki downstream wapas aaya ya nahi — sirf ek (ya kuch) probe request se, poora load nahi.' },
        { text: '✅ Test chala? → CLOSED (normal). Fail hua? → OPEN (phir cooldown)',
          explain: 'Half-open ka natija tay karta hai. Chal gaya toh normal wapas; fail hua toh phir se OPEN aur ek aur cooldown. Yeh loop tab tak chalta hai jab tak downstream sach mein wapas na aaye.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Circuit breaker ke sawal',
      questions: [
        {
          q: 'Achha jitter-wala backoff already hai. Circuit breaker ki kya zaroorat?',
          choices: [
            'Koi zaroorat nahi, backoff kaafi hai',
            'Backoff retry ke WAQT ko sudharta hai (kab aur kaise bikhar ke), par jab downstream SUSTAINED down ho toh backoff phir bhi retry karta rahega — har ek marte hue downstream par ek zakhm. Circuit breaker retry ki ZAROORAT ko khatam karta hai: down downstream par requests bhejna hi band kar deta hai, usse recover hone deta hai. Backoff chhoti failures ke liye, breaker lambi failures ke liye.',
            'Circuit breaker backoff ko replace karta hai',
            'Breaker sirf security ke liye hai',
          ],
          answer: 1,
          explain: 'Yeh sabse zaroori distinction hai. Log sochte hain "achha retry hai toh breaker nahi chahiye". Par retry (chahe kitna achha) ek DOWN service par bekaar hai — woh sirf use uthne nahi deta. Breaker retry rok deta hai.',
        },
        {
          q: 'Circuit breaker ka OPEN state kya karta hai?',
          choices: [
            'Requests ko queue mein rakhta hai',
            'Requests ko turant FAIL FAST karta hai — bina downstream ko chhue, bina timeout ke intezaar ke. Isse downstream ko recover hone ka mauka milta hai (koi naya load nahi) aur consumers 30s timeouts mein phanste nahi (toh woh bhi nahi marte).',
            'Requests ko dheema karta hai',
            'Downstream ko restart karta hai',
          ],
          answer: 1,
          explain: 'OPEN state ka poora point yahi "fail fast without touching downstream" hai. Dono taraf fayda: downstream ko saans, aur tumhare consumers ko timeout-hell se bachav.',
        },
        {
          q: 'HALF-OPEN state kyun zaroori hai?',
          choices: [
            'Woh zaroori nahi, OPEN se seedha CLOSED ho sakta hai',
            'Kyunki breaker ko pata karna hai ki downstream wapas aaya ya nahi — par woh POORA load ek saath nahi bhej sakta (woh use dobara gira dega). Half-open sirf EK (ya kuch) probe request bhejta hai; chala toh CLOSED, fail hua toh phir OPEN. Yeh downstream ko dobara girae bina test karne ka tareeqa hai.',
            'Woh performance ke liye hai',
            'Woh sirf logging ke liye hai',
          ],
          answer: 1,
          explain: 'Bina half-open ke breaker ko ya toh andha reopen karna padega (poora load, downstream phir gir sakta hai) ya hamesha band rehna. Half-open ek safe "test the water" mechanism hai — ek probe se recover check karo.',
        },
      ],
    },
    {
      type: 'match',
      title: '🔗 Reliability stack — kaunsa tool, kaunsi problem',
      instructions: 'Har failure mode ko usse sambhaalne wale pattern se jodo.',
      pairs: [
        { left: 'Ek call bahut lambe tak hang ho gaya', right: '⏱️ Timeout',
          explain: 'Har call ki ek seema honi chahiye. Timeout ke bina ek hang call thread ko hamesha ke liye rok deta hai.' },
        { left: 'Chhoti transient failure (503, network glitch)', right: '⏳ Retry with jitter',
          explain: 'Transient failures apne aap theek ho jaati hain. Jitter-wala backoff unhe bina thundering herd ke retry karta hai.' },
        { left: 'Downstream lambe samay se poori tarah down', right: '⚡ Circuit breaker',
          explain: 'Sustained failure par retry bekaar hai. Breaker requests band karke downstream ko saans deta hai.' },
        { left: 'Ek dheema downstream baaki sabke threads kha raha hai', right: '🧱 Bulkhead',
          explain: 'Alag-alag thread pools se failure isolate hoti hai — ek downstream ka dheemapan doosron ko nahi rokta.' },
        { left: 'Retries ke baad bhi permanently fail message', right: '💀 Dead Letter Queue',
          explain: 'Jo har koshish ke baad bhi fail ho, use DLQ mein bhejo taaki partition block na ho aur message gum na ho.' },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "Tumhare paas already retry with exponential backoff hai. Circuit breaker kyun chahiye?"',
        choices: [
          'Nahi chahiye, retry kaafi hai',
          'Woh do alag problems sambhaalte hain. Retry+backoff CHHOTI, transient failures ke liye hai — "shayad agli baar chal jaaye". Par jab downstream SUSTAINED down ho, toh retry (chahe kitna achha bikhra hua) sirf ek marte hue service ko pit-ta rehta hai aur use uthne nahi deta. Circuit breaker isse pehchanta hai (failure rate threshold) aur retry BAND kar deta hai — fail fast, downstream ko saans do, phir half-open probe se check karo ki woh wapas aaya. Ek retry ke waqt ko sudharta hai, doosra retry ki zaroorat ko khatam karta hai.',
          'Circuit breaker retry se tez hai',
          'Woh sirf microservices mein chahiye',
        ],
        answer: 1,
        explain: 'Yeh interview mein akser poocha jaata hai kyunki bahut log dono ko confuse karte hain. Achha jawab clear karta hai: backoff = kaise retry karein, breaker = kya retry karein hi. Sustained failure par breaker retry ko poori tarah rok deta hai.',
      },
      {
        q: '🧑‍💼 "Ek downstream dheema hone se tumhari poori service atak jaati hai. Kya karoge?"',
        choices: [
          'Us downstream ko tez karne bol dunga',
          'Yeh classic thread-pool exhaustion hai jo bulkhead se theek hota hai. Abhi shayad ek shared thread pool hai — ek dheema downstream uske saare threads le leta hai, toh baaki downstreams ke calls ko bhi thread nahi milta. Main har downstream ke liye ALAG pool (bulkhead) dunga taaki ek ka dheemapan doosron ko isolate rahe. Saath mein us dheeme downstream ke liye timeout + circuit breaker taaki jab woh sach mein gir jaaye toh use maarna band ho.',
          'Zyada threads add kar dunga',
          'Us downstream ke calls hata dunga',
        ],
        answer: 1,
        explain: 'Yeh bulkhead pattern ka core hai (aur Track 1 ke thread-pool exhaustion se connect karta hai). Shared resource pool ek failure ko poore system mein faila deta hai; bulkhead use compartmentalize karta hai. Breaker + timeout ke saath yeh complete answer hai.',
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki down downstream par retry recovery ko kaise rokta hai',
    'Main circuit breaker ke teen states (closed/open/half-open) bata sakta hoon',
    'Main jaanta hoon ki breaker backoff se alag KYUN hai (zaroorat vs waqt)',
    'Main bulkhead pattern se failure isolation samjha sakta hoon',
  ],
  homework: `<p>Circuit breaker ka behaviour dekho (opossum library ya khud):</p>
    <pre>// simple circuit breaker
class CircuitBreaker {
  constructor(threshold = 0.5, cooldownMs = 10000) {
    this.state = 'CLOSED'; this.fails = 0; this.total = 0;
    this.threshold = threshold; this.cooldownMs = cooldownMs;
  }
  async call(fn) {
    if (this.state === 'OPEN') {
      if (Date.now() - this.openedAt > this.cooldownMs) this.state = 'HALF';
      else throw new Error('circuit OPEN — fail fast');   // downstream ko chhua bhi nahi
    }
    try {
      const r = await fn();
      if (this.state === 'HALF') { this.state = 'CLOSED'; this.fails = 0; this.total = 0; }
      return r;
    } catch (e) {
      this.fails++; this.total++;
      if (this.total > 20 && this.fails / this.total > this.threshold) {
        this.state = 'OPEN'; this.openedAt = Date.now();
      }
      throw e;
    }
  }
}</pre>
    <p><b>Asli experiment:</b> ek fake downstream banao jo pehle 2 minute fail kare
    phir theek ho jaaye. Bina breaker ke dekho ki tumhare saare calls 30s timeout
    mein phanste hain. Breaker ke saath dekho ki woh OPEN hote hi turant fail-fast
    karta hai (no timeout wait) aur downstream ke wapas aane par half-open se recover
    karta hai.</p>`,
  cards: [
    { front: 'Down downstream par retry karna kyun bura hai?', back: 'Har retry ek marte hue service par ek aur zakhm hai — woh recover nahi kar paata kyunki uski connections tumhare timeout-wale retries se bhari rehti hain. Ek 2-min outage 25-min ka ho jaata hai. Retry (chahe jitter-wala) recovery ko rok deta hai.' },
    { front: 'Circuit breaker ke teen states?', back: '<b>🟢 CLOSED:</b> normal, failures gino.<br><b>🔴 OPEN:</b> threshold paar — requests turant fail-fast (downstream ko chhue bina), use saans do.<br><b>🟡 HALF-OPEN:</b> cooldown ke baad ek probe request — chala toh CLOSED, fail toh phir OPEN.' },
    { front: 'Backoff vs circuit breaker?', back: '<b>Backoff+jitter</b> = retry ke <b>WAQT</b> ko sudharta hai (chhoti transient failures ke liye).<br><b>Circuit breaker</b> = retry ki <b>ZAROORAT</b> ko khatam karta hai (sustained failures — down downstream par retry hi band). Dono chahiye.' },
    { front: 'OPEN state ka fayda dono taraf?', back: '1) Downstream ko <b>saans</b> milti hai (koi naya load nahi) → recover kar leta hai.<br>2) Tumhare consumers 30s <b>timeouts mein phanste nahi</b> → woh bhi nahi marte. Fail fast, no timeout wait.' },
    { front: 'Bulkhead pattern kya hai?', back: 'Har downstream ka <b>alag thread pool</b> — jahaaz ke water-tight compartments jaise. Ek downstream dheema hua toh sirf uska pool phanse, baaki chalte rahein. Shared pool ek failure ko poore system mein faila deta hai.' },
  ],
},

]);

/* ===================== Levels 8-10: durability, retention, boss ===================== */
KQ.registerLevels('intermediate', [

/* =====================================================================
   8. REPLICATION, ISR & acks
   ===================================================================== */
{
  id: 'replication-isr',
  icon: '🛡️',
  title: 'Replication & ISR — ek broker mara, data gum ho gaya',
  badge: 'ISR Ka Rakshak',
  objectives: [
    'Samjho ki replication factor aur ISR kaise data bachaate hain',
    'Jaano ki acks=all + min.insync.replicas kyun ek jodi hai',
    'unclean leader election ka jaal pehchano',
    'Durability vs availability ka trade-off samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "Ek broker restart hua aur 4,000 orders gayab"</h3>
     <p>Ek team ne <code>orders</code> topic RF=3 ke saath banaya — teen copies,
     durable lagta hai. Par unhone <code>min.insync.replicas</code> set nahi kiya
     (default 1 reh gaya) aur producer <code>acks=1</code> par tha.</p>
     <pre>Partition 2: leader = broker-1, followers = broker-2, broker-3

Normal: producer → leader (broker-1) ne likha → acks=1 mila ✅
        (followers ne abhi copy nahi kiya)

t=0    broker-1 (leader) ne 4,000 recent messages likhe
       acks=1 → producer ko "ho gaya" mil gaya
       par broker-2, broker-3 ne woh abhi tak copy nahi kiye
t=1s   broker-1 CRASH (hardware fault)
       → leader election: broker-2 naya leader banta hai
       → par broker-2 ke paas woh 4,000 messages the hi nahi!
       → woh HAMESHA ke liye gaye 💀</pre>
     <div class="war-story"><b>⚡ RF=3 hone ke bawajood data gaya.</b> Kyunki
     replication factor sirf yeh kehta hai ki "kitni copies BAN sakti hain" —
     yeh guarantee nahi karta ki likhne se PEHLE woh copies ban chuki thin.
     Woh guarantee <code>acks=all</code> + <code>min.insync.replicas</code> deti
     hai. RF akela ek jhoothi tasalli hai.</div>`,

    `<h3>🪜 ISR aur do settings ki jodi</h3>
     <p><b>ISR = In-Sync Replicas</b> — woh replicas jo leader ke saath poori tarah
     up-to-date hain. Yeh ek jeevit (dynamic) list hai:</p>
     <pre>Partition 2:  replicas = [1, 2, 3]
              leader = 1
              ISR = [1, 2, 3]   ← teeno sync mein

broker-3 dheema/down hua → woh peeche reh gaya
              ISR = [1, 2]      ← broker-3 ISR se nikal gaya

broker-3 wapas catch up kiya → ISR = [1, 2, 3] phir se</pre>
     <p><b>Ab do settings jo SAATH mein durability deti hain:</b></p>
     <pre>1️⃣ acks=all
   producer ko ACK tab milega jab SAARE ISR replicas ne likh liya
   (na ki sirf leader)

2️⃣ min.insync.replicas=2  (RF=3 ke saath)
   agar ISR 2 se neeche gir jaaye, toh acks=all waale produce
   REJECT ho jaayenge (NotEnoughReplicasException)

Dono saath = guarantee:
   har message likhne se pehle KAM SE KAM 2 replicas par hai
   → ek broker mara toh bhi doosre ke paas copy hai → data safe</pre>
     <p>Yeh Track 1 ke producer level se aage hai — wahan tumne acks seekha, ab
     dekho ki <code>min.insync.replicas</code> uska aadha-adhoora hissa poora karta hai.
     <code>acks=all</code> akela ISR ke 1 tak girne par chup-chaap <code>acks=1</code>
     ban jaata hai; <code>min.insync.replicas=2</code> use rokta hai.</p>`,

    `<h3>⚖️ Nayi musibat: unclean leader election aur availability ka mol</h3>
     <p>Durability free nahi hai — uska mol availability hai. Yeh dekho:</p>
     <pre>min.insync.replicas=2, RF=3, do brokers gir gaye
   → ISR mein sirf 1 (leader) bacha
   → acks=all waale produce ab REJECT (NotEnoughReplicas)
   → yaani likhna RUK gaya

   Yeh trade-off hai: tumne DURABILITY chuni AVAILABILITY ke upar.
   "Chup-chaap data khone se behtar hai ki likhna ruk jaaye."
   Payments ke liye sahi. Metrics ke liye shayad nahi.</pre>

     <h4>☠️ Aur ek chhupa hua qatil: unclean.leader.election</h4>
     <pre>Saare ISR replicas gir gaye. Sirf ek OUT-OF-SYNC replica zinda hai
(jo leader se peeche tha). Ab kya?

unclean.leader.election.enable = false  (DEFAULT, safe)
   → koi leader nahi banega jab tak koi ISR replica wapas na aaye
   → partition OFFLINE rehti hai (availability gayi)
   → par jo data committed tha woh SAFE hai

unclean.leader.election.enable = true  (KHATARNAAK)
   → woh out-of-sync replica leader ban jaayega
   → partition wapas online (availability aa gayi)
   → PAR: us replica ke paas jo messages nahi the woh
     ab HAMESHA ke liye gaye — committed data bhi TRUNCATE ho gaya 💀</pre>
     <p><b>Rule:</b> jahan data zaroori hai (orders, payments, ledger) wahan
     <code>unclean.leader.election.enable=false</code> rakho — availability se pehle
     durability. Jahan availability zaroori hai aur thoda data khona chalega
     (metrics, logs) wahan <code>true</code> theek ho sakta hai. Yeh ek business
     faisla hai, technical default nahi.</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: ISR shrink aur acks=all ko test karo',
      instructions: 'Brokers maar ke dekho ki min.insync.replicas kaise data ko chup-chaap khone se bachata hai.',
      goal: `<code>payments</code> topic hai: RF=3, <code>min.insync.replicas=2</code>.<br><br>
        1. Topic describe karo — ISR dekho (abhi teeno replicas sync mein)<br>
        2. Ek broker maaro (<code>sim broker kill</code>) — ISR shrink dekho, par acks=all abhi bhi chalega<br>
        3. Doosra broker bhi maaro — ab ISR sirf 1<br>
        4. <code>acks=all</code> se produce karo — dekho ki woh <b>REJECT</b> hota hai (yeh feature hai!)`,
      cluster: {
        brokers: 3,
        topics: { payments: { partitions: 1, rf: 3, config: { 'min.insync.replicas': 2 } } },
      },
      tasks: [
        { text: 'topic describe karo (ISR dekho)', check: s => s.history.some(h => /kafka-topics.*--describe/.test(h)) },
        { text: 'do brokers maaro', check: s => s.brokers.filter(b => !b.alive).length >= 2 },
        { text: 'acks=all try karo (reject hona chahiye)', check: s => !!s.lastError && /NotEnoughReplicas/.test(s.lastError || '') },
      ],
      success: s => s.brokers.filter(b => !b.alive).length >= 2 &&
                    s.history.some(h => /kafka-topics.*--describe/.test(h)) &&
                    s.history.some(h => /--acks all/.test(h)),
      successText: `Bahut khoob. Tumne ISR ka poora khel dekh liya:<br><br>
        🛡️ <b>Ek broker gaya:</b> ISR 3 se 2 hua, par min.insync.replicas=2 abhi bhi
        pura hai — toh acks=all chalta raha. Ek failure jheli gayi.<br><br>
        🛑 <b>Doosra broker gaya:</b> ISR sirf 1 bacha, min.insync.replicas=2 se kam.
        Ab acks=all ne <b>NotEnoughReplicasException</b> se produce REJECT kar diya.<br><br>
        Yeh <b>feature hai, bug nahi</b>. Broker keh raha hai: "main is message ki
        surakshit copy nahi bana sakta, toh main JHOOTH nahi bolunga ki ho gaya."
        Agar iske bajaye woh chup-chaap likh leta (jaise acks=1), aur woh akela broker
        bhi gir jaata, toh woh message hamesha ke liye gum ho jaata — bilkul us 4,000
        orders wale incident ki tarah.`,
      hints: [
        'kafka-topics --describe --topic payments   — Isr: column dekho',
        'sim broker kill 2    (phir describe karke ISR shrink dekho)',
        'sim broker kill 3    phir   kafka-console-producer --topic payments --message "test" --acks all',
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Durability ya availability — kya chunoge?',
      instructions: 'Har workload ke liye sahi setting. Sochо: is data ka ek bhi message khona kitna mehnga hai?',
      bins: [
        { id: 'durable', label: '🛡️ Durability first (acks=all, minISR=2, unclean=false)' },
        { id: 'available', label: '⚡ Availability first (acks=1, unclean=true chal jaayega)' },
      ],
      items: [
        { text: 'Bank transactions / ledger', bin: 'durable',
          explain: 'Ek transaction khona = audit fail, paisa gum. Durability par koi samjhauta nahi. Likhna ruk jaana behtar hai data khone se.' },
        { text: 'Server CPU/memory metrics', bin: 'available',
          explain: 'Ek metric data point khona = chart mein chhota gap. Availability zyada zaroori hai — dashboard chalta rehna chahiye. acks=1 theek hai.' },
        { text: 'E-commerce orders', bin: 'durable',
          explain: 'Order khona = customer ka paisa aur bharosa gaya. Durability first. Yahi woh 4,000-orders wala incident tha.' },
        { text: 'Application debug logs', bin: 'available',
          explain: 'Logs ki value low hai aur volume high. Thoda khona chalega, aur pipeline ka chalte rehna zyada zaroori hai.' },
        { text: 'Inventory stock changes', bin: 'durable',
          explain: 'Stock change khona = oversell ya phantom stock. Yeh paise se juda hai — durability first.' },
        { text: 'Real-time analytics clickstream', bin: 'available',
          explain: 'Aksar sampled/aggregate use hota hai — kuch events khona statistics ko nahi badalta. Availability aur throughput zyada matter karte hain.' },
        { text: 'Payment confirmation events', bin: 'durable',
          explain: 'Payment confirm hua par event kho gaya = customer ko double charge ya order gum. Kabhi bhi availability ke liye durability mat chhodo.' },
        { text: 'IoT sensor telemetry (hazaaron sensors)', bin: 'available',
          explain: 'Volume bahut high, per-reading value low, aur agla reading turant aa raha hai. Availability first — ek reading khona koi baat nahi.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Replication ke sawal',
      questions: [
        {
          q: 'Tumhare paas RF=3 hai. Kya tumhara data ek broker failure se safe hai?',
          choices: [
            'Haan, 3 copies hain',
            'Zaroori nahi. RF=3 sirf kehta hai ki kitni copies BAN sakti hain — yeh guarantee nahi karta ki likhne se PEHLE woh ban chuki thin. Agar acks=1 hai toh leader akela likh ke ACK de deta hai; woh mara toh followers ke paas woh messages nahi honge. Safety ke liye chahiye: acks=all + min.insync.replicas=2 + unclean.leader.election=false.',
            'Haan agar leader zinda ho',
            'Nahi, RF=5 chahiye',
          ],
          answer: 1,
          explain: 'Yeh sabse aam durability galtfehmi hai. RF ek CAPACITY setting hai (kitni copies possible), durability GUARANTEE nahi. Guarantee teen settings ki jodi se aati hai.',
        },
        {
          q: 'min.insync.replicas=2 (RF=3) hai. Do brokers gir gaye. Produce ka kya hoga?',
          choices: [
            'Chalta rahega, leader hai na',
            'acks=all waale produce REJECT ho jaayenge (NotEnoughReplicasException) — kyunki ISR sirf 1 (leader) bacha, jo min.insync.replicas=2 se kam hai. Yeh feature hai: chup-chaap data khone (agar woh akela leader bhi gir jaaye) se behtar hai ki likhna ruk jaaye. Durability ko availability ke upar chuna gaya.',
            'Data chup-chaap kho jaayega',
            'Broker restart ho jaayega',
          ],
          answer: 1,
          explain: 'Yeh durability vs availability ka core trade-off hai. Reject hona bura lagta hai par woh SAAF hai — tumhe pata chalta hai. Chup-chaap acks=1 par gir jaana aur phir data khona bahut bura hai.',
        },
        {
          q: 'unclean.leader.election.enable=true karne se kya risk hai?',
          choices: [
            'Koi risk nahi, availability badhti hai',
            'Agar saare ISR replicas gir jaayein aur sirf ek OUT-OF-SYNC replica zinda ho, toh woh leader ban jaayega — jisse partition wapas online ho jaayegi (availability), PAR us replica ke paas jo committed messages nahi the woh HAMESHA ke liye gum ho jaayenge (committed data truncate ho gaya). Data-critical topics par yeh false rakho.',
            'Broker slow ho jaata hai',
            'Sirf latency badhti hai',
          ],
          answer: 1,
          explain: 'unclean leader election availability ke liye durability ko bech deta hai — aur woh COMMITTED data ko truncate kar sakta hai, jo sabse buri kism ka data loss hai (jo tumne "safe" maana tha woh bhi gaya). Isliye default false hai.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "RF=3 laga diya. Ab data safe hai na?"',
        choices: [
          'Haan bilkul, 3 copies hain',
          'RF=3 zaroori hai par kaafi nahi — woh sirf kitni copies BAN sakti hain woh batata hai, na ki likhne se pehle woh ban chuki thin. Poori durability ke liye teen settings ki jodi chahiye: acks=all (saare ISR ne likha tab ACK), min.insync.replicas=2 (ISR 2 se neeche gaya toh produce reject, taaki acks=all chup-chaap acks=1 na ban jaaye), aur unclean.leader.election.enable=false (out-of-sync replica ko leader banne se roko, warna committed data truncate ho sakta hai). Yeh availability ka mol le kar durability deta hai — jo payments ke liye sahi trade-off hai.',
          'Haan agar acks=1 ho',
          'RF=5 karna padega',
        ],
        answer: 1,
        explain: 'Yeh senior-level durability jawab hai. Junior "RF=3, safe hai" bolta hai. Senior teeno settings, unka rishta, AUR trade-off (availability ka mol) batata hai. Yeh Track 1 ke acks level ka natural extension hai.',
      },
      {
        terminal: {
          title: '🖥️ Interview: durable topic banao',
          goal: `🧑‍💼 Interviewer: "Ek payments topic banao jo ek broker failure jhel sake
            bina data khoye. Batao ki tum kaunse config use karoge aur kyun.<br><br>
            Topic: <code>payments</code>, kam se kam 3 partitions, RF=3, aur
            <code>min.insync.replicas=2</code>. Phir ek broker maar ke dikhao ki
            acks=all abhi bhi chalta hai (ek failure jhel gaya)."`,
          cluster: { brokers: 3 },
          tasks: [
            { text: 'payments topic (RF=3, min.insync.replicas=2)',
              check: s => s.hasTopic('payments') && s.topics.payments.rf === 3 &&
                          String(s.topics.payments.config['min.insync.replicas']) === '2' },
            { text: 'ek broker maaro', check: s => s.brokers.some(b => !b.alive) },
            { text: 'acks=all se produce karo (chalna chahiye)',
              check: s => s.hasTopic('payments') && s.partitionCounts('payments').reduce((a, c) => a + c, 0) > 0 },
          ],
          success: s => s.hasTopic('payments') && s.topics.payments.rf === 3 &&
                        String(s.topics.payments.config['min.insync.replicas']) === '2' &&
                        s.brokers.some(b => !b.alive) &&
                        s.partitionCounts('payments').reduce((a, c) => a + c, 0) > 0,
          successText: `Bilkul sahi. RF=3 + min.insync.replicas=2 ka matlab hai ki har
            message kam se kam 2 brokers par hai. Ek broker gaya → ISR 2 raha →
            min.insync.replicas=2 pura → acks=all chala. <b>Ek failure jhel gayi,
            koi data nahi khoya.</b><br><br>
            Interview mein yahan bonus: bolo ki <b>"main unclean.leader.election.enable=false
            bhi rakhunga"</b> — taaki koi out-of-sync replica leader ban ke committed
            data truncate na kar de.`,
          hints: [
            'kafka-topics --create --topic payments --partitions 3 --replication-factor 3 --config min.insync.replicas=2',
            'sim broker kill 2',
            'kafka-console-producer --topic payments --message "pay-1" --acks all',
          ],
        },
      },
    ],
  },
  success: [
    'Main samjha sakta hoon ki RF akela durability guarantee nahi deta',
    'Main jaanta hoon ki acks=all + min.insync.replicas ek jodi kyun hai',
    'Main unclean leader election ke committed-data-loss risk ko samjha sakta hoon',
    'Main durability vs availability ka trade-off business terms mein bata sakta hoon',
  ],
  homework: `<p>ISR shrink apni aankhon se dekho:</p>
    <pre># durable topic banao
kafka-topics.sh --create --topic durable-test \\
  --partitions 1 --replication-factor 3 \\
  --config min.insync.replicas=2 \\
  --bootstrap-server localhost:9092

# ISR dekho — teeno replicas honge
kafka-topics.sh --describe --topic durable-test --bootstrap-server localhost:9092
# Replicas: 1,2,3   Isr: 1,2,3

# (multi-broker setup mein) ek broker band karo, phir describe:
# Isr: 1,3   ← ek replica ISR se nikal gaya, par min.insync=2 abhi pura

# acks=all se produce — chalega (2 replicas hain)
# doosra broker band karo → Isr: 1 → acks=all ab REJECT karega</pre>
    <p><b>Asli sabak:</b> <code>--describe</code> ka <b>Isr:</b> column woh live list
    hai jo batati hai ki abhi kitni surakshit copies hain. Jab woh
    min.insync.replicas se neeche jaaye, tumhare acks=all writes ruk jaayenge —
    aur yahi tumhe chup-chaap data loss se bachata hai.</p>`,
  cards: [
    { front: 'RF=3 se data safe hai?', back: 'Nahi — RF sirf kitni copies <b>BAN sakti hain</b> woh batata hai, na ki likhne se pehle woh ban chuki thin. Durability ke liye chahiye: <code>acks=all</code> + <code>min.insync.replicas=2</code> + <code>unclean.leader.election=false</code>. RF akela jhoothi tasalli hai.' },
    { front: 'ISR kya hai?', back: '<b>In-Sync Replicas</b> — woh replicas jo leader ke saath poori tarah up-to-date hain. Yeh ek DYNAMIC list hai: replica peeche gira toh ISR se nikal jaata hai, catch up kiya toh wapas aa jaata hai.' },
    { front: 'acks=all + min.insync.replicas=2 saath mein kya dete hain?', back: 'Guarantee ki har message likhne se pehle kam se kam 2 replicas par hai. <code>acks=all</code> akela ISR ke 1 tak girne par chup-chaap <code>acks=1</code> ban jaata hai; <code>min.insync.replicas=2</code> use rokta hai (produce reject karke).' },
    { front: 'unclean.leader.election.enable=true ka risk?', back: 'Saare ISR gir jaayein aur ek OUT-OF-SYNC replica zinda ho, toh woh leader ban jaayega — partition online (availability), PAR uske paas jo committed messages nahi the woh <b>truncate</b> ho jaate hain. Committed data loss = sabse buri kism. Default false.' },
    { front: 'min.insync.replicas=2, do brokers gire — produce ka kya?', back: 'acks=all waale produce <b>REJECT</b> (NotEnoughReplicasException) — ISR sirf 1 bacha. Yeh feature hai: durability ko availability ke upar chuna. "Chup-chaap data khone se behtar hai ki likhna ruk jaaye."' },
  ],
},

/* =====================================================================
   9. RETENTION & LOG COMPACTION
   ===================================================================== */
{
  id: 'retention-compaction',
  icon: '🗄️',
  title: 'Retention & Compaction — disk full se broker gir gaya',
  badge: 'Retention Rishi',
  objectives: [
    'Samjho ki retention (time/size) disk ko kaise sambhaalti hai',
    'Jaano ki peeche chhoot gaye consumer ko OffsetOutOfRange kaise milta hai',
    'Log compaction ka use-case seekho (cleanup.policy=compact)',
    'Tombstones aur compaction ke offsets samjho',
  ],
  lesson: [
    `<h3>😰 Dard: "3AM ko poora cluster gir gaya — disk full"</h3>
     <p>Ek team ne ek naya <code>debug-events</code> topic banaya troubleshooting ke
     liye. Bahut volume tha (~50,000 msg/s), par unhone default retention (7 din)
     rehne diya aur socha "baad mein dekhenge".</p>
     <pre>50,000 msg/s × ~1KB × 86,400 s/din × 7 din
= ~30 TB per week per partition set

Broker disk: 2 TB
t=0       topic bana
t=~10 ghante  disk 90% full → alerts (jo kisi ne dekhe nahi)
t=~11 ghante  disk 100% → broker naye writes accept nahi kar paaya
          → woh cluster se gir gaya
          → uski partitions doosre brokers par shift
          → unki disk bhi bharne lagi
          → 💥 CASCADING DISK FAILURE — teen brokers gir gaye</pre>
     <div class="war-story"><b>⚡ Ek debug topic ne poora cluster gira diya.</b>
     Retention ek "baad mein" wali setting nahi hai — woh disk capacity planning ka
     hissa hai. Aur disk-full ek broker ke liye sabse bura failure hai kyunki woh
     recovery ko bhi rok deta hai (log cleanup ke liye bhi thodi jagah chahiye).</div>`,

    `<h3>🪜 Retention: time aur size dono</h3>
     <p>Retention control karta hai ki purana data kab udega. Do knobs:</p>
     <pre>retention.ms   = 604800000   (7 din — waqt ke hisaab se)
retention.bytes = 1073741824  (1GB per partition — size ke hisaab se)

Jo PEHLE hit ho, wahi lagega. Best practice: DONO set karo —
   time se predictability, size se disk safety
   "7 din YA 1GB, jo pehle aaye"</pre>
     <p><b>Par retention ka ek chhupa hua asar hai</b> (jo tumne Track 1 mein chhua tha):</p>
     <pre>Consumer 10 din band raha. retention.ms = 7 din.
   → uska committed offset ab log mein hai HI NAHI
     (woh data retention ne uda diya)
   → consumer chalu → OffsetOutOfRangeException
   → auto.offset.reset ke hisaab se:
       earliest → poora topic DOBARA process (duplicates)
       latest   → beech ka data CHUP-CHAAP skip (data loss)

Isi liye lag alert itna zaroori hai — consumer ko retention se
zyada peeche kabhi nahi hone dena.</pre>`,

    `<h3>⚖️ Log compaction — jab tum "latest state" chahte ho, "har change" nahi</h3>
     <p>Normal retention purane messages ko WAQT ke hisaab se uda deti hai. Par kuch
     topics ke liye tum har message nahi, har KEY ka latest value rakhna chahte ho.
     Yahi <b>log compaction</b> hai (<code>cleanup.policy=compact</code>):</p>
     <pre>Ek "user-profiles" topic — key = userId, value = poora profile

Bina compaction (retention delete):
   user-1: {name: A}     ← 7 din baad UD jaayega
   user-1: {name: A2}    ← yeh bhi
   → 7 din purana user apne aap gayab! (galat — profile chahiye)

Compaction ke saath:
   har key ka sirf LATEST value bachta hai:
   user-1: {name: A}     ← purana, hata diya
   user-1: {name: A2}    ← latest, bacha rahega HAMESHA
   → topic ek "snapshot" ban jaata hai: har user ka current profile
   → naya consumer topic replay karke poora current state bana sakta hai</pre>
     <p><b>Compaction ke do zaroori detail:</b></p>
     <pre>🪦 TOMBSTONE: value = null bhejo → us key ko compaction UDAA deta hai
   (delete karne ka tareeqa — "user-1 ka account delete ho gaya")

🔢 OFFSETS: compaction offsets ko RENUMBER nahi karta
   purani values hatne se beech mein GAPS reh jaate hain
   (offset 5 ke baad seedha offset 9 — yeh normal hai)</pre>
     <p><b>Use-cases:</b> user profiles, current stock levels, config/feature-flags,
     ledger ka current balance, Kafka ka apna <code>__consumer_offsets</code> topic
     (haan, woh bhi compacted hai!). Rule: agar tum "har key ka current value"
     chahte ho toh compact, agar "har event ka itihaas" chahte ho toh delete.</p>`,
  ],
  stages: [
    {
      type: 'terminal',
      title: '🖥️ Mission: retention aur compaction ko chalo',
      instructions: 'Asli broker par retention se OffsetOutOfRange banao, phir compaction ko chala ke dekho.',
      goal: `Do cheezein karni hain:<br><br>
        <b>Part 1 — retention:</b><br>
        1. <code>events</code> topic mein kuch messages hain. Uska retention chhota karo
        (<code>kafka-configs --alter … retention.ms=1000</code>)<br>
        2. Ghadi aage badhao (<code>sim time +5m</code>) — dekho records ud gaye<br><br>
        <b>Part 2 — compaction:</b><br>
        3. <code>profiles</code> topic ko compact karo (<code>sim compact profiles</code>)<br>
        4. <code>dump profiles</code> se dekho ki har key ka sirf latest value bacha`,
      cluster: {
        brokers: 3,
        topics: {
          events: {
            partitions: 1, rf: 3, config: { 'retention.ms': 604800000 },
            messages: Array.from({ length: 40 }, (_, i) => ({ key: null, value: 'evt-' + i })),
          },
          profiles: {
            partitions: 1, rf: 3, config: { 'cleanup.policy': 'compact' },
            messages: [
              { key: 'user-1', value: 'name=Asha' },
              { key: 'user-2', value: 'name=Bilal' },
              { key: 'user-1', value: 'name=Asha-v2' },
              { key: 'user-3', value: 'name=Chetan' },
              { key: 'user-1', value: 'name=Asha-v3' },
              { key: 'user-2', value: 'name=Bilal-v2' },
            ],
          },
        },
      },
      tasks: [
        { text: 'events ka retention chhota karo', check: s => s.history.some(h => /kafka-configs.*--alter.*retention/.test(h)) },
        { text: 'ghadi aage badhao (records udne do)', check: s => s.partitionCounts('events')[0] === 0 },
        { text: 'profiles ko compact karo', check: s => s.history.some(h => /sim compact profiles/.test(h)) },
        { text: 'compaction ke baad har key ka latest dekho', check: s => s.partitionCounts('profiles')[0] === 3 },
      ],
      success: s => s.partitionCounts('events')[0] === 0 && s.partitionCounts('profiles')[0] === 3 &&
                    s.history.some(h => /sim compact profiles/.test(h)),
      successText: `Dono cheezein ho gayin:<br><br>
        🗄️ <b>Retention:</b> retention.ms chhota karte hi aur ghadi aage badhte hi
        purane events UD gaye. Yahi disk ko bharने se rokta hai — par yaad rakho,
        agar koi consumer un offsets par tha toh use ab OffsetOutOfRange milega.<br><br>
        🗜️ <b>Compaction:</b> 6 messages the (user-1 ke 3, user-2 ke 2, user-3 ka 1).
        Compaction ke baad sirf <b>3</b> bache — har key ka <b>latest</b> value
        (user-1=Asha-v3, user-2=Bilal-v2, user-3=Chetan). Topic ab ek "current state
        snapshot" hai. Aur agar tum <code>user-1</code> ke liye null (tombstone) bhejte,
        toh woh key poori tarah gayab ho jaati.`,
      hints: [
        'kafka-configs --alter --entity-type topics --entity-name events --add-config retention.ms=1000',
        'sim time +5m    (isse retention sweep chalega aur purane records udenge)',
        'sim compact profiles    phir   dump profiles',
      ],
    },
    {
      type: 'bins',
      title: '🗂️ Delete ya Compact?',
      instructions: 'Har topic ke liye sahi cleanup policy. Sawal: "har event ka itihaas" chahiye ya "har key ka current value"?',
      bins: [
        { id: 'delete', label: '🗑️ cleanup.policy=delete (itihaas, time/size retention)' },
        { id: 'compact', label: '🗜️ cleanup.policy=compact (har key ka latest value)' },
      ],
      items: [
        { text: 'Order events (order-created, order-shipped…)', bin: 'delete',
          explain: 'Yeh events ka itihaas hai — har event matter karta hai. Time-based retention (jaise 30 din) sahi hai. Compaction galat hoga (woh events kha jaayega).' },
        { text: 'User profiles (key=userId, value=current profile)', bin: 'compact',
          explain: 'Tumhe har user ka CURRENT profile chahiye, har change ka itihaas nahi. Compaction har key ka latest rakhta hai — perfect snapshot.' },
        { text: 'Current stock levels (key=sku, value=quantity)', bin: 'compact',
          explain: 'Sirf current stock matter karta hai. Compaction har SKU ka latest quantity rakhta hai, purani values uda deta hai.' },
        { text: 'Application logs', bin: 'delete',
          explain: 'Logs time-series hain — har line matter karti hai (thodi der ke liye). Time retention (jaise 3 din) sahi, phir udne do.' },
        { text: 'Feature flags / config (key=flagName, value=state)', bin: 'compact',
          explain: 'Sirf current config chahiye. Compaction har flag ka latest state rakhta hai — naya service topic replay karke current config bana leta hai.' },
        { text: 'Clickstream / analytics events', bin: 'delete',
          explain: 'Har click ek alag event hai — sab chahiye (analysis ke liye). Time retention sahi. Compaction ka koi matlab nahi (har click alag).' },
        { text: 'Account balances (key=accountId, value=balance)', bin: 'compact',
          explain: 'Current balance ek snapshot hai. Compaction har account ka latest balance rakhta hai. (Note: transaction itihaas ALAG topic mein delete-policy ke saath rakho.)' },
        { text: 'Audit trail (kisne kya kiya, kab)', bin: 'delete',
          explain: 'Audit ko har entry chahiye — compaction use kha jaayega. Long time-based retention (jaise 7 saal compliance ke liye) with delete policy.' },
      ],
    },
    {
      type: 'quiz',
      title: '🧠 Retention & compaction ke sawal',
      questions: [
        {
          q: 'Ek consumer 10 din band raha. retention.ms 7 din hai. Chalu karne par?',
          choices: [
            'Wahin se chalu hoga jahan chhoda',
            'OffsetOutOfRangeException — uska committed offset ab log mein hai hi nahi (retention ne uda diya). auto.offset.reset ke hisaab se ya toh earliest se sab dobara process karega (duplicates) ya latest par jump karega (beech ka data chup-chaap skip). Isi liye lag alert zaroori hai — consumer ko retention se zyada peeche nahi hone dena.',
            'Kafka retention apne aap badha dega',
            'Consumer chalu hi nahi hoga',
          ],
          answer: 1,
          explain: 'Yeh retention ka sabse khatarnaak asar hai. Dono raaste dukhdayi hain. Yahi Track 1 (Broker Anatomy) mein bhi aaya tha — ab tum poora mechanism jaante ho.',
        },
        {
          q: 'Log compaction ke baad kya bachta hai?',
          choices: [
            'Sirf pehle 100 messages',
            'Har KEY ka sirf LATEST value (aur null-value tombstones un keys ko poori tarah uda dete hain). Offsets renumber nahi hote, toh beech mein gaps reh jaate hain. Topic ek "current state snapshot" ban jaata hai — har key ka current value.',
            'Sabse naye 100 messages',
            'Sirf ek partition',
          ],
          answer: 1,
          explain: 'Compaction ka mental model: "har key ka current value ki snapshot". Isi liye user profiles, stock levels, config ke liye perfect hai — jahan itihaas nahi, current state chahiye.',
        },
        {
          q: 'Tombstone (null value) kya karta hai?',
          choices: [
            'Kuch nahi, ignore hota hai',
            'Compacted topic mein us KEY ko poori tarah uda deta hai — yeh compaction ke saath "delete" karne ka tareeqa hai. Jaise "user-1 ka account delete ho gaya" → user-1 key ke liye null bhejo → compaction use hata dega. (Tombstone khud thodi der rehta hai taaki sab consumers delete dekh lein, phir woh bhi udta hai.)',
            'Topic ko delete kar deta hai',
            'Partition badha deta hai',
          ],
          answer: 1,
          explain: 'Tombstone compacted topics mein delete ka mechanism hai. Bina iske compacted topic se kuch hata hi nahi sakte (har key ka latest hamesha rehta). Null value = "is key ko bhula do".',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 "cleanup.policy=delete aur compact mein farak kab use karoge?"',
        choices: [
          'Compact hamesha better hai',
          'Sawal yeh hai: tumhe har EVENT ka itihaas chahiye ya har KEY ka current value? Events ka itihaas (orders, logs, clickstream, audit) → delete with time/size retention. Har key ka latest state (user profiles, stock levels, config, balances) → compact. Compact har key ka sirf latest value rakhta hai, toh woh ek "current state snapshot" ban jaata hai jise naya consumer replay karke reconstruct kar sakta hai. Kafka ka apna __consumer_offsets bhi compacted hai.',
          'Delete tez hai',
          'Dono same kaam karte hain',
        ],
        answer: 1,
        explain: 'Yeh clean conceptual sawal hai. Achha jawab decision ko ek simple sawal mein todhta hai (itihaas vs current value) aur dono ke concrete use-cases deta hai. __consumer_offsets ka reference dikhata hai ki tum gehrai jaante ho.',
      },
      {
        q: '🧑‍💼 "Ek broker ki disk bhar gayi. Kya galat hua hoga aur kaise rokoge?"',
        choices: [
          'Bada disk laga do',
          'Sabse aam karan: kisi topic par retention theek se set nahi (bahut lambi retention.ms ya koi retention.bytes limit nahi), ya ek high-volume topic default 7-din retention ke saath. Rokne ke liye: har topic par retention.ms AUR retention.bytes dono set karo (jo pehle hit ho), high-volume topics ki retention specially tune karo, aur disk usage par alert lagao (80% par, taaki cleanup ke liye jagah rahe). Disk-full khatarnaak hai kyunki broker recovery ke liye bhi thodi jagah chahiye hoti hai.',
          'Broker restart kar do',
          'Retention infinite kar do',
        ],
        answer: 1,
        explain: 'Yeh operational maturity dikhata hai. Achha jawab root cause (retention misconfiguration), prevention (dono limits + alerts), aur ek gehri baat (disk-full recovery ko bhi rokta hai) batata hai.',
      },
    ],
  },
  success: [
    'Main retention (time aur size) se disk management samjha sakta hoon',
    'Main jaanta hoon ki retention se OffsetOutOfRange kaise banta hai',
    'Main compaction ka use-case (current state vs history) bata sakta hoon',
    'Main tombstones aur compaction ke gaps samjha sakta hoon',
  ],
  homework: `<p>Compaction ko asli Kafka par dekho:</p>
    <pre># compacted topic banao
kafka-topics.sh --create --topic profiles \\
  --partitions 1 --config cleanup.policy=compact \\
  --config segment.ms=100 --config min.cleanable.dirty.ratio=0.01 \\
  --bootstrap-server localhost:9092

# ek key ke kai versions bhejo
printf "u1:v1\\nu1:v2\\nu1:v3\\nu2:vA\\n" | \\
  kafka-console-producer.sh --topic profiles \\
  --property parse.key=true --property key.separator=: \\
  --bootstrap-server localhost:9092

# thodi der baad (compaction background mein chalta hai) padho:
kafka-console-consumer.sh --topic profiles --from-beginning \\
  --property print.key=true --bootstrap-server localhost:9092
# → u1:v3  u2:vA   (u1 ke v1,v2 gayab — sirf latest bacha)

# tombstone se delete:
printf "u1:\\n" | kafka-console-producer.sh --topic profiles \\
  --property parse.key=true --property key.separator=: \\
  --property null.marker=NULL --bootstrap-server localhost:9092
# → compaction ke baad u1 poori tarah gayab</pre>
    <p><b>Asli sabak:</b> compaction background mein chalta hai (turant nahi), isliye
    upar aggressive settings di hain. Dekho ki topic ek "har user ka latest profile"
    snapshot ban jaata hai — yahi event-sourcing aur CDC ka foundation hai.</p>`,
  cards: [
    { front: 'retention.ms aur retention.bytes — kaunsa use?', back: '<b>Dono</b> set karo — jo pehle hit ho wahi lagega. Time se predictability ("7 din"), size se disk safety ("1GB per partition"). "7 din YA 1GB, jo pehle aaye" — yeh disk ko bharne se rokta hai.' },
    { front: 'Retention se OffsetOutOfRange kaise banta hai?', back: 'Consumer retention se zyada der band raha → uska committed offset log se ud gaya → chalu hone par <code>OffsetOutOfRangeException</code> → earliest (sab dobara = duplicates) ya latest (beech ka skip = data loss). Isliye lag alert zaroori hai.' },
    { front: 'Log compaction kya rakhta hai?', back: 'Har <b>KEY</b> ka sirf <b>LATEST</b> value. Topic ek "current state snapshot" ban jaata hai. Offsets renumber nahi hote (beech mein gaps). Use: user profiles, stock levels, config, balances — jahan current value chahiye, itihaas nahi.' },
    { front: 'Tombstone kya hai?', back: '<code>value = null</code> wala message — compaction us <b>key ko poori tarah uda</b> deta hai. Yeh compacted topic mein "delete" karne ka tareeqa hai ("account delete ho gaya" → null bhejo).' },
    { front: 'Delete vs Compact — kaise chunein?', back: 'Sawal: har <b>event ka itihaas</b> chahiye ya har <b>key ka current value</b>?<br>Itihaas (orders, logs, audit) → <b>delete</b> + time/size retention.<br>Current state (profiles, stock, config) → <b>compact</b>.' },
  ],
},

/* =====================================================================
   10. BOSS: BLACK FRIDAY OUTAGE NIGHT
   ===================================================================== */
{
  id: 'boss-blackfriday',
  icon: '🏆',
  title: 'BOSS: Black Friday Outage Night — sab ek saath',
  badge: 'Vishwas Sadhak',
  objectives: [
    'Ek asli multi-failure outage ko sahi kram mein sambhaalo',
    'Lag, poison message, retry storm aur broker failure — sab ek saath',
    'Panic ke bajaye systematic nidaan karo',
    'Track 2 ke saare hathiyaar ek jagah use karo',
  ],
  lesson: [
    `<h3>🏆 Final imtihaan — Black Friday ki raat</h3>
     <p>Tum QuickCart ke on-call engineer ho. Saal ki sabse badi sale. Raat ke
     11:47 baje pager ek saath teen alerts ke saath bajta hai. Ek cheez nahi tooti —
     kai cheezein ek saath toot rahi hain, aur woh ek doosre ko bigaad rahi hain.</p>
     <pre>🔴 order-events group lag: 2.8M aur badh raha hai
🔴 payment-worker: 40,000 retries/sec (thundering herd?)
🔴 broker-2: disk 94% (retention issue?)
🔴 orders-DLQ: 12,000 messages (poison flood?)</pre>
     <p>Track 2 ka har hathiyaar aaj kaam aayega:</p>
     <ul>
       <li><b>Lag</b> — kahan hai, badh raha ya ghat raha, per-partition?</li>
       <li><b>Poison messages</b> — DLQ mein kya aa raha hai?</li>
       <li><b>Retry storm</b> — jitter/breaker theek se laga hai?</li>
       <li><b>Broker/disk</b> — retention, ISR, replication</li>
       <li><b>Sabse zaroori:</b> kis kram mein theek karoge? (panic vs system)</li>
     </ul>
     <p><b>Yaad rakho jo tumne seekha:</b> recovery chal rahi ho toh haath mat lagao;
     symptom vs bimari; ek metric jhooth bol sakta hai; aur sabse pehle
     <b>samjho, phir chhedo</b>.</p>`,
  ],
  stages: [
    {
      type: 'incident',
      title: '🚨 11:47 PM — teen alerts, ek raat',
      pager: `<b>🔥 BLACK FRIDAY PAGER 23:47</b><br>
        order-events lag 2.8M ⬆ · payment-worker 40k retries/s · broker-2 disk 94% · orders-DLQ 12k`,
      start: 'n1',
      nodes: {
        n1: {
          sitrep: `Teen aag ek saath. Panic mein sabko ek saath chhedne ka mann hai.

Available: kafka-consumer-groups, kafka-topics, logs, kafka-configs`,
          text: 'Sabse pehle kya karoge?',
          choices: [
            { t: 'Ruk kar poora picture dekho — har alert ka data ikattha karo, phir prioritize karo', good: true, next: 'n2',
              explain: 'Sahi. Multi-failure outage mein sabse badi galti hai andhadhund action. Pehle poora nakshaa banao: kaunsa failure ROOT hai aur kaunse uske LAKSHAN. Aksar ek cheez baaki ko trigger kar rahi hoti hai.' },
            { t: 'Turant saare consumers restart kar do', good: false,
              explain: 'Restart = mass rebalance = aur lag, aur tumhe abhi tak pata nahi ki root cause kya hai. Panic action outage ko lamba karta hai.' },
            { t: 'broker-2 ka disk sabse critical hai, use abhi delete-clean karo', good: false,
              explain: 'Bina samjhe cleanup khatarnaak hai — shayad woh disk isliye bhar raha hai kyunki DLQ ya retry topic flood ho raha hai. Root cause pehle.' },
            { t: 'payment retries band kar do', good: false,
              explain: 'Retries band karna data loss laa sakta hai, aur tumhe abhi tak nahi pata ki woh storm kyun hai. Pehle diagnose.' },
          ],
        },
        n2: {
          sitrep: `$ kafka-consumer-groups --describe --group order-events
PARTITION  LAG        CONSUMER
0          14         c-1
1          11         c-2
2          2,799,881  c-3     🔥  ← saara lag EK partition par
3          9          c-4

$ kafka-console-consumer --topic orders-DLQ --max-messages 3 --property print.key=true
order-8821  {"amount":"NaN", "_error":"parse", "_origin":"orders-2@45103"}
order-8844  {"amount":"NaN", "_error":"parse", "_origin":"orders-2@45119"}
order-8851  {"amount":"NaN", "_error":"parse", "_origin":"orders-2@45140"}`,
          text: 'Lag SIRF partition 2 par hai, aur DLQ mein saare "NaN" errors partition 2 se hain. Kya samajh aaya?',
          choices: [
            { t: 'Ek producer partition 2 par corrupt (NaN) messages bhej raha hai — yeh ROOT cause hai', good: true, next: 'n3',
              explain: 'Bilkul! Sab kuch ek jagah point kar raha hai: partition 2. Corrupt messages → consumer c-3 unpar atak raha (ya DLQ bhej ke aage badh raha par flood ho raha) → lag wahin. Baaki "aag" iske lakshan hain.' },
            { t: 'Partition 2 ka consumer c-3 slow hai, use scale karo', good: false,
              explain: 'c-3 slow nahi hai — woh corrupt messages se ladd raha hai. Ek partition ko ek hi consumer padh sakta hai, toh scale se kuch nahi hoga. Data corrupt hai, consumer nahi.' },
            { t: 'Lag ko reset kar do latest par', good: false,
              explain: '28 lakh orders chhodna — Black Friday par! Aur root cause (corrupt producer) abhi bhi chal raha hai, toh 10 minute mein phir yahi.' },
          ],
        },
        n3: {
          sitrep: `Ab tasveer saaf ho rahi hai. Chalo baaki alerts ko is root cause se jodein:

• broker-2 disk 94%  → orders-DLQ isi par host hai, aur woh
  12k corrupt messages se flood ho raha hai
• payment 40k retries → payment-worker order-events se padhta hai;
  woh partition-2 ke corrupt orders ko process nahi kar pa raha
  aur retry storm bana raha hai (bina circuit breaker ke)`,
          text: 'Root cause mil gaya (corrupt producer). Ab kis KRAM mein theek karoge?',
          choices: [
            { t: 'Pehle producer ka NaN bug roko (root cause), phir consumers recover honge', good: true, next: 'n4',
              explain: 'Sahi kram! Jab tak corrupt messages aa rahe hain, tum kitna bhi neeche saaf karo, woh phir bharega. Root cause (producer) pehle band karo — phir baaki apne aap ya thodi madad se theek honge.' },
            { t: 'Pehle DLQ clear karo taaki disk bache', good: false,
              explain: 'DLQ clear kar bhi do toh producer aur corrupt messages bhejta rahega aur DLQ phir bharega. Root cause pehle. (Aur DLQ ke messages investigate karne ke liye chahiye!)' },
            { t: 'Pehle payment retries ko circuit breaker se roko', good: false,
              explain: 'Circuit breaker achha hai (aur lagana chahiye) par woh lakshan sambhaalta hai. Jab tak corrupt orders aa rahe hain, payment-worker unse ladta rahega. Root cause pehle.' },
          ],
        },
        n4: {
          sitrep: `Producer ka NaN bug deploy karke roka (validation add ki).
Ab neeche ki taraf saaf karte hain:

$ kafka-consumer-groups --describe --group order-events
PARTITION 2  LAG 2,799,881  ← consumer c-3 abhi bhi corrupt purane
                               messages se ladd raha hai

Consumer c-3 mein DLQ logic hai par woh har corrupt message ke liye
10 retries kar raha hai pehle DLQ bhejne se — isliye dheema hai.`,
          text: 'Producer ruk gaya, par partition 2 ka 28 lakh backlog abhi bhi hai (corrupt + valid mix). Kya karoge?',
          choices: [
            { t: 'Consumer c-3 ko fix karo: parse-error (permanent) ko turant DLQ bhejo, retry mat karo', good: true, next: 'n5',
              explain: 'Sahi. Parse error PERMANENT hai — uspar 10 retries barbaadi hai (yaad hai: transient vs permanent?). Turant DLQ bhejo. Ab c-3 corrupt ko instant skip karega aur valid orders tezi se process honge.' },
            { t: 'Partition 2 ke offsets latest par reset kar do', good: false,
              explain: 'Usme 28 lakh mein se zyadatar VALID orders hain (sirf kuch corrupt). Reset = un valid orders ko Black Friday par phenkna. Corrupt ko DLQ karo, valid ko process.' },
            { t: 'c-3 ke liye 10 aur consumers add karo', good: false,
              explain: 'Ek partition = ek consumer. 10 add karo, partition 2 phir bhi c-3 hi padhega. Problem c-3 ka retry-logic hai, consumer count nahi.' },
          ],
        },
        n5: {
          sitrep: `c-3 fix deploy kiya (permanent errors turant DLQ).
Ab partition 2 tezi se process ho raha hai:

$ kafka-consumer-groups --describe --group order-events
PARTITION 2  LAG 2,799,881 → 1,400,000 → 600,000 → ghat raha ✅

payment-worker retries: 40k/s → 3k/s (ab corrupt orders nahi aa rahe)

Bas broker-2 ka disk abhi bhi 91% hai.`,
          text: 'Lag burn down ho raha hai, retry storm shaant. Aakhri cheez: broker-2 disk. Kya karoge?',
          choices: [
            { t: 'orders-DLQ ki retention check/tune karo (woh 12k+ messages se bhara hai) aur disk alert set karo', good: true, next: 'end',
              explain: 'Sahi. DLQ flood ne disk bharaya. Ab producer ruk gaya toh naye corrupt nahi aayenge, par purane DLQ messages ko investigate karke clear karo aur DLQ par disk-safe retention + alert lagao. Systematic cleanup.' },
            { t: 'broker-2 ko turant restart kar do', good: false,
              explain: 'Restart se disk khali nahi hoga (data wahin hai) aur Black Friday par ek broker down karna risky hai. Disk ka root cause (DLQ flood) address karo.' },
            { t: 'Sab theek hai, kuch mat karo', good: false,
              explain: '91% disk abhi bhi khatarnaak hai — thoda aur bhara toh broker girega. DLQ retention aur alert lagao taaki yeh dobara na ho.' },
          ],
        },
        end: {
          end: true,
          text: '✅ 40 minute mein sab shaant. Lag 0 ki taraf, retry storm khatam, disk safe, koi valid order nahi khoya. DLQ ke corrupt orders investigate ho rahe hain.',
          summary: `<b>🏆 Tumne Black Friday bacha li! Kya seekha:</b><br><br>
            1️⃣ <b>Multi-failure mein pehle SAMJHO, phir chhedo.</b> Teen "aag" thin
            par ek hi ROOT cause (corrupt producer) — baaki uske lakshan the.
            Panic action outage ko lamba karta.<br><br>
            2️⃣ <b>Root cause pehle band karo.</b> Producer rok-ne se pehle neeche saaf
            karna bekaar tha — woh phir bharta.<br><br>
            3️⃣ <b>Transient vs permanent yaad rakho.</b> Parse error par 10 retries
            barbaadi thi — permanent errors turant DLQ.<br><br>
            4️⃣ <b>Data phenkne se pehle socho.</b> Offset reset aasan tha par usme
            lakhon VALID orders the — sirf corrupt ko DLQ karo.<br><br>
            5️⃣ <b>Har hathiyaar ek jagah:</b> per-partition lag (skew pehchana), DLQ
            (poison isolate), retry/breaker (storm), retention (disk). Yahi Track 2 ka
            saar hai — aur yahi tumhe senior banata hai. 🔥`,
        },
      },
    },
    {
      type: 'quiz',
      title: '🧠 Boss quiz — Track 2 ka saara gyaan',
      bonus: true,
      questions: [
        {
          q: 'Multi-failure outage mein sabse pehla kadam?',
          choices: [
            'Sabko ek saath restart karo',
            'Ruko, poora picture ikattha karo, aur pata lagao ki kaunsa failure ROOT hai vs kaunse LAKSHAN — aksar ek cheez baaki ko trigger karti hai',
            'Sabse critical dikhne wale ko turant fix karo',
            'Sab kuch reset kar do',
          ],
          answer: 1,
          explain: 'Panic action outages ko lamba karta hai. Multi-failure mein aksar ek root cause hota hai jo kai lakshan banata hai — use pehle dhoondo.',
        },
        {
          q: 'Saara lag ek partition par, DLQ mein us partition ke "NaN" errors. Root cause?',
          choices: [
            'Us partition ka consumer slow hai',
            'Ek producer us partition par corrupt messages bhej raha hai — lag aur DLQ dono uske lakshan hain. Root cause producer ka bug hai, consumer nahi.',
            'Partition ko delete karna padega',
            'Broker mein bug hai',
          ],
          answer: 1,
          explain: 'Jab lag aur DLQ dono ek hi partition par point karein, toh data ki problem hai consumer ki nahi. Consumer scale karne se kuch nahi hoga.',
        },
        {
          q: 'Corrupt producer chal raha hai aur DLQ + lag dono bade hain. Pehle kya?',
          choices: [
            'DLQ clear karo',
            'Producer ka bug roko (root cause) — warna jitna bhi neeche saaf karo woh phir bharega',
            'Consumers scale karo',
            'Lag reset karo',
          ],
          answer: 1,
          explain: 'Root cause pehle. Neeche cleanup (DLQ, lag) tab tak bekaar hai jab tak upar se corrupt messages aa rahe hain.',
        },
        {
          q: 'Partition mein 28 lakh messages hain — zyadatar valid, kuch corrupt. Kya karoge?',
          choices: [
            'Offsets latest par reset (sab chhod do)',
            'Consumer fix karo taaki permanent errors (corrupt) turant DLQ jaayein aur valid orders process hon — Black Friday par valid orders phenkna galat hai',
            '28 lakh sabko DLQ bhejo',
            'Partition delete karo',
          ],
          answer: 1,
          explain: 'Reset karna aasan lagta hai par usme lakhon valid orders hain. Sahi approach: corrupt ko selectively DLQ, valid ko process. Yahi transient-vs-permanent ka practical use hai.',
        },
      ],
    },
  ],
  interview: {
    questions: [
      {
        q: '🧑‍💼 FINAL: "Ek raat teen alerts ek saath aaye — high lag, retry storm, disk full. Kaise sambhaaloge?"',
        choices: [
          'Teeno ko ek saath fix karne lagunga',
          'Pehle ruk kar poora picture ikattha karunga — teeno alerts ka data dekhunga aur pata lagaunga ki kaunsa ROOT cause hai vs kaunse LAKSHAN, kyunki aksar ek failure baaki ko trigger karti hai. Phir root cause pehle band karunga (warna neeche cleanup bekaar hai), phir lakshan ko kram se theek karunga. Beech mein yeh bhi dekhunga ki koi recovery pehle se chal rahi ho toh use na chhedoon. Aur data phenkne wale actions (offset reset) se bachunga jab tak zaroori na ho. Poore time systematic rahunga, panic-driven nahi.',
          'Sabse critical ko pehle fix karunga',
          'Sab kuch restart kar dunga',
        ],
        answer: 1,
        explain: 'Yeh poora Track 2 ka saar hai. Achha jawab: (1) samjho phir chhedo, (2) root vs symptom, (3) recovery ko mat chhedo, (4) data phenkne se bacho, (5) systematic vs panic. Yeh senior on-call maturity hai — aur yahi asli interview mein sabse zyada matter karta hai.',
      },
    ],
  },
  success: [
    'Maine ek multi-failure outage systematic tareeqe se sambhaala',
    'Main root cause aur lakshan mein farak kar sakta hoon',
    'Main jaanta hoon ki recovery chal rahi ho toh haath nahi lagana',
    'Maine Track 2 ke saare hathiyaar (lag, DLQ, retry/breaker, retention) ek jagah use kiye',
  ],
  homework: `<p>Apne cluster par ek "mini outage" banao aur sambhaalo:</p>
    <pre># 1. ek topic banao aur usme valid + ek corrupt message daalo
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092
> {"order":1,"amount":100}
> {"order":2,"amount":"NaN"}     ← poison
> {"order":3,"amount":300}

# 2. ek consumer chalao jo NaN par atak jaata hai (bina DLQ)
#    → dekho lag partition par atka

# 3. ab consumer ko DLQ logic ke saath fix karo:
#    - parse error (permanent) → turant orders.DLQ
#    - valid → process
#    → dekho lag burn down hota hai, poison DLQ mein safe

# 4. lag, DLQ depth, aur consumer throughput teeno monitor karo</pre>
    <p><b>Asli sabak:</b> yeh chhota version poore Black Friday scenario ka hai —
    poison message, partition block, DLQ, aur selective recovery. Jab tum ise
    apne haathon se karoge, teeno concepts (lag + poison + DLQ) ek saath jud jaayenge.</p>`,
  cards: [
    { front: 'Multi-failure outage mein pehla kadam?', back: '<b>Ruko aur samjho.</b> Poora picture ikattha karo, pata lagao kaunsa failure ROOT hai vs kaunse LAKSHAN — aksar ek cheez baaki ko trigger karti hai. Panic action (mass restart) outage ko lamba karta hai.' },
    { front: 'Root cause aur lakshan mein kaise farak karein?', back: 'Dekho ki alerts kahan CONVERGE karte hain. Agar lag, DLQ, aur retry storm sab ek partition/producer par point karein, toh wahi root hai — baaki uske lakshan. Ek jagah point karna = strong signal.' },
    { front: 'Corrupt producer + bhara DLQ + high lag — pehle kya?', back: '<b>Root cause (producer) pehle band karo.</b> Neeche cleanup (DLQ clear, lag burn) tab tak bekaar hai jab tak upar se corrupt messages aa rahe hain — woh phir bharega.' },
    { front: '28 lakh messages (zyadatar valid, kuch corrupt) — recover kaise?', back: 'Offset reset se sab mat phenko (valid orders bhi jaayenge). Consumer fix karo taaki <b>permanent errors (corrupt) turant DLQ</b> jaayein aur <b>valid process</b> hon. Selective recovery, blanket nahi.' },
    { front: 'On-call ka sabse zaroori sabak?', back: '<b>Samjho phir chhedo.</b> Recovery chal rahi ho toh haath mat lagao; symptom vs bimari pehchano; ek metric jhooth bol sakta hai; data phenkne (offset reset) se pehle socho. Systematic > panic.' },
  ],
},

]);
