/* =====================================================================
   Kafka Gurukul — Concept Visuals
   Har level ka ek signature diagram, style.css ke .dg-* primitives se.
   Pure HTML/CSS/emoji — koi dependency, koi image nahi.

   Entry: VISUALS['<level-id>'] = { t: chhota title, html: markup }

   Hotspot games ke liye clickable nodes par data-hs="<target>" hota hai.
   Saare diagrams ek saath dekhne ke liye: gallery.html
   ===================================================================== */

const VISUALS = {

/* ================= TRACK 1 — BEGINNER ================= */

'sync-death': {
  t: 'Sync ki maut vs Queue ka bachaav',
  html: `
    <div class="dg-split">
      <div class="dg-box is-bad"><span class="bx-label">❌ SYNC — sab jude hue</span>
        <div class="dg-col">
          <div class="dg-node is-bad" data-hs="order"><span class="ni">🛒</span><span class="nl">Order API</span><span class="ns">400 threads, sab BLOCKED</span></div>
          <div class="dg-down"></div>
          <div class="dg-flow">
            <div class="dg-node is-muted"><span class="ni">💳</span><span class="nl">payment</span><span class="ns">800ms</span></div>
            <div class="dg-node is-muted"><span class="ni">📦</span><span class="nl">inventory</span><span class="ns">200ms</span></div>
            <div class="dg-node is-bad pulse" data-hs="email"><span class="ni">📧</span><span class="nl">email</span><span class="ns">9s 🐌 KHARAAB</span></div>
          </div>
          <div class="dg-pill bad">Total 2.8s → capacity sirf 142/s → 💀 checkout gir gaya</div>
        </div>
      </div>

      <div class="dg-box is-good"><span class="bx-label">✅ ASYNC — sab alag</span>
        <div class="dg-col">
          <div class="dg-node is-good" data-hs="order2"><span class="ni">🛒</span><span class="nl">Order API</span><span class="ns">likho aur nikal jao — 2ms</span></div>
          <div class="dg-down"></div>
          <div class="dg-node is-accent" data-hs="log"><span class="ni">📜</span><span class="nl">orders topic</span><span class="ns">durable log</span></div>
          <div class="dg-down"></div>
          <div class="dg-flow">
            <div class="dg-node is-good"><span class="ni">💳</span><span class="nl">payment</span><span class="ns">apni raftaar</span></div>
            <div class="dg-node is-good"><span class="ni">📦</span><span class="nl">warehouse</span><span class="ns">apni raftaar</span></div>
            <div class="dg-node is-muted"><span class="ni">📧</span><span class="nl">email</span><span class="ns">💀 down? koi baat nahi</span></div>
          </div>
          <div class="dg-pill good">Checkout 2ms · email 1 ghanta down raha, kisi ko pata nahi chala</div>
        </div>
      </div>
    </div>
    <div class="dg-cap">Queue <b>speed</b> ke liye nahi, <b>failure isolation</b> ke liye hai.
      Sync mein sabse kamzor kadi poore system ko le doobti hai.</div>`,
},

broker: {
  t: 'Broker = append-only log (mailbox nahi)',
  html: `
    <div class="dg-col">
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">📤</span><span class="nl">Producer</span><span class="ns">sirf END mein likhta hai</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-box is-accent"><span class="bx-label">📜 partition = ek file</span>
          <div class="dg-flow">
            <div class="dg-node is-muted"><span class="ni">0</span><span class="nl">m0</span></div>
            <div class="dg-node is-muted"><span class="ni">1</span><span class="nl">m1</span></div>
            <div class="dg-node is-muted"><span class="ni">2</span><span class="nl">m2</span></div>
            <div class="dg-node is-muted"><span class="ni">3</span><span class="nl">m3</span></div>
            <div class="dg-node is-muted"><span class="ni">4</span><span class="nl">m4</span></div>
            <div class="dg-node is-gold pulse"><span class="ni">5</span><span class="nl">m5</span><span class="ns">naya yahan</span></div>
          </div>
        </div>
      </div>

      <div class="dg-flow">
        <div class="dg-node is-good" data-hs="analytics"><span class="ni">🔖</span><span class="nl">analytics</span><span class="ns">offset 2</span></div>
        <div class="dg-node is-good" data-hs="billing"><span class="ni">🔖</span><span class="nl">billing</span><span class="ns">offset 4</span></div>
        <div class="dg-node is-good" data-hs="search"><span class="ni">🔖</span><span class="nl">search</span><span class="ns">offset 5</span></div>
      </div>
      <div class="dg-pill acc">Har group ka APNA bookmark · padhne se message MITTA NAHI</div>

      <div class="dg-box"><span class="bx-label">🚀 tez kyun hai</span>
        <div class="dg-flow">
          <div class="dg-pill good">Sequential append (~100× tez)</div>
          <div class="dg-pill good">OS page cache</div>
          <div class="dg-pill good">Zero-copy sendfile</div>
        </div>
      </div>
    </div>
    <div class="dg-cap">Queue padhne par <b>delete</b> karti hai — isliye replay namumkin.
      Log sirf <b>offset</b> aage badhata hai — isliye time travel, fanout aur naye consumers muft milte hain.</div>`,
},

partitions: {
  t: 'Partition = parallelism ki HARD limit',
  html: `
    <div class="dg-col">
      <div class="dg-box is-accent"><span class="bx-label">🗂️ topic: orders (4 partitions)</span>
        <div class="dg-flow">
          <div class="dg-node is-accent" data-hs="p0"><span class="ni">📜</span><span class="nl">p0</span></div>
          <div class="dg-node is-accent" data-hs="p1"><span class="ni">📜</span><span class="nl">p1</span></div>
          <div class="dg-node is-accent" data-hs="p2"><span class="ni">📜</span><span class="nl">p2</span></div>
          <div class="dg-node is-accent" data-hs="p3"><span class="ni">📜</span><span class="nl">p3</span></div>
        </div>
      </div>
      <div class="dg-arrow two"></div>
      <div class="dg-box is-bad"><span class="bx-label">👥 group: order-workers (8 consumers)</span>
        <div class="dg-flow">
          <div class="dg-node is-good"><span class="ni">📥</span><span class="nl">c1</span><span class="ns">p0 ✓</span></div>
          <div class="dg-node is-good"><span class="ni">📥</span><span class="nl">c2</span><span class="ns">p1 ✓</span></div>
          <div class="dg-node is-good"><span class="ni">📥</span><span class="nl">c3</span><span class="ns">p2 ✓</span></div>
          <div class="dg-node is-good"><span class="ni">📥</span><span class="nl">c4</span><span class="ns">p3 ✓</span></div>
          <div class="dg-node is-muted" data-hs="idle"><span class="ni">😴</span><span class="nl">c5</span><span class="ns">khaali</span></div>
          <div class="dg-node is-muted" data-hs="idle"><span class="ni">😴</span><span class="nl">c6</span><span class="ns">khaali</span></div>
          <div class="dg-node is-muted" data-hs="idle"><span class="ni">😴</span><span class="nl">c7</span><span class="ns">khaali</span></div>
          <div class="dg-node is-muted" data-hs="idle"><span class="ni">😴</span><span class="nl">c8</span><span class="ns">khaali</span></div>
        </div>
      </div>
      <div class="dg-pill bad">4 partitions = zyada se zyada 4 consumers kaam karenge · baaki sirf bill badhayenge</div>

      <div class="dg-box"><span class="bx-label">🔖 teen offsets</span>
        <div class="dg-flow">
          <div class="dg-pill">log-start (retention ke baad)</div>
          <div class="dg-pill acc">committed (consumer ka bookmark)</div>
          <div class="dg-pill gold">log-end / LEO (agla write)</div>
        </div>
        <div class="dg-pill bad">LAG = LEO − committed</div>
      </div>
    </div>
    <div class="dg-cap">Formula: <b>partitions ≥ max(peak ÷ per-consumer, max consumers)</b>.
      Upar ki taraf galti karo — badhana <b>ordering todta hai</b>, ghatana <b>namumkin</b> hai.</div>`,
},

producer: {
  t: 'acks — kaun sa wada, kitna bharosa',
  html: `
    <div class="dg-ladder">
      <div class="dg-rung">
        <div class="dg-node is-bad" data-hs="acks0"><span class="ni">🏃</span><span class="nl">acks=0</span><span class="ns">~0.4ms</span></div>
        <div class="dg-pill bad">"bhej diya, bas" — broker ne liya ya nahi, pata hi nahi 💀 CHUP-CHAAP LOSS</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-gold" data-hs="acks1"><span class="ni">⚖️</span><span class="nl">acks=1</span><span class="ns">~5ms</span></div>
        <div class="dg-pill gold">"leader ne likh liya" — leader mara replication se pehle? gaya</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-good" data-hs="acksall"><span class="ni">🛡️</span><span class="nl">acks=all</span><span class="ns">~10ms</span></div>
        <div class="dg-pill good">"saare in-sync replicas ne likha" — asli durability</div>
      </div>
    </div>

    <div class="dg-box is-bad"><span class="bx-label">⚠️ jaal: acks=all AKELA kaafi nahi</span>
      <div class="dg-flow">
        <div class="dg-node is-bad"><span class="ni">💀</span><span class="nl">ISR ghat ke 1</span><span class="ns">2 broker gire</span></div>
        <div class="dg-arrow bad"></div>
        <div class="dg-node is-bad"><span class="ni">🤥</span><span class="nl">acks=all</span><span class="ns">ab matlab acks=1</span></div>
      </div>
      <div class="dg-pill good">✅ Hal: acks=all + min.insync.replicas=2 + RF=3 (teeno saath)</div>
    </div>
    <div class="dg-cap">ISR 2 se neeche gaya toh producer ko <b>NotEnoughReplicasException</b> milega —
      yeh <b>feature hai</b>: chup-chaap data khone se behtar hai ki likhna ruk jaaye.</div>`,
},

'consumer-groups': {
  t: 'Group ke andar baantna, groups ke beech fanout',
  html: `
    <div class="dg-col">
      <div class="dg-box is-accent"><span class="bx-label">🗂️ topic: orders (4 partitions)</span>
        <div class="dg-flow">
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">p0</span></div>
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">p1</span></div>
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">p2</span></div>
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">p3</span></div>
        </div>
      </div>
      <div class="dg-arrow two"></div>
      <div class="dg-split">
        <div class="dg-box is-good"><span class="bx-label">👥 group.id = billing</span>
          <div class="dg-flow">
            <div class="dg-node is-good" data-hs="billing"><span class="ni">📥</span><span class="nl">A</span><span class="ns">p0, p1</span></div>
            <div class="dg-node is-good"><span class="ni">📥</span><span class="nl">B</span><span class="ns">p2, p3</span></div>
          </div>
          <div class="dg-pill good">kaam BAANT rahe hain (load balancing)</div>
        </div>
        <div class="dg-box is-gold"><span class="bx-label">👥 group.id = analytics</span>
          <div class="dg-flow">
            <div class="dg-node is-gold" data-hs="analytics"><span class="ni">📥</span><span class="nl">X</span><span class="ns">p0,p1,p2,p3</span></div>
          </div>
          <div class="dg-pill gold">POORI copy le raha hai (fanout)</div>
        </div>
      </div>
      <div class="dg-box is-bad"><span class="bx-label">💥 aam galti</span>
        <div class="dg-pill bad">group.id = "notif-1", "notif-2", "notif-3", "notif-4" (per instance)
          → 4 alag groups → har customer ko 📧×4</div>
        <div class="dg-pill good">✅ group.id = SERVICE ka naam ("notification-service"), instance ka nahi</div>
      </div>
    </div>
    <div class="dg-cap">Poll loop ka kram: <b>poll → process → save → commit</b>.
      Commit hamesha aakhir mein — warna crash par messages chup-chaap gum.</div>`,
},

'keys-ordering': {
  t: 'Key = ordering ka taala',
  html: `
    <div class="dg-split">
      <div class="dg-box is-bad"><span class="bx-label">❌ bina key ke (round-robin)</span>
        <div class="dg-col">
          <div class="dg-flow">
            <div class="dg-node is-bad" data-hs="created"><span class="ni">🚗</span><span class="nl">ride-created</span><span class="ns">→ p2</span></div>
            <div class="dg-node is-bad" data-hs="cancelled"><span class="ni">🚫</span><span class="nl">ride-cancelled</span><span class="ns">→ p0</span></div>
          </div>
          <div class="dg-pill bad">alag partitions = koi ordering guarantee NAHI</div>
          <div class="dg-pill bad">p0 tez nikla → cancelled PEHLE process → 💀 cancelled ride zinda ho gayi</div>
        </div>
      </div>
      <div class="dg-box is-good"><span class="bx-label">✅ key = rideId</span>
        <div class="dg-col">
          <div class="dg-flow">
            <div class="dg-node is-good"><span class="ni">🚗</span><span class="nl">created</span><span class="ns">→ p1</span></div>
            <div class="dg-node is-good"><span class="ni">🚫</span><span class="nl">cancelled</span><span class="ns">→ p1</span></div>
          </div>
          <div class="dg-pill acc">partition = murmur2(key) % N</div>
          <div class="dg-pill good">ek hi partition = ek hi consumer = PERFECT ORDER ✅</div>
        </div>
      </div>
    </div>

    <div class="dg-box"><span class="bx-label">🔑 key chunne ke 2 niyam</span>
      <div class="dg-flow">
        <div class="dg-pill good">✅ orderId · userId · rideId · deviceId — lakhon values, load barabar</div>
        <div class="dg-pill bad">🔥 country · status · date · eventType — kam values, HOT PARTITION</div>
      </div>
    </div>
    <div class="dg-cap">Kafka ordering <b>sirf ek partition ke andar</b> deta hai — topic bhar mein kabhi nahi.
      Hot partition ka ilaaj "aur consumers" <b>nahi</b> hai (ek partition = ek consumer), balki behtar key hai.</div>`,
},

delivery: {
  t: 'Teen semantics — farak sirf commit ke waqt ka',
  html: `
    <div class="dg-ladder">
      <div class="dg-rung">
        <div class="dg-node is-bad" data-hs="atmost"><span class="ni">1️⃣</span><span class="nl">at-most-once</span></div>
        <div class="dg-flow">
          <div class="dg-pill">poll</div><div class="dg-arrow"></div>
          <div class="dg-pill bad">commit</div><div class="dg-arrow"></div>
          <div class="dg-pill">process</div>
        </div>
        <div class="dg-pill bad">crash beech mein → message HAMESHA ke liye gaya 💀</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-good" data-hs="atleast"><span class="ni">2️⃣</span><span class="nl">at-least-once</span><span class="ns">DEFAULT</span></div>
        <div class="dg-flow">
          <div class="dg-pill">poll</div><div class="dg-arrow"></div>
          <div class="dg-pill">process</div><div class="dg-arrow"></div>
          <div class="dg-pill good">commit</div>
        </div>
        <div class="dg-pill gold">crash beech mein → DOBARA aayega (duplicate) · par kuch khota nahi ✅</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-accent" data-hs="exactly"><span class="ni">3️⃣</span><span class="nl">exactly-once</span></div>
        <div class="dg-box is-accent"><span class="bx-label">ek hi transaction</span>
          <div class="dg-flow">
            <div class="dg-pill acc">process</div>
            <div class="dg-pill acc">+ commit offset</div>
          </div>
        </div>
        <div class="dg-pill bad">⚠️ SIRF Kafka→Kafka ke andar · payment/email/API par bekaar</div>
      </div>
    </div>

    <div class="dg-box is-good"><span class="bx-label">✅ asli duniya ka pattern</span>
      <div class="dg-flow">
        <div class="dg-pill good">at-least-once delivery</div>
        <div class="dg-pill">+</div>
        <div class="dg-pill good">idempotent processing</div>
        <div class="dg-pill">=</div>
        <div class="dg-pill gold">"effectively exactly once"</div>
      </div>
    </div>
    <div class="dg-cap">"Exactly-once" ka matlab <b>code ek baar chalega</b> nahi —
      <b>ASAR ek baar dikhega</b>. Duplicate ko rokne ki koshish mat karo, use <b>bekaar</b> bana do.</div>`,
},

serialization: {
  t: 'Schema evolution — safe vs breaking',
  html: `
    <div class="dg-split">
      <div class="dg-box is-good"><span class="bx-label">✅ SAFE (backward-compatible)</span>
        <div class="dg-col">
          <div class="dg-pill good">naya OPTIONAL field (default ke saath)</div>
          <div class="dg-pill good">field ko optional banana</div>
          <div class="dg-pill good">naya event type</div>
        </div>
      </div>
      <div class="dg-box is-bad"><span class="bx-label">💥 BREAKING (consumers marenge)</span>
        <div class="dg-col">
          <div class="dg-pill bad">field RENAME (userId → user_id)</div>
          <div class="dg-pill bad">field HATANA</div>
          <div class="dg-pill bad">TYPE badalna (string → int)</div>
          <div class="dg-pill bad">enum mein nayi value 🫥 (chupa hua!)</div>
        </div>
      </div>
    </div>

    <div class="dg-box is-accent"><span class="bx-label">🔄 EXPAND-CONTRACT — rename karne ka safe tareeqa</span>
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">1️⃣</span><span class="nl">EXPAND</span><span class="ns">naya field add, DONO bhejo</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-gold"><span class="ni">2️⃣</span><span class="nl">MIGRATE</span><span class="ns">consumers shift honi do (hafte)</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-good"><span class="ni">3️⃣</span><span class="nl">CONTRACT</span><span class="ns">purana field hatao</span></div>
      </div>
    </div>

    <div class="dg-box"><span class="bx-label">📦 event design</span>
      <div class="dg-flow">
        <div class="dg-pill bad">❌ command: "sendEmail" (kya KARNA hai)</div>
        <div class="dg-pill good">✅ event: "OrderPlaced" (kya HUA)</div>
      </div>
      <div class="dg-flow">
        <div class="dg-pill acc">headers → metadata (correlation-id, schema-version)</div>
        <div class="dg-pill acc">payload → business data</div>
      </div>
    </div>
    <div class="dg-cap">Fat events ki asli taakat bandwidth nahi — <b>us waqt ka sach preserve karna</b> hai.
      "Order tab ₹4990 ka tha" hamesha sach rahega.</div>`,
},

'boss-pipeline': {
  t: 'QuickCart — poora pipeline',
  html: `
    <div class="dg-col">
      <div class="dg-node is-accent"><span class="ni">🛒</span><span class="nl">Checkout API</span><span class="ns">2,000 orders/sec peak</span></div>
      <div class="dg-down"></div>
      <div class="dg-pill acc">key = orderId · acks=all · idempotent producer</div>
      <div class="dg-down"></div>

      <div class="dg-box is-accent"><span class="bx-label">📜 order-events · 12 partitions · RF=3 · min.insync=2</span>
        <div class="dg-flow">
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">p0</span><span class="ns">L:1 ISR 1,2,3</span></div>
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">p1</span><span class="ns">L:2 ISR 2,3,1</span></div>
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">…</span></div>
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">p11</span><span class="ns">L:3 ISR 3,1,2</span></div>
        </div>
      </div>
      <div class="dg-arrow two"></div>

      <div class="dg-flow">
        <div class="dg-box is-good"><span class="bx-label">👥 billing</span>
          <div class="dg-node is-good"><span class="ni">💳</span><span class="nl">4 consumers</span><span class="ns">lag 0 ✅</span></div>
        </div>
        <div class="dg-box is-good"><span class="bx-label">👥 warehouse</span>
          <div class="dg-node is-good"><span class="ni">📦</span><span class="nl">2 consumers</span><span class="ns">lag 0 ✅</span></div>
        </div>
        <div class="dg-box is-good"><span class="bx-label">👥 analytics</span>
          <div class="dg-node is-good"><span class="ni">📊</span><span class="nl">1 consumer</span><span class="ns">lag 0 ✅</span></div>
        </div>
      </div>

      <div class="dg-legend">
        <span>🗂️ 12 partitions = 2000÷250 + headroom</span>
        <span>🔑 orderId = ordering + high cardinality</span>
        <span>🛡️ RF=3 + acks=all + min.insync=2</span>
        <span>👥 3 groups = 3 poori copies</span>
      </div>
    </div>
    <div class="dg-cap">Har faisle ka <b>KYUN</b> bata sakte ho — yahi interview mein poocha jaata hai.
      Aur senior wahi hai jo apne design ki <b>nayi problems</b> khud bataye (duplicates → idempotency).</div>`,
},

/* ================= TRACK 2 — INTERMEDIATE (RELIABILITY) ================= */

'consumer-lag': {
  t: 'Lag — number nahi, WAQT aur DISHA dekho',
  html: `
    <div class="dg-col">
      <div class="dg-box is-accent"><span class="bx-label">📉 per-partition lag (total jhooth bolta hai)</span>
        <div class="dg-flow">
          <div class="dg-node is-good"><span class="ni">p0</span><span class="nl">lag 47</span></div>
          <div class="dg-node is-good"><span class="ni">p1</span><span class="nl">lag 57</span></div>
          <div class="dg-node is-bad pulse" data-hs="skew"><span class="ni">p2</span><span class="nl">lag 26 lakh</span><span class="ns">🔥 skew/poison</span></div>
          <div class="dg-node is-good"><span class="ni">p3</span><span class="nl">lag 47</span></div>
        </div>
        <div class="dg-pill bad">total lag ne is spike ko chhupa diya tha</div>
      </div>
      <div class="dg-split">
        <div class="dg-box"><span class="bx-label">✅ sahi tareeqa</span>
          <div class="dg-pill acc">lag ÷ throughput = WAQT ("40 min peeche")</div>
          <div class="dg-pill acc">derivative: badh raha ya ghat raha?</div>
        </div>
        <div class="dg-box is-bad"><span class="bx-label">🫥 lag = 0 bhi jhooth</span>
          <div class="dg-pill bad">producer ruka → sab 0, kuch chal nahi raha</div>
          <div class="dg-pill bad">exception nigal ke commit → data gum</div>
        </div>
      </div>
      <div class="dg-legend">
        <span>📉 lag_seconds — kitna peeche</span>
        <span>⚡ throughput — kaam ho raha?</span>
        <span>❌ errors — "ho gaya" sach tha?</span>
      </div>
    </div>
    <div class="dg-cap">Alert absolute number par nahi — <b>"lag N minute se lagataar badh raha hai"</b> par.
      <code>burn_down = capacity − normal_rate</code>; positive ho tabhi lag khatam hoga.</div>`,
},

rebalance: {
  t: 'Rebalance — eager (sab ruke) vs cooperative',
  html: `
    <div class="dg-split">
      <div class="dg-box is-bad"><span class="bx-label">❌ EAGER (purana)</span>
        <div class="dg-flow">
          <div class="dg-node is-bad pulse"><span class="ni">🛑</span><span class="nl">c1</span><span class="ns">sab chhoda</span></div>
          <div class="dg-node is-bad pulse"><span class="ni">🛑</span><span class="nl">c2</span><span class="ns">sab chhoda</span></div>
          <div class="dg-node is-bad pulse"><span class="ni">🛑</span><span class="nl">c3</span><span class="ns">sab chhoda</span></div>
        </div>
        <div class="dg-pill bad">stop-the-world — throughput ZERO</div>
        <div class="dg-pill bad">20 pod deploy = 40 rebalance = 6 min blackout</div>
      </div>
      <div class="dg-box is-good"><span class="bx-label">✅ COOPERATIVE STICKY</span>
        <div class="dg-flow">
          <div class="dg-node is-good"><span class="ni">▶️</span><span class="nl">c1</span><span class="ns">chalta raha</span></div>
          <div class="dg-node is-good"><span class="ni">▶️</span><span class="nl">c2</span><span class="ns">chalta raha</span></div>
          <div class="dg-node is-gold"><span class="ni">🔄</span><span class="nl">c3</span><span class="ns">sirf zaroori move</span></div>
        </div>
        <div class="dg-pill good">sirf zaroori partitions move — baaki chalte rahe</div>
        <div class="dg-pill good">+ group.instance.id → restart par rebalance HI NAHI</div>
      </div>
    </div>
    <div class="dg-box is-bad"><span class="bx-label">☠️ chupa qatil: max.poll.interval.ms</span>
      <div class="dg-pill bad">poll ka batch 5 min se zyada laga → "consumer mara" → rebalance → 🔁 infinite loop</div>
      <div class="dg-pill good">✅ ilaaj: max.poll.records ghatao (timeout badhana NAHI)</div>
    </div>
    <div class="dg-cap">session.timeout = heartbeat (background), max.poll.interval = poll ke beech ka waqt.
      Consumer "zinda" dikh ke bhi poll-timeout par nikaala ja sakta hai.</div>`,
},

'retries-idempotency': {
  t: 'Idempotency — retry se −47 na ho',
  html: `
    <div class="dg-split">
      <div class="dg-box is-bad"><span class="bx-label">❌ har baar badalta hai</span>
        <div class="dg-stack">
          <div class="dg-pill bad">UPDATE stock = stock − 1</div>
          <div class="dg-pill bad">retry 1 → 12→11</div>
          <div class="dg-pill bad">retry 2 → 11→10</div>
          <div class="dg-pill bad">retry 3 → 10→9 💀 stock −47</div>
        </div>
      </div>
      <div class="dg-box is-good"><span class="bx-label">✅ dobara chale, kuch na bigde</span>
        <div class="dg-stack">
          <div class="dg-pill good">INSERT … order_id UNIQUE</div>
          <div class="dg-pill good">retry → error 1062 → skip ✅</div>
          <div class="dg-pill acc">ya: UPDATE … WHERE status='PAID'</div>
          <div class="dg-pill acc">retry → 0 rows → no-op ✅</div>
        </div>
      </div>
    </div>
    <div class="dg-box is-bad"><span class="bx-label">💥 in-memory Set kyun fail hota hai</span>
      <div class="dg-flow">
        <div class="dg-pill bad">restart → Set khaali</div>
        <div class="dg-pill bad">5 instance → 5 Set</div>
        <div class="dg-pill bad">kabhi khaali nahi → OOM</div>
      </div>
    </div>
    <div class="dg-cap">Duplicate ROKO mat — HARMLESS banao. Sabse achhi idempotency woh jisme
      dedup store chahiye hi nahi. Dedup store rakho toh TTL ≥ retry window.</div>`,
},

'backoff-jitter': {
  t: 'Jitter — synchronized herd ko bikher do',
  html: `
    <div class="dg-split">
      <div class="dg-box is-bad"><span class="bx-label">❌ bina jitter (exponential)</span>
        <div class="dg-stack">
          <div class="dg-pill bad">10k clients EK SAATH fail</div>
          <div class="dg-pill bad">sab 1s par ▮▮▮▮▮▮▮▮</div>
          <div class="dg-pill bad">sab 2s par ▮▮▮▮▮▮▮▮</div>
          <div class="dg-pill bad">sab 4s par ▮▮▮▮▮▮▮▮</div>
          <div class="dg-pill bad">🌊 synchronized spikes → 30s outage = 8 min</div>
        </div>
      </div>
      <div class="dg-box is-good"><span class="bx-label">✅ full jitter: random(0, exp)</span>
        <div class="dg-stack">
          <div class="dg-pill good">wahi 10k, 0-8s mein BIKHRE</div>
          <div class="dg-pill good">▪ ▫ ▪ ▫▫ ▪ ▫ ▪▪ ▫ ▪</div>
          <div class="dg-pill good">🍃 steady kam load → downstream saans le liya</div>
        </div>
      </div>
    </div>
    <div class="dg-box"><span class="bx-label">📊 asli numbers (Lab se)</span>
      <div class="dg-flow">
        <div class="dg-pill bad">exponential: peak 5,200/s</div>
        <div class="dg-arrow good"></div>
        <div class="dg-pill good">+ jitter: peak 1,800/s</div>
      </div>
    </div>
    <div class="dg-cap">Jitter throughput NAHI badhata (woh capacity se bandha hai) — woh
      <b>peak downstream load</b> ghatata hai. Aur retry ke WAQT ko sudharta hai;
      retry ki ZAROORAT circuit breaker khatam karta hai.</div>`,
},

dlq: {
  t: 'DLQ — poison message partition block na kare',
  html: `
    <div class="dg-col">
      <div class="dg-box is-bad"><span class="bx-label">☠️ poison ne partition rok di</span>
        <div class="dg-flow">
          <div class="dg-node is-muted"><span class="ni">45231</span><span class="nl">✅ ok</span></div>
          <div class="dg-node is-bad pulse" data-hs="poison"><span class="ni">45232</span><span class="nl">"NaN"</span><span class="ns">🔁 atka</span></div>
          <div class="dg-node is-muted"><span class="ni">45233</span><span class="nl">⏳</span></div>
          <div class="dg-node is-muted"><span class="ni">…</span><span class="nl">2 lakh ⏳</span></div>
        </div>
        <div class="dg-pill bad">offset ek line mein badhta hai — poison chhod nahi sakte</div>
      </div>
      <div class="dg-arrow two"></div>
      <div class="dg-box is-good"><span class="bx-label">✅ tiered retry → DLQ</span>
        <div class="dg-flow">
          <div class="dg-node is-good"><span class="ni">📥</span><span class="nl">main</span></div>
          <div class="dg-arrow flow"></div>
          <div class="dg-node is-gold"><span class="ni">⏱️</span><span class="nl">retry-5s</span></div>
          <div class="dg-arrow flow"></div>
          <div class="dg-node is-gold"><span class="ni">⏱️</span><span class="nl">retry-1m</span></div>
          <div class="dg-arrow flow"></div>
          <div class="dg-node is-bad"><span class="ni">💀</span><span class="nl">DLQ</span><span class="ns">+ context</span></div>
        </div>
        <div class="dg-pill good">partition kabhi block nahi · message gum nahi</div>
      </div>
    </div>
    <div class="dg-cap">DLQ ka sabse zaroori hissa: <b>MONITORING</b>. Bina alert ke woh
      "jahan messages marne jaate hain" ban jaata hai. Replay se pehle root cause fix karo.</div>`,
},

backpressure: {
  t: 'Back-pressure — pull model = OOM se bachav',
  html: `
    <div class="dg-split">
      <div class="dg-box is-bad"><span class="bx-label">❌ PUSH (broker dhakelta hai)</span>
        <div class="dg-flow">
          <div class="dg-node is-accent"><span class="ni">📤</span><span class="nl">5000/s</span></div>
          <div class="dg-arrow bad"></div>
          <div class="dg-node is-bad pulse"><span class="ni">💥</span><span class="nl">consumer</span><span class="ns">buffer bhara → OOM</span></div>
        </div>
        <div class="dg-pill bad">slow consumer par messages RAM mein bharte hain</div>
      </div>
      <div class="dg-box is-good"><span class="bx-label">✅ PULL (Kafka — consumer maangta hai)</span>
        <div class="dg-flow">
          <div class="dg-node is-accent"><span class="ni">📜</span><span class="nl">log (disk)</span><span class="ns">messages yahan safe</span></div>
          <div class="dg-arrow good"></div>
          <div class="dg-node is-good"><span class="ni">📥</span><span class="nl">consumer</span><span class="ns">utna hi maanga jitna paccha</span></div>
        </div>
        <div class="dg-pill good">max.poll.records chhota → memory bounded → OOM namumkin</div>
      </div>
    </div>
    <div class="dg-box"><span class="bx-label">🌊 lag badh raha hai — teen jawab</span>
      <div class="dg-flow">
        <div class="dg-pill">🗑️ load shed (purana bekaar)</div>
        <div class="dg-pill">🚦 rate limit (downstream bachao)</div>
        <div class="dg-pill">📈 scale (data zaroori)</div>
      </div>
    </div>
    <div class="dg-cap">Kafka mein OOM lagbhag hamesha ek config galti hai (bada max.poll.records),
      natural nahi. Log disk par buffer ka kaam karta hai — tumhari RAM par nahi.</div>`,
},

'circuit-breaker': {
  t: 'Circuit Breaker — closed / open / half-open',
  html: `
    <div class="dg-flow">
      <div class="dg-node is-good" data-hs="closed"><span class="ni">🟢</span><span class="nl">CLOSED</span><span class="ns">normal, fail gino</span></div>
      <div class="dg-arrow bad"></div>
      <div class="dg-node is-bad pulse" data-hs="open"><span class="ni">🔴</span><span class="nl">OPEN</span><span class="ns">fail fast, downstream ko chhua bhi nahi</span></div>
      <div class="dg-arrow gold"></div>
      <div class="dg-node is-gold" data-hs="half"><span class="ni">🟡</span><span class="nl">HALF-OPEN</span><span class="ns">1 probe request</span></div>
    </div>
    <div class="dg-flow">
      <div class="dg-pill bad">failure rate > threshold → OPEN</div>
      <div class="dg-pill gold">cooldown ke baad → HALF</div>
      <div class="dg-pill good">probe chala → CLOSED · fail → OPEN</div>
    </div>
    <div class="dg-box is-bad"><span class="bx-label">😰 bina breaker</span>
      <div class="dg-pill bad">down downstream par 50,000 retries → use uthne hi nahi diya → 2min outage = 25min</div>
    </div>
    <div class="dg-box"><span class="bx-label">🧱 poora reliability stack</span>
      <div class="dg-flow">
        <div class="dg-pill">⏱️ timeout</div>
        <div class="dg-pill">⏳ retry+jitter</div>
        <div class="dg-pill">⚡ breaker</div>
        <div class="dg-pill">🧱 bulkhead</div>
        <div class="dg-pill">💀 DLQ</div>
      </div>
    </div>
    <div class="dg-cap">Backoff retry ke <b>waqt</b> ko sudharta hai; breaker retry ki
      <b>zaroorat</b> ko khatam karta hai. Dono chahiye — alag failure modes ke liye.</div>`,
},

'replication-isr': {
  t: 'ISR — RF akela durability nahi deta',
  html: `
    <div class="dg-col">
      <div class="dg-box is-accent"><span class="bx-label">🛡️ partition 2 · RF=3 · min.insync=2</span>
        <div class="dg-flow">
          <div class="dg-node is-good"><span class="ni">👑</span><span class="nl">broker-1</span><span class="ns">leader · ISR</span></div>
          <div class="dg-node is-good"><span class="ni">📋</span><span class="nl">broker-2</span><span class="ns">follower · ISR</span></div>
          <div class="dg-node is-good"><span class="ni">📋</span><span class="nl">broker-3</span><span class="ns">follower · ISR</span></div>
        </div>
      </div>
      <div class="dg-ladder">
        <div class="dg-rung">
          <div class="dg-node is-gold"><span class="ni">💀</span><span class="nl">1 down</span></div>
          <div class="dg-pill gold">ISR = [1,2] · min.insync=2 pura → acks=all chalta hai ✅</div>
        </div>
        <div class="dg-rung">
          <div class="dg-node is-bad"><span class="ni">💀💀</span><span class="nl">2 down</span></div>
          <div class="dg-pill bad">ISR = [1] · min.insync se kam → acks=all REJECT (feature!)</div>
        </div>
      </div>
      <div class="dg-box is-bad"><span class="bx-label">☠️ unclean.leader.election=true</span>
        <div class="dg-pill bad">out-of-sync replica leader ban gaya → committed data TRUNCATE → gum</div>
      </div>
    </div>
    <div class="dg-cap">RF sirf "kitni copies BAN sakti" batata hai. Durability = acks=all +
      min.insync.replicas=2 + unclean.leader.election=false. Teeno saath.</div>`,
},

'retention-compaction': {
  t: 'Retention (delete) vs Compaction',
  html: `
    <div class="dg-split">
      <div class="dg-box is-accent"><span class="bx-label">🗑️ delete — itihaas, time/size</span>
        <div class="dg-flow">
          <div class="dg-node is-muted"><span class="ni">✗</span><span class="nl">purana</span><span class="ns">retention.ms ne uda diya</span></div>
          <div class="dg-node is-good"><span class="ni">✓</span><span class="nl">naya</span></div>
        </div>
        <div class="dg-pill acc">orders, logs, clickstream, audit</div>
        <div class="dg-pill bad">⚠️ consumer retention se peeche → OffsetOutOfRange</div>
      </div>
      <div class="dg-box is-good"><span class="bx-label">🗜️ compact — har key ka LATEST</span>
        <div class="dg-stack">
          <div class="dg-pill bad">user-1: Asha ✗</div>
          <div class="dg-pill bad">user-1: Asha-v2 ✗</div>
          <div class="dg-pill good">user-1: Asha-v3 ✓ (bacha)</div>
          <div class="dg-pill acc">user-2: null 🪦 → key gayab (tombstone)</div>
        </div>
        <div class="dg-pill acc">profiles, stock, config, balances</div>
      </div>
    </div>
    <div class="dg-cap">Sawal: har <b>event ka itihaas</b> chahiye (delete) ya har
      <b>key ka current value</b> (compact)? Compacted topic = "current state snapshot".
      Kafka ka apna __consumer_offsets bhi compacted hai.</div>`,
},

'boss-blackfriday': {
  t: 'Black Friday — ek root cause, teen aag',
  html: `
    <div class="dg-col">
      <div class="dg-node is-bad pulse" data-hs="root"><span class="ni">🐛</span><span class="nl">ROOT: corrupt producer</span><span class="ns">partition 2 par "NaN" bhej raha</span></div>
      <div class="dg-arrow two bad"></div>
      <div class="dg-flow">
        <div class="dg-node is-bad"><span class="ni">📉</span><span class="nl">lag 2.8M</span><span class="ns">sirf p2 par (lakshan)</span></div>
        <div class="dg-node is-bad"><span class="ni">💀</span><span class="nl">DLQ 12k</span><span class="ns">saare p2 se (lakshan)</span></div>
        <div class="dg-node is-bad"><span class="ni">🔁</span><span class="nl">40k retry/s</span><span class="ns">payment (lakshan)</span></div>
        <div class="dg-node is-bad"><span class="ni">💾</span><span class="nl">disk 94%</span><span class="ns">DLQ flood (lakshan)</span></div>
      </div>
      <div class="dg-box is-good"><span class="bx-label">✅ sahi kram</span>
        <div class="dg-flow">
          <div class="dg-pill good">1. samjho (sab p2 par converge)</div>
          <div class="dg-pill good">2. root band (producer)</div>
          <div class="dg-pill good">3. permanent→DLQ (valid bachao)</div>
          <div class="dg-pill good">4. lag burn down</div>
          <div class="dg-pill good">5. DLQ retention + alert</div>
        </div>
      </div>
    </div>
    <div class="dg-cap">Multi-failure mein pehle <b>samjho phir chhedo</b>. Aksar ek root
      cause kai lakshan banata hai. Recovery chal rahi ho toh haath mat lagao;
      data phenkne (offset reset) se pehle socho.</div>`,
},

/* ================= TRACK 3 — ADVANCED (STREAM GURU) ================= */

'eos-transactions': {
  t: 'EOS — sirf Kafka→Kafka ke andar',
  html: `
    <div class="dg-box is-good"><span class="bx-label">✅ EOS yahan kaam karta hai (ek transaction)</span>
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">📥</span><span class="nl">input topic</span><span class="ns">read_committed</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-box is-good"><span class="bx-label">🎯 transaction</span>
          <div class="dg-flow">
            <div class="dg-pill good">send('fees')</div>
            <div class="dg-pill good">+ commit offset</div>
          </div>
        </div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-good"><span class="ni">📤</span><span class="nl">output topic</span></div>
      </div>
      <div class="dg-pill acc">output write + offset commit ATOMIC → exactly once</div>
    </div>
    <div class="dg-box is-bad"><span class="bx-label">❌ EOS yahan BEKAAR hai</span>
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">📥</span><span class="nl">Kafka</span></div>
        <div class="dg-arrow bad"></div>
        <div class="dg-node is-bad"><span class="ni">💳</span><span class="nl">payment gateway</span><span class="ns">transaction ka hissa NAHI</span></div>
      </div>
      <div class="dg-pill bad">external side-effect roll back nahi hoga → idempotency chahiye</div>
    </div>
    <div class="dg-cap">"Exactly-once" = code ek baar chalega NAHI — <b>ASAR ek baar dikhega</b>.
      Kafka→Kafka ke bahar (payment/email/DB) idempotency hi ek matra hal.</div>`,
},

'outbox-cdc': {
  t: 'Outbox — dual-write ghost orders se bachav',
  html: `
    <div class="dg-box is-bad"><span class="bx-label">❌ dual write (ghost orders)</span>
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">📝</span><span class="nl">DB save</span><span class="ns">✅ ho gaya</span></div>
        <div class="dg-node is-bad pulse"><span class="ni">💥</span><span class="nl">CRASH</span></div>
        <div class="dg-node is-muted"><span class="ni">📤</span><span class="nl">Kafka publish</span><span class="ns">👻 kabhi nahi hua</span></div>
      </div>
      <div class="dg-pill bad">order DB mein hai par event nahi → kabhi ship nahi hua</div>
    </div>
    <div class="dg-arrow two"></div>
    <div class="dg-box is-good"><span class="bx-label">✅ transactional outbox</span>
      <div class="dg-box is-good"><span class="bx-label">ek DB transaction (atomic)</span>
        <div class="dg-flow">
          <div class="dg-pill good">INSERT orders</div>
          <div class="dg-pill good">INSERT outbox (event)</div>
        </div>
      </div>
      <div class="dg-arrow flow"></div>
      <div class="dg-flow">
        <div class="dg-node is-gold"><span class="ni">🔄</span><span class="nl">relay / CDC</span><span class="ns">outbox ya DB log se</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-accent"><span class="ni">📤</span><span class="nl">Kafka</span></div>
      </div>
    </div>
    <div class="dg-cap">Do systems atomic nahi ho sakte — dono writes EK DB mein lao, phir relay karo.
      At-least-once hai, toh downstream <b>idempotent</b> chahiye. CDC (Debezium) DB log se seedha padhta hai.</div>`,
},

'time-windows': {
  t: 'Event-time vs Processing-time',
  html: `
    <div class="dg-box is-bad"><span class="bx-label">❌ processing-time (report galat)</span>
      <div class="dg-flow">
        <div class="dg-node is-muted"><span class="ni">📱</span><span class="nl">9:00 purchase</span><span class="ns">offline</span></div>
        <div class="dg-arrow bad"></div>
        <div class="dg-node is-bad"><span class="ni">📥</span><span class="nl">12:00 aaya</span><span class="ns">12-baje bucket mein count 💀</span></div>
      </div>
    </div>
    <div class="dg-box is-good"><span class="bx-label">✅ event-time (report sahi)</span>
      <div class="dg-flow">
        <div class="dg-node is-good"><span class="ni">📱</span><span class="nl">9:00 purchase</span><span class="ns">payload timestamp</span></div>
        <div class="dg-arrow good"></div>
        <div class="dg-node is-good"><span class="ni">🪟</span><span class="nl">9-10 window</span><span class="ns">chahe 12 baje aaye</span></div>
      </div>
    </div>
    <div class="dg-box"><span class="bx-label">⏰ late data — watermark + grace</span>
      <div class="dg-flow">
        <div class="dg-pill acc">watermark: "yahan tak ke events aa chuke"</div>
        <div class="dg-pill gold">grace: thodi der aur late events lo</div>
        <div class="dg-pill bad">grace ke baad → side-output/drop</div>
      </div>
      <div class="dg-pill">⚡ TIMELY ⟷ ✅ CORRECT (business trade-off)</div>
    </div>
    <div class="dg-cap">"Kab hua" (event-time) vs "kab dekha" (processing-time). Analytics ke liye
      lagbhag hamesha event-time. Fraud = timely (chhota grace); billing = correct (bada grace).</div>`,
},

'stream-processing': {
  t: 'KStream vs KTable + state restore',
  html: `
    <div class="dg-split">
      <div class="dg-box is-accent"><span class="bx-label">📜 KStream (har event naya fact)</span>
        <div class="dg-stack">
          <div class="dg-pill acc">user-1 +10 points</div>
          <div class="dg-pill acc">user-1 +5 points</div>
          <div class="dg-pill">transactions, clicks, readings</div>
        </div>
      </div>
      <div class="dg-box is-good"><span class="bx-label">📊 KTable (har key ka latest)</span>
        <div class="dg-stack">
          <div class="dg-pill good">user-1 score = 15 (upsert)</div>
          <div class="dg-pill">balance, profile, stock</div>
          <div class="dg-pill acc">= compacted changelog</div>
        </div>
      </div>
    </div>
    <div class="dg-flow">
      <div class="dg-pill acc">KStream --aggregate--▶ KTable</div>
      <div class="dg-pill acc">KTable --changelog--▶ KStream</div>
    </div>
    <div class="dg-box is-bad"><span class="bx-label">😰 state restore ka dard</span>
      <div class="dg-flow">
        <div class="dg-node is-bad"><span class="ni">💥</span><span class="nl">pod restart</span><span class="ns">local state gaya</span></div>
        <div class="dg-arrow bad"></div>
        <div class="dg-node is-bad"><span class="ni">🔄</span><span class="nl">changelog replay</span><span class="ns">45 min · stale data</span></div>
      </div>
      <div class="dg-pill good">✅ hal: num.standby.replicas (warm copy) + static membership</div>
    </div>
    <div class="dg-cap">Stateless kaam = simple consumer (Streams ka overhead mat lo).
      Streams sirf stateful (aggregations/joins/windows) + Kafka→Kafka ke liye.</div>`,
},

'schema-registry': {
  t: 'Schema Registry — breaking change deploy par ruke',
  html: `
    <div class="dg-col">
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">📤</span><span class="nl">producer</span><span class="ns">naya schema register</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-gold" data-hs="registry"><span class="ni">📜</span><span class="nl">Schema Registry</span><span class="ns">compatibility check</span></div>
        <div class="dg-arrow bad"></div>
        <div class="dg-node is-bad"><span class="ni">🚫</span><span class="nl">breaking?</span><span class="ns">deploy REJECT</span></div>
      </div>
      <div class="dg-pill good">galti PRODUCTION se pehle CI/deploy time par pakdi gayi ✅</div>

      <div class="dg-box"><span class="bx-label">compatibility modes = deploy order</span>
        <div class="dg-flow">
          <div class="dg-pill acc">🔙 BACKWARD → consumers pehle</div>
          <div class="dg-pill acc">🔜 FORWARD → producers pehle</div>
          <div class="dg-pill gold">🔄 FULL → koi bhi kram (safest)</div>
        </div>
      </div>
      <div class="dg-flow">
        <div class="dg-pill good">✅ optional field + default</div>
        <div class="dg-pill bad">🚫 rename · type change · required add · enum value</div>
      </div>
    </div>
    <div class="dg-cap">Payload mein sirf 5-byte schema-ID (messages chhote). Registry galti ko
      "shift left" karta hai — 3AM production se CI time par. Rename? expand-contract.</div>`,
},

'security-quotas': {
  t: 'Security — 3 layers + quotas',
  html: `
    <div class="dg-ladder">
      <div class="dg-rung">
        <div class="dg-node is-accent"><span class="ni">🔒</span><span class="nl">Encryption</span><span class="ns">TLS</span></div>
        <div class="dg-pill acc">"network par koi padh na le" (in-transit)</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-gold"><span class="ni">🪪</span><span class="nl">Authentication</span><span class="ns">SASL / mTLS</span></div>
        <div class="dg-pill gold">"tum KAUN ho?" — sirf known clients connect</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-good"><span class="ni">📋</span><span class="nl">Authorization</span><span class="ns">ACLs</span></div>
        <div class="dg-pill good">"tum KYA kar sakte ho?" — least privilege</div>
      </div>
    </div>
    <div class="dg-box is-bad"><span class="bx-label">🚦 quotas — noisy neighbour</span>
      <div class="dg-flow">
        <div class="dg-node is-bad"><span class="ni">🐘</span><span class="nl">1 team</span><span class="ns">50 lakh msg backfill</span></div>
        <div class="dg-arrow bad"></div>
        <div class="dg-node is-bad"><span class="ni">🐌</span><span class="nl">29 teams</span><span class="ns">slow</span></div>
      </div>
      <div class="dg-pill good">✅ produce/request-rate quota → THROTTLE (dheema, block nahi)</div>
    </div>
    <div class="dg-cap">Authenticated ≠ authorized. TLS zero-copy ko todta hai (CPU cost).
      Security din-1 se lagao — 100 clients ke baad retrofit 10× mushkil.</div>`,
},

'multi-dc-dr': {
  t: 'Multi-DC DR — offset translation ka jaal',
  html: `
    <div class="dg-box is-bad"><span class="bx-label">❌ offsets cross-cluster match NAHI karte</span>
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">🏢</span><span class="nl">us-east</span><span class="ns">offset 845231 = msg-A</span></div>
        <div class="dg-arrow bad"></div>
        <div class="dg-node is-bad"><span class="ni">🏢</span><span class="nl">us-west</span><span class="ns">offset 845231 = msg-Z 💀</span></div>
      </div>
      <div class="dg-pill bad">failover par consumer skip/duplicate — untested DR toota</div>
    </div>
    <div class="dg-box is-good"><span class="bx-label">✅ MirrorMaker 2 + offset translation</span>
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">🏢</span><span class="nl">primary</span></div>
        <div class="dg-arrow good"></div>
        <div class="dg-node is-gold"><span class="ni">🔄</span><span class="nl">MM2</span><span class="ns">offset mapping</span></div>
        <div class="dg-arrow good"></div>
        <div class="dg-node is-good"><span class="ni">🏢</span><span class="nl">DR</span><span class="ns">sahi jagah se shuru</span></div>
      </div>
    </div>
    <div class="dg-flow">
      <div class="dg-pill acc">📉 RPO = kitna data kho sakte (async lag)</div>
      <div class="dg-pill acc">⏱️ RTO = kitni der mein online</div>
    </div>
    <div class="dg-cap">Untested DR ek dua hai, plan nahi. Active-passive (simple) vs active-active
      (tez failover par conflict/ordering). DR ko game-day drill se TEST karo.</div>`,
},

'boss-platform': {
  t: 'Platform Owner — har faisla ek concept',
  html: `
    <div class="dg-col">
      <div class="dg-node is-gold"><span class="ni">🏛️</span><span class="nl">Platform Owner</span><span class="ns">40 teams · 800 topics</span></div>
      <div class="dg-arrow two"></div>
      <div class="dg-grid">
        <div class="dg-box is-accent"><span class="bx-label">📜 schema deploy block</span>
          <div class="dg-pill acc">required→optional+default</div>
        </div>
        <div class="dg-box is-accent"><span class="bx-label">🎯 EOS pipeline slow</span>
          <div class="dg-pill acc">external DB → at-least-once+idempotent</div>
        </div>
        <div class="dg-box is-accent"><span class="bx-label">👻 ghost orders</span>
          <div class="dg-pill acc">dual-write → outbox/CDC</div>
        </div>
        <div class="dg-box is-accent"><span class="bx-label">🌍 untested DR</span>
          <div class="dg-pill acc">offset translation + game-day</div>
        </div>
        <div class="dg-box is-accent"><span class="bx-label">🚦 noisy neighbour</span>
          <div class="dg-pill acc">quota → throttle</div>
        </div>
      </div>
      <div class="dg-legend">
        <span>🛡️ payments RF=3 minISR=2 (EOS-ready)</span>
        <span>💀 DLQ (poison)</span>
        <span>🗜️ state topic compact (KTable)</span>
      </div>
    </div>
    <div class="dg-cap">Stream Guru = ek engineer jo poora PLATFORM sambhaale, sirf ek pipeline nahi.
      Har faisle mein correctness / cost / complexity ka balance.</div>`,
},


/* ================= TRACK 4 — ARCHITECT (SYSTEM DESIGN) ================= */

'log-vs-queue': {
  t: 'Log vs Queue — do models, do duniya',
  html: `
    <div class="dg-split">
      <div class="dg-box is-accent"><span class="bx-label">📜 LOG (Kafka, Redis Streams)</span>
        <div class="dg-flow">
          <div class="dg-node is-muted"><span class="ni">0</span></div>
          <div class="dg-node is-muted"><span class="ni">1</span></div>
          <div class="dg-node is-muted"><span class="ni">2</span></div>
          <div class="dg-node is-accent" data-hs="offset"><span class="ni">3</span><span class="nl">📖</span></div>
        </div>
        <div class="dg-pill acc">padhne se MITTA NAHI — offset badhta hai</div>
        <div class="dg-pill good">✅ replay · fanout muft · high throughput</div>
        <div class="dg-pill bad">❌ per-message retry · priority · poison blocks</div>
      </div>
      <div class="dg-box is-gold"><span class="bx-label">📬 QUEUE (RabbitMQ, SQS)</span>
        <div class="dg-flow">
          <div class="dg-node is-gold"><span class="ni">📨</span><span class="nl">msg</span></div>
          <div class="dg-arrow gold"></div>
          <div class="dg-node is-good"><span class="ni">✓</span><span class="nl">ack</span><span class="ns">→ DELETE</span></div>
        </div>
        <div class="dg-pill gold">ack milte hi message GAYA</div>
        <div class="dg-pill good">✅ per-message ack/nack · priority · isolation</div>
        <div class="dg-pill bad">❌ replay nahi · fanout ke liye alag queues</div>
      </div>
    </div>
    <div class="dg-box"><span class="bx-label">❓ char sawal jinse tool chunte ho</span>
      <div class="dg-flow">
        <div class="dg-pill acc">1. Replay chahiye? → log</div>
        <div class="dg-pill acc">2. Fanout kaisa? → log</div>
        <div class="dg-pill gold">3. Per-message control? → queue</div>
        <div class="dg-pill acc">4. Throughput? lakhs/s → log</div>
      </div>
    </div>
    <div class="dg-cap">Bade systems mein aksar <b>DONO</b> hote hain — Kafka event backbone,
      RabbitMQ/SQS task queues. "Ek tool sab kuch" hi sabse aam architecture galti hai.</div>`,
},

'rabbitmq-model': {
  t: 'RabbitMQ — exchange routing + memory ka khatra',
  html: `
    <div class="dg-col">
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">📤</span><span class="nl">Producer</span><span class="ns">queue jaanta hi nahi</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-gold" data-hs="exchange"><span class="ni">🔀</span><span class="nl">EXCHANGE</span><span class="ns">direct/fanout/topic</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-flow">
          <div class="dg-node is-good"><span class="ni">📬</span><span class="nl">queue-A</span></div>
          <div class="dg-node is-good"><span class="ni">📬</span><span class="nl">queue-B</span></div>
        </div>
      </div>
      <div class="dg-box"><span class="bx-label">✅ per-message control (Kafka mein nahi)</span>
        <div class="dg-flow">
          <div class="dg-pill good">ack → delete</div>
          <div class="dg-pill gold">nack(requeue) → wapas</div>
          <div class="dg-pill bad">nack(false) → DLX 💀</div>
        </div>
      </div>
      <div class="dg-box is-bad"><span class="bx-label">😰 bada backlog = broker ka khatra</span>
        <div class="dg-flow">
          <div class="dg-node is-bad"><span class="ni">📈</span><span class="nl">40 lakh msgs</span><span class="ns">MEMORY mein</span></div>
          <div class="dg-arrow bad"></div>
          <div class="dg-node is-bad pulse"><span class="ni">🛑</span><span class="nl">producers BLOCK</span><span class="ns">flow control</span></div>
        </div>
        <div class="dg-pill good">✅ prefetch(N) LAZMI — push model mein yahi back-pressure hai</div>
      </div>
    </div>
    <div class="dg-cap">Kafka: backlog <b>disk</b> par (normal). RabbitMQ: backlog <b>memory</b> par (incident).
      Mantra: <b>"queues should be short"</b> — woh transport hai, storage nahi.</div>`,
},

'sqs-model': {
  t: 'SQS — visibility timeout ka lease',
  html: `
    <div class="dg-ladder">
      <div class="dg-rung">
        <div class="dg-node is-accent"><span class="ni">📥</span><span class="nl">receive</span></div>
        <div class="dg-pill acc">message INVISIBLE ho gaya (delete nahi hua) — 30s lease</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-good"><span class="ni">🗑️</span><span class="nl">delete</span></div>
        <div class="dg-pill good">time ke andar delete = "ack" → message gaya ✅</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-bad pulse"><span class="ni">⏰</span><span class="nl">timeout paar</span></div>
        <div class="dg-pill bad">wapas VISIBLE → koi aur uthayega → 📦📦 DUPLICATE</div>
      </div>
    </div>
    <div class="dg-box is-bad"><span class="bx-label">💥 classic bug</span>
      <div class="dg-pill bad">visibility timeout (30s) &lt; processing time (45s) = guaranteed duplicates</div>
      <div class="dg-pill good">✅ timeout &gt; p99 processing · ya ChangeMessageVisibility heartbeat</div>
    </div>
    <div class="dg-flow">
      <div class="dg-pill">📮 Standard: at-least-once, unlimited throughput</div>
      <div class="dg-pill gold">🔢 FIFO: ordered per MessageGroupId, ~300/s</div>
    </div>
    <div class="dg-flow">
      <div class="dg-pill acc">🎣 long polling (20s) → kam cost + kam latency</div>
      <div class="dg-pill bad">💀 maxReceiveCount → built-in DLQ</div>
    </div>
    <div class="dg-cap">Fanout ke liye <b>SNS → kai SQS queues</b> (ek queue par kai consumers
      messages BAANT lenge). Replay phir bhi nahi — delete ke baad message gaya.</div>`,
},

'mqtt-iot': {
  t: 'MQTT QoS — packets ki keemat',
  html: `
    <div class="dg-ladder">
      <div class="dg-rung">
        <div class="dg-node is-good"><span class="ni">📮</span><span class="nl">QoS 0</span><span class="ns">1 packet</span></div>
        <div class="dg-pill good">PUBLISH → bas. Sabse tez, kam battery. Kho sakta hai.</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-gold"><span class="ni">📬</span><span class="nl">QoS 1</span><span class="ns">2 packets</span></div>
        <div class="dg-pill gold">PUBLISH → PUBACK. Pahunchega, par DUPLICATE ho sakta. ⭐ DEFAULT</div>
      </div>
      <div class="dg-rung">
        <div class="dg-node is-bad"><span class="ni">🎯</span><span class="nl">QoS 2</span><span class="ns">4 packets 😰</span></div>
        <div class="dg-pill bad">PUBLISH→PUBREC→PUBREL→PUBCOMP = 4× traffic, broker state</div>
      </div>
    </div>
    <div class="dg-flow">
      <div class="dg-pill acc">📌 retained: naye subscriber ko turant current status</div>
      <div class="dg-pill acc">🪦 LWT: crash par apne aap "offline" publish</div>
    </div>
    <div class="dg-box is-good"><span class="bx-label">🌉 IoT ingest ka standard pattern</span>
      <div class="dg-flow">
        <div class="dg-node is-accent"><span class="ni">📡</span><span class="nl">devices</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-gold"><span class="ni">🏢</span><span class="nl">MQTT</span><span class="ns">transport</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-accent"><span class="ni">🟣</span><span class="nl">Kafka</span><span class="ns">storage+replay</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-good"><span class="ni">📊</span><span class="nl">analytics</span></div>
      </div>
    </div>
    <div class="dg-cap"><b>QoS 1 + idempotency</b> lagbhag hamesha QoS 2 se behtar — wahi "at-least-once
      + idempotency" sabak. MQTT storage nahi hai; Kafka bridge chahiye.</div>`,
},

fanout: {
  t: 'Fanout — celebrity problem aur hybrid',
  html: `
    <div class="dg-split">
      <div class="dg-box is-accent"><span class="bx-label">✍️ fanout-on-WRITE</span>
        <div class="dg-flow">
          <div class="dg-node is-accent"><span class="ni">📝</span><span class="nl">1 post</span></div>
          <div class="dg-arrow flow"></div>
          <div class="dg-node is-good"><span class="ni">📥</span><span class="nl">200 feeds</span><span class="ns">pre-computed</span></div>
        </div>
        <div class="dg-pill good">✅ read turant</div>
        <div class="dg-pill bad">❌ write mehngi (N followers)</div>
      </div>
      <div class="dg-box is-gold"><span class="bx-label">👁️ fanout-on-READ</span>
        <div class="dg-flow">
          <div class="dg-node is-gold"><span class="ni">📝</span><span class="nl">1 write</span></div>
          <div class="dg-arrow gold"></div>
          <div class="dg-node is-muted"><span class="ni">🔀</span><span class="nl">read par merge</span></div>
        </div>
        <div class="dg-pill good">✅ write sasti</div>
        <div class="dg-pill bad">❌ read mehngi (N queries)</div>
      </div>
    </div>
    <div class="dg-box is-bad"><span class="bx-label">🌟 celebrity problem</span>
      <div class="dg-flow">
        <div class="dg-node is-bad pulse"><span class="ni">⭐</span><span class="nl">1 post</span><span class="ns">5 crore followers</span></div>
        <div class="dg-arrow bad"></div>
        <div class="dg-node is-bad"><span class="ni">💥</span><span class="nl">5 crore writes</span><span class="ns">system block</span></div>
      </div>
      <div class="dg-pill good">✅ HYBRID: normal → write-fanout · celebrity → read-fanout (threshold se)</div>
    </div>
    <div class="dg-cap">Faisla <b>read/write ratio</b> se. Aur yaad rakho: <b>Kafka load SMOOTH
      karta hai, kaam KAM nahi karta</b> — celebrity problem ka hal algorithm hai, queue nahi.</div>`,
},

saga: {
  t: 'Saga — compensations ki zanjeer',
  html: `
    <div class="dg-col">
      <div class="dg-box is-good"><span class="bx-label">➡️ forward path</span>
        <div class="dg-flow">
          <div class="dg-node is-good"><span class="ni">1</span><span class="nl">order</span></div>
          <div class="dg-arrow flow"></div>
          <div class="dg-node is-good"><span class="ni">2</span><span class="nl">inventory</span></div>
          <div class="dg-arrow flow"></div>
          <div class="dg-node is-good"><span class="ni">3</span><span class="nl">payment</span></div>
          <div class="dg-arrow flow"></div>
          <div class="dg-node is-muted"><span class="ni">4</span><span class="nl">📧 email</span><span class="ns">IRREVERSIBLE</span></div>
        </div>
      </div>
      <div class="dg-box is-bad"><span class="bx-label">⬅️ fail hua? ULTE kram mein compensate</span>
        <div class="dg-flow">
          <div class="dg-node is-bad"><span class="ni">↩️</span><span class="nl">refund</span></div>
          <div class="dg-arrow bad"></div>
          <div class="dg-node is-bad"><span class="ni">↩️</span><span class="nl">release stock</span></div>
          <div class="dg-arrow bad"></div>
          <div class="dg-node is-bad"><span class="ni">↩️</span><span class="nl">cancel order</span></div>
        </div>
        <div class="dg-pill bad">compensation = NAYA transaction (rollback nahi) → idempotent hona chahiye</div>
      </div>
      <div class="dg-split">
        <div class="dg-box"><span class="bx-label">💃 choreography</span>
          <div class="dg-pill">events se, no central brain · 3-4 steps tak achha</div>
        </div>
        <div class="dg-box"><span class="bx-label">🎭 orchestration</span>
          <div class="dg-pill">coordinator, flow ek jagah · complex flows ke liye</div>
        </div>
      </div>
    </div>
    <div class="dg-cap">2PC avoid karo (blocking, dheema, third-party support nahi).
      Golden rule: <b>irreversible steps SABSE AAKHIR</b>. Saga = eventual consistency, atomicity nahi.</div>`,
},

'capacity-cost': {
  t: 'Capacity — ordering vs parallelism vs cost',
  html: `
    <div class="dg-box is-accent"><span class="bx-label">📐 partition formula</span>
      <div class="dg-flow">
        <div class="dg-pill acc">peak ÷ per_consumer</div>
        <div class="dg-pill">max(</div>
        <div class="dg-pill acc">max_consumers_ever</div>
        <div class="dg-pill">) × 1.5 headroom</div>
      </div>
    </div>
    <div class="dg-split">
      <div class="dg-box is-bad"><span class="bx-label">❌ bahut KAM partitions</span>
        <div class="dg-pill bad">parallelism ki limit · badhane par ordering TOOT-TI hai</div>
      </div>
      <div class="dg-box is-bad"><span class="bx-label">❌ bahut ZYADA partitions</span>
        <div class="dg-pill bad">failover 12 min · file handles · producer memory · latency</div>
      </div>
    </div>
    <div class="dg-box"><span class="bx-label">⚖️ ordering ⟷ parallelism (seedha takraav)</span>
      <div class="dg-flow">
        <div class="dg-pill">1 partition = global order, parallelism 1</div>
        <div class="dg-pill acc">key = per-entity order (99% cases)</div>
        <div class="dg-pill good">no key = max parallelism</div>
      </div>
    </div>
    <div class="dg-box is-gold"><span class="bx-label">💰 storage = throughput × size × retention × RF</span>
      <div class="dg-flow">
        <div class="dg-pill gold">50k/s × 1KB × 7din × RF3 = ~87 TB 😳</div>
      </div>
      <div class="dg-flow">
        <div class="dg-pill good">🗜️ compression 3-5×</div>
        <div class="dg-pill good">📉 retention</div>
        <div class="dg-pill good">🗃️ compaction</div>
        <div class="dg-pill good">🧊 tiered</div>
      </div>
    </div>
    <div class="dg-cap">RF multiply karna bhoolna sabse aam capacity galti hai.
      Aur "global ordering chahiye" lagbhag hamesha over-specification hai.</div>`,
},

'boss-gauntlet': {
  t: 'RideNow — poora platform, poora course',
  html: `
    <div class="dg-col">
      <div class="dg-flow">
        <div class="dg-node is-gold"><span class="ni">📡</span><span class="nl">MQTT</span><span class="ns">location · QoS 0</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-accent"><span class="ni">🌉</span><span class="nl">bridge</span></div>
        <div class="dg-arrow flow"></div>
        <div class="dg-node is-accent" data-hs="core"><span class="ni">🟣</span><span class="nl">Kafka</span><span class="ns">ride-events · key=rideId</span></div>
      </div>
      <div class="dg-arrow two"></div>
      <div class="dg-flow">
        <div class="dg-box is-good"><span class="bx-label">👥 payments</span>
          <div class="dg-pill good">outbox → queue → gateway</div>
          <div class="dg-pill bad">breaker + DLQ</div>
        </div>
        <div class="dg-box is-good"><span class="bx-label">👥 analytics</span>
          <div class="dg-pill good">event-time windows</div>
          <div class="dg-pill acc">replay chahiye</div>
        </div>
        <div class="dg-box is-good"><span class="bx-label">👥 notifications</span>
          <div class="dg-pill good">dedup + rate limit</div>
        </div>
      </div>
      <div class="dg-legend">
        <span>🌱 T1: partitions · keys · acks · groups</span>
        <span>🔥 T2: lag · DLQ · jitter · breaker</span>
        <span>🚀 T3: outbox · event-time · schema · DR</span>
        <span>🏛️ T4: tool choice · capacity · saga</span>
      </div>
    </div>
    <div class="dg-cap">👑 Har concept ek asli <b>PROBLEM</b> se shuru hota hai · har solution ki ek
      <b>KEEMAT</b> hai · aur senior wahi hai jo apne design ki <b>NAYI problems khud bataye</b>.</div>`,
},


'kraft-ops': {
  t: 'KRaft — metadata plane aur data plane alag hain',
  html: `
    <div class="dg-split">
      <div class="dg-box is-bad"><span class="bx-label">🕰️ PURANA — ZooKeeper ke saath</span>
        <div class="dg-col">
          <div class="dg-flow">
            <div class="dg-node is-bad"><span class="ni">🐘</span><span class="nl">ZooKeeper</span><span class="ns">alag cluster<br>alag ops, alag tuning</span></div>
          </div>
          <div class="dg-down"></div>
          <div class="dg-node is-muted"><span class="ni">👑</span><span class="nl">Controller broker</span><span class="ns">ZK se metadata padhta hai</span></div>
          <div class="dg-down"></div>
          <div class="dg-flow">
            <div class="dg-node is-muted"><span class="ni">📦</span><span class="nl">broker 1</span></div>
            <div class="dg-node is-muted"><span class="ni">📦</span><span class="nl">broker 2</span></div>
            <div class="dg-node is-muted"><span class="ni">📦</span><span class="nl">broker 3</span></div>
          </div>
        </div>
        <div class="dg-cap">Controller failover par <b>saara</b> metadata ZK se dobara padhna padta tha — bade cluster par minute lag jaate the.</div>
      </div>

      <div class="dg-box is-good"><span class="bx-label">⚡ AAJ — KRaft (Kafka 4.0 se sirf yehi)</span>
        <div class="dg-col">
          <div class="dg-flow">
            <div class="dg-node is-good" data-hs="quorum"><span class="ni">🗳️</span><span class="nl">controller 1</span><span class="ns">LEADER</span></div>
            <div class="dg-node is-accent" data-hs="quorum"><span class="ni">🗳️</span><span class="nl">controller 2</span><span class="ns">follower</span></div>
            <div class="dg-node is-accent" data-hs="quorum"><span class="ni">🗳️</span><span class="nl">controller 3</span><span class="ns">follower</span></div>
          </div>
          <div class="dg-cap" style="margin:4px 0">metadata khud ek <b>Raft log</b> hai — __cluster_metadata</div>
          <div class="dg-down"></div>
          <div class="dg-flow">
            <div class="dg-node is-ok" data-hs="data"><span class="ni">📦</span><span class="nl">broker 1</span><span class="ns">metadata log follow karta hai</span></div>
            <div class="dg-node is-ok" data-hs="data"><span class="ni">📦</span><span class="nl">broker 2</span></div>
            <div class="dg-node is-ok" data-hs="data"><span class="ni">📦</span><span class="nl">broker 3</span></div>
          </div>
        </div>
        <div class="dg-cap">Har broker ke paas metadata ki <b>apni copy</b> hai. Controller failover = bas naya leader chunna, seconds mein.</div>
      </div>
    </div>

    <div class="dg-rule"></div>
    <div class="dg-legend">
      <span class="dg-pill is-warn">🗳️ METADATA PLANE — quorum chahiye</span>
      <span>topic create/delete · partition reassign · config change · ACL</span>
    </div>
    <div class="dg-legend">
      <span class="dg-pill is-good">📦 DATA PLANE — quorum ke bina bhi chalta hai</span>
      <span>produce · consume · offset commit — brokers apni aakhri maloom metadata par kaam karte rehte hain</span>
    </div>
    <div class="dg-cap"><b>3 voters = 1 failure jhel sakte ho. 5 voters = 2.</b>
      Majority formula: <code>floor(n/2) + 1</code>. Quorum toota toh <b>admin commands</b> marte hain,
      par tumhara traffic chalta rehta hai — yeh farak 3AM par jaan bachata hai.</div>`,
},

'kafka-connect': {
  t: 'Kafka Connect — sink connector ek consumer group hi hai',
  html: `
    <div class="dg-flow">
      <div class="dg-node is-muted"><span class="ni">🗄️</span><span class="nl">Postgres</span><span class="ns">source system</span></div>
      <div class="dg-arrow"></div>
      <div class="dg-node is-accent" data-hs="source"><span class="ni">📥</span><span class="nl">Source connector</span><span class="ns">JdbcSource<br>tasks.max = 4</span></div>
      <div class="dg-arrow"></div>
      <div class="dg-node is-gold" data-hs="topic"><span class="ni">📚</span><span class="nl">orders topic</span><span class="ns">6 partitions</span></div>
      <div class="dg-arrow"></div>
      <div class="dg-node is-accent" data-hs="sink"><span class="ni">📤</span><span class="nl">Sink connector</span><span class="ns">S3Sink<br>= consumer group<br><b>connect-s3-sink</b></span></div>
      <div class="dg-arrow"></div>
      <div class="dg-node is-muted"><span class="ni">🪣</span><span class="nl">S3</span><span class="ns">sink system</span></div>
    </div>

    <div class="dg-rule"></div>

    <div class="dg-split">
      <div class="dg-box is-warn"><span class="bx-label">🔧 SMT — beech mein chhota transform</span>
        <div class="dg-stack">
          <div class="dg-node is-muted"><span class="ni">1️⃣</span><span class="nl">MaskField</span><span class="ns">card number chhupao</span></div>
          <div class="dg-node is-muted"><span class="ni">2️⃣</span><span class="nl">InsertField</span><span class="ns">ingested_at timestamp jodo</span></div>
          <div class="dg-node is-muted"><span class="ni">3️⃣</span><span class="nl">Filter</span><span class="ns">heartbeat records girao</span></div>
        </div>
        <div class="dg-cap">Per-record, stateless. <b>Join ya aggregation SMT se nahi hote</b> — uske liye Streams/Flink chahiye.</div>
      </div>

      <div class="dg-box is-bad"><span class="bx-label">☠️ POISON PILL — ek record, poori pipeline band</span>
        <div class="dg-stack">
          <div class="dg-node is-ok"><span class="ni">✅</span><span class="nl">offset 0-12</span><span class="ns">S3 par likhe gaye</span></div>
          <div class="dg-node is-bad" data-hs="poison"><span class="ni">💀</span><span class="nl">offset 13</span><span class="ns">CORRUPT → DataException<br>task FAILED</span></div>
          <div class="dg-node is-muted"><span class="ni">⏸️</span><span class="nl">offset 14+</span><span class="ns">kabhi process hi nahi hue</span></div>
        </div>
        <div class="dg-cap"><b>Lag chadhta rehta hai par connector "chal raha" dikhta hai.</b>
          Connect fail hue task ko <b>apne aap restart nahi karta</b>.</div>
      </div>
    </div>

    <div class="dg-rule"></div>
    <div class="dg-legend">
      <span class="dg-pill is-bad">errors.tolerance = none</span>
      <span>ek kharab record = task FAILED = pipeline rukhi (default)</span>
    </div>
    <div class="dg-legend">
      <span class="dg-pill is-good">errors.tolerance = all + DLQ</span>
      <span>kharab record DLQ topic mein, baaki chalte rehte hain ✅</span>
    </div>
    <div class="dg-legend">
      <span class="dg-pill is-bad">errors.tolerance = all, DLQ NAHI</span>
      <span>☠️ sabse khatarnak — data <b>chup-chaap</b> gayab, koi alert nahi</span>
    </div>
    <div class="dg-cap"><b>Sink tasks kabhi partitions se zyada nahi ho sakte.</b>
      <code>tasks.max=10</code> par topic mein 3 partitions = 7 tasks khaali baithe.
      Kyunki sink connector andar se ek consumer group hi hai — wahi purana niyam.</div>`,
},

};
