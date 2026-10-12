/* =====================================================================
   Kafka Gurukul — Ranks & Tracks
   Naya track add karna ho? Yahan ek entry + ek nayi data-<track>.js file.
   Tracks order mein unlock hote hain (is array ka order = unlock order).
   ===================================================================== */

const RANKS = [
  { xp: 0,    name: 'Sandesh Shishya',      icon: '🐣' },
  { xp: 200,  name: 'Producer Pujari',      icon: '📤' },
  { xp: 450,  name: 'Consumer Chela',       icon: '📥' },
  { xp: 750,  name: 'Partition Pandit',     icon: '🗂️' },
  { xp: 1100, name: 'Offset Ojha',          icon: '🔖' },
  { xp: 1500, name: 'Lag Lakshak',          icon: '📉' },
  { xp: 1950, name: 'Retry Rishi',          icon: '🔁' },
  { xp: 2450, name: 'DLQ Dwarpal',          icon: '💀' },
  { xp: 3000, name: 'Stream Sadhak',        icon: '🌊' },
  { xp: 3600, name: 'Exactly-Once Acharya', icon: '🎯' },
  { xp: 4300, name: 'Kafka Mahaguru',       icon: '👑' },
];

/* Tracks: Beginner → Reliability → Streams → Architect.
   `levels` lazily KQ.levelSets se resolve hote hain (data-*.js bharti hain). */
KQ.tracks = [
  {
    id: 'beginner',
    icon: '🌱',
    name: 'Beginner — Sandesh Shishya Path',
    blurb: 'Pehle samjho ki queue chahiye hi KYUN — sync call ki maut se shuru. Phir broker, topics, partitions, offsets, producer, consumer groups, keys aur delivery semantics. Yahan se sab shuru hota hai.',
    certTitle: 'SANDESH PANDIT',
    certSub: 'Certificate of Messaging Foundations',
    motto: 'Sync call jodta hai, message alag karta hai. Alag hona hi scale hai.',
    formula: 'Topic = Partitions ka set · Partition = ordered log · Offset = tumhara bookmark',
  },
  {
    id: 'intermediate',
    icon: '🔥',
    name: 'Intermediate — Vishwas Sadhak Path',
    blurb: 'Yahan asli production rehta hai — consumer lag, rebalance ka toofan, retries, exponential backoff + jitter, Dead Letter Queue, back-pressure, circuit breaker, ISR/acks, retention aur compaction. Jahan 3AM ko pager bajta hai, wahan ke hathiyaar.',
    certTitle: 'VISHWAS SADHAK',
    certSub: 'Certificate of Production Reliability',
    motto: 'Har cheez fail hogi. Sawal yeh hai ki fail hone par tumhara system kya karega.',
    formula: 'Bharosa = Idempotency + Backoff(jitter) + DLQ + Back-pressure + Circuit Breaker',
  },
  {
    id: 'advanced',
    icon: '🚀',
    name: 'Advanced — Stream Guru Path',
    blurb: 'Senior engineer ka shastra — exactly-once & transactions, transactional outbox + CDC, event-time windows & late data, Kafka Streams, Schema Registry evolution, security & quotas, multi-DC disaster recovery.',
    certTitle: 'STREAM GURU',
    certSub: 'Certificate of Streaming Mastery',
    motto: 'Dual-write jhooth bolta hai. Log sach bolta hai — usi ko source of truth banao.',
    formula: 'EOS = Idempotent producer + Transactions + read_committed consumer',
  },
  {
    id: 'sysdesign',
    icon: '🏛️',
    name: 'Architect — System Design Path',
    blurb: 'Ab interview waali table par baitho. Log vs Queue ka asli farak, RabbitMQ / SQS / MQTT / Redis Streams ke models, fanout systems, saga & event-driven microservices, partition math aur cost. Final Boss: poora pipeline design karo.',
    certTitle: 'KAFKA MAHAGURU',
    certSub: 'Certificate of Messaging System Design',
    motto: 'Sahi tool chunna aadha design hai. Baaki aadha failure modes jaanna hai.',
    formula: 'Choice = Replay chahiye? × Fanout kaisa? × Ordering kitni? × Ack per message?',
  },
];
