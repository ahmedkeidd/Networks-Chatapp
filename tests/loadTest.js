// Load test + performance numbers for the PDF.
// Starts fake clients, sends messages in a ring (client i -> client i+1),
// and measures latency, protocol cost (JSON vs text) and scalability (2 / 5 / 20 clients).
//
// Run the server first (node server/index.js), then: node tests/loadTest.js
// To test over Wi-Fi, set HOST to the laptop's IP.

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const HOST = process.env.HOST || 'localhost';
const PORT = 3000;
const MSGS_PER_CLIENT = 50;        // messages each client sends
const INTERVAL_MS = 50;            // pause between a client's messages
const CLIENT_COUNTS = [2, 5, 20];  // 1 client can't chat alone, so 2 is the smallest conversation
const PROTOCOLS = ['json', 'text'];
const RUN_ID = Math.random().toString(36).slice(2, 7);

let scenarioCounter = 0;           // makes usernames unique for every scenario

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => performance.now();

// ---- how each protocol writes the packets we need ----
const encode = {
  json: {
    register: (u, p) => JSON.stringify({ type: 'register', username: u, password: p }),
    login: (u, p) => JSON.stringify({ type: 'login', username: u, password: p }),
    dm: (id, to, text) => JSON.stringify({ type: 'message', id, to, text })
  },
  text: {
    register: (u, p) => `REGISTER|${u}|${p}`,
    login: (u, p) => `LOGIN|${u}|${p}`,
    dm: (id, to, text) => `DM|${id}|${to}|${text}`
  }
};

// Turn a raw server reply into { kind: 'ack' | 'message' | 'other', id }
function parseIncoming(proto, raw) {
  if (proto === 'json') {
    try {
      const m = JSON.parse(raw);
      if (m.type === 'ack') return { kind: 'ack', id: m.id };
      if (m.type === 'message') return { kind: 'message', id: m.id };
    } catch { /* ignore */ }
    return { kind: 'other' };
  }
  const p = raw.split('|');
  if (p[0] === 'ACK') return { kind: 'ack', id: p[1] };
  if (p[0] === 'MSG') return { kind: 'message', id: p[1] };
  return { kind: 'other' };
}

// ---- small client helper ----
function connect(proto) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`ws://${HOST}:${PORT}/?proto=${proto}`);
    const client = { socket, proto, waiters: [], onData: null };
    socket.on('message', (d) => {
      const raw = d.toString();
      client.waiters = client.waiters.filter((w) => {
        if (w.test(raw)) { w.done(raw); return false; }
        return true;
      });
      if (client.onData) client.onData(raw);
    });
    socket.on('open', () => resolve(client));
    socket.on('error', reject);
  });
}

// Resolve when a reply matching `test` arrives (or fail after 5s)
function waitFor(client, test, ms = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timeout waiting for server reply')), ms);
    client.waiters.push({ test, done: (raw) => { clearTimeout(timer); resolve(raw); } });
  });
}

async function setupClient(proto, name) {
  const c = await connect(proto);
  c.name = name;

  let p = waitFor(c, (r) => r.includes('register_ok') || r.startsWith('REGISTER_OK'));
  c.socket.send(encode[proto].register(name, 'pw'));
  await p;

  p = waitFor(c, (r) => r.includes('login_ok') || r.startsWith('LOGIN_OK'));
  c.socket.send(encode[proto].login(name, 'pw'));
  await p;
  return c;
}

const getMetrics = async () => (await fetch(`http://${HOST}:${PORT}/metrics`)).json();

// ---- statistics helpers ----
const avg = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);
const percentile = (a, p) => {
  if (!a.length) return 0;
  const s = [...a].sort((x, y) => x - y);
  return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)];
};
const r2 = (n) => Number(n.toFixed(2));

// ---- one test run ----
async function runScenario(proto, numClients, msgsPerClient, intervalMs) {
  const scenarioNo = ++scenarioCounter;
  const names = Array.from(
    { length: numClients },
    (_, i) => `lt${RUN_ID}_s${scenarioNo}_${proto}_u${i}`
  );
  const clients = await Promise.all(names.map((n) => setupClient(proto, n)));

  const before = await getMetrics(); // taken after login, so bytes below are messaging only

  const sentAt = new Map();      // message id -> send time
  const receivedIds = new Set();
  const latencies = [];          // sender -> recipient
  const ackTimes = [];           // sender -> server ack (round trip)
  let duplicates = 0;
  let lastDelivery = 0;

  for (const c of clients) {
    c.onData = (raw) => {
      const m = parseIncoming(proto, raw);
      const t = sentAt.get(m.id);
      if (t === undefined) return;
      if (m.kind === 'ack') {
        ackTimes.push(now() - t);
      } else if (m.kind === 'message') {
        if (receivedIds.has(m.id)) { duplicates++; return; }
        receivedIds.add(m.id);
        latencies.push(now() - t);
        lastDelivery = now();
      }
    };
  }

  const expected = numClients * msgsPerClient;
  const startSend = now();

  // every client sends to the next one in the ring, all at the same time
  await Promise.all(clients.map(async (c, i) => {
    const target = names[(i + 1) % numClients];
    for (let k = 0; k < msgsPerClient; k++) {
      const id = `${RUN_ID}-s${scenarioNo}-${i}-${k}`;
      sentAt.set(id, now());
      c.socket.send(encode[proto].dm(id, target, 'load test message ' + k));
      await wait(intervalMs);
    }
  }));

  // wait up to 3s for the last messages to arrive
  const deadline = now() + 3000;
  while (receivedIds.size < expected && now() < deadline) await wait(50);

  const after = await getMetrics();
  const seconds = Math.max((lastDelivery - startSend) / 1000, 0.001);
  const bp = (key) => after.byProtocol[proto][key] - before.byProtocol[proto][key];

  clients.forEach((c) => c.socket.close());
  await wait(300);

  return {
    protocol: proto,
    clients: numClients,
    sent: expected,
    delivered: receivedIds.size,
    lost: expected - receivedIds.size,
    duplicates,
    avgLatencyMs: r2(avg(latencies)),
    p95LatencyMs: r2(percentile(latencies, 95)),
    maxLatencyMs: r2(Math.max(0, ...latencies)),
    avgAckMs: r2(avg(ackTimes)),
    msgsPerSec: r2(receivedIds.size / seconds),
    bytesInPerMsg: r2(bp('bytesIn') / expected),    // client -> server
    bytesOutPerMsg: r2(bp('bytesOut') / expected),  // server -> clients (ack + delivery)
    serverConnections: before.activeConnections
  };
}

(async () => {
  console.log(`Load test against ${HOST}:${PORT}  (${MSGS_PER_CLIENT} msgs per client, ${INTERVAL_MS} ms apart)\n`);

  console.log('Warm-up (not counted)...');
  await runScenario('json', 2, 10, INTERVAL_MS);

  const results = [];
  for (const proto of PROTOCOLS) {
    for (const n of CLIENT_COUNTS) {
      console.log(`Running ${proto.toUpperCase()} with ${n} clients...`);
      results.push(await runScenario(proto, n, MSGS_PER_CLIENT, INTERVAL_MS));
    }
  }

  console.log('\n=== RESULTS ===');
  console.table(results);

  const out = path.join(__dirname, 'loadTestResults.json');
  fs.writeFileSync(out, JSON.stringify({ host: HOST, date: new Date().toISOString(), results }, null, 2));
  console.log('Saved to', out);

  const final = await getMetrics();
  console.log('\nServer /metrics at the end:', final);
  process.exit(0);
})().catch((e) => {
  console.error('Load test failed:', e.message);
  console.error('Is the server running? Start it with: node server/index.js');
  process.exit(1);
});