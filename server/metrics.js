const PROTOS = ['json', 'text'];
const startTime = Date.now();
const total = { connections: 0, messages: 0, delivered: 0, errors: 0, bytesIn: 0, bytesOut: 0 };
let active = 0;
let online = 0;
const byProtocol = Object.fromEntries(
  PROTOS.map((p) => [p, { active: 0, connections: 0, messages: 0, bytesIn: 0, bytesOut: 0 }])
);
let last = { messages: 0, bytesIn: 0, bytesOut: 0 };
let rate = { messagesPerSecond: 0, bytesInPerSecond: 0, bytesOutPerSecond: 0 };
let rttSum = 0, rttCount = 0;

// Once per second, turn running totals into per-second rates
setInterval(() => {
  rate = {
    messagesPerSecond: total.messages - last.messages,
    bytesInPerSecond: total.bytesIn - last.bytesIn,
    bytesOutPerSecond: total.bytesOut - last.bytesOut
  };
  last = { messages: total.messages, bytesIn: total.bytesIn, bytesOut: total.bytesOut };
}, 1000).unref();

module.exports = {
  connectionOpened(p) { active++; total.connections++; byProtocol[p].active++; byProtocol[p].connections++; },
  connectionClosed(p) { active--; byProtocol[p].active--; },
  setOnlineUsers(n) { online = n; },
  messageSent(p) { total.messages++; byProtocol[p].messages++; },
  messageDelivered() { total.delivered++; },
  error() { total.errors++; },
  bytesReceived(p, n) { total.bytesIn += n; byProtocol[p].bytesIn += n; },
  bytesSent(p, n) { total.bytesOut += n; byProtocol[p].bytesOut += n; },
  pingRtt(ms) { rttSum += ms; rttCount++; },
  getStats() {
    return {
      activeConnections: active,
      onlineUsers: online,
      ...total,
      ...rate,
      byProtocol,
      avgPingRttMs: rttCount ? Number((rttSum / rttCount).toFixed(2)) : null,
      uptimeSeconds: Math.floor((Date.now() - startTime) / 1000)
    };
  }
};