const stats = {
  active: 0,
  totalConnections: 0,
  totalMessages: 0,
  bytesIn: 0,
  bytesOut: 0,
  connectionsByProtocol: { json: 0, text: 0 }
};
const startTime = Date.now();
let lastCount = 0;
let perSecond = 0;
let rttSum = 0;
let rttCount = 0;

setInterval(() => {
  perSecond = stats.totalMessages - lastCount;
  lastCount = stats.totalMessages;
}, 1000);

module.exports = {
  connectionOpened(protocol) {
    stats.active++;
    stats.totalConnections++;
    stats.connectionsByProtocol[protocol]++;
  },
  connectionClosed() { stats.active--; },
  messageSent() { stats.totalMessages++; },
  bytesReceived(n) { stats.bytesIn += n; },
  bytesSent(n) { stats.bytesOut += n; },
  pingRtt(ms) { rttSum += ms; rttCount++; },
  getStats() {
    return {
      ...stats,
      messagesPerSecond: perSecond,
      avgPingRttMs: rttCount ? Number((rttSum / rttCount).toFixed(2)) : null,
      uptimeSeconds: Math.floor((Date.now() - startTime) / 1000)
    };
  }
};