const stats = { active: 0, totalConnections: 0, totalMessages: 0 };
const startTime = Date.now();
let lastCount = 0;
let perSecond = 0;

setInterval(() => {
  perSecond = stats.totalMessages - lastCount;
  lastCount = stats.totalMessages;
}, 1000);

module.exports = {
  connectionOpened() { stats.active++; stats.totalConnections++; },
  connectionClosed() { stats.active--; },
  messageSent() { stats.totalMessages++; },
  getStats() {
    return { ...stats, messagesPerSecond: perSecond,
             uptimeSeconds: Math.floor((Date.now() - startTime) / 1000) };
  }
};
