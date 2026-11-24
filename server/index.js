const http = require('http');
const path = require('path');
const express = require('express');
const { startWsServer } = require('./wsServer');
const metrics = require('./metrics');
const db = require('./db');

const app = express();
app.use(express.static(path.join(__dirname, '..', 'client'))); // adjust if your folder differs
app.get('/metrics', (req, res) => res.json(metrics.getStats()));

const server = http.createServer(app);
const ws = startWsServer(server);
server.listen(3000, '0.0.0.0', () => console.log('Server on port 3000'));

// Graceful shutdown on Ctrl+C
function shutdown() {
  ws.close();
  server.closeAllConnections();
  server.close(() => { db.close(); process.exit(0); });
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);