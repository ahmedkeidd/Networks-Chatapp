const http = require('http');
const express = require('express');
const { startWsServer } = require('./wsServer');
const metrics = require('./metrics');

const app = express();
app.use(express.static('client'));
app.get('/metrics', (req, res) => res.json(metrics.getStats()));

const server = http.createServer(app);
startWsServer(server);

server.listen(3000, '0.0.0.0', () => console.log('Server on port 3000'));
