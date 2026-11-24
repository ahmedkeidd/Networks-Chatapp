const http = require('http');
const express = require('express');
const { startWsServer } = require('./wsServer');

const app = express();
app.use(express.static('client'));

const server = http.createServer(app);
startWsServer(server);

server.listen(3000, '0.0.0.0', () => console.log('Server on port 3000'));
