const { WebSocketServer } = require('ws');
const db = require('./db');
const metrics = require('./metrics');
const protocols = require('./protocols');

const clients = new Map();

function send(client, packet) {
  if (client.socket.readyState !== 1) return;
  for (const out of client.codec.encode(packet)) {
    client.socket.send(out);
    metrics.bytesSent(Buffer.byteLength(out));
  }
}

function toMessage(row) {
  return {
    id: row.id,
    from: row.sender,
    to: row.receiver,
    room: row.room_id,
    text: row.text,
    ts: row.ts,
    seq: row.seq
  };
}

function handle(client, msg) {
  const user = client.user;

  if (msg.type === 'register') {
    const ok = db.registerUser(msg.username, msg.password);
    return send(client, ok
      ? { type: 'register_ok' }
      : { type: 'error', text: 'username taken' });
  }

  if (msg.type === 'login') {
    if (!db.loginUser(msg.username, msg.password))
      return send(client, { type: 'error', text: 'login failed' });
    client.user = msg.username;
    clients.set(msg.username, client);
    return send(client, { type: 'login_ok', rooms: db.getRoomsOf(msg.username) });
  }

  if (!user) return send(client, { type: 'error', text: 'login first' });

  if (msg.type === 'sync') {
    const missed = db.getMessagesSince(user, msg.lastSeq || 0).map(toMessage);
    return send(client, { type: 'history', messages: missed });
  }

  if (msg.type === 'create_room') {
    const id = db.createRoom(msg.name, user);
    return send(client, id
      ? { type: 'room_ok', id }
      : { type: 'error', text: 'room name taken' });
  }

  if (msg.type === 'join_room') {
    return send(client, db.joinRoom(msg.room, user)
      ? { type: 'join_ok' }
      : { type: 'error', text: 'already in room' });
  }

  if (msg.type === 'message') {
    if (!msg.id || !msg.text || (!msg.to && !msg.room))
      return send(client, { type: 'error', text: 'invalid message' });

    if (msg.room && !db.getRoomMembers(msg.room).includes(user))
      return send(client, { type: 'error', text: 'not in room' });

    const saved = db.saveMessage({
      id: msg.id,
      from: user,
      to: msg.to,
      room: msg.room,
      text: msg.text,
      ts: Date.now()
    });
    if (!saved) return send(client, { type: 'ack', id: msg.id });

    metrics.messageSent();
    send(client, { type: 'ack', id: saved.id, seq: saved.seq });

    const outgoing = { type: 'message', ...saved };
    if (saved.room) {
      for (const member of db.getRoomMembers(saved.room)) {
        if (member === user) continue;
        const target = clients.get(member);
        if (target) send(target, outgoing);
      }
    } else {
      const target = clients.get(saved.to);
      if (target) send(target, outgoing);
    }
  }
}

function startWsServer(server) {
  const wss = new WebSocketServer({ server });

  setInterval(() => {
    for (const s of wss.clients) {
      if (!s.isAlive) {
        s.terminate();
        continue;
      }
      s.isAlive = false;
      s.pingSent = Date.now();
      s.ping();
    }
  }, 15000);

  wss.on('connection', (socket, req) => {
    const proto = new URL(req.url, 'http://localhost').searchParams.get('proto');
    const codec = proto === 'text' ? protocols.text : protocols.json;
    const client = { socket, codec, user: null };

    metrics.connectionOpened(codec.name);
    socket.isAlive = true;

    socket.on('pong', () => {
      socket.isAlive = true;
      metrics.pingRtt(Date.now() - socket.pingSent);
    });

    socket.on('message', (raw) => {
      const data = raw.toString();
      metrics.bytesReceived(Buffer.byteLength(data));
      try {
        handle(client, codec.decode(data));
      } catch {
        send(client, { type: 'error', text: 'bad request' });
      }
    });

    socket.on('close', () => {
      metrics.connectionClosed();
      if (client.user && clients.get(client.user) === client) clients.delete(client.user);
    });

    socket.on('error', () => socket.terminate());
  });
}

module.exports = { startWsServer };