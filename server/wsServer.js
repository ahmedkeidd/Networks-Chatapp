const { WebSocketServer } = require('ws');
const db = require('./db');
const metrics = require('./metrics');
const protocols = require('./protocols');

const MAX_TEXT = 4000;
const clients = new Map(); // username -> Set of client objects (one per device)

// Encode with the client's own protocol and send
function send(client, packet) {
  if (client.socket.readyState !== 1) return;
  for (const out of client.codec.encode(packet)) {
    client.socket.send(out);
    metrics.bytesSent(client.codec.name, Buffer.byteLength(out));
  }
}

const fail = (client, text) => { metrics.error(); send(client, { type: 'error', text }); };

// Send to every device of a user, optionally skipping the originating socket
function deliver(username, packet, except) {
  for (const c of clients.get(username) || []) {
    if (c === except) continue;
    send(c, packet);
    metrics.messageDelivered();
  }
}

function addClient(client, user) {
  client.user = user;
  if (!clients.has(user)) clients.set(user, new Set());
  clients.get(user).add(client);
  metrics.setOnlineUsers(clients.size);
}

function removeClient(client) {
  if (!client.user) return;
  const set = clients.get(client.user);
  if (set) {
    set.delete(client);
    if (!set.size) clients.delete(client.user);
  }
  client.user = null;
  metrics.setOnlineUsers(clients.size);
}

function toMessage(row) {
  return { id: row.id, from: row.sender, to: row.receiver, room: row.room_id,
           text: row.text, ts: row.ts, seq: row.seq };
}

const handlers = {
  register(client, msg) {
    send(client, db.registerUser(msg.username, msg.password)
      ? { type: 'register_ok' }
      : { type: 'error', text: 'username taken or invalid' });
  },

  login(client, msg) {
    if (!db.loginUser(msg.username, msg.password)) return fail(client, 'login failed');
    removeClient(client);               // re-login on the same socket: drop the old session
    addClient(client, msg.username);
    send(client, { type: 'login_ok', rooms: db.getRoomsOf(msg.username) });
  },

  sync(client, msg) {
    const missed = db.getMessagesSince(client.user, Number(msg.lastSeq) || 0).map(toMessage);
    send(client, { type: 'history', messages: missed });
  },

  create_room(client, msg) {
    const id = db.createRoom(msg.name, client.user);
    send(client, id ? { type: 'room_ok', id } : { type: 'error', text: 'room name taken or invalid' });
  },

  join_room(client, msg) {
    send(client, db.joinRoom(msg.room, client.user)
      ? { type: 'join_ok' }
      : { type: 'error', text: 'cannot join room' });
  },

  // NEW: lets a client discover room ids so it can join them
  list_rooms(client) {
    send(client, { type: 'rooms', rooms: db.listRooms() });
  },

  message(client, msg) {
    const user = client.user;
    const isRoom = Number.isInteger(msg.room);

    if (!db.isValidId(msg.id) || typeof msg.text !== 'string' || !msg.text || msg.text.length > MAX_TEXT)
      return fail(client, 'invalid message');

    let members = null;
    if (isRoom) {
      members = db.getRoomMembers(msg.room);
      if (!members.includes(user)) return fail(client, 'not in room');
    } else if (typeof msg.to !== 'string' || !db.userExists(msg.to)) {
      return fail(client, 'unknown recipient');
    }

    let result;
    try {
      result = db.saveMessage({
        id: msg.id, from: user, to: isRoom ? undefined : msg.to,
        room: isRoom ? msg.room : undefined, text: msg.text, ts: Date.now()
      });
    } catch {
      return fail(client, 'server error, please retry'); // NOT acked, so the client will retry
    }

    const { message: saved, duplicate } = result;
    send(client, { type: 'ack', id: saved.id, seq: saved.seq }); // duplicates get the original seq
    if (duplicate) return;                                        // already delivered once, don't resend

    metrics.messageSent(client.codec.name);
    const outgoing = { type: 'message', ...saved };
    const targets = isRoom ? members : [user, saved.to];          // sender's other devices included
    for (const name of new Set(targets)) deliver(name, outgoing, client);
  }
};

const PUBLIC = new Set(['register', 'login']);

function handle(client, msg) {
  if (!msg || !Object.hasOwn(handlers, msg.type)) return fail(client, 'unknown type');
  if (!PUBLIC.has(msg.type) && !client.user) return fail(client, 'login first');
  handlers[msg.type](client, msg);
}

function startWsServer(server) {
  const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

  // Heartbeat: ping every 15s, drop sockets that never answered; also measures RTT
  const heartbeat = setInterval(() => {
    for (const s of wss.clients) {
      if (!s.isAlive) { s.terminate(); continue; }
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
      metrics.bytesReceived(codec.name, Buffer.byteLength(data));
      try {
        handle(client, codec.decode(data));
      } catch {
        fail(client, 'bad request');
      }
    });

    // A closed socket only removes itself; everyone else is untouched
    socket.on('close', () => {
      metrics.connectionClosed(codec.name);
      removeClient(client);
    });

    socket.on('error', () => socket.terminate());
  });

  return {
    close() {
      clearInterval(heartbeat);
      for (const s of wss.clients) s.terminate();
      wss.close();
    }
  };
}

module.exports = { startWsServer };