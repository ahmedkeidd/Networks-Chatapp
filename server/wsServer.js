const { WebSocketServer } = require('ws');
const db = require('./db');

const clients = new Map(); // username -> socket

function send(socket, obj) {
  if (socket.readyState === 1) socket.send(JSON.stringify(obj));
}

function startWsServer(server) {
  const wss = new WebSocketServer({ server });

  wss.on('connection', (socket) => {
    let user = null;

    socket.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); }
      catch { return send(socket, { type: 'error', text: 'bad json' }); }

      if (msg.type === 'register') {
        const ok = db.registerUser(msg.username, msg.password);
        return send(socket, ok
          ? { type: 'register_ok' }
          : { type: 'error', text: 'username taken' });
      }

      if (msg.type === 'login') {
        if (!db.loginUser(msg.username, msg.password))
          return send(socket, { type: 'error', text: 'login failed' });
        user = msg.username;
        clients.set(user, socket);
        return send(socket, { type: 'login_ok', rooms: db.getRoomsOf(user) });
      }

      if (!user) return send(socket, { type: 'error', text: 'login first' });

      if (msg.type === 'sync') {
        const missed = db.getMessagesSince(user, msg.lastSeq || 0);
        return send(socket, { type: 'history', messages: missed });
      }

      if (msg.type === 'create_room') {
        const id = db.createRoom(msg.name, user);
        return send(socket, id
          ? { type: 'room_ok', id }
          : { type: 'error', text: 'room name taken' });
      }

      if (msg.type === 'join_room') {
        return send(socket, db.joinRoom(msg.room, user)
          ? { type: 'join_ok' }
          : { type: 'error', text: 'already in room' });
      }

      if (msg.type === 'message') {
        const saved = db.saveMessage({ ...msg, from: user });
        if (!saved) return; // duplicate, ignore
        send(socket, { type: 'ack', id: saved.id, seq: saved.seq });

        if (saved.room) {
          for (const member of db.getRoomMembers(saved.room)) {
            if (member === user) continue;
            const s = clients.get(member);
            if (s) send(s, { type: 'message', ...saved });
          }
        } else {
          const target = clients.get(saved.to);
          if (target) send(target, { type: 'message', ...saved });
        }
      }
    });

    socket.on('close', () => { if (user) clients.delete(user); });
    socket.on('error', () => { if (user) clients.delete(user); });
  });
}

module.exports = { startWsServer };
