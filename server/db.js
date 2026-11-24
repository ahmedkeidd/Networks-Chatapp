const { DatabaseSync } = require('node:sqlite');
const crypto = require('crypto');
const path = require('path');
const db = new DatabaseSync(path.join(__dirname, 'chat.db'));

db.exec(`
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT UNIQUE NOT NULL,
  sender TEXT NOT NULL,
  receiver TEXT,
  room_id INTEGER,
  text TEXT NOT NULL,
  ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rooms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS room_members (
  room_id INTEGER NOT NULL,
  username TEXT NOT NULL,
  PRIMARY KEY (room_id, username)
);
`);

function hash(pw, salt) {
  return crypto.scryptSync(pw, salt, 32).toString('hex');
}

function registerUser(username, password) {
  const salt = crypto.randomBytes(8).toString('hex');
  try {
    db.prepare('INSERT INTO users (username, password_hash) VALUES (?, ?)')
      .run(username, salt + ':' + hash(password, salt));
    return true;
  } catch { return false; } // username taken
}

function loginUser(username, password) {
  const row = db.prepare('SELECT password_hash FROM users WHERE username = ?').get(username);
  if (!row) return false;
  const [salt, h] = row.password_hash.split(':');
  return hash(password, salt) === h;
}

function saveMessage(m) {
  try {
    const info = db.prepare(
      'INSERT INTO messages (id, sender, receiver, room_id, text, ts) VALUES (?, ?, ?, ?, ?, ?)'
    ).run(m.id, m.from, m.to || null, m.room || null, m.text, m.ts);
    return { ...m, seq: Number(info.lastInsertRowid) };
  } catch { return null; } // duplicate id
}

function getMessagesSince(username, lastSeq) {
  return db.prepare(`
    SELECT * FROM messages
    WHERE seq > ?
      AND (sender = ? OR receiver = ?
           OR room_id IN (SELECT room_id FROM room_members WHERE username = ?))
    ORDER BY seq
  `).all(lastSeq, username, username, username);
}

function createRoom(name, creator) {
  try {
    const info = db.prepare('INSERT INTO rooms (name) VALUES (?)').run(name);
    const roomId = Number(info.lastInsertRowid);
    db.prepare('INSERT INTO room_members (room_id, username) VALUES (?, ?)').run(roomId, creator);
    return roomId;
  } catch { return null; } // name taken
}

function joinRoom(roomId, username) {
  try {
    db.prepare('INSERT INTO room_members (room_id, username) VALUES (?, ?)').run(roomId, username);
    return true;
  } catch { return false; } // already a member
}

function getRoomMembers(roomId) {
  return db.prepare('SELECT username FROM room_members WHERE room_id = ?')
    .all(roomId).map(r => r.username);
}

function getRoomsOf(username) {
  return db.prepare(
    'SELECT rooms.id, rooms.name FROM rooms JOIN room_members ON rooms.id = room_members.room_id WHERE username = ?'
  ).all(username);
}

module.exports = {
  registerUser, loginUser, saveMessage, getMessagesSince,
  createRoom, joinRoom, getRoomMembers, getRoomsOf
};