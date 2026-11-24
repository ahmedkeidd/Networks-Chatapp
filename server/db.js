const { DatabaseSync } = require('node:sqlite');
const crypto = require('crypto');
const path = require('path');
const db = new DatabaseSync(path.join(__dirname, 'chat.db'));

db.exec(`
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rooms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS room_members (
  room_id INTEGER NOT NULL REFERENCES rooms(id),
  username TEXT NOT NULL REFERENCES users(username),
  PRIMARY KEY (room_id, username)
);
CREATE TABLE IF NOT EXISTS messages (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL,
  sender TEXT NOT NULL,
  receiver TEXT,
  room_id INTEGER,
  text TEXT NOT NULL,
  ts INTEGER NOT NULL,
  UNIQUE (sender, id)              -- dedup is per sender, not global
);
CREATE INDEX IF NOT EXISTS idx_msg_receiver ON messages(receiver);
CREATE INDEX IF NOT EXISTS idx_msg_room ON messages(room_id);
CREATE INDEX IF NOT EXISTS idx_members_user ON room_members(username);
`);

// Characters like | , : would break the text protocol, so they are rejected.
const NAME_OK = /^[A-Za-z0-9_.-]{1,32}$/;
const ROOM_OK = /^[A-Za-z0-9_. -]{1,32}$/;
const ID_OK = /^[A-Za-z0-9_.-]{1,64}$/;

// Prepared once, reused on every call (faster under load)
const q = {
  addUser: db.prepare('INSERT INTO users (username, password_hash) VALUES (?, ?)'),
  getUser: db.prepare('SELECT password_hash FROM users WHERE username = ?'),
  addMsg: db.prepare('INSERT INTO messages (id, sender, receiver, room_id, text, ts) VALUES (?, ?, ?, ?, ?, ?)'),
  findMsg: db.prepare('SELECT seq FROM messages WHERE sender = ? AND id = ?'),
  since: db.prepare(`
    SELECT * FROM messages
    WHERE seq > ?
      AND (sender = ? OR receiver = ?
           OR room_id IN (SELECT room_id FROM room_members WHERE username = ?))
    ORDER BY seq`),
  addRoom: db.prepare('INSERT INTO rooms (name) VALUES (?)'),
  addMember: db.prepare('INSERT INTO room_members (room_id, username) VALUES (?, ?)'),
  members: db.prepare('SELECT username FROM room_members WHERE room_id = ?'),
  roomsOf: db.prepare('SELECT rooms.id, rooms.name FROM rooms JOIN room_members ON rooms.id = room_members.room_id WHERE username = ?'),
  allRooms: db.prepare('SELECT id, name FROM rooms')
};

const hash = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');

function registerUser(username, password) {
  if (!NAME_OK.test(username || '') || typeof password !== 'string' || !password) return false;
  const salt = crypto.randomBytes(8).toString('hex');
  try {
    q.addUser.run(username, salt + ':' + hash(password, salt));
    return true;
  } catch { return false; } // username taken
}

function loginUser(username, password) {
  if (typeof username !== 'string' || typeof password !== 'string') return false;
  const row = q.getUser.get(username);
  if (!row) return false;
  const [salt, h] = row.password_hash.split(':');
  const a = Buffer.from(hash(password, salt), 'hex');
  const b = Buffer.from(h, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

const userExists = (u) => !!q.getUser.get(u);

// Returns { message, duplicate }. Only a UNIQUE violation counts as a duplicate;
// any other DB error is rethrown so the caller never ACKs an unsaved message.
function saveMessage(m) {
  try {
    const info = q.addMsg.run(m.id, m.from, m.to || null, m.room || null, m.text, m.ts);
    return { message: { ...m, seq: Number(info.lastInsertRowid) }, duplicate: false };
  } catch (e) {
    if (/UNIQUE constraint failed/.test(e.message)) {
      const row = q.findMsg.get(m.from, m.id);
      return { message: { ...m, seq: row.seq }, duplicate: true };
    }
    throw e;
  }
}

const getMessagesSince = (user, lastSeq) => q.since.all(lastSeq, user, user, user);

function createRoom(name, creator) {
  if (!ROOM_OK.test(name || '')) return null;
  db.exec('BEGIN');
  try {
    const id = Number(q.addRoom.run(name).lastInsertRowid);
    q.addMember.run(id, creator);
    db.exec('COMMIT');
    return id;
  } catch { db.exec('ROLLBACK'); return null; } // name taken
}

function joinRoom(roomId, username) {
  try { q.addMember.run(roomId, username); return true; }
  catch { return false; } // already a member, or room doesn't exist (foreign key)
}

const getRoomMembers = (id) => q.members.all(id).map((r) => r.username);
const getRoomsOf = (u) => q.roomsOf.all(u);
const listRooms = () => q.allRooms.all();
const isValidId = (id) => typeof id === 'string' && ID_OK.test(id);
const close = () => db.close();

module.exports = {
  registerUser, loginUser, userExists, saveMessage, getMessagesSince,
  createRoom, joinRoom, getRoomMembers, getRoomsOf, listRooms, isValidId, close
};