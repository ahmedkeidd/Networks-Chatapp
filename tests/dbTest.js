const db = require('../server/db');
const s = Math.random().toString(36).slice(2, 7);
const [eid, ahmed] = ['eid' + s, 'ahmed' + s];
const check = (n, c) => console.log((c ? 'PASS' : 'FAIL') + '  ' + n);

check('register', db.registerUser(eid, '1234') === true);
check('register again', db.registerUser(eid, '1234') === false);
check('register bad name', db.registerUser('a|b', 'x') === false);
db.registerUser(ahmed, 'pw');
check('login ok', db.loginUser(eid, '1234'));
check('login bad', !db.loginUser(eid, 'wrong'));

const m = { id: 'abc' + s, from: eid, to: ahmed, text: 'hi', ts: Date.now() };
const r1 = db.saveMessage(m), r2 = db.saveMessage(m);
check('save', !r1.duplicate && r1.message.seq > 0);
check('duplicate keeps same seq', r2.duplicate && r2.message.seq === r1.message.seq);
check('same id, other sender is NOT a duplicate', !db.saveMessage({ ...m, from: ahmed, to: eid }).duplicate);
check('sync', db.getMessagesSince(ahmed, 0).length >= 1);

const room = db.createRoom('room' + s, eid);
check('create room', Number.isInteger(room));
check('duplicate room name', db.createRoom('room' + s, eid) === null);
check('join room', db.joinRoom(room, ahmed));
check('join twice', !db.joinRoom(room, ahmed));
check('join ghost room', !db.joinRoom(999999, ahmed));
check('members', db.getRoomMembers(room).length === 2);