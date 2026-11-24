const db = require('../server/db');

console.log('register:', db.registerUser('eid', '1234'));      // true
console.log('register again:', db.registerUser('eid', '1234')); // false
console.log('login ok:', db.loginUser('eid', '1234'));          // true
console.log('login bad:', db.loginUser('eid', 'wrong'));        // false

const m = { id: 'abc1', from: 'eid', to: 'ahmed', text: 'hi', ts: Date.now() };
console.log('save:', db.saveMessage(m));        // object with seq
console.log('duplicate:', db.saveMessage(m));   // null
console.log('sync:', db.getMessagesSince('ahmed', 0));