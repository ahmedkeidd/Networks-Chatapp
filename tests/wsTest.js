const WebSocket = require('ws');

const suffix = Math.random().toString(36).slice(2, 7);
const alice = 'alice' + suffix;
const bob = 'bob' + suffix;
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

function connect(query) {
  return new Promise((resolve) => {
    const socket = new WebSocket('ws://localhost:3000/' + query);
    const inbox = [];
    socket.on('message', (d) => inbox.push(d.toString()));
    socket.on('open', () => resolve({ socket, inbox }));
  });
}

function check(name, condition) {
  console.log((condition ? 'PASS' : 'FAIL') + '  ' + name);
}

(async () => {
  const a = await connect('');
  const b = await connect('?proto=text');

  a.socket.send(JSON.stringify({ type: 'register', username: alice, password: 'pw' }));
  b.socket.send(`REGISTER|${bob}|pw`);
  await wait(300);
  a.socket.send(JSON.stringify({ type: 'login', username: alice, password: 'pw' }));
  b.socket.send(`LOGIN|${bob}|pw`);
  await wait(300);
  check('json login', a.inbox.some((m) => m.includes('login_ok')));
  check('text login', b.inbox.some((m) => m.startsWith('LOGIN_OK')));

  const msg = { type: 'message', id: 'm' + suffix, to: bob, text: 'hello bob' };
  a.socket.send(JSON.stringify(msg));
  a.socket.send(JSON.stringify(msg));
  await wait(300);
  check('text client received json message', b.inbox.some((m) => m.endsWith('|hello bob')));
  check('duplicate was acked again', a.inbox.filter((m) => m.includes('"ack"')).length === 2);
  check('duplicate was not delivered twice', b.inbox.filter((m) => m.endsWith('|hello bob')).length === 1);

  b.socket.send(`DM|r${suffix}|${alice}|hi | alice`);
  await wait(300);
  check('json client received text message', a.inbox.some((m) => m.includes('hi | alice')));

  b.socket.close();
  await wait(300);
  a.socket.send(JSON.stringify({ type: 'message', id: 'o' + suffix, to: bob, text: 'while offline' }));
  await wait(300);
  const b2 = await connect('?proto=text');
  b2.socket.send(`LOGIN|${bob}|pw`);
  await wait(300);
  b2.socket.send('SYNC|0');
  await wait(300);
  check('offline message arrives on sync', b2.inbox.some((m) => m.endsWith('|while offline')));
  check('sync ends with marker', b2.inbox.some((m) => m.startsWith('HISTORY_END')));

  a.socket.send('not json');
  await wait(200);
  check('bad packet does not crash server', a.inbox.some((m) => m.includes('bad request')));

  const stats = await (await fetch('http://localhost:3000/metrics')).json();
  console.log('metrics:', stats);
  process.exit(0);
})();