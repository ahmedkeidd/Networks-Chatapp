/* Chat client (JSON protocol over WebSocket).
 *
 * Key ideas (these are what you explain in the oral defense):
 *  - Every message has a client-generated id. The server dedups per (sender, id),
 *    so we dedup the same way: key = from + ':' + id.  -> no duplicates
 *  - A sent message is shown immediately as "pending" and stored in an outbox.
 *    It is removed only when the server ACKs it. After a reconnect the whole outbox
 *    is re-sent (same ids, so the server ignores any it already saved). -> no loss
 *  - We store lastSeq (highest server sequence number we have seen). After login we send
 *    {type:'sync', lastSeq} and the server replies with everything we missed.
 *  - Messages, lastSeq, outbox are kept in localStorage per user, so a refresh keeps the chat.
 */
'use strict';

const NAME_OK = /^[A-Za-z0-9_.-]{1,32}$/;
const ROOM_OK = /^[A-Za-z0-9_. -]{1,32}$/;
const $ = (id) => document.getElementById(id);

// ------------------------------------------------------------------ state
const S = {
  ws: null, connected: false, loggedIn: false,
  user: null, pass: null,
  lastSeq: 0,
  msgs: new Map(),        // key -> message
  outbox: [],             // keys of own messages not yet ACKed
  inflight: [],           // keys sent on this socket, waiting for ack/error (FIFO)
  rooms: new Map(),       // joined rooms: id -> name
  allRooms: [],           // every room on the server (for the join list)
  peers: new Set(),       // usernames we have a DM chat with
  active: null,           // 'u:name' or 'r:id'
  unread: {},             // chat key -> count
  pending: null,          // what we are waiting for: {kind:'register'|'login'|'create'|'join', ...}
  retry: 0, retryTimer: null
};

const keyOf = (m) => m.from + ':' + m.id;
const chatOf = (m) => (m.room != null ? 'r:' + m.room : 'u:' + (m.from === S.user ? m.to : m.from));

// ------------------------------------------------------------------ helpers
let toastTimer;
function toast(text) {
  const t = $('toast'); t.textContent = text; t.classList.add('show');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove('show'), 2500);
}
function newId() {
  // letters/digits/_ . - only, max 64 chars (server rule)
  return S.user + '-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
}
function setAuthStatus(text, err) {
  const el = $('authStatus'); el.textContent = text; el.classList.toggle('err', !!err);
}

// ------------------------------------------------------------------ storage
const storeKey = () => 'chat:' + S.user;
function save() {
  try {
    localStorage.setItem(storeKey(), JSON.stringify({
      lastSeq: S.lastSeq, outbox: S.outbox, messages: [...S.msgs.values()],
      peers: [...S.peers], rooms: [...S.rooms]
    }));
  } catch { /* storage full or disabled: app still works in memory */ }
}
function load() {
  S.lastSeq = 0; S.msgs = new Map(); S.outbox = []; S.peers = new Set(); S.rooms = new Map(); S.unread = {};
  try {
    const d = JSON.parse(localStorage.getItem(storeKey()) || 'null');
    if (!d) return;
    S.lastSeq = d.lastSeq || 0;
    S.outbox = d.outbox || [];
    S.peers = new Set(d.peers || []);
    S.rooms = new Map(d.rooms || []);
    for (const m of d.messages || []) S.msgs.set(keyOf(m), m);
  } catch { /* corrupted: start clean */ }
}

// ------------------------------------------------------------------ connection
function connect() {
  clearTimeout(S.retryTimer);
  // protocol is chosen with the query string; this client always speaks JSON
  const ws = new WebSocket('ws://' + location.host + '/?proto=json');
  S.ws = ws;
  ws.onopen = () => {
    S.connected = true; S.retry = 0; updateConn();
    if (S.user && S.pass) sendRaw({ type: 'login', username: S.user, password: S.pass }); // auto re-login
    else setAuthStatus('Connected. Log in or register.');
  };
  ws.onmessage = (e) => { try { onPacket(JSON.parse(e.data)); } catch (err) { console.error(err); } };
  ws.onclose = () => {
    S.connected = false; S.loggedIn = false; S.inflight = []; updateConn();
    if (ws !== S.ws) return;
    const wait = Math.min(10000, 500 * 2 ** S.retry++);   // exponential back-off
    S.retryTimer = setTimeout(connect, wait);
    if (!S.user) setAuthStatus('Disconnected. Retrying…', true);
  };
  ws.onerror = () => ws.close();
}
function sendRaw(p) {
  if (S.ws && S.ws.readyState === 1) { S.ws.send(JSON.stringify(p)); return true; }
  return false;
}
function updateConn() { $('conn').classList.toggle('on', S.connected && S.loggedIn); }

// ------------------------------------------------------------------ incoming packets
function onPacket(p) {
  switch (p.type) {
    case 'register_ok':
      setAuthStatus('Registered! Logging in…');
      doLogin(S.pending.username, S.pending.password);
      break;

    case 'login_ok':
      if (!S.user) return;
      S.loggedIn = true; updateConn();
      for (const r of p.rooms) S.rooms.set(r.id, r.name);
      localStorage.setItem('chat:session', JSON.stringify({ u: S.user, p: S.pass }));
      showApp();
      sendRaw({ type: 'list_rooms' });
      sendRaw({ type: 'sync', lastSeq: S.lastSeq });  // ask for everything we missed
      flushOutbox();                                  // re-send anything never ACKed
      break;

    case 'history':           // reply to sync: already-known messages are skipped by key
      p.messages.forEach((m) => addMessage(m, false));
      save(); renderAll();
      break;

    case 'message':           // live message
      addMessage(p, true); save(); renderAll();
      break;

    case 'ack': {             // server stored our message
      const k = S.user + ':' + p.id;
      S.inflight = S.inflight.filter((x) => x !== k);
      const m = S.msgs.get(k);
      if (m) { m.pending = false; m.seq = p.seq; }
      S.outbox = S.outbox.filter((x) => x !== k);
      save(); renderMessages();
      break;
    }

    case 'rooms':
      S.allRooms = p.rooms; renderRoomList();
      break;

    case 'room_ok':
      S.rooms.set(p.id, S.pending.name); openChat('r:' + p.id);
      save(); renderAll(); $('dlg').close();
      break;

    case 'join_ok': {
      const r = S.allRooms.find((x) => x.id === S.pending.room);
      S.rooms.set(S.pending.room, r ? r.name : 'Group ' + S.pending.room);
      sendRaw({ type: 'sync', lastSeq: 0 });          // fetch the group's earlier messages too
      openChat('r:' + S.pending.room);
      save(); renderAll(); $('dlg').close();
      break;
    }

    case 'error': onError(p.text); break;
  }
}

function onError(text) {
  // Errors for sent messages come back in the same order we sent them.
  if (['invalid message', 'unknown recipient', 'not in room', 'server error, please retry'].includes(text)) {
    const k = S.inflight.shift();
    const m = k && S.msgs.get(k);
    if (m && text !== 'server error, please retry') {
      m.pending = false; m.failed = true;               // permanent failure: stop retrying
      S.outbox = S.outbox.filter((x) => x !== k);
      save(); renderMessages();
    } else if (m) setTimeout(flushOutbox, 2000);        // temporary: try again
    return toast(text);
  }
  if (!S.loggedIn) setAuthStatus(text === 'login failed' ? 'Wrong username or password' : text, true);
  toast(text);
}

// store a message if new; returns true if it was new
function addMessage(m, live) {
  const k = keyOf(m);
  if (m.seq > S.lastSeq) S.lastSeq = m.seq;
  const old = S.msgs.get(k);
  if (old) {                                  // duplicate: only fill in the server fields
    old.seq = m.seq; old.ts = m.ts; old.pending = false;
    S.outbox = S.outbox.filter((x) => x !== k);
    return false;
  }
  S.msgs.set(k, { id: m.id, from: m.from, to: m.to ?? null, room: m.room ?? null,
                  text: m.text, ts: m.ts, seq: m.seq });
  const c = chatOf(m);
  if (c.startsWith('u:')) S.peers.add(c.slice(2));
  if (c.startsWith('r:') && !S.rooms.has(m.room)) S.rooms.set(m.room, 'Group ' + m.room);
  if (live && m.from !== S.user && c !== S.active) S.unread[c] = (S.unread[c] || 0) + 1;
  return true;
}

// ------------------------------------------------------------------ sending
function sendMessage(text) {
  if (!S.active || !text) return;
  const m = { id: newId(), from: S.user, to: null, room: null, text, ts: Date.now(), seq: null, pending: true };
  if (S.active.startsWith('r:')) m.room = Number(S.active.slice(2));
  else m.to = S.active.slice(2);
  S.msgs.set(keyOf(m), m);
  S.outbox.push(keyOf(m));
  save(); renderAll();
  flushOutbox();
}
function flushOutbox() {
  if (!S.loggedIn) return;                    // stays in outbox until we are logged in again
  for (const k of S.outbox) {
    if (S.inflight.includes(k)) continue;
    const m = S.msgs.get(k);
    if (!m) continue;
    const p = { type: 'message', id: m.id, text: m.text };
    if (m.room != null) p.room = m.room; else p.to = m.to;
    if (sendRaw(p)) S.inflight.push(k);
  }
}

// ------------------------------------------------------------------ auth actions
function doLogin(username, password) {
  S.pending = { kind: 'login', username, password };
  if (S.user !== username) { S.user = username; load(); }
  S.pass = password;
  if (!sendRaw({ type: 'login', username, password })) setAuthStatus('Not connected yet…', true);
}
function logout() {
  localStorage.removeItem('chat:session');
  S.user = S.pass = null; S.loggedIn = false; S.active = null;
  S.ws.close();                                // server drops the session; we reconnect clean
  $('app').classList.add('hidden'); $('auth').classList.remove('hidden');
  $('password').value = ''; setAuthStatus('Logged out.');
}

// ------------------------------------------------------------------ rendering
function showApp() {
  $('auth').classList.add('hidden'); $('app').classList.remove('hidden');
  $('me').textContent = S.user;
  renderAll();
}
function chatName(c) { return c.startsWith('r:') ? '# ' + (S.rooms.get(Number(c.slice(2))) || c) : c.slice(2); }
function chatMsgs(c) {
  return [...S.msgs.values()].filter((m) => chatOf(m) === c).sort((a, b) => a.ts - b.ts);
}
function renderAll() { renderChatList(); renderMessages(); }

function renderChatList() {
  const keys = [...S.peers].map((u) => 'u:' + u).concat([...S.rooms.keys()].map((r) => 'r:' + r));
  const last = {}; keys.forEach((k) => { const l = chatMsgs(k).pop(); last[k] = l; });
  keys.sort((a, b) => (last[b]?.ts || 0) - (last[a]?.ts || 0));
  const ul = $('chatList'); ul.textContent = '';
  $('emptyList').classList.toggle('hidden', keys.length > 0);
  for (const k of keys) {
    const li = document.createElement('li');
    if (k === S.active) li.className = 'active';
    const av = el('div', 'avatar', chatName(k).replace('# ', '')[0].toUpperCase());
    const info = el('div', 'info');
    info.append(el('div', 'name', chatName(k)), el('div', 'preview', last[k] ? last[k].text : 'No messages yet'));
    li.append(av, info);
    if (S.unread[k]) li.append(el('span', 'badge', S.unread[k]));
    li.onclick = () => openChat(k);
    ul.append(li);
  }
}

function renderMessages() {
  const box = $('messages'); box.textContent = '';
  if (!S.active) return;
  $('chatTitle').textContent = chatName(S.active);
  const isRoom = S.active.startsWith('r:');
  for (const m of chatMsgs(S.active)) {
    const mine = m.from === S.user;
    const d = el('div', 'msg' + (mine ? ' mine' : '') + (m.failed ? ' failed' : ''));
    if (isRoom && !mine) d.append(el('div', 'who', m.from));
    d.append(document.createTextNode(m.text));                    // textContent: no HTML injection
    const time = new Date(m.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    d.append(el('div', 'meta', time + (m.failed ? ' ⚠ not sent' : m.pending ? ' …' : mine ? ' ✓' : '')));
    box.append(d);
  }
  box.scrollTop = box.scrollHeight;
}

function renderRoomList() {
  const ul = $('roomList'); ul.textContent = '';
  const open = S.allRooms.filter((r) => !S.rooms.has(r.id));
  if (!open.length) ul.append(el('li', 'hint', 'No other groups'));
  for (const r of open) {
    const li = el('li');
    const b = el('button', '', 'Join'); b.type = 'button';
    b.onclick = () => { S.pending = { kind: 'join', room: r.id }; sendRaw({ type: 'join_room', room: r.id }); };
    li.append(el('span', '', '# ' + r.name), b);
    ul.append(li);
  }
}

function openChat(c) {
  S.active = c; S.unread[c] = 0;
  $('app').classList.add('chat-open');
  renderAll(); $('msgInput').focus();
}
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

// ------------------------------------------------------------------ UI events
function creds() {
  const username = $('username').value.trim(), password = $('password').value;
  if (!NAME_OK.test(username)) { setAuthStatus('Username: letters, digits, _ . - (max 32)', true); return null; }
  if (!password) { setAuthStatus('Enter a password', true); return null; }
  return { username, password };
}
$('authForm').onsubmit = (e) => { e.preventDefault(); const c = creds(); if (c) doLogin(c.username, c.password); };
$('registerBtn').onclick = () => {
  const c = creds(); if (!c) return;
  S.pending = { kind: 'register', ...c };
  if (!sendRaw({ type: 'register', username: c.username, password: c.password })) setAuthStatus('Not connected yet…', true);
};
$('logoutBtn').onclick = logout;
$('backBtn').onclick = () => { S.active = null; $('app').classList.remove('chat-open'); renderAll(); };
$('sendForm').onsubmit = (e) => {
  e.preventDefault();
  const t = $('msgInput').value.trim();
  if (!t) return;
  $('msgInput').value = ''; sendMessage(t);
};
$('newBtn').onclick = () => { sendRaw({ type: 'list_rooms' }); renderRoomList(); $('dlg').showModal(); };
$('closeDlg').onclick = () => $('dlg').close();
$('dmForm').onsubmit = (e) => {
  e.preventDefault();
  const u = $('dmUser').value.trim();
  if (!NAME_OK.test(u) || u === S.user) return toast('Enter another user\'s name');
  S.peers.add(u); $('dmUser').value = ''; save(); $('dlg').close(); openChat('u:' + u);
};
$('createForm').onsubmit = (e) => {
  e.preventDefault();
  const name = $('roomName').value.trim();
  if (!ROOM_OK.test(name)) return toast('Group name: letters, digits, space, _ . -');
  S.pending = { kind: 'create', name }; $('roomName').value = '';
  sendRaw({ type: 'create_room', name });
};

// ------------------------------------------------------------------ start
(function init() {
  try {                                      // remember me: log in automatically after a refresh
    const s = JSON.parse(localStorage.getItem('chat:session') || 'null');
    if (s) { S.user = s.u; S.pass = s.p; load(); $('username').value = s.u; }
  } catch { /* ignore */ }
  connect();
})();
