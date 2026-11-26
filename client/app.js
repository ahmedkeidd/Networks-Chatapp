/* Chat client (JSON protocol over WebSocket).
 *
 * Key ideas (these are what you explain in the oral defense):
 *  - Every message has a client-generated id. The server dedups per (sender, id),
 *    so we dedup the same way: key = from + ':' + id.            -> no duplicates
 *  - A sent message is shown immediately as "pending" and stored in an outbox.
 *    It is removed only when the server ACKs it. After a reconnect the whole outbox
 *    is re-sent (same ids, so the server ignores any it already saved).  -> no loss
 *  - We keep lastSeq (highest server sequence number we have seen). After login we send
 *    {type:'sync', lastSeq} and the server replies with everything we missed.
 *  - Messages, lastSeq and outbox are kept in localStorage per user, so a refresh keeps the chat.
 *
 * File layout: state -> helpers -> storage -> connection -> incoming packets
 *              -> sending -> auth -> rendering -> UI events -> start
 */
'use strict';

// Same rules as the server (server/db.js), checked here so the user gets instant feedback
const NAME_OK = /^[A-Za-z0-9_.-]{1,32}$/;      // usernames
const ROOM_OK = /^[A-Za-z0-9_. -]{1,32}$/;     // group names
const $ = (id) => document.getElementById(id); // shortcut for getElementById

// ---state
// Everything the app knows lives in this one object.
const S = {
  ws: null, connected: false, loggedIn: false,
  user: null, pass: null,          // kept in memory so we can log in again after a reconnect
  lastSeq: 0,                      // highest server seq we have received
  msgs: new Map(),                 // key (from:id) -> message object. A Map = automatic dedup
  outbox: [],                      // keys of own messages the server has not ACKed yet
  inflight: [],                    // keys sent on THIS socket, waiting for ack/error (FIFO)
  rooms: new Map(),                // groups I am in: id -> name
  allRooms: [],                    // every group on the server (for the "join" list)
  peers: new Set(),                // usernames I have a direct chat with
  active: null,                    // open chat: 'u:username' or 'r:roomId'
  unread: {},                      // chat key -> unread count
  pending: null,                   // last request awaiting a reply (register/login/create/join)
  retry: 0, retryTimer: null       // reconnect back-off
};

// Unique key of a message. Ids are only unique per sender, so the sender is part of the key.
const keyOf = (m) => m.from + ':' + m.id;
// Which chat a message belongs to: a group, or the OTHER person in a direct chat.
const chatOf = (m) => (m.room != null ? 'r:' + m.room : 'u:' + (m.from === S.user ? m.to : m.from));

// ---helpers
let toastTimer;
/** Show a short popup message at the top of the screen. */
function toast(text) {
  const t = $('toast'); t.textContent = text; t.classList.add('show');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove('show'), 2500);
}
/** Make a unique message id (only letters/digits/_ . - and max 64 chars, as the server requires). */
function newId() {
  return S.user + '-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
}
/** Set the status line on the login screen (red if it is an error). */
function setAuthStatus(text, err) {
  const el = $('authStatus'); el.textContent = text; el.classList.toggle('err', !!err);
}

// ---storage
// One localStorage entry per user, so two accounts on the same browser don't mix.
const storeKey = () => 'chat:' + S.user;

/** Save lastSeq, outbox, messages, chats to localStorage (called after every change). */
function save() {
  try {
    localStorage.setItem(storeKey(), JSON.stringify({
      lastSeq: S.lastSeq, outbox: S.outbox, messages: [...S.msgs.values()],
      peers: [...S.peers], rooms: [...S.rooms]
    }));
  } catch { /* storage full or disabled: the app still works in memory */ }
}
/** Load the saved data of S.user (or start empty). */
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
  } catch { /* corrupted data: start clean */ }
}

// ---connection
/** Open the WebSocket. Called at start and again after every disconnect. */
function connect() {
  clearTimeout(S.retryTimer);
  // The protocol is chosen with the query string; this client always speaks JSON.
  const ws = new WebSocket('ws://' + location.host + '/?proto=json');
  S.ws = ws;

  ws.onopen = () => {
    S.connected = true; S.retry = 0; updateConn();
    // If we were logged in before the drop, log in again automatically.
    if (S.user && S.pass) sendRaw({ type: 'login', username: S.user, password: S.pass });
    else setAuthStatus('Connected. Log in or register.');
  };
  ws.onmessage = (e) => { try { onPacket(JSON.parse(e.data)); } catch (err) { console.error(err); } };
  ws.onclose = () => {
    S.connected = false; S.loggedIn = false; S.inflight = []; updateConn();
    if (ws !== S.ws) return;                               // an old socket closing: ignore
    const wait = Math.min(10000, 500 * 2 ** S.retry++);    // exponential back-off: 0.5s, 1s, 2s ... max 10s
    S.retryTimer = setTimeout(connect, wait);
    if (!S.user) setAuthStatus('Disconnected. Retrying…', true);
  };
  ws.onerror = () => ws.close();
}
/** Send a packet as JSON. Returns false if the socket is not open. */
function sendRaw(p) {
  if (S.ws && S.ws.readyState === 1) { S.ws.send(JSON.stringify(p)); return true; }
  return false;
}
/** Update the green/red connection dot. */
function updateConn() { $('conn').classList.toggle('on', S.connected && S.loggedIn); }

// ---incoming packets
/** Handle every packet from the server, by its "type". */
function onPacket(p) {
  switch (p.type) {
    case 'register_ok':                       // account created -> log straight in
      setAuthStatus('Registered! Logging in…');
      doLogin(S.pending.username, S.pending.password);
      break;

    case 'login_ok':                          // logged in: load chats, sync, resend unsent
      if (!S.user) return;
      S.loggedIn = true; updateConn();
      for (const r of p.rooms) S.rooms.set(r.id, r.name);
      localStorage.setItem('chat:session', JSON.stringify({ u: S.user, p: S.pass })); // auto-login after refresh
      showApp();
      sendRaw({ type: 'list_rooms' });
      sendRaw({ type: 'sync', lastSeq: S.lastSeq });  // ask for everything we missed while offline
      flushOutbox();                                  // re-send anything never ACKed
      break;

    case 'history':                           // reply to sync: known messages are skipped by key
      p.messages.forEach((m) => addMessage(m, false));
      save(); renderAll();
      break;

    case 'message':                           // a live message from someone else
      addMessage(p, true); save(); renderAll();
      break;

    case 'ack': {                             // server saved our message: it is no longer "pending"
      const k = S.user + ':' + p.id;
      S.inflight = S.inflight.filter((x) => x !== k);
      const m = S.msgs.get(k);
      if (m) { m.pending = false; m.seq = p.seq; }
      S.outbox = S.outbox.filter((x) => x !== k);
      save(); renderMessages();
      break;
    }

    case 'rooms':                             // list of all groups (for the join list)
      S.allRooms = p.rooms; renderRoomList();
      break;

    case 'room_ok':                           // our create_room worked
      S.rooms.set(p.id, S.pending.name); openChat('r:' + p.id);
      save(); renderAll(); $('dlg').close();
      break;

    case 'join_ok': {                         // our join_room worked
      const r = S.allRooms.find((x) => x.id === S.pending.room);
      S.rooms.set(S.pending.room, r ? r.name : 'Group ' + S.pending.room);
      sendRaw({ type: 'sync', lastSeq: 0 });  // fetch the group's earlier messages too
      openChat('r:' + S.pending.room);
      save(); renderAll(); $('dlg').close();
      break;
    }

    case 'error': onError(p.text); break;
  }
}

/** Handle an error packet. Message errors are matched to the message that caused them. */
function onError(text) {
  // The server answers messages in the order we sent them, so the error belongs to inflight[0].
  if (['invalid message', 'unknown recipient', 'not in room', 'server error, please retry'].includes(text)) {
    const k = S.inflight.shift();
    const m = k && S.msgs.get(k);
    if (m && text !== 'server error, please retry') {
      m.pending = false; m.failed = true;                // permanent failure: mark it and stop retrying
      S.outbox = S.outbox.filter((x) => x !== k);
      save(); renderMessages();
    } else if (m) setTimeout(flushOutbox, 2000);         // temporary failure: try again shortly
    return toast(text);
  }
  if (!S.loggedIn) setAuthStatus(text === 'login failed' ? 'Wrong username or password' : text, true);
  toast(text);
}

/**
 * Store a message if we don't have it yet.
 * Returns true if it was new, false if it was a duplicate.
 * @param live true for a live message (counts as unread), false for history
 */
function addMessage(m, live) {
  const k = keyOf(m);
  if (m.seq > S.lastSeq) S.lastSeq = m.seq;              // lastSeq only moves forward
  const old = S.msgs.get(k);
  if (old) {                                             // duplicate: just fill in the server's fields
    old.seq = m.seq; old.ts = m.ts; old.pending = false;
    S.outbox = S.outbox.filter((x) => x !== k);
    return false;
  }
  S.msgs.set(k, { id: m.id, from: m.from, to: m.to ?? null, room: m.room ?? null,
                  text: m.text, ts: m.ts, seq: m.seq });
  const c = chatOf(m);
  if (c.startsWith('u:')) S.peers.add(c.slice(2));       // a new person messaged us: add them to the list
  if (c.startsWith('r:') && !S.rooms.has(m.room)) S.rooms.set(m.room, 'Group ' + m.room);
  if (live && m.from !== S.user && c !== S.active) S.unread[c] = (S.unread[c] || 0) + 1;
  return true;
}

// ---sending
/** Send a message in the open chat. It appears at once as "pending" until the server ACKs it. */
function sendMessage(text) {
  if (!S.active || !text) return;
  const m = { id: newId(), from: S.user, to: null, room: null, text, ts: Date.now(), seq: null, pending: true };
  if (S.active.startsWith('r:')) m.room = Number(S.active.slice(2));
  else m.to = S.active.slice(2);
  S.msgs.set(keyOf(m), m);
  S.outbox.push(keyOf(m));        // stays here until acked, so it survives a disconnect/refresh
  save(); renderAll();
  flushOutbox();
}
/** Send every outbox message that is not already waiting for a reply. Safe to call repeatedly. */
function flushOutbox() {
  if (!S.loggedIn) return;                   // we will be called again after the next login
  for (const k of S.outbox) {
    if (S.inflight.includes(k)) continue;
    const m = S.msgs.get(k);
    if (!m) continue;
    const p = { type: 'message', id: m.id, text: m.text };
    if (m.room != null) p.room = m.room; else p.to = m.to;
    if (sendRaw(p)) S.inflight.push(k);
  }
}

// ---auth actions
/** Send a login request and load that user's saved data. */
function doLogin(username, password) {
  S.pending = { kind: 'login', username, password };
  if (S.user !== username) { S.user = username; load(); }
  S.pass = password;
  if (!sendRaw({ type: 'login', username, password })) setAuthStatus('Not connected yet…', true);
}
/** Forget the session and go back to the login screen. */
function logout() {
  localStorage.removeItem('chat:session');
  S.user = S.pass = null; S.loggedIn = false; S.active = null;
  S.ws.close();                              // the server drops our session; connect() reopens a clean socket
  $('app').classList.add('hidden'); $('auth').classList.remove('hidden');
  $('password').value = ''; setAuthStatus('Logged out.');
}

// ---rendering
// The UI is redrawn from the state above; the state is the single source of truth.

/** Switch from the login screen to the main app. */
function showApp() {
  $('auth').classList.add('hidden'); $('app').classList.remove('hidden');
  $('me').textContent = S.user;
  renderAll();
}
/** Display name of a chat key: '# group' for groups, the username for direct chats. */
function chatName(c) { return c.startsWith('r:') ? '# ' + (S.rooms.get(Number(c.slice(2))) || c) : c.slice(2); }
/** All messages of one chat, oldest first. */
function chatMsgs(c) {
  return [...S.msgs.values()].filter((m) => chatOf(m) === c).sort((a, b) => a.ts - b.ts);
}
function renderAll() { renderChatList(); renderMessages(); }

/** Draw the list of chats, most recently active first, with last-message preview and unread badge. */
function renderChatList() {
  const keys = [...S.peers].map((u) => 'u:' + u).concat([...S.rooms.keys()].map((r) => 'r:' + r));
  const last = {}; keys.forEach((k) => { const l = chatMsgs(k).pop(); last[k] = l; });
  keys.sort((a, b) => (last[b]?.ts || 0) - (last[a]?.ts || 0));
  const ul = $('chatList'); ul.textContent = '';
  $('emptyList').classList.toggle('hidden', keys.length > 0);
  for (const k of keys) {
    const li = document.createElement('li');
    if (k === S.active) li.className = 'active';
    const av = el('div', 'avatar', chatName(k).replace('# ', '')[0].toUpperCase());  // first letter as avatar
    const info = el('div', 'info');
    info.append(el('div', 'name', chatName(k)), el('div', 'preview', last[k] ? last[k].text : 'No messages yet'));
    li.append(av, info);
    if (S.unread[k]) li.append(el('span', 'badge', S.unread[k]));
    li.onclick = () => openChat(k);
    ul.append(li);
  }
}

/** Draw the messages of the open chat. */
function renderMessages() {
  const box = $('messages'); box.textContent = '';
  if (!S.active) return;
  $('chatTitle').textContent = chatName(S.active);
  const isRoom = S.active.startsWith('r:');
  for (const m of chatMsgs(S.active)) {
    const mine = m.from === S.user;
    const d = el('div', 'msg' + (mine ? ' mine' : '') + (m.failed ? ' failed' : ''));
    if (isRoom && !mine) d.append(el('div', 'who', m.from));       // show sender name in groups
    d.append(document.createTextNode(m.text));                     // text node, not innerHTML: no HTML injection
    const time = new Date(m.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    // status: … sending, ✓ saved on server, ⚠ failed
    d.append(el('div', 'meta', time + (m.failed ? ' ⚠ not sent' : m.pending ? ' …' : mine ? ' ✓' : '')));
    box.append(d);
  }
  box.scrollTop = box.scrollHeight;                                // jump to the newest message
}

/** Draw the "join group" list in the popup (groups I'm not in yet). */
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

/** Open a chat (on phones this swaps the list for the conversation). */
function openChat(c) {
  S.active = c; S.unread[c] = 0;
  $('app').classList.add('chat-open');
  renderAll(); $('msgInput').focus();
}
/** Small helper: create an element with a class and text. */
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

// ---UI events
/** Read and validate the login form. Returns null (and shows an error) if invalid. */
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
// "＋" button: refresh the group list from the server, then show the popup
$('newBtn').onclick = () => { sendRaw({ type: 'list_rooms' }); renderRoomList(); $('dlg').showModal(); };
$('closeDlg').onclick = () => $('dlg').close();
// Start a direct chat: the server has no "list users", so we just open a chat with that name.
// (If the user doesn't exist, the first message fails with "unknown recipient".)
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

// ---start
(function init() {
  try {                                      // "remember me": log in automatically after a refresh
    const s = JSON.parse(localStorage.getItem('chat:session') || 'null');
    if (s) { S.user = s.u; S.pass = s.p; load(); $('username').value = s.u; }
  } catch { /* ignore */ }
  connect();
})();
