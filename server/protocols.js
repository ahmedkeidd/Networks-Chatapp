function line(m) {
  return `MSG|${m.id}|${m.from}|${m.to || ''}|${m.room || ''}|${m.seq}|${m.ts}|${m.text}`;
}

const json = {
  name: 'json',
  decode(raw) {
    return JSON.parse(raw);
  },
  encode(packet) {
    return [JSON.stringify(packet)];
  }
};

const text = {
  name: 'text',
  decode(raw) {
    const p = raw.split('|');
    switch (p[0]) {
      case 'REGISTER':
        return { type: 'register', username: p[1], password: p.slice(2).join('|') };
      case 'LOGIN':
        return { type: 'login', username: p[1], password: p.slice(2).join('|') };
      case 'SYNC':
        return { type: 'sync', lastSeq: Number(p[1]) || 0 };
      case 'CREATE_ROOM':
        return { type: 'create_room', name: p.slice(1).join('|') };
      case 'JOIN_ROOM':
        return { type: 'join_room', room: Number(p[1]) };
      case 'DM':
        return { type: 'message', id: p[1], to: p[2], text: p.slice(3).join('|') };
      case 'ROOM':
        return { type: 'message', id: p[1], room: Number(p[2]), text: p.slice(3).join('|') };
      default:
        throw new Error('unknown command');
    }
  },
  encode(packet) {
    switch (packet.type) {
      case 'register_ok':
        return ['REGISTER_OK'];
      case 'login_ok':
        return [`LOGIN_OK|${packet.rooms.map((r) => `${r.id}:${r.name}`).join(',')}`];
      case 'room_ok':
        return [`ROOM_OK|${packet.id}`];
      case 'join_ok':
        return ['JOIN_OK'];
      case 'ack':
        return [`ACK|${packet.id}|${packet.seq ?? ''}`];
      case 'message':
        return [line(packet)];
      case 'history':
        return [...packet.messages.map(line), `HISTORY_END|${packet.messages.length}`];
      default:
        return [`ERR|${packet.text}`];
    }
  }
};

module.exports = { json, text };