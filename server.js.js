const { WebSocketServer } = require('ws');
const { createServer } = require('http');

const PORT = process.env.PORT || 8080;
const MAX_ROOMS = 500;

// partyKey -> { clients: Map<uid, ws>, capacity, private, name, hostUid }
const rooms = new Map();

function makeKey() {
  const chars = 'abcdefghjkmnpqrstuvwxyz23456789';
  let k = '';
  for (let i = 0; i < 6; i++) k += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(k) ? makeKey() : k;
}

const server = createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('ok'); return; }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.uid = null; ws.nick = 'Player'; ws.partyKey = null;

  ws.on('message', (raw) => {
    const dv = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    const u8 = new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength);
    if (dv.byteLength < 1) return;
    const op = dv.getUint8(0);
    let o = 1;
    const readStr1 = () => { const n = dv.getUint8(o); o += 1; const s = new TextDecoder().decode(u8.subarray(o, o + n)); o += n; return s; };
    const readStr2 = () => { const n = dv.getUint16(o, false); o += 2; const s = new TextDecoder().decode(u8.subarray(o, o + n)); o += n; return s; };

    switch (op) {
      case 0x01: { // HELLO
        ws.uid = readStr1(); ws.nick = readStr1();
        readStr1(); // handle (ignored for now)
        break;
      }
      case 0x02: { // CREATE
        const priv = dv.getUint8(o) === 1; o += 1;
        const cap = dv.getUint8(o); o += 1;
        const name = readStr1();
        const key = makeKey();
        const room = { clients: new Map(), capacity: cap, private: priv, name, hostUid: ws.uid };
        room.clients.set(ws.uid, ws);
        rooms.set(key, room);
        ws.partyKey = key;
        const kb = new TextEncoder().encode(key);
        const buf = new ArrayBuffer(2 + kb.length);
        const out = new DataView(buf);
        out.setUint8(0, 0x81);
        out.setUint8(1, kb.length);
        new Uint8Array(buf).set(kb, 2);
        ws.send(buf);
        broadcastRoster(key);
        break;
      }
      case 0x03: { // JOIN
        const key = readStr1();
        const room = rooms.get(key);
        if (!room) { sendDenied(ws, 'no such party'); return; }
        if (room.clients.size >= room.capacity) { sendDenied(ws, 'party full'); return; }
        room.clients.set(ws.uid, ws);
        ws.partyKey = key;
        const kb = new TextEncoder().encode(key);
        const hb = new TextEncoder().encode(room.hostUid || '');
        const buf = new ArrayBuffer(3 + kb.length + hb.length);
        const out = new DataView(buf);
        let p = 0;
        out.setUint8(p++, 0x82);
        out.setUint8(p++, kb.length); new Uint8Array(buf).set(kb, p); p += kb.length;
        out.setUint8(p++, hb.length); new Uint8Array(buf).set(hb, p);
        ws.send(buf);
        broadcastRoster(key);
        break;
      }
      case 0x04: leaveRoom(ws); break;
      case 0x05: { // CHAT
        const text = readStr2();
        if (!ws.partyKey) return;
        const room = rooms.get(ws.partyKey);
        if (!room) return;
        const nb = new TextEncoder().encode(ws.nick);
        const tb = new TextEncoder().encode(text);
        const buf = new ArrayBuffer(3 + nb.length + tb.length);
        const out = new DataView(buf);
        let p = 0;
        out.setUint8(p++, 0x84);
        out.setUint8(p++, nb.length); new Uint8Array(buf).set(nb, p); p += nb.length;
        out.setUint16(p, tb.length, false); p += 2;
        new Uint8Array(buf).set(tb, p);
        for (const [, c] of room.clients) if (c.readyState === 1) c.send(buf);
        break;
      }
      case 0x06: { // POS
        if (!ws.partyKey) return;
        const x = dv.getInt16(o, false); o += 2;
        const y = dv.getInt16(o, false); o += 2;
        const xp = dv.getUint32(o, false); o += 4;
        const nick = readStr1();
        const room = rooms.get(ws.partyKey);
        if (!room) return;
        const ub = new TextEncoder().encode(ws.uid);
        const nb = new TextEncoder().encode(nick);
        const buf = new ArrayBuffer(9 + ub.length + nb.length);
        const out = new DataView(buf);
        let p = 0;
        out.setUint8(p++, 0x86);
        out.setUint8(p++, ub.length); new Uint8Array(buf).set(ub, p); p += ub.length;
        out.setInt16(p, x, false); p += 2;
        out.setInt16(p, y, false); p += 2;
        out.setUint32(p, xp, false); p += 4;
        out.setUint8(p++, nb.length); new Uint8Array(buf).set(nb, p);
        for (const [uid, c] of room.clients) if (uid !== ws.uid && c.readyState === 1) c.send(buf);
        break;
      }
      case 0x07: { // LIST
        const pub = [...rooms.entries()].filter(([, r]) => !r.private);
        const buf = new ArrayBuffer(1 + 1 + pub.length * 40);
        const out = new DataView(buf);
        let p = 0;
        out.setUint8(p++, 0x87);
        out.setUint8(p++, pub.length);
        for (const [key, room] of pub) {
          const kb = new TextEncoder().encode(key);
          const nb = new TextEncoder().encode(room.name || 'party');
          out.setUint8(p++, kb.length); new Uint8Array(buf).set(kb, p); p += kb.length;
          out.setUint8(p++, nb.length); new Uint8Array(buf).set(nb, p); p += nb.length;
          out.setUint8(p++, room.clients.size);
          out.setUint8(p++, room.capacity);
          out.setUint8(p++, 0);
        }
        ws.send(buf.subarray(0, p));
        break;
      }
    }
  });

  ws.on('close', () => leaveRoom(ws));
});

function sendDenied(ws, reason) {
  const rb = new TextEncoder().encode(reason);
  const buf = new ArrayBuffer(2 + rb.length);
  const out = new DataView(buf);
  out.setUint8(0, 0x83); out.setUint8(1, rb.length);
  new Uint8Array(buf).set(rb, 2);
  ws.send(buf);
}

function leaveRoom(ws) {
  if (!ws.partyKey) return;
  const room = rooms.get(ws.partyKey);
  if (!room) return;
  room.clients.delete(ws.uid);
  if (room.clients.size === 0) rooms.delete(ws.partyKey);
  else broadcastRoster(ws.partyKey);
  ws.partyKey = null;
}

function broadcastRoster(key) {
  const room = rooms.get(key); if (!room) return;
  const entries = [...room.clients.entries()];
  const buf = new ArrayBuffer(1 + 1 + entries.length * 80);
  const out = new DataView(buf);
  let p = 0;
  out.setUint8(p++, 0x85);
  out.setUint8(p++, entries.length);
  for (const [uid, c] of entries) {
    const ub = new TextEncoder().encode(uid);
    const nb = new TextEncoder().encode(c.nick || 'Player');
    out.setUint8(p++, ub.length); new Uint8Array(buf).set(ub, p); p += ub.length;
    out.setUint8(p++, nb.length); new Uint8Array(buf).set(nb, p); p += nb.length;
    out.setUint8(p++, (c.readyState === 1 ? 1 : 0) | (uid === room.hostUid ? 2 : 0));
    out.setUint8(p++, 0); // empty role
  }
  const packet = buf.slice(0, p);
  for (const [, c] of room.clients) if (c.readyState === 1) c.send(packet);
}

server.listen(PORT, () => console.log(`Party server on ${PORT}`));