const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// rooms: Map<roomId, Set<ws>>
const rooms = new Map();

wss.on('connection', (ws) => {
  ws._pid  = null;
  ws._name = 'Unknown';
  ws._skin = '#ffffff';
  ws._room = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── JOIN ──────────────────────────────────────────────────────────────
    if (msg.type === 'join') {
      ws._pid  = msg.id   || ws._pid;
      ws._name = msg.name || 'Unknown';
      ws._skin = msg.skin || '#ffffff';
      ws._room = msg.room || 'default';

      if (!rooms.has(ws._room)) rooms.set(ws._room, new Set());
      const room = rooms.get(ws._room);

      // Send the newcomer a list of everyone already in the room
      const existing = [...room].filter(c => c !== ws && c.readyState === 1);
      ws.send(JSON.stringify({
        type: 'player_list',
        players: existing.map(c => ({ id: c._pid, name: c._name, skin: c._skin }))
      }));

      // Tell everyone else this player joined
      broadcast(room, ws, {
        type: 'player_join',
        id:   ws._pid,
        name: ws._name,
        skin: ws._skin
      });

      room.add(ws);
      console.log(`[+] ${ws._name} (${ws._pid}) joined room "${ws._room}"  (${room.size} players)`);
    }

    if (!ws._room) return;
    const room = rooms.get(ws._room);
    if (!room) return;

    // ── RELAY ─────────────────────────────────────────────────────────────
    if (msg.type === 'player_update') broadcast(room, ws, msg);
    if (msg.type === 'block_break')   broadcast(room, ws, msg);
    if (msg.type === 'block_place')   broadcast(room, ws, msg);
    if (msg.type === 'chat')          broadcast(room, ws, msg);
  });

  ws.on('close', () => {
    if (!ws._room) return;
    const room = rooms.get(ws._room);
    if (!room) return;
    room.delete(ws);
    broadcast(room, ws, { type: 'player_leave', id: ws._pid });
    console.log(`[-] ${ws._name} left room "${ws._room}"  (${room.size} players)`);
    if (room.size === 0) rooms.delete(ws._room);
  });

  ws.on('error', () => {});
});

function broadcast(room, sender, msg) {
  const data = JSON.stringify(msg);
  for (const client of room) {
    if (client !== sender && client.readyState === 1) {
      client.send(data);
    }
  }
}

console.log(`WebSocket server listening on port ${PORT}`);
