/**
 * server.js — WebSocket multiplayer server for game_pvp.html
 *
 * Install deps:  npm install ws
 * Run:           node server.js
 * Default port:  8080  (set PORT env var to override)
 *
 * In your game HTML, set:
 *   const MP_SERVER_URL = 'ws://localhost:8080';
 * Or for a deployed server:
 *   const MP_SERVER_URL = 'wss://your-domain.com';
 */

const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const wss  = new WebSocketServer({ port: PORT });

// ── State ─────────────────────────────────────────────────────────────────────

/**
 * rooms: Map<roomId, Room>
 *
 * Room = {
 *   id:      string,
 *   hostId:  string | null,
 *   locked:  boolean,
 *   clients: Map<playerId, ClientInfo>
 * }
 *
 * ClientInfo = {
 *   ws:     WebSocket,
 *   id:     string,   // playerId (random hex from client)
 *   name:   string,
 *   skin:   string,
 *   room:   string,
 *   x:number, y:number, z:number, ry:number
 * }
 */
const rooms = new Map();

// ── Helpers ───────────────────────────────────────────────────────────────────

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(room, obj, excludeId = null) {
  for (const [pid, client] of room.clients) {
    if (pid !== excludeId) send(client.ws, obj);
  }
}

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, { id: roomId, hostId: null, locked: false, clients: new Map() });
  }
  return rooms.get(roomId);
}

function removeClient(client) {
  const room = rooms.get(client.room);
  if (!room) return;

  room.clients.delete(client.id);

  broadcast(room, { type: 'player_leave', id: client.id, name: client.name });

  // Re-assign host if the host left
  if (room.hostId === client.id) {
    room.hostId = null;
    const next = room.clients.values().next().value;
    if (next) {
      room.hostId = next.id;
      send(next.ws, { type: 'host_assigned', msg: 'You are now the host.' });
    }
  }

  // Clean up empty rooms
  if (room.clients.size === 0) {
    rooms.delete(room.id);
  }
}

// ── Message handlers ──────────────────────────────────────────────────────────

const handlers = {

  // ── join ──────────────────────────────────────────────────────────────────
  join(client, msg, room) {
    if (room.locked) {
      send(client.ws, { type: 'error', msg: 'This world is locked — no new players allowed.' });
      client.ws.close();
      return;
    }

    client.name = msg.name  || 'Unknown';
    client.skin = msg.skin  || '#ffffff';

    room.clients.set(client.id, client);

    // Notify existing players
    broadcast(room, { type: 'player_join', id: client.id, name: client.name, skin: client.skin }, client.id);

    // Send existing player list to the newcomer
    const players = [];
    for (const [pid, c] of room.clients) {
      if (pid !== client.id) {
        players.push({ id: c.id, name: c.name, skin: c.skin, x: c.x, y: c.y, z: c.z, ry: c.ry });
      }
    }
    send(client.ws, { type: 'player_list', players });

    // Host assignment
    if (msg.wantHost && !room.hostId) {
      room.hostId = client.id;
      send(client.ws, { type: 'host_assigned', msg: 'You are the host of this world.' });
    } else if (!room.hostId) {
      room.hostId = client.id;
      send(client.ws, { type: 'host_assigned', msg: 'You are the host of this world.' });
    }

    send(client.ws, { type: 'join_ack', playerCount: room.clients.size });
  },

  // ── player_update (position broadcast) ───────────────────────────────────
  player_update(client, msg, room) {
    client.x  = msg.x  ?? client.x;
    client.y  = msg.y  ?? client.y;
    client.z  = msg.z  ?? client.z;
    client.ry = msg.ry ?? client.ry;
    broadcast(room, {
      type: 'player_update',
      id:   client.id,
      name: client.name,
      skin: client.skin,
      x:    client.x,
      y:    client.y,
      z:    client.z,
      ry:   client.ry,
    }, client.id);
  },

  // ── block_break ───────────────────────────────────────────────────────────
  block_break(client, msg, room) {
    broadcast(room, { type: 'block_break', pid: client.id, x: msg.x, y: msg.y, z: msg.z }, client.id);
  },

  // ── block_place ───────────────────────────────────────────────────────────
  block_place(client, msg, room) {
    if (room.locked && client.id !== room.hostId) return; // optionally enforce lock on building too
    broadcast(room, { type: 'block_place', pid: client.id, x: msg.x, y: msg.y, z: msg.z, t: msg.t }, client.id);
  },

  // ── chat ──────────────────────────────────────────────────────────────────
  chat(client, msg, room) {
    broadcast(room, {
      type: 'chat',
      id:   client.id,
      name: msg.name || client.name,
      skin: msg.skin || client.skin,
      text: String(msg.text || '').slice(0, 512),
    });
  },

  // ── map_marker ────────────────────────────────────────────────────────────
  map_marker(client, msg, room) {
    broadcast(room, {
      type:  'map_marker',
      pid:   client.id,
      name:  client.name,
      x:     msg.x,
      z:     msg.z,
      label: msg.label,
      color: msg.color,
    }, client.id);
  },

  // ── pvp_hit ───────────────────────────────────────────────────────────────
  pvp_hit(client, msg, room) {
    const victim = room.clients.get(msg.target);
    if (!victim) return;
    send(victim.ws, {
      type:         'pvp_hit',
      attacker:     client.id,
      attackerName: client.name,
      target:       msg.target,
      damage:       Math.max(1, Math.min(100, msg.damage || 15)),
    });
  },

  // ── pvp_kill (client reports kill; server relays to room) ────────────────
  pvp_kill(client, msg, room) {
    broadcast(room, {
      type:        'pvp_kill',
      attacker:    client.id,
      attackerName: client.name,
      victimName:  msg.victimName || '?',
    });
  },

  // ── tornado_spawn ─────────────────────────────────────────────────────────
  tornado_spawn(client, msg, room) {
    if (client.id !== room.hostId) return; // host-only
    broadcast(room, {
      type:     'tornado_spawn',
      x:        msg.x,
      z:        msg.z,
      strength: msg.strength,
    }, client.id);
  },

  // ── weather_toggle ────────────────────────────────────────────────────────
  weather_toggle(client, msg, room) {
    if (client.id !== room.hostId) return;
    broadcast(room, { type: 'weather_toggle', enabled: !!msg.enabled }, client.id);
  },

  // ── admin_weather (day/night/calm/storm) ──────────────────────────────────
  admin_weather(client, msg, room) {
    if (client.id !== room.hostId) return;
    broadcast(room, { type: 'admin_weather', action: msg.action }, client.id);
  },

  // ── admin_kick ────────────────────────────────────────────────────────────
  admin_kick(client, msg, room) {
    if (client.id !== room.hostId) return;
    const target = room.clients.get(msg.target);
    if (!target) return;
    send(target.ws, { type: 'kicked', reason: msg.reason || 'Kicked by host.' });
    target.ws.close();
  },

  // ── admin_summon (tp a player to host's position) ────────────────────────
  admin_summon(client, msg, room) {
    if (client.id !== room.hostId) return;
    const target = room.clients.get(msg.target);
    if (!target) return;
    send(target.ws, { type: 'admin_summon', x: msg.x, y: msg.y, z: msg.z });
  },

  // ── admin_lock (prevent new joins) ───────────────────────────────────────
  admin_lock(client, msg, room) {
    if (client.id !== room.hostId) return;
    room.locked = !!msg.locked;
    broadcast(room, {
      type:   'chat',
      name:   '[SERVER]',
      skin:   '#ff8800',
      text:   room.locked ? '🔒 World is now locked.' : '🔓 World is now unlocked.',
    });
  },
};

// ── Connection handling ───────────────────────────────────────────────────────

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`[+] Client connected from ${ip}  (total: ${wss.clients.size})`);

  // Temporary client object — filled in on 'join'
  const client = {
    ws,
    id:   null,
    name: 'Unknown',
    skin: '#ffffff',
    room: null,
    x: 0, y: 64, z: 0, ry: 0,
  };

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const type = msg.type;
    if (!type) return;

    // 'join' is the first message — sets up client.id and client.room
    if (type === 'join') {
      if (!msg.id || !msg.room) return;
      client.id   = String(msg.id).slice(0, 32);
      client.room = String(msg.room).slice(0, 64);
      const room  = getOrCreateRoom(client.room);
      handlers.join(client, msg, room);
      return;
    }

    // All other messages require the client to have joined a room
    if (!client.id || !client.room) return;
    const room = rooms.get(client.room);
    if (!room) return;

    const handler = handlers[type];
    if (handler) {
      handler(client, msg, room);
    }
    // Unknown message types are silently ignored
  });

  ws.on('close', () => {
    if (client.id && client.room) removeClient(client);
    console.log(`[-] Client disconnected  (total: ${wss.clients.size})`);
  });

  ws.on('error', (err) => {
    console.error('[!] WebSocket error:', err.message);
  });
});

console.log(`✅  Game server running on ws://localhost:${PORT}`);
console.log(`    Set MP_SERVER_URL = 'ws://localhost:${PORT}' in your HTML`);
console.log(`    (use wss:// when served over HTTPS / deployed)`);
