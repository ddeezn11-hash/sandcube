// ══════════════════════════════════════════════════════════════════════════════
//  server.js  —  Survival Game Multiplayer Server
//  Node.js + ws  (npm install ws)
//  Run:  node server.js
//  Port: 8080  (set env PORT to override)
// ══════════════════════════════════════════════════════════════════════════════

'use strict';

const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;

const wss = new WebSocket.Server({ port: PORT });
console.log(`[server] Listening on ws://localhost:${PORT}`);

// ── Room state ────────────────────────────────────────────────────────────────
// rooms[roomId] = {
//   hostId        : string | null
//   locked        : bool
//   weather       : { enabled, wind, gustStrength, action }
//   worldMods     : Map<"x,y,z", blockType|null>  (null = broken)
//   players       : Map<playerId, playerState>
//   tornadoes     : Array<tornado>
//   outfitSettings: object  (lockEdit, visibility, playerOverrides)
//   ropes         : Map<ropeId, ropeState>
//   animals       : Map<animalId, animalState>
//   fires         : Map<fireId, fireState>
//   markers       : Array<markerState>
//   chatHistory   : Array  (last 50)
//   createdAt     : timestamp
// }
const rooms = new Map();

// clients[ws] = { id, room, name, skin }
const clients = new WeakMap();

// ── Helpers ───────────────────────────────────────────────────────────────────
function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      hostId: null,
      locked: false,
      weather: { enabled: true, action: null, wind: 0, gustStrength: 0 },
      worldMods: new Map(),
      players: new Map(),
      tornadoes: [],
      outfitSettings: { lockEdit: false, visibility: 'none', playerOverrides: {} },
      ropes: new Map(),
      animals: new Map(),
      fires: new Map(),
      markers: [],
      chatHistory: [],
      createdAt: Date.now(),
    });
  }
  return rooms.get(roomId);
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(roomId, obj, excludeWs = null) {
  wss.clients.forEach(ws => {
    const c = clients.get(ws);
    if (c && c.room === roomId && ws !== excludeWs) {
      send(ws, obj);
    }
  });
}

function broadcastAll(roomId, obj) {
  broadcast(roomId, obj, null);
}

function getRoomPlayers(roomId) {
  const out = [];
  wss.clients.forEach(ws => {
    const c = clients.get(ws);
    if (c && c.room === roomId) {
      out.push({ ws, ...c });
    }
  });
  return out;
}

function pickNewHost(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  const players = getRoomPlayers(roomId);
  if (!players.length) return;
  const next = players[0];
  room.hostId = next.id;
  send(next.ws, { type: 'host_assigned', msg: 'You are now the host of this world.' });
  broadcast(roomId, { type: 'chat', name: '[SERVER]', skin: '#ffaa00',
    text: `👑 ${next.name} is now the host.` }, next.ws);
  console.log(`[server] [${roomId}] Host reassigned to ${next.name} (${next.id})`);
}

function pushChat(roomId, msg) {
  const room = getRoom(roomId);
  room.chatHistory.push(msg);
  if (room.chatHistory.length > 50) room.chatHistory.shift();
}

// ── Connection ────────────────────────────────────────────────────────────────
wss.on('connection', ws => {
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    handleMessage(ws, msg);
  });

  ws.on('close', () => handleDisconnect(ws));
  ws.on('error', () => handleDisconnect(ws));
});

// ── Message router ────────────────────────────────────────────────────────────
function handleMessage(ws, msg) {
  const type = msg.type;

  // ── JOIN ──────────────────────────────────────────────────────────────────
  if (type === 'join') {
    const { id, room: roomId, name, skin, wantHost, seed, worldType } = msg;
    if (!id || !roomId) return;

    const room = getRoom(roomId);

    // Reject if world locked (unless this client is already in it — reconnect)
    const existingClient = clients.get(ws);
    if (room.locked && !existingClient) {
      send(ws, { type: 'error', msg: 'This world is locked — no new players allowed.' });
      return;
    }

    clients.set(ws, { id, room: roomId, name: name || 'Player', skin: skin || '#c8a46e' });
    room.players.set(id, { id, name: name || 'Player', skin: skin || '#c8a46e',
      x: 0, y: 64, z: 0, ry: 0, swimming: false, lastSeen: Date.now() });

    // Host assignment
    if (!room.hostId || wantHost) {
      room.hostId = id;
      send(ws, { type: 'host_assigned', msg: 'You are the host of this world.' });
    }

    // Send current player list to newcomer
    const others = [];
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && c.id !== id) {
        others.push({ id: c.id, name: c.name, skin: c.skin });
      }
    });
    if (others.length) send(ws, { type: 'player_list', players: others });

    // Send world state to newcomer
    const modsObj = {};
    room.worldMods.forEach((t, k) => { modsObj[k] = t; });

    send(ws, {
      type: 'join_ack',
      playerCount: getRoomPlayers(roomId).length,
      mods: modsObj,
      weather: room.weather,
      outfitSettings: room.outfitSettings,
      tornadoes: room.tornadoes,
      fires: [...room.fires.values()],
      ropes: [...room.ropes.values()],
      markers: room.markers,
      chatHistory: room.chatHistory,
    });

    // Notify everyone else
    broadcast(roomId, { type: 'player_join', id, name: name || 'Player', skin: skin || '#c8a46e' }, ws);

    // Re-send outfit settings to newcomer if they exist
    if (room.outfitSettings) {
      send(ws, { type: 'outfit_settings', settings: room.outfitSettings });
    }

    const playerCount = getRoomPlayers(roomId).length;
    console.log(`[server] [${roomId}] ${name} (${id}) joined. Players: ${playerCount}`);
    return;
  }

  // All other messages require the client to be registered
  const client = clients.get(ws);
  if (!client) return;
  const { id, room: roomId } = client;
  const room = rooms.get(roomId);
  if (!room) return;

  // ── PLAYER POSITION ───────────────────────────────────────────────────────
  if (type === 'player_update') {
    const { x, y, z, ry, name, skin, swimming } = msg;
    const p = room.players.get(id);
    if (p) { Object.assign(p, { x, y, z, ry, name, skin, swimming, lastSeen: Date.now() }); }
    broadcast(roomId, { type: 'player_update', id, name, skin, x, y, z, ry, swimming }, ws);
    return;
  }

  // ── BLOCK BREAK / PLACE ───────────────────────────────────────────────────
  if (type === 'block_break') {
    const { x, y, z } = msg;
    room.worldMods.set(`${x},${y},${z}`, null);
    broadcast(roomId, { type: 'block_break', pid: id, x, y, z }, ws);
    return;
  }

  if (type === 'block_place') {
    const { x, y, z, t } = msg;
    room.worldMods.set(`${x},${y},${z}`, t);
    broadcast(roomId, { type: 'block_place', pid: id, x, y, z, t }, ws);
    return;
  }

  // ── CHAT ─────────────────────────────────────────────────────────────────
  if (type === 'chat') {
    const chatMsg = { type: 'chat', name: msg.name || client.name, skin: msg.skin || '#fff', text: msg.text || '' };
    pushChat(roomId, chatMsg);
    broadcastAll(roomId, chatMsg);
    return;
  }

  // ── MAP MARKER ────────────────────────────────────────────────────────────
  if (type === 'map_marker') {
    const marker = { x: msg.x, z: msg.z, label: msg.label || `${msg.x},${msg.z}`,
      color: msg.color || '#ffff00', pid: id, name: client.name };
    room.markers.push(marker);
    if (room.markers.length > 200) room.markers.shift();
    broadcastAll(roomId, { type: 'map_marker', ...marker });
    return;
  }

  // ── PVP ───────────────────────────────────────────────────────────────────
  if (type === 'pvp_hit') {
    // Relay hit to target only
    const { target, attacker, attackerName, damage } = msg;
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && c.id === target) {
        send(other, { type: 'pvp_hit', attacker, attackerName, target, damage });
      }
    });
    // If the attacker is reporting a kill (hp <= 0), broadcast kill event
    if (msg.kill) {
      const killMsg = { type: 'pvp_kill', attacker: id, attackerName: client.name,
        victimName: msg.victimName || target };
      broadcastAll(roomId, killMsg);
      pushChat(roomId, { type: 'chat', name: '[WORLD]', skin: '#f88',
        text: `💀 ${client.name} killed ${msg.victimName || target}` });
    }
    return;
  }

  // ── OUTFIT SETTINGS ───────────────────────────────────────────────────────
  if (type === 'outfit_settings') {
    if (id !== room.hostId) return; // only host can push
    room.outfitSettings = msg.settings || {};
    broadcast(roomId, { type: 'outfit_settings', settings: room.outfitSettings }, ws);
    return;
  }

  // ── ROPE ─────────────────────────────────────────────────────────────────
  if (type === 'rope_place') {
    const rope = { id: msg.ropeId, x1: msg.x1, y1: msg.y1, z1: msg.z1,
      x2: msg.x2, y2: msg.y2, z2: msg.z2, pid: id };
    room.ropes.set(msg.ropeId, rope);
    broadcast(roomId, { type: 'rope_place', ...rope }, ws);
    return;
  }

  if (type === 'rope_remove') {
    room.ropes.delete(msg.ropeId);
    broadcast(roomId, { type: 'rope_remove', ropeId: msg.ropeId }, ws);
    return;
  }

  // ── FIRE / CAMPFIRE ───────────────────────────────────────────────────────
  if (type === 'fire_place') {
    const fire = { id: msg.fireId, x: msg.x, y: msg.y, z: msg.z,
      lit: msg.lit || false, fuel: msg.fuel || 0 };
    room.fires.set(msg.fireId, fire);
    broadcast(roomId, { type: 'fire_place', ...fire }, ws);
    return;
  }

  if (type === 'fire_update') {
    const fire = room.fires.get(msg.fireId);
    if (fire) { Object.assign(fire, { lit: msg.lit, fuel: msg.fuel }); }
    broadcast(roomId, { type: 'fire_update', fireId: msg.fireId, lit: msg.lit, fuel: msg.fuel }, ws);
    return;
  }

  if (type === 'fire_remove') {
    room.fires.delete(msg.fireId);
    broadcast(roomId, { type: 'fire_remove', fireId: msg.fireId }, ws);
    return;
  }

  // ── ANIMAL SYNC ──────────────────────────────────────────────────────────
  if (type === 'animal_update') {
    // Host-authoritative: only trust host's animal positions
    if (id !== room.hostId) return;
    const { animalId, x, y, z, hp, dead, animalType } = msg;
    room.animals.set(animalId, { animalId, x, y, z, hp, dead, animalType });
    broadcast(roomId, { type: 'animal_update', animalId, x, y, z, hp, dead, animalType }, ws);
    return;
  }

  if (type === 'animal_kill') {
    room.animals.delete(msg.animalId);
    broadcastAll(roomId, { type: 'animal_kill', animalId: msg.animalId, pid: id, name: client.name });
    return;
  }

  // ── WEATHER ───────────────────────────────────────────────────────────────
  if (type === 'weather_toggle') {
    if (id !== room.hostId) return;
    room.weather.enabled = !!msg.enabled;
    broadcast(roomId, { type: 'weather_toggle', enabled: room.weather.enabled }, ws);
    return;
  }

  if (type === 'admin_weather') {
    if (id !== room.hostId) return;
    room.weather.action = msg.action;
    broadcast(roomId, { type: 'admin_weather', action: msg.action }, ws);
    return;
  }

  // ── TORNADO ───────────────────────────────────────────────────────────────
  if (type === 'tornado_spawn') {
    if (id !== room.hostId) return;
    const tornado = { x: msg.x, z: msg.z, strength: msg.strength };
    room.tornadoes.push(tornado);
    setTimeout(() => {
      const idx = room.tornadoes.indexOf(tornado);
      if (idx !== -1) room.tornadoes.splice(idx, 1);
    }, 90000); // remove after 90s
    broadcast(roomId, { type: 'tornado_spawn', ...tornado }, ws);
    return;
  }

  // ── ADMIN: LOCK WORLD ─────────────────────────────────────────────────────
  if (type === 'admin_lock') {
    if (id !== room.hostId) return;
    room.locked = !!msg.locked;
    broadcast(roomId, { type: 'chat', name: '[SERVER]', skin: '#ffaa00',
      text: room.locked ? '🔒 World locked by host.' : '🔓 World unlocked by host.' }, ws);
    return;
  }

  // ── ADMIN: KICK ───────────────────────────────────────────────────────────
  if (type === 'admin_kick') {
    if (id !== room.hostId) return;
    const targetId = msg.target;
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && c.id === targetId) {
        send(other, { type: 'kicked', reason: msg.reason || 'Kicked by host.' });
        other.close();
      }
    });
    return;
  }

  // ── ADMIN: SUMMON ─────────────────────────────────────────────────────────
  if (type === 'admin_summon') {
    if (id !== room.hostId) return;
    const { target, x, y, z } = msg;
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && (c.id === target || target === '*')) {
        send(other, { type: 'admin_summon', x, y, z });
      }
    });
    return;
  }

  // ── ADMIN: FORCE WALK ─────────────────────────────────────────────────────
  if (type === 'admin_forcewalk') {
    if (id !== room.hostId) return;
    const { target, x, y, z, stopDist } = msg;
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && (c.id === target || target === '*')) {
        send(other, { type: 'admin_forcewalk', target, x, y, z, stopDist });
      }
    });
    return;
  }

  // ── ADMIN: STOP WALK ──────────────────────────────────────────────────────
  if (type === 'admin_stopwalk') {
    if (id !== room.hostId) return;
    const { target } = msg;
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && (c.id === target || target === '*')) {
        send(other, { type: 'admin_stopwalk', target });
      }
    });
    return;
  }

  // ── SLEEP (broadcast to room) ─────────────────────────────────────────────
  if (type === 'player_sleep') {
    broadcast(roomId, { type: 'chat', name: '[WORLD]', skin: '#aaffcc',
      text: `😴 ${client.name} slept through the night.` }, ws);
    return;
  }

  // ── SKIN / OUTFIT UPDATE ─────────────────────────────────────────────────
  if (type === 'skin_update') {
    client.skin = msg.skin;
    const p = room.players.get(id);
    if (p) p.skin = msg.skin;
    broadcast(roomId, { type: 'skin_update', id, skin: msg.skin }, ws);
    return;
  }

  // ── CLOTHING VISIBILITY SYNC ─────────────────────────────────────────────
  if (type === 'clothing_update') {
    broadcast(roomId, { type: 'clothing_update', id, zones: msg.zones }, ws);
    return;
  }
}

// ── Disconnect ────────────────────────────────────────────────────────────────
function handleDisconnect(ws) {
  const client = clients.get(ws);
  if (!client) return;
  const { id, room: roomId, name } = client;
  const room = rooms.get(roomId);

  clients.delete(ws);

  if (!room) return;
  room.players.delete(id);

  broadcast(roomId, { type: 'player_leave', id });

  // Reassign host if needed
  if (room.hostId === id) {
    room.hostId = null;
    pickNewHost(roomId);
  }

  // Clean up empty rooms after 10 minutes
  const remaining = getRoomPlayers(roomId).length;
  console.log(`[server] [${roomId}] ${name} (${id}) left. Players remaining: ${remaining}`);

  if (remaining === 0) {
    setTimeout(() => {
      if (getRoomPlayers(roomId).length === 0) {
        rooms.delete(roomId);
        console.log(`[server] [${roomId}] Empty room cleaned up.`);
      }
    }, 10 * 60 * 1000);
  }
}

// ── Heartbeat: ping clients every 30s to detect stale connections ─────────────
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  });
}, 30000);

// ── Stats logging every 5 minutes ─────────────────────────────────────────────
setInterval(() => {
  let total = 0;
  rooms.forEach((room, id) => {
    const count = getRoomPlayers(id).length;
    total += count;
    if (count > 0) console.log(`[server] Room ${id}: ${count} player(s)`);
  });
  if (total > 0) console.log(`[server] Total online: ${total}`);
}, 5 * 60 * 1000);

// ── Graceful shutdown ─────────────────────────────────────────────────────────
process.on('SIGTERM', () => {
  console.log('[server] Shutting down…');
  wss.clients.forEach(ws => {
    send(ws, { type: 'error', msg: 'Server is restarting. Please reconnect in a moment.' });
    ws.close();
  });
  process.exit(0);
});
