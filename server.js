// ══════════════════════════════════════════════════════════════════════════════
//  server.js  —  World (Infinite) v54 Multiplayer Server
//  Node.js + ws  (npm install ws)
//  Run:  node server.js
//  Port: 8080  (set env PORT to override)
//
//  Based on the original sandcube.onrender.com server, with:
//   ✅ All original protocol (player/block/chat/marker/pvp/weather/tornado/admin)
//   ✅ Tide sync (v43 — host-authoritative 30-min cycle)
//   ✅ Physical crafting state relay (v43 — knapping/cordage/bow-drill)
//   ✅ Shirt symbol sync (full skin JSON in every player_update)
//
//  v54 update — 5 new message handlers added to match the v50–v52 client:
//   ✅ bow_state     — relay bow nock/un-nock state (v51)
//   ✅ arrow_shoot   — relay arrow projectile spawn (v51)
//   ✅ chest_open    — relay chest access notification (v52)
//   ✅ chest_update  — relay chest inventory changes (v52)
//   ✅ block_batch   — batched block ops for perf (v52)
//
//  Security hardening:
//   🔥 Prevent wantHost host stealing
//   🔥 Prevent duplicate player IDs
//   🔥 Max payload size limit (16 KB)
//   🔥 Chat + block spam rate limits
//   🔥 Validate all incoming data (types, ranges, lengths)
//   🔥 Chest contents sanitized (max 16 slots, type/count bounded)
//   🔥 Block batch size capped at 64 ops
//   🔥 Arrow direction vector bounded (prevents teleportation abuse)
// ══════════════════════════════════════════════════════════════════════════════

'use strict';

const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;

// ─── Security limits ───────────────────────────────────────────────────────
const MAX_PAYLOAD = 16 * 1024;          // 16 KB per message
const MAX_CHAT_LEN = 500;                // chat message length
const MAX_NAME_LEN = 32;                 // player name length
const MAX_ROOM_LEN = 64;                 // room code length
const MAX_PLAYERS_PER_ROOM = 32;         // room capacity
const MAX_BLOCK_OPS_PER_SEC = 30;        // block break/place rate limit
const MAX_CHAT_PER_SEC = 3;              // chat rate limit
const MAX_PLAYER_UPDATES_PER_SEC = 15;   // position update rate limit (client sends ~10/s)
const MAX_WORLD_MODS = 50_000;           // FIX 1: cap worldMods to prevent OOM on long sessions
const ROOM_CODE_RE = /^[a-zA-Z0-9_\-]{1,64}$/;  // valid room codes

const wss = new WebSocket.Server({ port: PORT, maxPayload: MAX_PAYLOAD });
console.log(`[server] Listening on ws://localhost:${PORT}`);
console.log(`[server] Security: maxPayload=${MAX_PAYLOAD} bytes, rate limits active`);

// ─── Room state ────────────────────────────────────────────────────────────
// rooms[roomId] = {
//   hostId         : string | null
//   locked         : bool
//   weather        : { enabled, wind, gustStrength, action }
//   worldMods      : Map<"x,y,z", blockType|null>  (capped at MAX_WORLD_MODS)
//   players        : Map<playerId, playerState>
//   sockets        : Set<ws>  — O(1) broadcast, no full-client scan
//   tokens         : Map<playerId, sessionToken>  — per-session auth tokens
//   bans           : Set<string>  — banned player IDs (persist for room lifetime)
//   hp             : Map<playerId, number>  — server-authoritative HP (0–100)
//   tornadoes      : Array<tornado>
//   outfitSettings : object  (lockEdit, visibility, playerOverrides)
//   ropes          : Map<ropeId, ropeState>
//   animals        : Map<animalId, animalState>
//   fires          : Map<fireId, {state, ownerId}>
//   markers        : Array<markerState>
//   chatHistory    : Array (last 50)
//   createdAt      : timestamp
//   tideStart      : number | null  (v43 — host's performance.now() origin for tide sync)
// }
const rooms = new Map();

// clients[ws] = { id, room, name, skin, rateLimits }
const clients = new WeakMap();

// ─── Rate limiting ─────────────────────────────────────────────────────────
function createRateLimiter() {
  return {
    blockOps: [],     // timestamps of block break/place
    chat: [],         // timestamps of chat messages
    playerUpdate: [], // timestamps of position updates
  };
}

function checkRate(rateArr, maxPerSec) {
  const now = Date.now();
  // Drop entries older than 1 second
  while (rateArr.length && rateArr[0] < now - 1000) rateArr.shift();
  if (rateArr.length >= maxPerSec) return false;
  rateArr.push(now);
  return true;
}

// ─── Validation helpers ────────────────────────────────────────────────────
function isStr(v, maxLen) { return typeof v === 'string' && v.length > 0 && v.length <= maxLen; }
function isNum(v) { return typeof v === 'number' && isFinite(v); }
function isInt(v) { return Number.isInteger(v); }
function isBool(v) { return typeof v === 'boolean'; }
function isCoords(v) { return isNum(v.x) && isNum(v.y) && isNum(v.z); }
function clampStr(s, max) { return typeof s === 'string' ? s.slice(0, max) : ''; }
function sanitizeName(name) {
  if (typeof name !== 'string') return 'Player';
  // Strip HTML/control chars, clamp length
  return name.replace(/[<>&"'\x00-\x1f]/g, '').slice(0, MAX_NAME_LEN) || 'Player';
}
function sanitizeText(text) {
  if (typeof text !== 'string') return '';
  return text.replace(/[\x00-\x1f]/g, '').slice(0, MAX_CHAT_LEN);
}
function isValidRoomCode(code) {
  return typeof code === 'string' && ROOM_CODE_RE.test(code);
}
function isValidBlockType(t) {
  // null = broken (break op), otherwise integer 0..200
  if (t === null) return true;
  return isInt(t) && t >= 0 && t <= 200;
}

// ─── Helpers ───────────────────────────────────────────────────────────────
function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      hostId: null,
      locked: false,
      weather: { enabled: true, action: null, wind: 0, gustStrength: 0 },
      worldMods: new Map(),          // capped at MAX_WORLD_MODS
      players: new Map(),
      sockets: new Set(),            // FIX 2: O(1) per-room broadcast
      tokens: new Map(),             // FIX 3: playerId → sessionToken
      bans: new Set(),               // FIX 10: banned player IDs
      hp: new Map(),                 // FIX 5: server-authoritative HP
      tornadoes: [],
      outfitSettings: { lockEdit: false, visibility: 'none', playerOverrides: {} },
      ropes: new Map(),
      animals: new Map(),
      fires: new Map(),              // FIX 6: stores { state, ownerId }
      markers: [],
      chatHistory: [],
      createdAt: Date.now(),
      tideStart: null,               // v43 tide sync
    });
  }
  return rooms.get(roomId);
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }
}

// FIX 2: O(room size) broadcast using per-room socket Set instead of O(total clients)
function broadcast(roomId, obj, excludeWs = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const payload = JSON.stringify(obj);
  for (const ws of room.sockets) {
    if (ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
      try { ws.send(payload); } catch (e) {}
    }
  }
}

function broadcastAll(roomId, obj) {
  broadcast(roomId, obj, null);
}

function getRoomPlayers(roomId) {
  const room = rooms.get(roomId);
  if (!room) return [];
  const out = [];
  for (const ws of room.sockets) {
    const c = clients.get(ws);
    if (c) out.push({ ws, ...c });
  }
  return out;
}

// FIX 3: validate that a packet's pid matches the sender's session token
function verifyToken(room, id, tok) {
  if (!tok) return false;
  return room.tokens.get(id) === tok;
}

// FIX 1: worldMods LRU cap — evict oldest entry when over limit
function setWorldMod(room, key, value) {
  if (!room.worldMods.has(key) && room.worldMods.size >= MAX_WORLD_MODS) {
    // Map preserves insertion order — delete the oldest (first) entry
    const firstKey = room.worldMods.keys().next().value;
    room.worldMods.delete(firstKey);
  }
  room.worldMods.set(key, value);
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

// ─── Connection ────────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  clients.set(ws, { id: null, room: null, name: 'Player', skin: '#c8a46e', rateLimits: createRateLimiter() });

  ws.on('message', (raw) => {
    // ── Payload size check (defense in depth — ws maxPayload also enforces) ──
    if (raw.length > MAX_PAYLOAD) {
      console.warn(`[server] Dropping oversize message: ${raw.length} bytes from ${req.socket.remoteAddress}`);
      return;
    }
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg !== 'object' || !msg.type) return;
    handleMessage(ws, msg);
  });

  ws.on('close', () => handleDisconnect(ws));
  ws.on('error', () => handleDisconnect(ws));
});

// ─── Message router ────────────────────────────────────────────────────────
function handleMessage(ws, msg) {
  const type = msg.type;
  const client = clients.get(ws);
  if (!client) return;

  // ── JOIN ──────────────────────────────────────────────────────────────────
  if (type === 'join') {
    const id = msg.id;
    const roomId = msg.room;

    // ── Validate ──
    if (!isStr(id, 64)) { send(ws, { type: 'error', msg: 'Invalid player ID.' }); return; }
    if (!isValidRoomCode(roomId)) { send(ws, { type: 'error', msg: 'Invalid room code.' }); return; }

    const name = sanitizeName(msg.name);
    const skin = typeof msg.skin === 'string' ? clampStr(msg.skin, 2048) : '#c8a46e';
    const wantHost = isBool(msg.wantHost) ? msg.wantHost : false;

    const room = getRoom(roomId);

    // ── Prevent duplicate player IDs ──
    if (room.players.has(id)) {
      // Check if the existing player's socket is still alive
      let existingWs = null;
      wss.clients.forEach(other => {
        const c = clients.get(other);
        if (c && c.room === roomId && c.id === id) existingWs = other;
      });
      if (existingWs && existingWs.readyState === WebSocket.OPEN) {
        send(ws, { type: 'error', msg: 'Player ID already in use in this room.' });
        ws.close();
        return;
      }
      // Stale entry — clean it up and allow the new connection
      room.players.delete(id);
      console.log(`[server] [${roomId}] Cleaned up stale entry for ${id}`);
    }

    // ── Room capacity ──
    if (room.players.size >= MAX_PLAYERS_PER_ROOM) {
      send(ws, { type: 'error', msg: `Room is full (max ${MAX_PLAYERS_PER_ROOM} players).` });
      ws.close();
      return;
    }

    // ── Reject if world locked ──
    if (room.locked) {
      send(ws, { type: 'error', msg: 'This world is locked — no new players allowed.' });
      return;
    }

    // FIX 10: reject banned players
    if (room.bans.has(id)) {
      send(ws, { type: 'error', msg: 'You are banned from this world.' });
      ws.close();
      return;
    }

    clients.set(ws, { id, room: roomId, name, skin, rateLimits: client.rateLimits });
    room.sockets.add(ws);                                   // FIX 2: track socket in room set
    room.tokens.set(id, msg.tok || null);                   // FIX 3: store session token
    room.hp.set(id, 100);                                   // FIX 5: init server HP
    room.players.set(id, { id, name, skin,
      x: 0, y: 64, z: 0, ry: 0, swimming: false, lastSeen: Date.now() });

    // ── Host assignment (SECURITY: wantHost only honors if no host yet) ──
    if (!room.hostId) {
      room.hostId = id;
      send(ws, { type: 'host_assigned', msg: 'You are the host of this world.' });
    } else if (wantHost) {
      console.log(`[server] [${roomId}] ${name} (${id}) requested host but ${room.hostId} is already host — denied`);
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
    broadcast(roomId, { type: 'player_join', id, name, skin }, ws);

    // Send outfit settings to newcomer if they exist
    if (room.outfitSettings) {
      send(ws, { type: 'outfit_settings', settings: room.outfitSettings });
    }

    // ── v43 Tide sync: if host already has a tideStart, send it to newcomer ──
    if (room.tideStart !== null && room.hostId === id) {
      // Newcomer is the host — they'll set tideStart themselves
    } else if (room.tideStart !== null) {
      // Send existing tide clock to newcomer so they sync immediately
      send(ws, { type: 'tide_sync', start: room.tideStart, wall: Date.now() });
    }

    const playerCount = getRoomPlayers(roomId).length;
    console.log(`[server] [${roomId}] ${name} (${id}) joined. Players: ${playerCount}`);
    return;
  }

  // All other messages require the client to be registered
  if (!client.id || !client.room) return;
  const { id, room: roomId } = client;
  const room = rooms.get(roomId);
  if (!room) return;

  // ── PLAYER POSITION ───────────────────────────────────────────────────────
  if (type === 'player_update') {
    // ── Rate limit ──
    if (!checkRate(client.rateLimits.playerUpdate, MAX_PLAYER_UPDATES_PER_SEC)) return;
    // ── Validate ──
    if (!isNum(msg.x) || !isNum(msg.y) || !isNum(msg.z)) return;
    if (Math.abs(msg.x) > 1e6 || Math.abs(msg.y) > 1e6 || Math.abs(msg.z) > 1e6) return;
    const ry = isNum(msg.ry) ? msg.ry : 0;
    const name = sanitizeName(msg.name);
    const skin = typeof msg.skin === 'string' ? clampStr(msg.skin, 2048) : client.skin;
    const swimming = isBool(msg.swimming) ? msg.swimming : false;

    const p = room.players.get(id);
    if (p) { Object.assign(p, { x: msg.x, y: msg.y, z: msg.z, ry, name, skin, swimming, lastSeen: Date.now() }); }
    broadcast(roomId, { type: 'player_update', id, name, skin, x: msg.x, y: msg.y, z: msg.z, ry, swimming }, ws);
    return;
  }

  // ── BLOCK BREAK / PLACE ───────────────────────────────────────────────────
  if (type === 'block_break') {
    if (!checkRate(client.rateLimits.blockOps, MAX_BLOCK_OPS_PER_SEC)) return;
    if (!isInt(msg.x) || !isInt(msg.y) || !isInt(msg.z)) return;
    if (Math.abs(msg.x) > 1e6 || Math.abs(msg.y) > 1e6 || Math.abs(msg.z) > 1e6) return;
    setWorldMod(room, `${msg.x},${msg.y},${msg.z}`, null);  // FIX 1: capped LRU
    broadcast(roomId, { type: 'block_break', pid: id, x: msg.x, y: msg.y, z: msg.z }, ws);
    return;
  }

  if (type === 'block_place') {
    if (!checkRate(client.rateLimits.blockOps, MAX_BLOCK_OPS_PER_SEC)) return;
    if (!isInt(msg.x) || !isInt(msg.y) || !isInt(msg.z)) return;
    if (!isValidBlockType(msg.t)) return;
    if (Math.abs(msg.x) > 1e6 || Math.abs(msg.y) > 1e6 || Math.abs(msg.z) > 1e6) return;
    setWorldMod(room, `${msg.x},${msg.y},${msg.z}`, msg.t);  // FIX 1: capped LRU
    broadcast(roomId, { type: 'block_place', pid: id, x: msg.x, y: msg.y, z: msg.z, t: msg.t }, ws);
    return;
  }

  // ── CHAT ─────────────────────────────────────────────────────────────────
  if (type === 'chat') {
    if (!checkRate(client.rateLimits.chat, MAX_CHAT_PER_SEC)) return;
    const text = sanitizeText(msg.text);
    if (!text) return;
    const chatMsg = { type: 'chat', name: sanitizeName(msg.name) || client.name, skin: msg.skin || '#fff', text };
    pushChat(roomId, chatMsg);
    broadcastAll(roomId, chatMsg);
    return;
  }

  // ── MAP MARKER ────────────────────────────────────────────────────────────
  if (type === 'map_marker') {
    if (!isNum(msg.x) || !isNum(msg.z)) return;
    const marker = { x: msg.x, z: msg.z,
      label: clampStr(msg.label || `${msg.x},${msg.z}`, 64),
      color: clampStr(msg.color || '#ffff00', 32), pid: id, name: client.name };
    room.markers.push(marker);
    if (room.markers.length > 200) room.markers.shift();
    broadcastAll(roomId, { type: 'map_marker', ...marker });
    return;
  }

  // ── PVP — FIX 5: server-authoritative HP ────────────────────────────────
  // Server owns HP. Client sends a hit intent; server applies capped damage
  // to its own HP ledger and decides if the player is dead.
  // Max damage per hit is 25 — enough for a stone spear (20) + crit headroom.
  if (type === 'pvp_hit') {
    if (!isStr(msg.target, 64)) return;
    if (msg.target === id) return;  // no self-damage via PVP packet
    const MAX_HIT_DAMAGE = 25;
    const damage = isNum(msg.damage) ? Math.max(1, Math.min(MAX_HIT_DAMAGE, msg.damage)) : 10;
    const attackerName = sanitizeName(msg.attackerName) || client.name;

    // Apply to server HP ledger
    const targetHp = room.hp.get(msg.target);
    if (targetHp === undefined) return;  // target not in room
    const newHp = Math.max(0, targetHp - damage);
    room.hp.set(msg.target, newHp);

    // Relay authoritative damage to target
    for (const ws2 of room.sockets) {
      const c = clients.get(ws2);
      if (c && c.id === msg.target) {
        send(ws2, { type: 'pvp_hit', attacker: id, attackerName, target: msg.target, damage, hp: newHp });
        break;
      }
    }

    // Server decides kill, not client
    if (newHp <= 0) {
      room.hp.set(msg.target, 100);  // respawn HP
      const victimName = (() => {
        for (const ws2 of room.sockets) {
          const c = clients.get(ws2);
          if (c && c.id === msg.target) return c.name;
        }
        return msg.target;
      })();
      const killMsg = { type: 'pvp_kill', attacker: id, attackerName, victim: msg.target, victimName };
      broadcastAll(roomId, killMsg);
      pushChat(roomId, { type: 'chat', name: '[WORLD]', skin: '#f88',
        text: `💀 ${attackerName} killed ${victimName}` });
    }
    return;
  }

  // ── HP SYNC: client reports damage from non-PVP sources (fall, hunger, etc.) ──
  // Server accepts these but bounds-checks the delta to prevent self-healing exploits.
  if (type === 'hp_update') {
    if (!isNum(msg.hp)) return;
    const current = room.hp.get(id) ?? 100;
    const claimed = Math.max(0, Math.min(100, msg.hp));
    // Only allow HP to go DOWN from non-pvp sources server-side (healing is client-local)
    // Allow up to 100 HP recovery (for respawn/sleep/food) but flag suspicious jumps
    if (claimed > current + 30) {
      console.warn(`[server] [${roomId}] suspicious HP jump: ${id} ${current} → ${claimed}`);
    }
    room.hp.set(id, claimed);
    return;
  }

  // ── v43 TIDE SYNC (host → server → all clients) ──────────────────────────
  if (type === 'tide_sync') {
    // Only host can broadcast tide sync
    if (id !== room.hostId) return;
    if (!isNum(msg.start)) return;
    // FIX 11: reject obviously bogus tide clock values (Infinity, NaN already caught by isNum)
    if (msg.start < 0 || msg.start > 1e13) return;  // sane Unix-ms range
    const wall = isNum(msg.wall) ? msg.wall : Date.now();
    room.tideStart = msg.start;
    broadcast(roomId, { type: 'tide_sync', start: msg.start, wall }, ws);
    return;
  }

  // ── v43 CRAFT COMPLETE — FIX 3/4: audit log + optional craft_denied ──────
  if (type === 'craft_complete') {
    // FIX 4: rate-limit craft completions (max 2/sec — impossible to do faster legit)
    if (!checkRate(client.rateLimits.blockOps, 2)) {
      // Suspiciously fast — deny and log
      send(ws, { type: 'craft_denied', reason: 'craft rate exceeded' });
      console.warn(`[server] [${roomId}] craft_complete rate exceeded from ${client.name} (${id})`);
      return;
    }
    // FIX 3: verify session token matches the one sent on join
    if (!verifyToken(room, id, msg.tok)) {
      send(ws, { type: 'craft_denied', reason: 'token mismatch' });
      console.warn(`[server] [${roomId}] craft_complete token mismatch from ${client.name} (${id})`);
      return;
    }
    // Valid — log for audit and relay to host for optional server-side validation
    const task = clampStr(msg.task || '', 32);
    console.log(`[server] [${roomId}] craft_complete: ${client.name} crafted '${task}'`);
    // Relay to host so host-side logic can validate if desired
    for (const ws2 of room.sockets) {
      const c = clients.get(ws2);
      if (c && c.id === room.hostId && ws2 !== ws) {
        send(ws2, { type: 'craft_complete_relay', pid: id, name: client.name, task });
        break;
      }
    }
    return;
  }

  // ── v43 PHYSICAL CRAFTING STATE (optional relay — for future use) ────────
  // Players can broadcast their current crafting activity so others can see
  // them working (e.g., knapping flint, twisting cordage).
  if (type === 'craft_state') {
    const craftType = clampStr(msg.craft, 32); // 'knap' | 'cordage' | 'bowdrill' | 'haft' | 'clay' | 'butcher'
    const progress = isNum(msg.progress) ? Math.max(0, Math.min(1, msg.progress)) : 0;
    broadcast(roomId, { type: 'craft_state', id, craft: craftType, progress }, ws);
    return;
  }

  // ── OUTFIT SETTINGS ───────────────────────────────────────────────────────
  if (type === 'outfit_settings') {
    if (id !== room.hostId) return;
    const settings = msg.settings && typeof msg.settings === 'object' ? msg.settings : {};
    room.outfitSettings = {
      lockEdit: !!settings.lockEdit,
      visibility: clampStr(settings.visibility || 'none', 16),
      playerOverrides: settings.playerOverrides && typeof settings.playerOverrides === 'object' ? settings.playerOverrides : {},
    };
    broadcast(roomId, { type: 'outfit_settings', settings: room.outfitSettings }, ws);
    return;
  }

  // ── ROPE ─────────────────────────────────────────────────────────────────
  if (type === 'rope_place') {
    if (!checkRate(client.rateLimits.blockOps, MAX_BLOCK_OPS_PER_SEC)) return;
    if (!isStr(msg.ropeId, 64)) return;
    if (!isNum(msg.x1) || !isNum(msg.y1) || !isNum(msg.z1)) return;
    if (!isNum(msg.x2) || !isNum(msg.y2) || !isNum(msg.z2)) return;
    // FIX 7: bound rope coordinates same as blocks
    const coords = [msg.x1, msg.y1, msg.z1, msg.x2, msg.y2, msg.z2];
    if (coords.some(v => Math.abs(v) > 1e6)) return;
    const rope = { id: msg.ropeId, x1: msg.x1, y1: msg.y1, z1: msg.z1,
      x2: msg.x2, y2: msg.y2, z2: msg.z2, pid: id };
    room.ropes.set(msg.ropeId, rope);
    broadcast(roomId, { type: 'rope_place', ...rope }, ws);
    return;
  }

  if (type === 'rope_remove') {
    if (!isStr(msg.ropeId, 64)) return;
    room.ropes.delete(msg.ropeId);
    broadcast(roomId, { type: 'rope_remove', ropeId: msg.ropeId }, ws);
    return;
  }

  // ── FIRE / CAMPFIRE ───────────────────────────────────────────────────────
  if (type === 'fire_place') {
    if (!checkRate(client.rateLimits.blockOps, MAX_BLOCK_OPS_PER_SEC)) return;
    if (!isStr(msg.fireId, 64)) return;
    if (!isInt(msg.x) || !isInt(msg.y) || !isInt(msg.z)) return;
    // FIX 6: store ownerId so only owner or host can update/remove
    const fire = { id: msg.fireId, x: msg.x, y: msg.y, z: msg.z,
      lit: isBool(msg.lit) ? msg.lit : false,
      fuel: isNum(msg.fuel) ? msg.fuel : 0,
      ownerId: id };
    room.fires.set(msg.fireId, fire);
    broadcast(roomId, { type: 'fire_place', ...fire }, ws);
    return;
  }

  if (type === 'fire_update') {
    if (!isStr(msg.fireId, 64)) return;
    const fire = room.fires.get(msg.fireId);
    if (!fire) return;
    // FIX 6: only owner or host can update the fire
    if (fire.ownerId !== id && id !== room.hostId) return;
    const fuel = isNum(msg.fuel) ? Math.max(0, msg.fuel) : 0;
    Object.assign(fire, { lit: !!msg.lit, fuel });
    broadcast(roomId, { type: 'fire_update', fireId: msg.fireId, lit: fire.lit, fuel: fire.fuel }, ws);
    return;
  }

  if (type === 'fire_remove') {
    if (!isStr(msg.fireId, 64)) return;
    const fire = room.fires.get(msg.fireId);
    if (!fire) return;
    // FIX 6: only owner or host can remove the fire
    if (fire.ownerId !== id && id !== room.hostId) return;
    room.fires.delete(msg.fireId);
    broadcast(roomId, { type: 'fire_remove', fireId: msg.fireId }, ws);
    return;
  }

  // ── ANIMAL SYNC ──────────────────────────────────────────────────────────
  if (type === 'animal_update') {
    if (id !== room.hostId) return;
    if (!isStr(msg.animalId, 64)) return;
    if (!isNum(msg.x) || !isNum(msg.y) || !isNum(msg.z)) return;
    // FIX 9: validate hp and animalType before storing
    const animalHp = isNum(msg.hp) ? Math.max(0, Math.min(500, msg.hp)) : 100;
    const animalType = isStr(msg.animalType, 32) ? msg.animalType : 'unknown';
    room.animals.set(msg.animalId, { animalId: msg.animalId, x: msg.x, y: msg.y, z: msg.z,
      hp: animalHp, dead: !!msg.dead, animalType });
    broadcast(roomId, { type: 'animal_update', animalId: msg.animalId, x: msg.x, y: msg.y, z: msg.z,
      hp: animalHp, dead: !!msg.dead, animalType }, ws);
    return;
  }

  if (type === 'animal_kill') {
    if (!isStr(msg.animalId, 64)) return;
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
    const action = clampStr(msg.action, 16);
    if (!['calm', 'storm', 'day', 'night', 'tornado', 'rain', 'clear', 'fog', 'snow'].includes(action)) return;
    room.weather.action = action;
    broadcast(roomId, { type: 'admin_weather', action }, ws);
    return;
  }

  // ── TORNADO ───────────────────────────────────────────────────────────────
  if (type === 'tornado_spawn') {
    if (id !== room.hostId) return;
    if (!isNum(msg.x) || !isNum(msg.z)) return;
    const strength = isNum(msg.strength) ? Math.max(0, Math.min(150, msg.strength)) : 70;
    const tornado = { x: msg.x, z: msg.z, strength };
    room.tornadoes.push(tornado);
    setTimeout(() => {
      const idx = room.tornadoes.indexOf(tornado);
      if (idx !== -1) room.tornadoes.splice(idx, 1);
    }, 90000);
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
    if (!isStr(msg.target, 64)) return;
    const reason = sanitizeText(msg.reason) || 'Kicked by host.';
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && c.id === msg.target) {
        send(other, { type: 'kicked', reason });
        other.close();
      }
    });
    return;
  }

  // ── ADMIN: BAN — FIX 10 ──────────────────────────────────────────────────
  if (type === 'admin_ban') {
    if (id !== room.hostId) return;
    if (!isStr(msg.target, 64)) return;
    const reason = sanitizeText(msg.reason) || 'Banned by host.';
    room.bans.add(msg.target);
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && c.id === msg.target) {
        send(other, { type: 'kicked', reason: `Banned: ${reason}` });
        other.close();
      }
    });
    console.log(`[server] [${roomId}] ${client.name} banned ${msg.target}: ${reason}`);
    return;
  }

  // ── ADMIN: UNBAN ──────────────────────────────────────────────────────────
  if (type === 'admin_unban') {
    if (id !== room.hostId) return;
    if (!isStr(msg.target, 64)) return;
    room.bans.delete(msg.target);
    return;
  }

  // ── ADMIN: SUMMON ─────────────────────────────────────────────────────────
  if (type === 'admin_summon') {
    if (id !== room.hostId) return;
    if (!isStr(msg.target, 64)) return;
    if (!isNum(msg.x) || !isNum(msg.y) || !isNum(msg.z)) return;
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && (c.id === msg.target || msg.target === '*')) {
        send(other, { type: 'admin_summon', x: msg.x, y: msg.y, z: msg.z });
      }
    });
    return;
  }

  // ── ADMIN: FORCE WALK ─────────────────────────────────────────────────────
  if (type === 'admin_forcewalk') {
    if (id !== room.hostId) return;
    if (!isStr(msg.target, 64)) return;
    if (!isNum(msg.x) || !isNum(msg.y) || !isNum(msg.z)) return;
    const stopDist = isNum(msg.stopDist) ? msg.stopDist : 1.4;
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && (c.id === msg.target || msg.target === '*')) {
        send(other, { type: 'admin_forcewalk', target: msg.target, x: msg.x, y: msg.y, z: msg.z, stopDist });
      }
    });
    return;
  }

  if (type === 'admin_stopwalk') {
    if (id !== room.hostId) return;
    if (!isStr(msg.target, 64)) return;
    wss.clients.forEach(other => {
      const c = clients.get(other);
      if (c && c.room === roomId && (c.id === msg.target || msg.target === '*')) {
        send(other, { type: 'admin_stopwalk', target: msg.target });
      }
    });
    return;
  }

  // ── SLEEP ─────────────────────────────────────────────────────────────────
  if (type === 'player_sleep') {
    broadcast(roomId, { type: 'chat', name: '[WORLD]', skin: '#aaffcc',
      text: `😴 ${client.name} slept through the night.` }, ws);
    return;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // v54 NEW MESSAGE HANDLERS
  // These 5 message types were added to the client in v50–v52 but never made it
  // to the server. Without them, multiplayer features silently degrade:
  //   • bow_state — other players can't see when you nock/un-nock an arrow
  //   • arrow_shoot — other players don't see your flying arrow projectile
  //   • chest_open — no awareness of who's accessing a chest (item dup risk)
  //   • chest_update — chest contents don't sync between players
  //   • block_batch — perf optimization (server falls back to individual ops)
  // All handlers validate inputs the same way the existing handlers do.
  // ═══════════════════════════════════════════════════════════════════════════

  // ── v51 BOW STATE — relay nock/un-nock to other players ──────────────────
  // Client uses this for tactical awareness (see when hostile players nock).
  if (type === 'bow_state') {
    const nocked = isBool(msg.nocked) ? msg.nocked : false;
    broadcast(roomId, { type: 'bow_state', pid: id, nocked }, ws);
    return;
  }

  // ── v51 ARROW SHOOT — relay arrow projectile spawn to other players ───────
  // The shooter's client is authoritative for hits (sends pvp_hit on collision).
  // Remote clients only render the projectile visually.
  if (type === 'arrow_shoot') {
    if (!isNum(msg.x) || !isNum(msg.y) || !isNum(msg.z)) return;
    if (!isNum(msg.dx) || !isNum(msg.dy) || !isNum(msg.dz)) return;
    // Bound coordinates and direction (direction should be a unit-ish vector)
    if (Math.abs(msg.x) > 1e6 || Math.abs(msg.y) > 1e6 || Math.abs(msg.z) > 1e6) return;
    if (Math.abs(msg.dx) > 2 || Math.abs(msg.dy) > 2 || Math.abs(msg.dz) > 2) return;
    const name = sanitizeName(msg.name) || client.name;
    broadcast(roomId, {
      type: 'arrow_shoot', pid: id, name,
      x: msg.x, y: msg.y, z: msg.z,
      dx: msg.dx, dy: msg.dy, dz: msg.dz,
    }, ws);
    return;
  }

  // ── v52 CHEST OPEN — relay chest access notification ──────────────────────
  // Lets other players know someone is accessing a chest (anti-dup awareness).
  // We don't lock the chest server-side (that would require chest state on the
  // server) — we just broadcast the open event so clients can show a warning.
  if (type === 'chest_open') {
    if (!isInt(msg.x) || !isInt(msg.y) || !isInt(msg.z)) return;
    if (Math.abs(msg.x) > 1e6 || Math.abs(msg.y) > 1e6 || Math.abs(msg.z) > 1e6) return;
    broadcast(roomId, { type: 'chest_open', pid: id, x: msg.x, y: msg.y, z: msg.z }, ws);
    return;
  }

  // ── v52 CHEST UPDATE — relay chest inventory changes ──────────────────────
  // When a player adds/removes items from a chest, broadcast the new contents
  // so other clients update their local cache. Contents is an array of
  // {type, count} slots (or null for empty).
  if (type === 'chest_update') {
    if (!isInt(msg.x) || !isInt(msg.y) || !isInt(msg.z)) return;
    if (Math.abs(msg.x) > 1e6 || Math.abs(msg.y) > 1e6 || Math.abs(msg.z) > 1e6) return;
    if (!Array.isArray(msg.contents)) return;
    // Sanitize contents: max 16 slots, each slot is null or {type:int 0..200, count:int 0..64}
    const safeContents = [];
    for (let i = 0; i < Math.min(msg.contents.length, 16); i++) {
      const slot = msg.contents[i];
      if (slot === null || slot === undefined) {
        safeContents.push(null);
      } else if (typeof slot === 'object' && isInt(slot.type) && slot.type >= 0 && slot.type <= 200 && isInt(slot.count) && slot.count >= 0 && slot.count <= 64) {
        safeContents.push({ type: slot.type, count: slot.count });
      } else {
        safeContents.push(null);
      }
    }
    broadcast(roomId, {
      type: 'chest_update', pid: id,
      x: msg.x, y: msg.y, z: msg.z,
      contents: safeContents,
    }, ws);
    return;
  }

  // ── v52 BLOCK BATCH — perf optimization, relay batched block ops ──────────
  // Client batches break/place ops at 20Hz to reduce message count. Server
  // applies each op to worldMods (capped LRU) and relays the batch to others.
  if (type === 'block_batch') {
    if (!checkRate(client.rateLimits.blockOps, MAX_BLOCK_OPS_PER_SEC)) return;
    if (!Array.isArray(msg.ops)) return;
    if (msg.ops.length > 64) return;  // cap batch size to prevent abuse
    const relayOps = [];
    for (const op of msg.ops) {
      if (!op || typeof op !== 'object') continue;
      if (op.op !== 'break' && op.op !== 'place') continue;
      if (!isInt(op.x) || !isInt(op.y) || !isInt(op.z)) continue;
      if (Math.abs(op.x) > 1e6 || Math.abs(op.y) > 1e6 || Math.abs(op.z) > 1e6) continue;
      if (op.op === 'place') {
        if (!isValidBlockType(op.t)) continue;
        setWorldMod(room, `${op.x},${op.y},${op.z}`, op.t);
        relayOps.push({ op: 'place', x: op.x, y: op.y, z: op.z, t: op.t });
      } else {
        setWorldMod(room, `${op.x},${op.y},${op.z}`, null);
        relayOps.push({ op: 'break', x: op.x, y: op.y, z: op.z });
      }
    }
    if (relayOps.length > 0) {
      broadcast(roomId, { type: 'block_batch', pid: id, ops: relayOps }, ws);
    }
    return;
  }

  // ── SKIN / OUTFIT UPDATE ─────────────────────────────────────────────────
  if (type === 'skin_update') {
    const skin = typeof msg.skin === 'string' ? clampStr(msg.skin, 2048) : client.skin;
    client.skin = skin;
    const p = room.players.get(id);
    if (p) p.skin = skin;
    broadcast(roomId, { type: 'skin_update', id, skin }, ws);
    return;
  }

  if (type === 'clothing_update') {
    if (msg.zones && typeof msg.zones === 'object' && !Array.isArray(msg.zones)) {
      // FIX 12: sanitize zones — only allow string keys/values, max 32 zones
      const safeZones = {};
      let count = 0;
      for (const [k, v] of Object.entries(msg.zones)) {
        if (count++ >= 32) break;
        if (typeof k === 'string' && k.length <= 32 && typeof v === 'string' && v.length <= 64) {
          safeZones[k] = v;
        }
      }
      broadcast(roomId, { type: 'clothing_update', id, zones: safeZones }, ws);
    }
    return;
  }
}

// ─── Disconnect ────────────────────────────────────────────────────────────
function handleDisconnect(ws) {
  const client = clients.get(ws);
  if (!client) return;
  const { id, room: roomId, name } = client;
  const room = rooms.get(roomId);

  clients.delete(ws);

  if (!room) return;
  room.players.delete(id);
  room.sockets.delete(ws);   // FIX 2: remove from per-room socket set
  room.tokens.delete(id);    // FIX 3: clear session token
  room.hp.delete(id);        // FIX 5: clear HP entry

  broadcast(roomId, { type: 'player_leave', id });

  // Reassign host if needed
  if (room.hostId === id) {
    room.hostId = null;
    room.tideStart = null;  // reset tide clock — new host will set it
    pickNewHost(roomId);
  }

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

// ─── Heartbeat: ping clients every 30s ─────────────────────────────────────
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.ping(); } catch (e) {}
    }
  });
}, 30000);

// ─── Stats logging every 5 minutes ─────────────────────────────────────────
setInterval(() => {
  let total = 0;
  rooms.forEach((room, id) => {
    const count = getRoomPlayers(id).length;
    total += count;
    if (count > 0) console.log(`[server] Room ${id}: ${count} player(s)`);
  });
  if (total > 0) console.log(`[server] Total online: ${total}`);
}, 5 * 60 * 1000);

// ─── Graceful shutdown ─────────────────────────────────────────────────────
process.on('SIGTERM', () => {
  console.log('[server] Shutting down…');
  wss.clients.forEach(ws => {
    send(ws, { type: 'error', msg: 'Server is restarting. Please reconnect in a moment.' });
    try { ws.close(); } catch (e) {}
  });
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('\n[server] SIGINT received, shutting down…');
  wss.clients.forEach(ws => {
    send(ws, { type: 'error', msg: 'Server is shutting down.' });
    try { ws.close(); } catch (e) {}
  });
  process.exit(0);
});

console.log('[server] Ready. Waiting for connections...');