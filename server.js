// ─────────────────────────────────────────────────────────────────────────────
// World (Infinite) v58 — COMPLETE Multiplayer Server
// ─────────────────────────────────────────────────────────────────────────────
// Supports every feature in the game including:
//
// CORE: join, player_update, chat, block_break, block_place, block_batch,
//       map_marker, tide_sync, pvp_hit, pvp_kill_feed, arrow_shoot,
//       chest_open, chest_update, weather_toggle, outfit_settings, bow_state,
//       join_ack, player_list, player_join, player_leave, host_assigned,
//       kicked, error
//
// ADMIN: admin_kick, admin_summon, admin_forcewalk, admin_stopwalk,
//        admin_lock, admin_weather, admin_weather, tornado_spawn,
//        admin_broadcast, admin_gamemode, admin_teleport_all, admin_give,
//        admin_heal
//
// v79: player_cmd — generic host-only relay used by every targetable chat
//      command (/heal /god /fly /noclip /creative /speed /gamemode /hunger
//      /thirst /stamina /fatigue /strength /freeze /flyspeed /instabreak
//      /clearinv /repair /kill /respawn /tp <player> to <player>, etc.)
//      when the command targets @a or another player's username instead of
//      yourself. One message type instead of one admin_* type per command.
//
// FORGING & METALWORK: forge_state (crucible heat, smelting progress,
//       bloom creation, bloom→ingot at anvil), anvil_state (hammering
//       progress, item being forged), hammer_hit
//
// COOKING: cooking_pot_state (pot contents, cook progress, recipe),
//          kiln_state (clay/charcoal kiln progress, fuel),
//          campfire_state (fuel level, cooking slots, heat),
//          furnace_state (smelting input/output, progress)
//
// FIRE: fire_start, fire_spread, fire_extinguish, fire_state
//
// ANIMALS: animal_spawn, animal_update, animal_kill, animal_despawn,
//          animal_hurt, animal_tame
//
// ROPE: rope_place, rope_lash, rope_break, rope_bridge
//
// ITEMS: drop_item_spawn, drop_item_pickup, drop_item_update, pile_sync
//
// VEHICLES: boat_mount, boat_dismount, boat_update, boat_place, boat_break
//
// WATER: water_flow, water_drain
//
// CLAY: clay_shape_update, clay_carve
//
// PLAYER STATE: health_update, hunger_update, death_broadcast
//
// ENTITIES: entity_sync, entity_despawn
//
// SERVER-SIDE STATE TRACKING:
//   - Block modification log (for join_ack world sync)
//   - Room weather/fire/animal/rope/vehicle state
//   - Per-player health for pvp_kill_feed generation
//   - Auto host promotion on disconnect
//   - Room locking
//   - Graceful shutdown
// ─────────────────────────────────────────────────────────────────────────────

const WebSocket = require("ws");
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = parseInt(process.env.PORT || "8080", 10);
const INDEX_HTML = path.join(__dirname, "world_game_v58_fixed.html");

// ═══════════════════════════════════════════════════════════════════════════════
// ROOM STATE — Each room tracks world modifications and entity state
// ═══════════════════════════════════════════════════════════════════════════════

const rooms = new Map();

function createRoom(key, seed, worldType) {
  return {
    key,
    seed,
    worldType,
    hostId: null,
    locked: false,
    weatherEnabled: true,
    outfitSettings: null,
    players: new Map(),          // id → { ws, name, skin, joinTime, lastPos, health }
    // ── World state sent to new joiners ──
    blockMods: {},               // "x,y,z" → blockType (null = broken)
    // ── Fire state ──
    fires: {},                   // "x,y,z" → { fuel, heat, burning, spreadTo? }
    // ── Animal state ──
    animals: new Map(),          // animalId → { type, x, y, z, hp, name, tamed }
    // ── Rope state ──
    ropes: new Map(),            // ropeId → { points, lashed, broken }
    // ── Vehicle state ──
    boats: new Map(),            // boatId → { x, y, z, ry, rider, blocks }
    // ── Forge/Crucible state ──
    crucibles: {},               // "x,y,z" → { heat, fuel, input, output, progress }
    anvils: {},                  // "x,y,z" → { item, progress, type }
    // ── Cooking state ──
    cookingPots: {},             // "x,y,z" → { contents, progress, heat, recipe }
    kilns: {},                   // "x,y,z" → { fuel, progress, input, output, type }
    campfires: {},               // "x,y,z" → { fuel, heat, slots[] }
    furnaces: {},                // "x,y,z" → { input, fuel, progress, output }
    // ── Chests (authoritative — client localStorage is only a fallback cache) ──
    chests: new Map(),           // "x,y,z" → [{type,count,spoilTimer}, ...]
    // ── Dropped items ──
    droppedItems: new Map(),     // itemId → { type, x, y, z, count, vx, vy, vz }
    // ── Piles ──
    piles: {},                   // "x,y,z" → { type, count }
    // ── Clay sculptures ──
    clayShapes: {},              // "x,y,z" → { shape, carved }
    // ── Water flow ──
    waterFlow: {},               // "x,y,z" → { level, direction, source }
  };
}

function getOrCreateRoom(key, seed, worldType) {
  if (!rooms.has(key)) {
    rooms.set(key, createRoom(key, seed, worldType));
  }
  return rooms.get(key);
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (_) {}
  }
}

function roomBroadcast(room, msg, excludeId) {
  for (const [pid, p] of room.players) {
    if (pid !== excludeId) send(p.ws, msg);
  }
}

function sendPlayerList(room, targetWs) {
  const players = [];
  for (const [pid, p] of room.players) {
    players.push({ id: pid, name: p.name, skin: p.skin });
  }
  send(targetWs, { type: "player_list", players });
}

function isHost(room, pid) {
  return room.hostId === pid;
}

// ═══════════════════════════════════════════════════════════════════════════════
// VALIDATION + RATE LIMITING
// ═══════════════════════════════════════════════════════════════════════════════
// The server previously trusted almost everything a socket sent it as long
// as isHost() passed — no bounds on coordinates, no checks on item/type ids
// or counts, and no rate limiting. That's fine between friends, but it means
// any client (host or not) could forge a block_place a million blocks away,
// an admin_give for 99999 of any item id, or spam any handler as fast as it
// can serialize JSON. The helpers below give every handler a cheap, uniform
// way to reject obviously-forged input before it touches room state.

// "Infinite" world, but nothing legitimate ever sends a coordinate outside
// this range — reject non-finite/absurd values so they can't corrupt world
// state, bloat memory via Map/object keys, or crash other clients trying to
// render them.
const COORD_LIMIT = 2_000_000;
const Y_MIN = -1024;
const Y_MAX = 1024;

function isFiniteNum(n) {
  return typeof n === "number" && Number.isFinite(n);
}

function isValidCoord(x, y, z) {
  return (
    isFiniteNum(x) && isFiniteNum(y) && isFiniteNum(z) &&
    Math.abs(x) <= COORD_LIMIT && Math.abs(z) <= COORD_LIMIT &&
    y >= Y_MIN && y <= Y_MAX
  );
}

// Block/item "type" ids index into the client's own registry, which this
// server doesn't load, so it can't confirm an id is a *real* item — but it
// can reject what actually gets abused: non-integers, negatives, NaN, or
// wildly out-of-range numbers used to smuggle bad data through.
const TYPE_ID_MAX = 4096;
function isValidTypeId(t) {
  return Number.isInteger(t) && t >= 0 && t <= TYPE_ID_MAX;
}

function isValidCount(n, max = 9999) {
  return Number.isInteger(n) && n >= 0 && n <= max;
}

function clampNum(n, lo, hi, fallback) {
  if (!isFiniteNum(n)) return fallback;
  return Math.min(hi, Math.max(lo, n));
}

// ── Per-connection rate limiting (token bucket per message "class") ────────
// Keyed by class rather than exact type, so e.g. spamming block_break can't
// starve chat's budget and vice versa. Refills continuously; bursts capped.
const RATE_LIMITS = {
  movement: { burst: 40, perSec: 25 },  // player_update — legitimately frequent
  block: { burst: 40, perSec: 20 },     // block_break / block_place / block_batch
  chat: { burst: 5, perSec: 1 },
  admin: { burst: 10, perSec: 3 },      // admin_* and player_cmd (host-only anyway)
  action: { burst: 60, perSec: 30 },    // pvp, arrows, drops, animals, ropes, boats...
  state: { burst: 30, perSec: 15 },     // fire/campfire/kiln/crucible/water/etc. state syncs
};

// Every message type is bucketed into one of the classes above. Anything not
// listed (e.g. "join", which is handled before this runs) is left unlimited
// at this layer.
const MSG_CLASS = {
  player_update: "movement",
  block_break: "block", block_place: "block", block_batch: "block",
  chat: "chat",
  map_marker: "action", tide_sync: "state",
  pvp_hit: "action", arrow_shoot: "action", bow_state: "action",
  chest_open: "action", chest_update: "action",
  weather_toggle: "admin", outfit_settings: "admin",
  fire_start: "state", fire_spread: "state", fire_extinguish: "state", fire_state: "state",
  campfire_state: "state",
  animal_spawn: "action", animal_update: "action", animal_kill: "action",
  animal_hurt: "action", animal_tame: "action", animal_despawn: "action",
  rope_place: "action", rope_lash: "action", rope_break: "action", rope_bridge: "action",
  crucible_state: "state", crucible_result: "state", bloomery_result: "state",
  player_stats: "action", anvil_state: "state", anvil_result: "state", hammer_hit: "action",
  cooking_pot_state: "state", cooking_pot_result: "state",
  kiln_state: "state", kiln_result: "state", furnace_state: "state",
  drop_item_spawn: "action", drop_item_pickup: "action",
  drop_item_update: "action", drop_item_despawn: "action",
  pile_sync: "state", pile_consume: "state",
  boat_place: "action", boat_break: "action", boat_mount: "action",
  boat_dismount: "action", boat_update: "state",
  water_flow: "state", water_drain: "state",
  clay_shape_update: "state",
  health_update: "action", death_broadcast: "action",
  entity_sync: "action", entity_despawn: "action",
  admin_kick: "admin", admin_summon: "admin", admin_forcewalk: "admin",
  admin_stopwalk: "admin", admin_lock: "admin", admin_weather: "admin",
  tornado_spawn: "admin", admin_broadcast: "admin", admin_gamemode: "admin",
  admin_teleport_all: "admin", admin_give: "admin", admin_heal: "admin",
  player_cmd: "admin",
};

function makeLimiterState() {
  const buckets = {};
  for (const k in RATE_LIMITS) buckets[k] = { tokens: RATE_LIMITS[k].burst, last: Date.now() };
  return buckets;
}

function allowRate(bucketState, cls) {
  const cfg = RATE_LIMITS[cls];
  const b = bucketState && bucketState[cls];
  if (!cfg || !b) return true;
  const now = Date.now();
  const elapsed = (now - b.last) / 1000;
  b.last = now;
  b.tokens = Math.min(cfg.burst, b.tokens + elapsed * cfg.perSec);
  if (b.tokens < 1) return false;
  b.tokens -= 1;
  return true;
}

/** Build a snapshot of all room state for a new joiner */
function buildJoinSnapshot(room) {
  return {
    mods: room.blockMods,
    fires: room.fires,
    animals: Object.fromEntries(room.animals),
    ropes: Object.fromEntries(room.ropes),
    boats: Object.fromEntries(room.boats),
    crucibles: room.crucibles,
    anvils: room.anvils,
    cookingPots: room.cookingPots,
    kilns: room.kilns,
    campfires: room.campfires,
    furnaces: room.furnaces,
    droppedItems: Object.fromEntries(room.droppedItems),
    piles: room.piles,
    clayShapes: room.clayShapes,
    waterFlow: room.waterFlow,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PLAYER CLEANUP
// ═══════════════════════════════════════════════════════════════════════════════

function removePlayer(room, pid) {
  const p = room.players.get(pid);
  if (!p) return;
  room.players.delete(pid);
  roomBroadcast(room, { type: "player_leave", id: pid, name: p.name });

  // Auto-promote host
  if (room.hostId === pid) {
    room.hostId = null;
    const entries = [...room.players.entries()].sort(
      (a, b) => a[1].joinTime - b[1].joinTime
    );
    if (entries.length > 0) {
      const [newHostId, newHost] = entries[0];
      room.hostId = newHostId;
      send(newHost.ws, {
        type: "host_assigned",
        msg: "You are now the host (previous host left).",
      });
      roomBroadcast(room, {
        type: "chat", pid: "server", name: "[SERVER]", skin: "#00aaff",
        text: `${newHost.name} is now the host.`,
      });
    }
  }

  // Clean up empty rooms after delay
  if (room.players.size === 0) {
    setTimeout(() => {
      if (rooms.has(room.key) && rooms.get(room.key).players.size === 0) {
        rooms.delete(room.key);
      }
    }, 600_000);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTTP SERVER — Serves the game HTML + static files
// ═══════════════════════════════════════════════════════════════════════════════

const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    let totalPlayers = 0;
    for (const [, room] of rooms) totalPlayers += room.players.size;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size, players: totalPlayers }));
    return;
  }
  if (req.method === "GET") {
    let filePath = req.url === "/" ? INDEX_HTML : path.join(__dirname, req.url);
    if (!filePath.startsWith(__dirname)) {
      res.writeHead(403); res.end("Forbidden"); return;
    }
    fs.readFile(filePath, (err, data) => {
      if (err) {
        fs.readFile(INDEX_HTML, (err2, data2) => {
          if (err2) { res.writeHead(500); res.end("Server error"); }
          else { res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" }); res.end(data2); }
        });
      } else {
        const ext = path.extname(filePath).toLowerCase();
        const mimeMap = {
          ".html": "text/html; charset=utf-8", ".js": "application/javascript; charset=utf-8",
          ".css": "text/css; charset=utf-8", ".json": "application/json; charset=utf-8",
          ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
          ".gif": "image/gif", ".svg": "image/svg+xml", ".ico": "image/x-icon",
          ".wav": "audio/wav", ".mp3": "audio/mpeg", ".ogg": "audio/ogg",
          ".wasm": "application/wasm", ".mp4": "video/mp4", ".webm": "video/webm",
        };
        res.writeHead(200, { "Content-Type": mimeMap[ext] || "application/octet-stream" });
        res.end(data);
      }
    });
  } else {
    res.writeHead(405); res.end("Method not allowed");
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// WEBSOCKET SERVER — Complete message router
// ═══════════════════════════════════════════════════════════════════════════════

const wss = new WebSocket.Server({ server: httpServer });

const HEARTBEAT_INTERVAL = 30000; // 30s
const MAX_MSG_BYTES = 64 * 1024; // block_batch/clay/chest payloads are the largest legit messages; well above them

wss.on("connection", (ws) => {
  let currentRoom = null;
  let playerId = null;
  const limiter = makeLimiterState();

  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    if (raw.length > MAX_MSG_BYTES) return; // drop silently — no point acking an oversized/abusive payload

    let msg;
    try { msg = JSON.parse(raw.toString("utf8")); }
    catch (_) { send(ws, { type: "error", msg: "Invalid JSON" }); return; }
    if (!msg || typeof msg.type !== "string") return;

    // ─────────────────────────────────────────────────────────────────────
    // JOIN
    // ─────────────────────────────────────────────────────────────────────
    if (msg.type === "join") {
      // Basic shape checks on identity fields — these become Map keys and
      // get echoed to every other client, so garbage/oversized values here
      // are a memory and UI-corruption vector even before we get to the
      // host-hijack issue below.
      if (typeof msg.id !== "string" || msg.id.length < 4 || msg.id.length > 64) {
        send(ws, { type: "error", msg: "Invalid session id." });
        ws.close(); return;
      }
      playerId = msg.id;
      const roomKey = String(msg.room || `${msg.seed || 0}-${msg.worldType || "plains"}`).slice(0, 128);
      const room = getOrCreateRoom(roomKey, msg.seed, msg.worldType);

      if (room.locked) {
        send(ws, { type: "error", msg: "Room is locked. No new joins allowed." });
        ws.close(); return;
      }

      // SECURITY: a join whose id is already active in this room is never a
      // legitimate reconnect — the client always generates a fresh random
      // id on every connection attempt (see connectMultiplayer() /
      // playerId = Math.random()... in the client). The old behavior here
      // kicked the existing connection and let the new socket take over its
      // id, which meant anyone who learned another player's id — trivial,
      // since player_list broadcasts it to the whole room — could hijack
      // their session. Hijacking the host's id was the dangerous case: the
      // attacker inherits room.hostId === (that id) for free and can then
      // admin_give itself anything, teleport everyone, kick players, etc.
      // Reject the collision instead of honoring it.
      if (room.players.has(playerId)) {
        send(ws, { type: "error", msg: "That session id is already in use in this room." });
        ws.close(); return;
      }

      // Host assignment
      const wantHost = !!msg.wantHost;
      if ((wantHost || room.players.size === 0) && room.hostId === null) {
        room.hostId = playerId;
        send(ws, { type: "host_assigned", msg: "You are the host of this room." });
      }

      // Store player
      room.players.set(playerId, {
        ws,
        name: String(msg.name || "Player").slice(0, 24),
        skin: typeof msg.skin === "string" ? msg.skin.slice(0, 4000) : null,
        joinTime: Date.now(), lastPos: null, health: 100,
      });
      currentRoom = room;

      // Send full join_ack with ALL world state
      const snap = buildJoinSnapshot(room);
      send(ws, {
        type: "join_ack",
        playerCount: room.players.size,
        mods: snap.mods,
        fires: snap.fires,
        animals: snap.animals,
        ropes: snap.ropes,
        boats: snap.boats,
        crucibles: snap.crucibles,
        anvils: snap.anvils,
        cookingPots: snap.cookingPots,
        kilns: snap.kilns,
        campfires: snap.campfires,
        furnaces: snap.furnaces,
        droppedItems: snap.droppedItems,
        piles: snap.piles,
        clayShapes: snap.clayShapes,
        waterFlow: snap.waterFlow,
      });

      // Notify others
      roomBroadcast(room, {
        type: "player_join", id: playerId, name: msg.name || "Player", skin: msg.skin,
      }, playerId);
      sendPlayerList(room, ws);

      // Push current settings to new player
      if (room.outfitSettings) send(ws, { type: "outfit_settings", settings: room.outfitSettings });
      if (!room.weatherEnabled) send(ws, { type: "weather_toggle", enabled: false });
      return;
    }

    // All further messages require room + player
    if (!currentRoom || !playerId) {
      send(ws, { type: "error", msg: "Not joined a room yet." }); return;
    }
    const room = currentRoom;
    const player = room.players.get(playerId);
    const pname = player ? player.name : "Player";

    // SECURITY: uniform rate limit, applied by message class, before any
    // handler runs. Drops silently rather than erroring — an error reply to
    // a flood just doubles the outbound traffic without helping a
    // legitimate client (which shouldn't be hitting these limits anyway).
    const _cls = MSG_CLASS[msg.type];
    if (_cls && !allowRate(limiter, _cls)) return;

    // ═════════════════════════════════════════════════════════════════════
    // CORE SYNC
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "player_update") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      if (player) {
        player.lastPos = { x: msg.x, y: msg.y, z: msg.z };
        if (msg.name) player.name = String(msg.name).slice(0, 24);
        if (msg.skin) player.skin = String(msg.skin).slice(0, 4000);
      }
      roomBroadcast(room, { ...msg, id: playerId, name: pname, skin: player?.skin }, playerId);
      return;
    }

    if (msg.type === "chat") {
      if (typeof msg.text !== "string" || !msg.text.trim()) return;
      roomBroadcast(room, {
        type: "chat", pid: playerId, name: msg.name || pname,
        skin: msg.skin || (player ? player.skin : "#fff"), text: msg.text.slice(0, 300),
      });
      return;
    }

    if (msg.type === "block_break") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.blockMods[key] = null;
      room.chests.delete(key); // drop stale chest contents if this was a chest
      roomBroadcast(room, { type: "block_break", pid: playerId, x: msg.x, y: msg.y, z: msg.z }, playerId);
      return;
    }

    if (msg.type === "block_place") {
      if (!isValidCoord(msg.x, msg.y, msg.z) || !isValidTypeId(msg.t)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.blockMods[key] = msg.t;
      roomBroadcast(room, { type: "block_place", pid: playerId, x: msg.x, y: msg.y, z: msg.z, t: msg.t }, playerId);
      return;
    }

    if (msg.type === "block_batch") {
      // Cap batch size (a legit batch is a few dozen blocks at most — a
      // multi-plot or structure paste) and validate every op inside it so
      // one forged entry can't corrupt the whole batch.
      if (Array.isArray(msg.ops) && msg.ops.length <= 512) {
        const validOps = [];
        for (const op of msg.ops) {
          if (!op || !isValidCoord(op.x, op.y, op.z)) continue;
          if (op.op === "break") {
            const key = `${op.x},${op.y},${op.z}`;
            room.blockMods[key] = null;
            room.chests.delete(key);
            validOps.push(op);
          } else if (isValidTypeId(op.t)) {
            const key = `${op.x},${op.y},${op.z}`;
            room.blockMods[key] = op.t;
            validOps.push(op);
          }
        }
        roomBroadcast(room, { type: "block_batch", pid: playerId, ops: validOps }, playerId);
      }
      return;
    }

    if (msg.type === "map_marker") {
      if (!isFiniteNum(msg.x) || !isFiniteNum(msg.z) || Math.abs(msg.x) > COORD_LIMIT || Math.abs(msg.z) > COORD_LIMIT) return;
      const label = typeof msg.label === "string" ? msg.label.slice(0, 64) : msg.label;
      roomBroadcast(room, { type: "map_marker", pid: playerId, name: msg.name || pname, x: msg.x, z: msg.z, label, color: msg.color }, playerId);
      return;
    }

    if (msg.type === "tide_sync") {
      roomBroadcast(room, { type: "tide_sync", start: msg.start, wall: msg.wall, pid: playerId }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // PVP COMBAT
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "pvp_hit") {
      // Damage is entirely client-reported (no server-side combat sim), so
      // this can't stop a modified client from lying about a hit — but it
      // caps the blast radius: without this, "damage: 999999" one-shots
      // anyone regardless of weapon. Clamp to something above the game's
      // legitimate max hit (heaviest weapon + full-strength crit) instead.
      const damage = clampNum(msg.damage, 0, 60, 15);
      const target = room.players.get(msg.target);
      if (target) {
        send(target.ws, {
          type: "pvp_hit", attacker: playerId, attackerName: msg.attackerName || pname,
          target: msg.target, damage, weapon: msg.weapon || "unknown",
        });
        // Track health server-side for kill feed
        if (target.health !== undefined) {
          target.health = Math.max(0, (target.health || 100) - damage);
          if (target.health <= 0) {
            // Auto-generate kill feed
            roomBroadcast(room, {
              type: "pvp_kill", attacker: playerId, attackerName: msg.attackerName || pname,
              victim: msg.target, victimName: target.name, weapon: msg.weapon || "unknown",
            });
            // Reset victim health (they'll respawn client-side)
            target.health = 100;
          }
        }
      }
      return;
    }

    if (msg.type === "arrow_shoot") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      roomBroadcast(room, {
        type: "arrow_shoot", pid: playerId, name: pname,
        x: msg.x, y: msg.y, z: msg.z, dx: msg.dx, dy: msg.dy, dz: msg.dz,
      }, playerId);
      return;
    }

    if (msg.type === "bow_state") {
      roomBroadcast(room, { type: "bow_state", pid: playerId, nocked: !!msg.nocked }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // CHEST SYNC
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "chest_open") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const cKey = `${msg.x},${msg.y},${msg.z}`;
      const authoritative = room.chests.get(cKey);
      // Reply to the opener ONLY with whatever the server currently thinks
      // this chest holds — overwrites their local cache so a chest another
      // (now-disconnected) player modified doesn't show stale contents.
      // pid:"server" so the client's own-echo filter doesn't skip it.
      if (authoritative) {
        send(ws, { type: "chest_update", pid: "server", x: msg.x, y: msg.y, z: msg.z, contents: authoritative });
      }
      roomBroadcast(room, { type: "chest_open", pid: playerId, x: msg.x, y: msg.y, z: msg.z }, playerId);
      return;
    }

    if (msg.type === "chest_update") {
      // Basic sanity check so a malformed/hostile message can't crash the
      // server or bloat memory with a huge payload.
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      if (!Array.isArray(msg.contents) || msg.contents.length > 27) return;
      for (const slot of msg.contents) {
        if (slot == null) continue; // empty slot
        if (typeof slot !== "object" || !isValidTypeId(slot.type) || !isValidCount(slot.count)) return;
      }
      const cKey = `${msg.x},${msg.y},${msg.z}`;
      room.chests.set(cKey, msg.contents);
      roomBroadcast(room, { type: "chest_update", pid: playerId, x: msg.x, y: msg.y, z: msg.z, contents: msg.contents }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // WEATHER
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "weather_toggle") {
      room.weatherEnabled = !!msg.enabled;
      roomBroadcast(room, { type: "weather_toggle", enabled: room.weatherEnabled });
      return;
    }

    if (msg.type === "outfit_settings") {
      room.outfitSettings = msg.settings;
      roomBroadcast(room, { type: "outfit_settings", settings: msg.settings });
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // FIRE / CAMPFIRE SYNC
    // Tracks: fire starts, spread, extinguish, campfire fuel/heat state
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "fire_start") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.fires[key] = { x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel ?? 100, heat: msg.heat ?? 1, burning: true };
      roomBroadcast(room, { type: "fire_start", pid: playerId, x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel, heat: msg.heat }, playerId);
      return;
    }

    if (msg.type === "fire_spread") {
      // Fire spreading to new block(s). Cap the batch and validate each
      // target — an attacker could otherwise use this to plant thousands of
      // "burning" entries anywhere in the world in one message.
      const rawTargets = Array.isArray(msg.targets) ? msg.targets : [{ x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel }];
      if (rawTargets.length > 64) return;
      const targets = [];
      for (const t of rawTargets) {
        if (!t || !isValidCoord(t.x, t.y, t.z)) continue;
        const key = `${t.x},${t.y},${t.z}`;
        room.fires[key] = { x: t.x, y: t.y, z: t.z, fuel: t.fuel ?? 60, heat: 1, burning: true };
        targets.push(t);
      }
      if (targets.length) roomBroadcast(room, { type: "fire_spread", pid: playerId, targets }, playerId);
      return;
    }

    if (msg.type === "fire_extinguish") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      delete room.fires[key];
      roomBroadcast(room, { type: "fire_extinguish", pid: playerId, x: msg.x, y: msg.y, z: msg.z }, playerId);
      return;
    }

    if (msg.type === "fire_state") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      // Periodic fire state update (fuel consumption, heat level)
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (room.fires[key]) {
        Object.assign(room.fires[key], { fuel: msg.fuel, heat: msg.heat, burning: msg.burning });
      } else {
        room.fires[key] = { x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel, heat: msg.heat, burning: msg.burning };
      }
      roomBroadcast(room, { type: "fire_state", pid: playerId, x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel, heat: msg.heat, burning: msg.burning }, playerId);
      return;
    }

    if (msg.type === "campfire_state") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.campfires[key] = { x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel, heat: msg.heat, slots: msg.slots };
      roomBroadcast(room, { type: "campfire_state", pid: playerId, x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel, heat: msg.heat, slots: msg.slots }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // ANIMAL SYNC
    // Host spawns animals, all clients see them; kills relayed.
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "animal_spawn") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const aid = msg.animalId || `a_${msg.x}_${msg.z}_${Date.now()}`;
      room.animals.set(aid, {
        type: msg.animalType, x: msg.x, y: msg.y, z: msg.z,
        hp: clampNum(msg.hp, 0, 1000, 100), name: msg.name || "", tamed: msg.tamed || false,
      });
      roomBroadcast(room, { type: "animal_spawn", animalId: aid, animalType: msg.animalType, x: msg.x, y: msg.y, z: msg.z, hp: msg.hp, name: msg.name, tamed: msg.tamed, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "animal_update") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      if (room.animals.has(msg.animalId)) {
        const a = room.animals.get(msg.animalId);
        a.x = msg.x; a.y = msg.y; a.z = msg.z;
        if (msg.hp !== undefined) a.hp = clampNum(msg.hp, 0, 1000, a.hp);
        if (msg.ry !== undefined) a.ry = msg.ry;
        if (msg.state !== undefined) a.state = msg.state;
        if (msg.tamed !== undefined) a.tamed = msg.tamed;
      }
      roomBroadcast(room, { type: "animal_update", animalId: msg.animalId, x: msg.x, y: msg.y, z: msg.z, ry: msg.ry, hp: msg.hp, state: msg.state, tamed: msg.tamed }, playerId);
      return;
    }

    if (msg.type === "animal_kill") {
      room.animals.delete(msg.animalId);
      roomBroadcast(room, { type: "animal_kill", animalId: msg.animalId, killer: pname, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "animal_hurt") {
      const damage = clampNum(msg.damage, 0, 200, 10);
      if (room.animals.has(msg.animalId)) {
        room.animals.get(msg.animalId).hp = Math.max(0, (room.animals.get(msg.animalId).hp || 100) - damage);
      }
      roomBroadcast(room, { type: "animal_hurt", animalId: msg.animalId, damage, attacker: pname, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "animal_tame") {
      if (room.animals.has(msg.animalId)) {
        room.animals.get(msg.animalId).tamed = true;
      }
      roomBroadcast(room, { type: "animal_tame", animalId: msg.animalId, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "animal_despawn") {
      room.animals.delete(msg.animalId);
      roomBroadcast(room, { type: "animal_despawn", animalId: msg.animalId }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // ROPE / LASHING SYNC
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "rope_place") {
      if (!Array.isArray(msg.points) || msg.points.length > 128) return;
      const rid = msg.ropeId || `r_${Date.now()}`;
      room.ropes.set(rid, { points: msg.points, lashed: false, broken: false });
      roomBroadcast(room, { type: "rope_place", ropeId: rid, points: msg.points, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "rope_lash") {
      if (!Array.isArray(msg.points) || msg.points.length > 128) return;
      if (room.ropes.has(msg.ropeId)) {
        room.ropes.get(msg.ropeId).lashed = true;
        room.ropes.get(msg.ropeId).points = msg.points;
      }
      roomBroadcast(room, { type: "rope_lash", ropeId: msg.ropeId, points: msg.points, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "rope_break") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      if (room.ropes.has(msg.ropeId)) {
        room.ropes.get(msg.ropeId).broken = true;
      }
      roomBroadcast(room, { type: "rope_break", ropeId: msg.ropeId, x: msg.x, y: msg.y, z: msg.z, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "rope_bridge") {
      if (!Array.isArray(msg.points) || msg.points.length > 128) return;
      const rid = msg.ropeId || `rb_${Date.now()}`;
      room.ropes.set(rid, { points: msg.points, lashed: true, broken: false, isBridge: true });
      roomBroadcast(room, { type: "rope_bridge", ropeId: rid, points: msg.points, x: msg.x, z: msg.z, pid: playerId }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // FORGE / CRUCIBLE / IRON SMELTING SYNC
    // Crucible: ore + fuel → bloom (heat tracking, progress)
    // Anvil: bloom/ingot shaping (hammer hits, progress)
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "crucible_state") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.crucibles[key] = {
        x: msg.x, y: msg.y, z: msg.z,
        heat: msg.heat, fuel: msg.fuel,
        input: msg.input,          // ore type
        output: msg.output,        // null or "bloom"
        progress: msg.progress,    // 0-100
        smelting: msg.smelting,    // boolean
      };
      roomBroadcast(room, { type: "crucible_state", pid: playerId, x: msg.x, y: msg.y, z: msg.z, heat: msg.heat, fuel: msg.fuel, input: msg.input, output: msg.output, progress: msg.progress, smelting: msg.smelting }, playerId);
      return;
    }

    if (msg.type === "crucible_result") {
      // Smelting complete → broadcast the result item
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (room.crucibles[key]) {
        room.crucibles[key].output = msg.output;
        room.crucibles[key].progress = 100;
        room.crucibles[key].smelting = false;
      }
      roomBroadcast(room, { type: "crucible_result", pid: playerId, x: msg.x, y: msg.y, z: msg.z, output: msg.output, outputType: msg.outputType }, playerId);
      return;
    }

    if (msg.type === "bloomery_result") {
      // Bloomery smelting complete → broadcast the hot-bloom event
      roomBroadcast(room, { type: "bloomery_result", pid: playerId, x: msg.x, y: msg.y, z: msg.z, output: msg.output, outputType: msg.outputType, hot: msg.hot }, playerId);
      return;
    }

    if (msg.type === "player_stats") {
      roomBroadcast(room, { type: "player_stats", pid: playerId, stats: msg.stats }, playerId);
      return;
    }

    if (msg.type === "anvil_state") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.anvils[key] = {
        x: msg.x, y: msg.y, z: msg.z,
        item: msg.item,              // what's being forged
        progress: msg.progress,      // 0-100
        forgeType: msg.forgeType,    // "pickaxe_head", "knife_head", "axe_head", "ingot"
        hits: msg.hits || 0,
      };
      roomBroadcast(room, { type: "anvil_state", pid: playerId, x: msg.x, y: msg.y, z: msg.z, item: msg.item, progress: msg.progress, forgeType: msg.forgeType, hits: msg.hits }, playerId);
      return;
    }

    if (msg.type === "anvil_result") {
      // Forging complete
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (room.anvils[key]) {
        room.anvils[key].progress = 100;
      }
      roomBroadcast(room, { type: "anvil_result", pid: playerId, x: msg.x, y: msg.y, z: msg.z, result: msg.result, resultType: msg.resultType }, playerId);
      return;
    }

    if (msg.type === "hammer_hit") {
      // Relay hammer hit on anvil for visual/audio sync
      roomBroadcast(room, { type: "hammer_hit", pid: playerId, x: msg.x, y: msg.y, z: msg.z }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // COOKING SYSTEM SYNC
    // Cooking pot, kiln (clay/charcoal), furnace
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "cooking_pot_state") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.cookingPots[key] = {
        x: msg.x, y: msg.y, z: msg.z,
        contents: msg.contents,    // array of { type, count }
        progress: msg.progress,    // 0-100
        heat: msg.heat,            // boolean or heat level
        recipe: msg.recipe,        // detected recipe name or null
        waterLevel: msg.waterLevel,
      };
      roomBroadcast(room, { type: "cooking_pot_state", pid: playerId, x: msg.x, y: msg.y, z: msg.z, contents: msg.contents, progress: msg.progress, heat: msg.heat, recipe: msg.recipe, waterLevel: msg.waterLevel }, playerId);
      return;
    }

    if (msg.type === "cooking_pot_result") {
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (room.cookingPots[key]) {
        room.cookingPots[key].progress = 100;
        room.cookingPots[key].recipe = msg.recipe;
      }
      roomBroadcast(room, { type: "cooking_pot_result", pid: playerId, x: msg.x, y: msg.y, z: msg.z, recipe: msg.recipe, outputs: msg.outputs }, playerId);
      return;
    }

    if (msg.type === "kiln_state") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.kilns[key] = {
        x: msg.x, y: msg.y, z: msg.z,
        fuel: msg.fuel,
        progress: msg.progress,
        input: msg.input,          // "clay" or "wood"
        output: msg.output,        // null or result type
        burning: msg.burning,
        kilnType: msg.kilnType,    // "clay" or "charcoal"
      };
      roomBroadcast(room, { type: "kiln_state", pid: playerId, x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel, progress: msg.progress, input: msg.input, output: msg.output, burning: msg.burning, kilnType: msg.kilnType }, playerId);
      return;
    }

    if (msg.type === "kiln_result") {
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (room.kilns[key]) {
        room.kilns[key].output = msg.output;
        room.kilns[key].progress = 100;
        room.kilns[key].burning = false;
      }
      roomBroadcast(room, { type: "kiln_result", pid: playerId, x: msg.x, y: msg.y, z: msg.z, output: msg.output, outputType: msg.outputType }, playerId);
      return;
    }

    if (msg.type === "furnace_state") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.furnaces[key] = {
        x: msg.x, y: msg.y, z: msg.z,
        input: msg.input,
        fuel: msg.fuel,
        progress: msg.progress,
        output: msg.output,
        burning: msg.burning,
      };
      roomBroadcast(room, { type: "furnace_state", pid: playerId, x: msg.x, y: msg.y, z: msg.z, input: msg.input, fuel: msg.fuel, progress: msg.progress, output: msg.output, burning: msg.burning }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // DROPPED ITEMS + PILES
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "drop_item_spawn") {
      if (!isValidCoord(msg.x, msg.y, msg.z) || !isValidTypeId(msg.itemType) || !isValidCount(msg.count ?? 1)) return;
      const itemId = msg.itemId || `di_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
      room.droppedItems.set(itemId, {
        type: msg.itemType, x: msg.x, y: msg.y, z: msg.z,
        count: msg.count || 1, vx: msg.vx, vy: msg.vy, vz: msg.vz,
        spoilTimer: msg.spoilTimer || 0,
      });
      roomBroadcast(room, { type: "drop_item_spawn", itemId, itemType: msg.itemType, x: msg.x, y: msg.y, z: msg.z, count: msg.count, vx: msg.vx, vy: msg.vy, vz: msg.vz, spoilTimer: msg.spoilTimer, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "drop_item_pickup") {
      room.droppedItems.delete(msg.itemId);
      roomBroadcast(room, { type: "drop_item_pickup", itemId: msg.itemId, pid: playerId, name: pname }, playerId);
      return;
    }

    if (msg.type === "drop_item_update") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      if (room.droppedItems.has(msg.itemId)) {
        const di = room.droppedItems.get(msg.itemId);
        di.x = msg.x; di.y = msg.y; di.z = msg.z;
        if (msg.vx !== undefined) di.vx = msg.vx;
        if (msg.vy !== undefined) di.vy = msg.vy;
        if (msg.vz !== undefined) di.vz = msg.vz;
      }
      roomBroadcast(room, { type: "drop_item_update", itemId: msg.itemId, x: msg.x, y: msg.y, z: msg.z, count: msg.count, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "drop_item_despawn") {
      room.droppedItems.delete(msg.itemId);
      roomBroadcast(room, { type: "drop_item_despawn", itemId: msg.itemId }, playerId);
      return;
    }

    if (msg.type === "pile_sync") {
      if (!isValidCoord(msg.x, msg.y, msg.z) || !isValidTypeId(msg.itemType) || !isValidCount(msg.count)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.piles[key] = { type: msg.itemType, count: msg.count, x: msg.x, y: msg.y, z: msg.z };
      roomBroadcast(room, { type: "pile_sync", pid: playerId, x: msg.x, y: msg.y, z: msg.z, itemType: msg.itemType, count: msg.count }, playerId);
      return;
    }

    if (msg.type === "pile_consume") {
      if (!isValidCoord(msg.x, msg.y, msg.z) || !isFiniteNum(msg.remaining)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (room.piles[key]) {
        room.piles[key].count = msg.remaining;
        if (msg.remaining <= 0) delete room.piles[key];
      }
      roomBroadcast(room, { type: "pile_consume", pid: playerId, x: msg.x, y: msg.y, z: msg.z, remaining: msg.remaining }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // BOAT / SHIP SYNC
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "boat_place") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      if (msg.blocks !== undefined && (!Array.isArray(msg.blocks) || msg.blocks.length > 256)) return;
      const bid = msg.boatId || `b_${Date.now()}`;
      room.boats.set(bid, { x: msg.x, y: msg.y, z: msg.z, ry: msg.ry || 0, rider: null, blocks: msg.blocks || [] });
      roomBroadcast(room, { type: "boat_place", boatId: bid, x: msg.x, y: msg.y, z: msg.z, ry: msg.ry, blocks: msg.blocks, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "boat_break") {
      room.boats.delete(msg.boatId);
      roomBroadcast(room, { type: "boat_break", boatId: msg.boatId, pid: playerId }, playerId);
      return;
    }

    if (msg.type === "boat_mount") {
      if (room.boats.has(msg.boatId)) {
        room.boats.get(msg.boatId).rider = playerId;
      }
      roomBroadcast(room, { type: "boat_mount", boatId: msg.boatId, pid: playerId, name: pname }, playerId);
      return;
    }

    if (msg.type === "boat_dismount") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      if (room.boats.has(msg.boatId)) {
        room.boats.get(msg.boatId).rider = null;
      }
      roomBroadcast(room, { type: "boat_dismount", boatId: msg.boatId, pid: playerId, name: pname, x: msg.x, y: msg.y, z: msg.z }, playerId);
      return;
    }

    if (msg.type === "boat_update") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      if (room.boats.has(msg.boatId)) {
        const b = room.boats.get(msg.boatId);
        b.x = msg.x; b.y = msg.y; b.z = msg.z; b.ry = msg.ry;
      }
      roomBroadcast(room, { type: "boat_update", boatId: msg.boatId, x: msg.x, y: msg.y, z: msg.z, ry: msg.ry, vx: msg.vx, vy: msg.vy, vz: msg.vz, pid: playerId }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // FLOWING WATER SYNC
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "water_flow") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.waterFlow[key] = { x: msg.x, y: msg.y, z: msg.z, level: msg.level, direction: msg.direction, source: msg.source };
      roomBroadcast(room, { type: "water_flow", pid: playerId, x: msg.x, y: msg.y, z: msg.z, level: msg.level, direction: msg.direction, source: msg.source }, playerId);
      return;
    }

    if (msg.type === "water_drain") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      delete room.waterFlow[key];
      roomBroadcast(room, { type: "water_drain", pid: playerId, x: msg.x, y: msg.y, z: msg.z }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // CLAY SCULPTING SYNC
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "clay_shape_update") {
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      // vertices/faces come from free-hand carving — cap them generously
      // rather than trying to bound "a reasonable sculpture."
      if (Array.isArray(msg.vertices) && msg.vertices.length > 5000) return;
      if (Array.isArray(msg.faces) && msg.faces.length > 5000) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      room.clayShapes[key] = { shape: msg.shape, carved: msg.carved, vertices: msg.vertices, faces: msg.faces };
      roomBroadcast(room, { type: "clay_shape_update", pid: playerId, x: msg.x, y: msg.y, z: msg.z, shape: msg.shape, carved: msg.carved, vertices: msg.vertices, faces: msg.faces }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // PLAYER STATE (health, death, hunger sync for UI display)
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "health_update") {
      const health = clampNum(msg.health, 0, 100, player ? player.health : 100);
      if (player) player.health = health;
      roomBroadcast(room, { type: "health_update", pid: playerId, health }, playerId);
      return;
    }

    if (msg.type === "death_broadcast") {
      if (player) player.health = 100; // Reset for respawn
      roomBroadcast(room, { type: "death_broadcast", pid: playerId, name: pname, cause: msg.cause, killer: msg.killer }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // GENERIC ENTITY SYNC (catch-all for any entity type)
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "entity_sync") {
      roomBroadcast(room, { type: "entity_sync", pid: playerId, entityId: msg.entityId, entityType: msg.entityType, x: msg.x, y: msg.y, z: msg.z, data: msg.data }, playerId);
      return;
    }

    if (msg.type === "entity_despawn") {
      roomBroadcast(room, { type: "entity_despawn", pid: playerId, entityId: msg.entityId, entityType: msg.entityType }, playerId);
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // ADMIN COMMANDS (host-only)
    // ═════════════════════════════════════════════════════════════════════

    if (msg.type === "admin_kick") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can kick players." }); return; }
      const target = room.players.get(msg.target);
      if (target) {
        send(target.ws, { type: "kicked", reason: "Kicked by host" });
        try { target.ws.close(); } catch (_) {}
        roomBroadcast(room, { type: "chat", pid: "server", name: "[SERVER]", skin: "#00aaff", text: `${target.name} was kicked by the host.` });
      }
      return;
    }

    if (msg.type === "admin_summon") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can summon players." }); return; }
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const target = room.players.get(msg.target);
      if (target) send(target.ws, { type: "admin_summon", x: msg.x, y: msg.y, z: msg.z });
      return;
    }

    if (msg.type === "admin_forcewalk") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can force-walk players." }); return; }
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      const stopDist = clampNum(msg.stopDist, 0, 64, 1);
      const targets = msg.target === "*" ? [...room.players.keys()].filter(id => id !== playerId) : [msg.target];
      for (const tid of targets) {
        const tp = room.players.get(tid);
        if (tp) send(tp.ws, { type: "admin_forcewalk", target: tid, x: msg.x, y: msg.y, z: msg.z, stopDist, lockControls: !!msg.lockControls, targetName: tp.name });
      }
      return;
    }

    if (msg.type === "admin_stopwalk") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can stop forced walks." }); return; }
      const targets = msg.target === "*" ? [...room.players.keys()] : [msg.target];
      for (const tid of targets) {
        const tp = room.players.get(tid);
        if (tp) send(tp.ws, { type: "admin_stopwalk", target: tid });
      }
      return;
    }

    if (msg.type === "admin_lock") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can lock/unlock the world." }); return; }
      room.locked = !!msg.locked;
      roomBroadcast(room, { type: "chat", pid: "server", name: "[SERVER]", skin: "#00aaff", text: room.locked ? "World locked." : "World unlocked." });
      return;
    }

    if (msg.type === "admin_weather") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can control weather." }); return; }
      roomBroadcast(room, { type: "admin_weather", action: msg.action });
      return;
    }

    if (msg.type === "tornado_spawn") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can spawn tornadoes." }); return; }
      if (!isFiniteNum(msg.x) || !isFiniteNum(msg.z) || Math.abs(msg.x) > COORD_LIMIT || Math.abs(msg.z) > COORD_LIMIT) return;
      const strength = clampNum(msg.strength, 0, 10, 1);
      roomBroadcast(room, { type: "tornado_spawn", x: msg.x, z: msg.z, strength }, playerId);
      return;
    }

    if (msg.type === "admin_broadcast") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can broadcast." }); return; }
      if (typeof msg.text !== "string" || !msg.text.trim()) return;
      roomBroadcast(room, { type: "chat", pid: "server", name: "[SERVER]", skin: "#00aaff", text: msg.text.slice(0, 500) });
      return;
    }

    if (msg.type === "admin_gamemode") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can change gamemode." }); return; }
      roomBroadcast(room, { type: "admin_gamemode", mode: msg.mode, pid: playerId });
      return;
    }

    if (msg.type === "admin_teleport_all") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can teleport all players." }); return; }
      if (!isValidCoord(msg.x, msg.y, msg.z)) return;
      for (const [tid, tp] of room.players) {
        if (tid !== playerId) {
          send(tp.ws, { type: "admin_summon", x: msg.x, y: msg.y, z: msg.z });
        }
      }
      roomBroadcast(room, { type: "chat", pid: "server", name: "[SERVER]", skin: "#00aaff", text: `All players summoned to ${msg.x}, ${msg.y}, ${msg.z}` });
      return;
    }

    if (msg.type === "admin_give") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can give items." }); return; }
      if (!isValidTypeId(msg.itemType) || !isValidCount(msg.count ?? 1, 999)) return;
      const target = room.players.get(msg.target);
      if (target) {
        send(target.ws, { type: "admin_give", itemType: msg.itemType, count: msg.count || 1 });
      }
      return;
    }

    if (msg.type === "admin_heal") {
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can heal." }); return; }
      const targets = msg.target === "*" ? [...room.players.keys()] : [msg.target];
      for (const tid of targets) {
        const tp = room.players.get(tid);
        if (tp) {
          tp.health = 100;
          send(tp.ws, { type: "admin_heal", health: 100 });
        }
      }
      return;
    }

    // ═════════════════════════════════════════════════════════════════════
    // v79: GENERIC PLAYER-TARGETED CHEAT/STATUS COMMAND RELAY
    // ═════════════════════════════════════════════════════════════════════
    // The client's chat commands (/heal, /god, /fly, /noclip, /creative,
    // /speed, /gamemode, /hunger, /thirst, /stamina, /fatigue, /strength,
    // /freeze, /flyspeed, /instabreak, /clearinv, /repair, /kill, /respawn,
    // /tp <player> to <player>, /killall & /healall with a player target,
    // etc.) all funnel through this single message type when the target is
    // a different real player, instead of one bespoke admin_* type each.
    // Applying an effect to yourself never touches the network at all — this
    // only fires when targeting @a or another player's username — so, same
    // as the existing admin_* commands, it's host-only.
    if (msg.type === "player_cmd") {
      const ALLOWED_PLAYER_CMDS = new Set([
        "heal", "god", "fly", "noclip", "creative", "speed", "gamemode",
        "hunger", "thirst", "stamina", "fatigue", "strength", "freeze",
        "flyspeed", "instabreak", "clearinv", "repair", "kill", "respawn",
        "teleport_to", "tag", "label", "boat", "unboat",
      ]);
      if (typeof msg.cheat !== "string" || !ALLOWED_PLAYER_CMDS.has(msg.cheat)) return;
      if (!isHost(room, playerId)) { send(ws, { type: "error", msg: "Only the host can affect other players." }); return; }
      // args is a free-form payload per cheat (e.g. {value: 50}), so we
      // can't validate its shape without duplicating the client's cheat
      // table here — but we can bound it so it can't be used to smuggle a
      // huge payload through an otherwise-innocuous relay.
      const args = (msg.args && typeof msg.args === "object" && !Array.isArray(msg.args)) ? msg.args : {};
      if (JSON.stringify(args).length > 1000) return;
      const targets = msg.target === "*" ? [...room.players.keys()].filter((id) => id !== playerId) : [msg.target];
      for (const tid of targets) {
        const tp = room.players.get(tid);
        if (tp) send(tp.ws, { type: "player_cmd", cheat: msg.cheat, args, from: pname });
      }
      return;
    }

    // ── Unknown message type: ignore (future-proof) ──
  });

  ws.on("close", () => {
    if (currentRoom && playerId) removePlayer(currentRoom, playerId);
  });

  ws.on("error", () => {});
});

// ═══════════════════════════════════════════════════════════════════════════════
// START + GRACEFUL SHUTDOWN
// ═══════════════════════════════════════════════════════════════════════════════

// Detect zombie connections (network drop without a clean close event —
// phone sleep, wifi loss, etc.) and terminate them so rooms/host status
// don't get stuck waiting on a socket that's never coming back.
const heartbeatTimer = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; } // fires 'close' -> removePlayer runs
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL);

httpServer.listen(PORT, () => {
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  World (Infinite) v58 — Complete Multiplayer Server");
  console.log(`  HTTP  : http://localhost:${PORT}`);
  console.log(`  WS    : ws://localhost:${PORT}`);
  console.log("═══════════════════════════════════════════════════════════");
});

function shutdown() {
  console.log("\nShutting down...");
  for (const [, room] of rooms) {
    for (const [pid, p] of room.players) {
      send(p.ws, { type: "kicked", reason: "Server shutting down" });
      try { p.ws.close(); } catch (_) {}
    }
  }
  wss.close(() => {
    httpServer.close(() => { console.log("Server stopped."); process.exit(0); });
  });
  setTimeout(() => process.exit(0), 5000);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
