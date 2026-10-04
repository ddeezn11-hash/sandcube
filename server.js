// ═══════════════════════════════════════════════════════════════════════════
// server.js — World (Infinite) multiplayer server (v155-compatible)
// ═══════════════════════════════════════════════════════════════════════════
//
// A self-contained Node.js WebSocket server that matches the client wire
// protocol used by game_v155_full_fix.html. No external dependencies beyond
// the Node.js standard library + the `ws` package.
//
// Install:
//   npm init -y && npm install ws
//   node server.js            # listens on 0.0.0.0:8080
//   PORT=9000 node server.js   # custom port
//
// Or run with the built-in `--experimental-wasm-bigint` no-deps fallback that
// uses Node's own WebSocket (Node 22+ has global WebSocket). If `ws` is not
// installed and the runtime doesn't have global WebSocket, the server exits
// with a clear install hint.
//
// Protocol summary (reverse-engineered from the client):
//
//  JOIN HANDSHAKE
//    client → server: { type: "join", id, room, name, skin, wantHost,
//                       seed, worldType, clientVersion }
//    server → client: { type: "join_ack", pid, sessionToken, authoritative,
//                       playerCount, fires, droppedItems, mods }
//    server → room:   { type: "player_join", id, name, skin }
//    server → client: { type: "player_list", players: [{id,name,skin}, ...] }
//
//  PER-FRAME STATE
//    client → server: { type: "player_update", id, x, y, z, ry, name, skin,
//                       swimming, backpack }
//    server relays to room (excluding sender)
//
//  BLOCKS
//    client → server: { type: "block_break"|"block_place",
//                       pid, x, y, z, [t], seq, sessionToken, reqId }
//    server relays to room (excluding sender)
//    batches: { type: "block_batch", pid, ops: [{op, x, y, z, [t]}] }
//
//  ENTITIES
//    drop_item_spawn/pickup/update/despawn, fire_start/extinguish,
//    campfire_state, chest_open/update, cooking_pot_state/result
//
//  COMBAT
//    bow_state, bow_draw_start, bow_release, arrow_shoot,
//    pvp_hit { target, attacker, attackerName, damage, zone },
//    pvp_kill { attacker, attackerName, victim, victimName },
//    animal_hurt/kill
//
//  CHAT & SOCIAL
//    chat { text }, map_marker, death_broadcast, player_stats (periodic)
//
//  ADMIN (host-only)
//    player_cmd { target, cheat, args, from },
//    admin_summon/teleport/forcewalk/stopwalk/weather, admin_kick
//
//  HOST MANAGEMENT
//    server → first player in room: { type: "host_assigned", msg }
//    host → server → target: { type: "tide_sync", start, wall }
//
//  ERROR / LIFECYCLE
//    error { code, msg }, kicked { reason }, weather_toggle { enabled }
//
//  CHEST REVISIONS
//    chest_update carries `expectedRevision` and server echoes `revision` so
//    clients can detect stale writes — we keep a per-chest counter.
//
// This server is intentionally permissive — it relays almost everything
// straight through to the room. The only server-authoritative state is:
//   * chest revisions (per-chest counter)
//   * drop item IDs (server mints the id when a client says spawn without one)
//   * PvP damage cap (server clamps damage ≤ 30 per hit and 1 per 250ms per
//     attacker — mirrors the client-side cap added in v155 #10)
//   * host assignment (first player in a room is the host)
// Everything else (block ops, drops, fire state, etc.) is relayed
// unmodified — the server does NOT validate blocks or run game logic.

const PROTOCOL_VERSION = "v175.7.3";
const DEFAULT_PORT = 8080;
const MAX_ROOM_PLAYERS = 32;
const PVP_DAMAGE_CAP = 30;            // v155 #10 — same cap as client
const PVP_RATE_LIMIT_MS = 250;        // v155 #10 — per-attacker rate limit
const MAX_MSG_SIZE = 1024 * 1024;     // 1 MB cap per message
const STALE_SOCKET_MS = 60_000;       // drop sockets that haven't pinged

// ── WebSocket bootstrap ────────────────────────────────────────────────────
let WebSocketImpl = null;
try {
  // Try the `ws` npm package first (most compatible).
  WebSocketImpl = require("ws");
} catch (_) {
  // Fall back to Node's global WebSocket (Node 22+ has it).
  if (typeof WebSocket !== "undefined" && WebSocket.Server) {
    WebSocketImpl = WebSocket;
  } else {
    console.error("\n[server.js] No WebSocket implementation available.");
    console.error("  Install the `ws` package:  npm install ws");
    console.error("  Or upgrade to Node 22+ which has a built-in WebSocket.");
    process.exit(1);
  }
}

const PORT = Number(process.env.PORT) || DEFAULT_PORT;
const wss = new WebSocketImpl.Server({ port: PORT, maxPayload: MAX_MSG_SIZE });

// ── Server state ───────────────────────────────────────────────────────────
// rooms: Map<roomKey, Room>
//   Room: { name, players: Map<pid, Socket>, hostPid, droppedItems: Map, fires: Map, chests: Map, mods: Map }
const rooms = new Map();

function getRoom(name) {
  if (!rooms.has(name)) {
    rooms.set(name, {
      name,
      players: new Map(),
      hostPid: null,
      droppedItems: new Map(),  // id → { type, x, y, z, vx, vy, vz }
      fires: new Map(),         // "x,y,z" → { x, y, z, fuel, heat }
      chests: new Map(),        // "x,y,z" → { revision, contents }
      mods: new Map(),          // "x,y,z" → blockType | null
      // v175: Server-side animal mark registry. Keyed by "x,y,z" (rounded block
      // position of the animal) so different clients — each spawning their own
      // local animals with random sids — can match the same logical animal.
      // Value: { name, type, color, role, ownerPid, ownerName, appliedAt, animalType }
      marks: new Map(),
      tideStart: 0,
      weatherEnabled: true,
    });
  }
  return rooms.get(name);
}

function roomSnapshot(room, excludePid) {
  // Build the join_ack payload: list of existing players, current fires,
  // dropped items, and block mods.
  const players = [];
  for (const [pid, sock] of room.players) {
    if (pid === excludePid) continue;
    players.push({ id: pid, name: sock._wfName || "Player", skin: sock._wfSkin || "" });
  }
  return {
    players,
    fires: Object.fromEntries(room.fires),
    droppedItems: Object.fromEntries(room.droppedItems),
    mods: Object.fromEntries(room.mods),
    // v175: include the marks registry so new joiners see all existing marks
    marks: Object.fromEntries(room.marks),
  };
}

// ── Send helpers ──────────────────────────────────────────────────────────
function safeSend(ws, obj) {
  if (!ws || ws.readyState !== (WebSocketImpl.OPEN || 1)) return false;
  try {
    const text = JSON.stringify(obj);
    if (text.length > MAX_MSG_SIZE) {
      console.warn(`[server.js] dropping oversized message (${text.length} bytes) type=${obj.type}`);
      return false;
    }
    ws.send(text);
    return true;
  } catch (e) {
    console.warn("[server.js] send failed:", e.message);
    return false;
  }
}

function broadcast(room, msg, excludePid) {
  for (const [pid, sock] of room.players) {
    if (pid === excludePid) continue;
    safeSend(sock, msg);
  }
}

// ── Connection lifecycle ───────────────────────────────────────────────────
wss.on("connection", (ws, req) => {
  ws._wfId = null;
  ws._wfName = "Player";
  ws._wfSkin = "";
  ws._wfRoom = null;
  ws._wfSessionToken = null;
  ws._wfPvpHits = new Map(); // attacker → timestamp (for rate limit)
  ws._wfLastSeen = Date.now();
  ws._wfIsHost = false;

  // Heartbeat: drop sockets that haven't sent anything in 60s.
  ws._wfStaleTimer = setInterval(() => {
    if (Date.now() - ws._wfLastSeen > STALE_SOCKET_MS) {
      try { ws.close(); } catch (_) {}
    }
  }, 15_000);

  ws.on("message", (raw) => {
    ws._wfLastSeen = Date.now();
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      console.warn("[server.js] invalid JSON from client:", e.message);
      return;
    }
    if (!msg || typeof msg !== "object" || !msg.type) return;
    handleMessage(ws, msg);
  });

  ws.on("close", () => {
    if (ws._wfStaleTimer) clearInterval(ws._wfStaleTimer);
    handleLeave(ws);
  });

  ws.on("error", (e) => {
    console.warn("[server.js] socket error:", e.message);
  });
});

// ── Message dispatch ───────────────────────────────────────────────────────
function handleMessage(ws, msg) {
  const t = msg.type;

  // JOIN is the first message — it associates the socket with a room.
  if (t === "join") return handleJoin(ws, msg);

  // Everything else requires the socket to have joined first.
  if (!ws._wfId || !ws._wfRoom) {
    return safeSend(ws, { type: "error", code: "NOT_JOINED", msg: "Send a join message first." });
  }

  const room = getRoom(ws._wfRoom);
  if (!room.players.has(ws._wfId)) {
    return safeSend(ws, { type: "error", code: "STALE_SOCKET", msg: "Your socket is no longer in the room. Reconnect." });
  }

  // Session token check on mutation messages (best-effort; client includes it
  // when present). We don't strictly require it, but log mismatches.
  if (msg.sessionToken && ws._wfSessionToken && msg.sessionToken !== ws._wfSessionToken) {
    console.warn(`[server.js] sessionToken mismatch from pid=${ws._wfId}`);
  }

  switch (t) {
    case "player_update":     return relay(room, msg, ws._wfId);
    case "chat":              return handleChat(room, msg, ws);
    case "map_marker":        return relay(room, msg, ws._wfId);
    case "death_broadcast":   return relay(room, msg, ws._wfId);
    case "player_stats":      return relay(room, msg, ws._wfId);

    case "block_break":
    case "block_place":       return handleBlockOp(room, msg, ws);
    case "block_batch":       return handleBlockBatch(room, msg, ws);

    case "drop_item_spawn":   return handleDropSpawn(room, msg, ws);
    case "drop_item_pickup":
    case "drop_item_despawn":
    case "drop_item_update":  return handleDropMutate(room, msg, ws);

    case "fire_start":        return handleFireStart(room, msg, ws);
    case "fire_extinguish":   return handleFireExtinguish(room, msg, ws);
    case "campfire_state":
    case "cooking_pot_state":
    case "cooking_pot_result":
    case "kiln_state":
    case "kiln_result":
    case "crucible_state":
    case "crucible_result":
    case "anvil_state":
    case "anvil_result":
    case "hammer_hit":
    case "clay_shape_update":  return relay(room, msg, ws._wfId);

    case "chest_open":        return handleChestOpen(room, msg, ws);
    case "chest_update":      return handleChestUpdate(room, msg, ws);

    case "bow_state":
    case "bow_draw_start":
    case "bow_release":
    case "arrow_shoot":
    case "arrow_hit_intent":  return relay(room, msg, ws._wfId);

    case "pvp_hit":           return handlePvpHit(room, msg, ws);
    case "pvp_kill":          return relay(room, msg, ws._wfId);
    case "animal_hurt":
    case "animal_kill":       return relay(room, msg, ws._wfId);

    case "tide_sync":         return handleTideSync(room, msg, ws);

    // Admin / host commands — must come from the host.
    case "player_cmd":        return handlePlayerCmd(room, msg, ws);
    case "admin_summon":
    case "admin_teleport":
    case "admin_forcewalk":
    case "admin_stopwalk":
    case "admin_weather":
    case "admin_kick":
    case "admin_gamemode":
    case "admin_broadcast":
    case "admin_give":
    case "admin_heal":        return handleAdmin(room, msg, ws);

    case "weather_toggle":    return handleWeatherToggle(room, msg, ws);
    case "respawn_intent":    return relay(room, msg, ws._wfId);
    case "inventory_sync":   return relay(room, msg, ws._wfId);
    case "forage_result":
    case "berry_bush_harvested":
    case "harvest_grant_request": return handleHarvestGrant(room, msg, ws);
    case "craft_request":     return relay(room, msg, ws._wfId); // server just relays; client owns crafting

    case "micro_voxel":       return relay(room, msg, ws._wfId);

    // ── v175: Animal depth systems ───────────────────────────────────────
    // animal_mark:        { type, pid, animalSid, x, y, z, animalType, mark:{name,type,color,role} | null }
    //   Server stores marks in room.marks keyed by "x,y,z" so any client can
    //   re-apply them on join or on animal spawn. Position is the animal's
    //   rounded block position at mark time.
    // animal_carry:       { type, pid, animalSid, action:"pickup"|"drop", x,y,z }
    // animal_product:     { type, pid, animalSid, product:"milk"|"egg" }
    // animal_teleport:    { type, pid, animalSid, x, y, z }   (host-only)
    // animal_trait_sync:  { type, pid, animalSid, traits, age, mark }   (host → peers)
    case "animal_mark":      return handleAnimalMark(room, msg, ws);
    case "animal_carry":
    case "animal_product":
    case "animal_trait_sync": return relay(room, msg, ws._wfId);

    case "animal_teleport":
      // Only host can teleport animals (anti-grief)
      if (ws._wfId !== room.hostPid) {
        return safeSend(ws, { type: "error", code: "NOT_HOST",
          msg: "Only the host can teleport animals." });
      }
      return relay(room, msg, ws._wfId);

    default:
      // Unknown message types are relayed as-is (forward-compat with future
      // client versions) but logged at debug level.
      console.log(`[server.js] unknown msg type '${t}' from pid=${ws._wfId}, relaying`);
      return relay(room, msg, ws._wfId);
  }
}

// ── JOIN ───────────────────────────────────────────────────────────────────
function handleJoin(ws, msg) {
  const id = String(msg.id || Math.random().toString(36).slice(2, 10));
  const roomName = String(msg.room || "default");
  const name = sanitizeName(String(msg.name || "Player"));
  const skin = msg.skin || "";
  const wantHost = !!msg.wantHost;
  const clientVersion = String(msg.clientVersion || "unknown");

  // Version check — refuse mismatched versions so clients don't get
  // confused by an incompatible peer's messages.
  if (clientVersion !== PROTOCOL_VERSION) {
    console.log(`[server.js] version mismatch: client=${clientVersion} server=${PROTOCOL_VERSION}`);
    safeSend(ws, { type: "error", code: "VERSION_MISMATCH",
      msg: `Client version ${clientVersion} does not match server version ${PROTOCOL_VERSION}. Refresh the page.` });
    // Still allow the join — the client will display the warning and offer to
    // disconnect itself. Strict rejection would lock players out during a
    // rolling upgrade.
  }

  // If this socket was already in a room (rejoin), leave it first.
  if (ws._wfRoom) handleLeave(ws);

  const room = getRoom(roomName);
  if (room.players.size >= MAX_ROOM_PLAYERS) {
    return safeSend(ws, { type: "error", code: "ROOM_FULL",
      msg: `Room '${roomName}' is full (${MAX_ROOM_PLAYERS} players).` });
  }

  // Assign player id + session token.
  ws._wfId = id;
  ws._wfName = name;
  ws._wfSkin = skin;
  ws._wfRoom = roomName;
  ws._wfSessionToken = id + "_" + Date.now().toString(36);

  // First player in the room becomes the host. If the room already has a
  // host and they're still connected, the new player is a guest. If the
  // previous host left, promote the first remaining player.
  if (room.players.size === 0) {
    room.hostPid = id;
    ws._wfIsHost = true;
  } else if (room.hostPid && !room.players.has(room.hostPid)) {
    // Stale host record — promote the new joiner.
    room.hostPid = id;
    ws._wfIsHost = true;
  } else {
    ws._wfIsHost = false;
  }

  room.players.set(id, ws);

  // Build the join_ack with snapshot of current room state.
  const snap = roomSnapshot(room, id);
  safeSend(ws, {
    type: "join_ack",
    pid: id,
    sessionToken: ws._wfSessionToken,
    authoritative: true,
    playerCount: room.players.size,
    fires: snap.fires,
    droppedItems: snap.droppedItems,
    mods: snap.mods,
    // v175: send the marks registry so the new joiner sees all existing marks
    marks: snap.marks,
  });

  // Notify the new player of everyone already in the room.
  safeSend(ws, { type: "player_list", players: snap.players });

  // Announce the new player to the room.
  broadcast(room, { type: "player_join", id, name, skin }, id);

  // Promote to host if applicable.
  if (ws._wfIsHost) {
    safeSend(ws, { type: "host_assigned", msg: "You are the host." });
    // Host owns the tide clock — start it now if not already running.
    if (room.tideStart === 0) room.tideStart = Date.now();
    broadcast(room, { type: "tide_sync", start: room.tideStart, wall: Date.now(), pid: id }, id);
  }

  console.log(`[server.js] +join  room='${roomName}' pid=${id} name='${name}' players=${room.players.size} host=${ws._wfIsHost ? "Y" : "N"}`);
}

// ── LEAVE ──────────────────────────────────────────────────────────────────
function handleLeave(ws) {
  if (!ws._wfId || !ws._wfRoom) return;
  const room = getRoom(ws._wfRoom);
  if (!room || !room.players.has(ws._wfId)) return;

  const wasHost = ws._wfIsHost;
  const leavingId = ws._wfId;
  const leavingName = ws._wfName;
  room.players.delete(leavingId);

  // Announce departure.
  broadcast(room, { type: "player_leave", id: leavingId, name: leavingName });

  // Promote a new host if necessary.
  if (wasHost && room.players.size > 0) {
    const [newHostPid, newHostSock] = room.players.entries().next().value;
    newHostSock._wfIsHost = true;
    room.hostPid = newHostPid;
    safeSend(newHostSock, { type: "host_assigned", msg: "You are now the host (previous host left)." });
    broadcast(room, { type: "tide_sync", start: room.tideStart || Date.now(), wall: Date.now(), pid: newHostPid }, newHostPid);
    console.log(`[server.js] host-promote room='${room.name}' newHost=${newHostPid}`);
  }

  // Empty room cleanup.
  if (room.players.size === 0) {
    rooms.delete(room.name);
    console.log(`[server.js] -empty  room='${room.name}' (all players left)`);
  }

  console.log(`[server.js] -leave  room='${ws._wfRoom}' pid=${leavingId} name='${leavingName}' players=${room.players.size}`);
  ws._wfId = null;
  ws._wfRoom = null;
  ws._wfIsHost = false;
}

// ── Helpers ────────────────────────────────────────────────────────────────
function relay(room, msg, excludePid) {
  // Strip the seq/reqId/sessionToken fields the server doesn't need.
  broadcast(room, msg, excludePid);
}

// ── BLOCK OPS ──────────────────────────────────────────────────────────────
function handleBlockOp(room, msg, ws) {
  // Server records the mod so future joiners see it in their snapshot.
  if (Number.isFinite(msg.x) && Number.isFinite(msg.y) && Number.isFinite(msg.z)) {
    const key = `${msg.x},${msg.y},${msg.z}`;
    if (msg.type === "block_place" && Number.isFinite(msg.t)) {
      room.mods.set(key, msg.t);
    } else if (msg.type === "block_break") {
      room.mods.set(key, null);
    }
    // Cap the mods map to ~10k entries (LRU-ish: just drop oldest when full).
    if (room.mods.size > 10_000) {
      const firstKey = room.mods.keys().next().value;
      room.mods.delete(firstKey);
    }
  }
  relay(room, msg, ws._wfId);
}

function handleBlockBatch(room, msg, ws) {
  if (Array.isArray(msg.ops)) {
    for (const op of msg.ops) {
      if (!op || !Number.isFinite(op.x) || !Number.isFinite(op.y) || !Number.isFinite(op.z)) continue;
      const key = `${op.x},${op.y},${op.z}`;
      if (op.op === "place" && Number.isFinite(op.t)) room.mods.set(key, op.t);
      else if (op.op === "break") room.mods.set(key, null);
    }
    if (room.mods.size > 10_000) {
      const firstKey = room.mods.keys().next().value;
      room.mods.delete(firstKey);
    }
  }
  relay(room, msg, ws._wfId);
}

// ── DROP ITEMS ────────────────────────────────────────────────────────────
function handleDropSpawn(room, msg, ws) {
  // If the client didn't supply an id, mint one server-side.
  let id = msg.itemId;
  if (!id) {
    id = ws._wfId + "_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 6);
  }
  room.droppedItems.set(id, {
    type: msg.itemType,
    x: msg.x, y: msg.y, z: msg.z,
    vx: msg.vx || 0, vy: msg.vy || 0, vz: msg.vz || 0,
    potWaterType: msg.potWaterType || null,
  });
  // Cap droppedItems map at 200 per room to prevent runaway memory.
  if (room.droppedItems.size > 200) {
    const firstKey = room.droppedItems.keys().next().value;
    room.droppedItems.delete(firstKey);
  }
  // Relay with the server-minted id.
  relay(room, { ...msg, itemId: id, pid: ws._wfId }, ws._wfId);
}

function handleDropMutate(room, msg, ws) {
  // pickup / despawn — remove from server map.
  if (msg.type === "drop_item_pickup" || msg.type === "drop_item_despawn") {
    if (msg.itemId) room.droppedItems.delete(msg.itemId);
  } else if (msg.type === "drop_item_update" && msg.itemId) {
    const d = room.droppedItems.get(msg.itemId);
    if (d) {
      if (Number.isFinite(msg.x)) d.x = msg.x;
      if (Number.isFinite(msg.y)) d.y = msg.y;
      if (Number.isFinite(msg.z)) d.z = msg.z;
    }
  }
  relay(room, msg, ws._wfId);
}

// ── FIRE ───────────────────────────────────────────────────────────────────
function handleFireStart(room, msg, ws) {
  if (Number.isFinite(msg.x) && Number.isFinite(msg.y) && Number.isFinite(msg.z)) {
    const key = `${msg.x},${msg.y},${msg.z}`;
    room.fires.set(key, { x: msg.x, y: msg.y, z: msg.z, fuel: msg.fuel || 30, heat: msg.heat || 1 });
  }
  relay(room, msg, ws._wfId);
}

function handleFireExtinguish(room, msg, ws) {
  if (Number.isFinite(msg.x) && Number.isFinite(msg.y) && Number.isFinite(msg.z)) {
    room.fires.delete(`${msg.x},${msg.y},${msg.z}`);
  }
  relay(room, msg, ws._wfId);
}

// ── CHEST ──────────────────────────────────────────────────────────────────
function handleChestOpen(room, msg, ws) {
  // Just relay — opening a chest is a read-only notification to other players.
  relay(room, msg, ws._wfId);
}

function handleChestUpdate(room, msg, ws) {
  if (!Number.isFinite(msg.x) || !Number.isFinite(msg.y) || !Number.isFinite(msg.z)) {
    return relay(room, msg, ws._wfId);
  }
  const key = `${msg.x},${msg.y},${msg.z}`;
  let chest = room.chests.get(key);
  if (!chest) {
    chest = { revision: 0, contents: null };
    room.chests.set(key, chest);
  }
  // Optimistic concurrency: if the client sent an expectedRevision, only
  // accept the update if it matches. Otherwise bump unconditionally.
  if (Number.isInteger(msg.expectedRevision) && msg.expectedRevision !== chest.revision) {
    return safeSend(ws, {
      type: "error",
      code: "CHEST_STALE_REVISION",
      msg: `Chest at ${key} was modified by another player. Refresh.`,
      x: msg.x, y: msg.y, z: msg.z,
      currentRevision: chest.revision,
    });
  }
  chest.revision = (chest.revision || 0) + 1;
  chest.contents = Array.isArray(msg.contents) ? msg.contents : null;
  // Relay with the new authoritative revision so all peers stay in sync.
  relay(room, { ...msg, pid: ws._wfId, revision: chest.revision }, ws._wfId);
}

// ── ANIMAL MARK (server-authoritative mark registry) ───────────────────────
function handleAnimalMark(room, msg, ws) {
  // v175: server stores marks so new joiners see them and players who wander
  // far enough to despawn+respawn an animal re-apply the mark when it returns.
  // Key is the rounded block position so it's stable across clients.
  if (!Number.isFinite(msg.x) || !Number.isFinite(msg.z)) {
    // No position — just relay (back-compat with old clients)
    return relay(room, msg, ws._wfId);
  }
  const x = Math.round(msg.x);
  const y = Math.round(msg.y || 0);
  const z = Math.round(msg.z);
  const key = `${x},${y},${z}`;
  if (msg.mark) {
    // Add/update mark
    const markData = {
      name: sanitizeText(msg.mark.name || "Animal", MAX_MARK_NAME_LENGTH),
      type: String(msg.mark.type || "collar"),
      color: String(msg.mark.color || "#ffe07f"),
      role: msg.mark.role || null,
      ownerPid: ws._wfId,
      ownerName: sanitizeName(ws._wfName || "Player"),
      appliedAt: Date.now(),
      animalType: Number.isFinite(msg.animalType) ? msg.animalType : -1,
      x, y, z
    };
    room.marks.set(key, markData);
    // Cap marks at 200 per room
    if (room.marks.size > 200) {
      const firstKey = room.marks.keys().next().value;
      room.marks.delete(firstKey);
    }
    console.log(`[server.js] animal-mark  room='${room.name}' key=${key} name='${markData.name}' by=${ws._wfName}`);
    // Relay with the authoritative mark data
    relay(room, { ...msg, mark: markData, pid: ws._wfId }, ws._wfId);
  } else {
    // Remove mark
    if (room.marks.has(key)) {
      room.marks.delete(key);
      console.log(`[server.js] animal-unmark  room='${room.name}' key=${key} by=${ws._wfName}`);
    }
    relay(room, msg, ws._wfId);
  }
}

// ── PVP HIT (server-authoritative damage cap + rate limit) ─────────────────
function handlePvpHit(room, msg, ws) {
  const target = msg.target;
  const attacker = msg.attacker || ws._wfId;
  if (!target || target === attacker) return; // invalid

  const targetSock = room.players.get(target);
  if (!targetSock) return; // target disconnected

  // Rate limit per (attacker → target) pair: max 1 hit per 250ms.
  const now = Date.now();
  const rateKey = attacker + ">" + target;
  const last = ws._wfPvpHits.get(rateKey) || 0;
  if (now - last < PVP_RATE_LIMIT_MS) {
    // Silently drop — don't relay. The attacker's client will see no effect
    // and they'll naturally slow down. (Their client also rate-limits, but
    // we don't trust the client.)
    return;
  }
  ws._wfPvpHits.set(rateKey, now);
  if (ws._wfPvpHits.size > 30) {
    for (const [k, t] of ws._wfPvpHits) if (now - t > 5000) ws._wfPvpHits.delete(k);
  }

  // Cap damage.
  const rawDmg = Number(msg.damage) || 15;
  const dmg = Math.max(1, Math.min(PVP_DAMAGE_CAP, rawDmg));

  // Relay with the clamped damage to the target only (PvP hits are
  // point-to-point; other peers don't need the number).
  // v175: include victimX/victimZ so animals can observe the attack location.
  safeSend(targetSock, {
    type: "pvp_hit",
    target,
    attacker,
    attackerName: msg.attackerName || ws._wfName || "Player",
    damage: dmg,
    zone: msg.zone || "chest",
    victimX: msg.victimX || null,
    victimZ: msg.victimZ || null,
  });

  // Log PvP for debugging.
  console.log(`[server.js] pvp-hit  room='${room.name}' ${attacker}→${target} dmg=${dmg} zone=${msg.zone || "?"}`);
}

// ── SECURITY: Input sanitization ────────────────────────────────────────────
// v175.7.3: Server-side XSS protection. All text fields from clients are
// sanitized before being relayed to other players. This prevents script
// injection through chat, names, marks, or any other text field.
const MAX_CHAT_LENGTH = 200;
const MAX_NAME_LENGTH = 24;
const MAX_MARK_NAME_LENGTH = 24;
const MAX_BROADCAST_LENGTH = 500;

function sanitizeText(text, maxLen) {
  if (!text) return "";
  let s = String(text);
  if (s.length > maxLen) s = s.slice(0, maxLen);
  // Strip all HTML tags and event handlers
  s = s.replace(/<script[^>]*>.*?<\/script>/gi, "");
  s = s.replace(/<[^>]+>/g, "");
  s = s.replace(/on\w+\s*=/gi, "");
  s = s.replace(/javascript:/gi, "");
  s = s.replace(/data:text\/html/gi, "");
  s = s.replace(/vbscript:/gi, "");
  // Escape remaining HTML entities (defense in depth)
  s = s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  return s;
}

function sanitizeName(name) {
  return sanitizeText(name, MAX_NAME_LENGTH);
}

// Reject messages that are clearly XSS payloads (contain unescaped script tags
// after basic parsing). This is a second layer of defense — even if sanitization
// misses something, we reject messages that still contain dangerous patterns.
function isXssPayload(text) {
  if (!text) return false;
  const lower = String(text).toLowerCase();
  if (lower.includes("<script")) return true;
  if (lower.includes("onerror=")) return true;
  if (lower.includes("onload=")) return true;
  if (lower.includes("onclick=")) return true;
  if (lower.includes("onmouseover=")) return true;
  if (lower.includes("javascript:")) return true;
  if (lower.includes("eval(")) return true;
  if (lower.includes("document.cookie")) return true;
  if (lower.includes("document.domain")) return true;
  if (lower.includes("window.location")) return true;
  return false;
}

// ── CHAT ───────────────────────────────────────────────────────────────────
function handleChat(room, msg, ws) {
  // v175.7.3: Server-side XSS protection
  let text = String(msg.text || "").slice(0, MAX_CHAT_LENGTH);
  // Reject XSS payloads entirely
  if (isXssPayload(text)) {
    console.warn(`[server.js] XSS payload rejected from pid=${ws._wfId} name='${ws._wfName}'`);
    safeSend(ws, { type: "error", code: "XSS_REJECTED", msg: "Message rejected: HTML/script tags are not allowed in chat." });
    return;
  }
  // Sanitize and relay
  text = sanitizeText(text, MAX_CHAT_LENGTH);
  relay(room, {
    type: "chat",
    id: ws._wfId,
    name: sanitizeName(ws._wfName),
    text,
    skin: ws._wfSkin || "",
  }, ws._wfId);
}

// ── HARVEST GRANT ───────────────────────────────────────────────────────────
function handleHarvestGrant(room, msg, ws) {
  // Forage / harvest requests are server-mediated so a single peer can't
  // double-grant. Server currently just relays and replies with a forage_result
  // to the requesting peer (cheat-mode; production would track resource state).
  if (msg.type === "harvest_grant_request") {
    safeSend(ws, {
      type: "forage_result",
      found: true,
      kind: msg.kind || "shell",
    });
    return;
  }
  if (msg.type === "forage_request") {
    safeSend(ws, { type: "forage_result", found: Math.random() < 0.7, kind: msg.kind || "shell" });
    return;
  }
  if (msg.type === "berry_bush_harvested") {
    relay(room, msg, ws._wfId);
    return;
  }
}

// ── TIDE SYNC (host → server → all) ─────────────────────────────────────────
function handleTideSync(room, msg, ws) {
  // Only the host should broadcast tide syncs. If a non-host tries, ignore.
  if (ws._wfId !== room.hostPid) return;
  room.tideStart = Number(msg.start) || room.tideStart || Date.now();
  relay(room, msg, ws._wfId);
}

// ── ADMIN / HOST COMMANDS ──────────────────────────────────────────────────
function handleAdmin(room, msg, ws) {
  if (ws._wfId !== room.hostPid) {
    return safeSend(ws, { type: "error", code: "NOT_HOST",
      msg: "Only the host can run admin commands." });
  }
  // Resolve the target pid for admin_* messages that include one.
  const target = msg.pid || msg.target;
  if (target && target !== ws._wfId && !room.players.has(target)) {
    return safeSend(ws, { type: "error", code: "NO_SUCH_TARGET",
      msg: `No player with id ${target} in this room.` });
  }
  // For admin_kick, drop the target socket.
  if (msg.type === "admin_kick" && target) {
    const targetSock = room.players.get(target);
    if (targetSock) {
      safeSend(targetSock, { type: "kicked", reason: sanitizeText(msg.reason || "Kicked by host", 200) });
      try { targetSock.close(); } catch (_) {}
    }
    return;
  }
  // Otherwise relay to the target.
  if (target) {
    const targetSock = room.players.get(target);
    if (targetSock) safeSend(targetSock, { ...msg, from: sanitizeName(ws._wfName) });
    return;
  }
  // No target — broadcast to the room (e.g. admin_broadcast).
  relay(room, { ...msg, from: sanitizeName(ws._wfName), text: sanitizeText(msg.text || msg.msg || "", MAX_BROADCAST_LENGTH) }, ws._wfId);
}

function handlePlayerCmd(room, msg, ws) {
  if (ws._wfId !== room.hostPid) {
    return safeSend(ws, { type: "error", code: "NOT_HOST", msg: "Only the host can issue player commands." });
  }
  const target = msg.target || msg.pid;
  if (!target) return safeSend(ws, { type: "error", code: "NO_TARGET", msg: "player_cmd requires a target." });
  const targetSock = room.players.get(target);
  if (!targetSock) return safeSend(ws, { type: "error", code: "NO_SUCH_TARGET", msg: `No player with id ${target}.` });
  safeSend(targetSock, {
    type: "player_cmd",
    cheat: msg.cheat,
    args: msg.args || {},
    from: ws._wfName,
  });
}

// ── WEATHER ────────────────────────────────────────────────────────────────
function handleWeatherToggle(room, msg, ws) {
  if (ws._wfId !== room.hostPid) {
    return safeSend(ws, { type: "error", code: "NOT_HOST", msg: "Only the host can toggle weather." });
  }
  room.weatherEnabled = !!msg.enabled;
  broadcast(room, { type: "weather_toggle", enabled: room.weatherEnabled });
  console.log(`[server.js] weather room='${room.name}' enabled=${room.weatherEnabled}`);
}

// ── STATS / LOG ────────────────────────────────────────────────────────────
function printStats() {
  let total = 0;
  for (const [name, room] of rooms) {
    console.log(`  room '${name}': ${room.players.size} player(s), host=${room.hostPid}, drops=${room.droppedItems.size}, fires=${room.fires.size}, chests=${room.chests.size}, mods=${room.mods.size}, marks=${room.marks.size}`);
    total += room.players.size;
  }
  console.log(`[server.js] stats: ${rooms.size} room(s), ${total} player(s) total`);
}
setInterval(printStats, 60_000);

// ── STARTUP ────────────────────────────────────────────────────────────────
console.log(`╔════════════════════════════════════════════════════════════╗`);
console.log(`║  World (Infinite) multiplayer server  —  ${PROTOCOL_VERSION}            ║`);
console.log(`╚════════════════════════════════════════════════════════════╝`);
console.log(`Listening on ws://0.0.0.0:${PORT}`);
console.log(`Limits: max ${MAX_ROOM_PLAYERS} players/room, ${MAX_MSG_SIZE} bytes/msg, ${STALE_SOCKET_MS / 1000}s stale timeout`);
console.log(`PvP cap: ${PVP_DAMAGE_CAP} dmg, ${PVP_RATE_LIMIT_MS}ms rate limit per attacker→target`);
console.log("");
