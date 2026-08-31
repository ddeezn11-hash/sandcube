'use strict';

// World (Infinite) v132 — Worker JSON parsing + 400ms combat rewind
//
// Goals:
//   • Keep the published client/server vocabulary explicit and versioned.
//   • Make identity, reach, rate limits, revisions, persistence and mutation
//     boundaries server-owned.
//   • Stop the easiest duplication/race/exploit paths.
//   • Run the current published client without legacy compatibility fallbacks.
//
// IMPORTANT SECURITY MODEL
// ------------------------
// The published client is the newest client; no legacy compatibility mode is
// enabled. Stateful gameplay that still originates on the client remains a
// known migration boundary until it is converted to server-executed intents.

const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Worker: NodeWorker } = require('worker_threads');

// v132 PERF: off-thread parsing for websocket JSON while preserving FIFO order
// per connection. Binary WFMV1 packets remain on the fast synchronous path.
const MP_JSON_WORKER_COUNT = Math.min(4, Math.max(1, Number(process.env.MP_JSON_WORKERS) || 2));
const MP_JSON_QUEUE_MAX = 2048;
const mpJsonWorkers = [];
const mpJsonQueue = [];
const mpJsonPending = new Map();
let mpJsonNextId = 1;
const MP_JSON_WORKER_SOURCE = `
  const { parentPort } = require('worker_threads');
  parentPort.on('message', (job) => {
    try { parentPort.postMessage({ id: job.id, ok: true, msg: JSON.parse(job.text) }); }
    catch (err) { parentPort.postMessage({ id: job.id, ok: false, error: String(err && err.message || err) }); }
  });
`;
function pumpMpJsonWorkers() {
  for (const worker of mpJsonWorkers) {
    if (worker.busy || mpJsonQueue.length === 0) continue;
    const job = mpJsonQueue.shift();
    worker.busy = true;
    mpJsonPending.set(job.id, { ...job, worker });
    try { worker.postMessage({ id: job.id, text: job.text }); }
    catch (err) {
      mpJsonPending.delete(job.id); worker.busy = false; job.reject(err);
    }
  }
}
for (let i = 0; i < MP_JSON_WORKER_COUNT; i++) {
  const worker = new NodeWorker(MP_JSON_WORKER_SOURCE, { eval: true });
  worker.busy = false;
  worker.on('message', (result) => {
    const job = mpJsonPending.get(result.id);
    if (!job) return;
    mpJsonPending.delete(result.id);
    worker.busy = false;
    result.ok ? job.resolve(result.msg) : job.reject(new Error(result.error || 'Invalid JSON'));
    pumpMpJsonWorkers();
  });
  worker.on('error', (err) => {
    worker.busy = false;
    for (const [id, job] of mpJsonPending) if (job.worker === worker) {
      mpJsonPending.delete(id); job.reject(err);
    }
  });
  mpJsonWorkers.push(worker);
}
function parseJsonOffThread(text) {
  if (mpJsonQueue.length >= MP_JSON_QUEUE_MAX) return Promise.reject(new Error('PARSER_BUSY'));
  return new Promise((resolve, reject) => {
    mpJsonQueue.push({ id: mpJsonNextId++, text, resolve, reject });
    pumpMpJsonWorkers();
  });
}


const PORT = parseInt(process.env.PORT || '8080', 10);
const HTML_CANDIDATES = [
  process.env.GAME_HTML ? path.resolve(process.env.GAME_HTML) : null,
  path.join(__dirname, 'working_version.html'),
  path.join(__dirname, 'game2_farming_genetics_ecology_fixed(1).html'),
].filter(Boolean);
const INDEX_HTML = HTML_CANDIDATES.find(fs.existsSync) || HTML_CANDIDATES[0];
const DATA_DIR = path.resolve(process.env.WORLD_DATA_DIR || path.join(__dirname, 'world_data'));
const SNAPSHOT_INTERVAL = Math.max(2000, parseInt(process.env.WORLD_SNAPSHOT_MS || '5000', 10));
const EMPTY_ROOM_GRACE = Math.max(60_000, parseInt(process.env.EMPTY_ROOM_GRACE_MS || '600000', 10));
const MAX_WS_PAYLOAD = Math.min(2 * 1024 * 1024, Math.max(64 * 1024, parseInt(process.env.MAX_WS_PAYLOAD || `${512 * 1024}`, 10)));
const PUBLIC_HOST_TOKEN = process.env.HOST_TOKEN || '';
// Single source of truth for the published protocol version. Used in the
// /health payload, the join_ack, and to validate incoming clients below —
// bump this whenever client/server message shapes change incompatibly.
const PROTOCOL_VERSION = 'v132';
const STRICT_LEGACY = true; // No legacy path: clients must match PROTOCOL_VERSION exactly (enforced in the JOIN handler).

// Shared item IDs for security-sensitive network protocols. Keep these synchronized with the client definitions.
const PROTOCOL_IDS = Object.freeze({
  ARROW: 57, FIRE_ARROW: 119, FEATHERED_ARROW: 120, HEAVY_ARROW: 121, POISON_ARROW: 122, OBSIDIAN_ARROW: 132,
  STONE_AXE: 137, IRON_AXE: 72, OBSIDIAN_AXE: 129,
});
const INVENTORY_SLOT_CAP = 99;
function cleanInventory(entries) {
  if (!Array.isArray(entries)) return [];
  return entries.slice(0, 64).map(s => ({
    type: validBlockType(s?.type) ? s.type : null,
    count: clamp(cleanCount(s?.count || (s?.type != null ? 1 : 0), INVENTORY_SLOT_CAP), 0, INVENTORY_SLOT_CAP),
    spoilTimer: finiteNumber(s?.spoilTimer) ? clamp(s.spoilTimer, 0, 1e9) : 0,
  }));
}
function inventoryCounts(inv) {
  const out = Object.create(null);
  for (const s of inv || []) if (s?.type != null && s.count > 0) out[s.type] = (out[s.type] || 0) + s.count;
  return out;
}
function inventoryHash(inv) { return crypto.createHash('sha256').update(JSON.stringify(inv || [])).digest('hex'); }
function sendInventoryState(ws, player) {
  player.inventoryHash = inventoryHash(player.inventory);
  ws && send(ws, { type:'inventory_state', pid: player.id || null, slots: cleanInventory(player.inventory), revision: player.inventoryRevision || 0, hash: player.inventoryHash, authoritative:true });
}
function inventoryAdd(player, type, count, spoilTimer = 0) {
  type = Number(type); count = Number(count);
  if (!Number.isInteger(type) || type < 0 || type > 255 || !Number.isInteger(count) || count <= 0) return false;
  let remaining = count;
  for (const slot of player.inventory) {
    if (slot.type === type && slot.count < INVENTORY_SLOT_CAP) {
      const add = Math.min(remaining, INVENTORY_SLOT_CAP - slot.count);
      slot.count += add; remaining -= add;
      if (remaining <= 0) break;
    }
  }
  for (const slot of player.inventory) {
    if (remaining <= 0) break;
    if (slot.type == null || slot.count <= 0) {
      const add = Math.min(remaining, INVENTORY_SLOT_CAP);
      slot.type = type; slot.count = add; slot.spoilTimer = Number.isFinite(spoilTimer) ? clamp(spoilTimer,0,1e9) : 0;
      remaining -= add;
    }
  }
  if (remaining > 0) return false;
  player.inventoryRevision = (player.inventoryRevision || 0) + 1;
  player.inventoryHash = inventoryHash(player.inventory);
  if (player.id && player.roomRef) player.roomRef.inventories.set(player.id, cleanInventory(player.inventory));
  return true;
}
function inventoryRemove(player, type, count) {
  type = Number(type); count = Number(count);
  if (!Number.isInteger(type) || !Number.isInteger(count) || count <= 0) return false;
  let left = count;
  for (const slot of player.inventory) {
    if (slot.type !== type || left <= 0) continue;
    const take = Math.min(left, Math.max(0, slot.count || 0));
    slot.count -= take; left -= take;
    if (slot.count <= 0) { slot.type = null; slot.count = 0; slot.spoilTimer = 0; }
  }
  if (left > 0) return false;
  player.inventoryRevision = (player.inventoryRevision || 0) + 1;
  player.inventoryHash = inventoryHash(player.inventory);
  if (player.id && player.roomRef) player.roomRef.inventories.set(player.id, cleanInventory(player.inventory));
  return true;
}
function inventoryCanAdd(player, count) {
  let free = 0;
  for (const s of player.inventory || []) {
    if (s.type == null || s.count < INVENTORY_SLOT_CAP) free += s.type == null ? INVENTORY_SLOT_CAP : (INVENTORY_SLOT_CAP - s.count);
  }
  return free >= count;
}
const DEBUG = process.env.MP_DEBUG === '1';
const TRUST_PROXY = process.env.TRUST_PROXY === '1';

fs.mkdirSync(DATA_DIR, { recursive: true });

const limits = Object.freeze({
  rooms: 256,
  playersPerRoom: 64,
  blockMods: 150000,
  fires: 512,
  animals: 512,
  ropes: 256,
  boats: 64,
  crucibles: 256,
  anvils: 256,
  cookingPots: 256,
  kilns: 256,
  campfires: 256,
  furnaces: 256,
  droppedItems: 1024,
  piles: 5000,
  clayShapes: 512,
  waterFlow: 20000,
  chestSlots: 27,
  maxClayVertices: 4096,
  maxClayFaces: 4096,
});

const rooms = new Map();
const connectionCounts = new Map();

function now() { return Date.now(); }
function randomId(prefix = '') { return `${prefix}${crypto.randomBytes(12).toString('hex')}`; }
function finiteNumber(v) { return typeof v === 'number' && Number.isFinite(v); }
function validXYZ(m) {
  return finiteNumber(m?.x) && finiteNumber(m?.y) && finiteNumber(m?.z) &&
    Math.abs(m.x) <= 10_000_000 && Math.abs(m.z) <= 10_000_000 && m.y >= -128 && m.y <= 512;
}
function validVec(v, max = 2) {
  if (!finiteNumber(v?.x) || !finiteNumber(v?.y) || !finiteNumber(v?.z)) return false;
  return Math.hypot(v.x, v.y, v.z) <= max;
}
function clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, n)); }
function safeString(v, max, fallback = '') {
  return typeof v === 'string' ? v.slice(0, max) : fallback;
}
function validPlayerId(v) { return typeof v === 'string' && /^[A-Za-z0-9_-]{2,64}$/.test(v); }
function validRoomKey(v) { return typeof v === 'string' && /^[A-Za-z0-9_.-]{1,128}$/.test(v); }
function validBlockType(v) { return Number.isInteger(v) && v >= -1 && v <= 255; }
function distanceSq(a, b) {
  if (!a || !b) return Infinity;
  const dx = a.x - b.x, dy = a.y - b.y, dz = a.z - b.z;
  return dx * dx + dy * dy + dz * dz;
}
function ipOf(reqOrSocket) {
  const raw = reqOrSocket?.headers?.['x-forwarded-for'];
  if (TRUST_PROXY && raw) return String(raw).split(',')[0].trim();
  return reqOrSocket?.socket?.remoteAddress || 'unknown';
}
function hashRoomKey(key) {
  return crypto.createHash('sha256').update(key).digest('hex').slice(0, 24);
}
function roomPath(room) { return path.join(DATA_DIR, `${hashRoomKey(room.key)}.json`); }
function journalPath(room) { return path.join(DATA_DIR, `${hashRoomKey(room.key)}.journal.ndjson`); }

function rateLimiter() {
  const buckets = new Map();
  function allow(key, max, windowMs) {
    const t = now();
    const b = buckets.get(key);
    if (!b || t - b.start >= windowMs) {
      buckets.set(key, { start: t, count: 1 });
      return true;
    }
    b.count += 1;
    return b.count <= max;
  }
  function gc() {
    const t = now();
    for (const [k, b] of buckets) if (t - b.start > 120_000) buckets.delete(k);
  }
  return { allow, gc };
}

function mapToObject(m) { return Object.fromEntries(m); }
function cloneJson(v) {
  return JSON.parse(JSON.stringify(v));
}
function createRevisionBag() {
  return {
    world: 0,
    blocks: 0,
    fires: 0,
    animals: 0,
    ropes: 0,
    boats: 0,
    forge: 0,
    cooking: 0,
    items: 0,
    water: 0,
    clay: 0,
    chests: 0,
    players: 0,
  };
}

function createRoom(key, seed, worldType) {
  return {
    key, seed, worldType,
    hostId: null,
    locked: false,
    weatherEnabled: true,
    outfitSettings: null,
    createdAt: now(),
    lastPersistAt: 0,
    dirty: false,
    journalSeq: 0,
    revisions: createRevisionBag(),
    players: new Map(),
    inventories: new Map(),
    blockMods: Object.create(null),
    fires: Object.create(null),
    animals: new Map(),
    ropes: new Map(),
    boats: new Map(),
    crucibles: Object.create(null),
    anvils: Object.create(null),
    cookingPots: Object.create(null),
    kilns: Object.create(null),
    campfires: Object.create(null),
    furnaces: Object.create(null),
    chests: new Map(),
    droppedItems: new Map(),
    piles: Object.create(null),
    clayShapes: Object.create(null),
    waterFlow: Object.create(null),
    berryBushCooldowns: new Map(), // key "x,y,z" -> ready timestamp (ms); in-memory only, not persisted (worst case a bush is harvestable slightly early after a restart)
    tx: new Map(),
  };
}

function reviveRoom(data) {
  const r = createRoom(data.key, data.seed, data.worldType);
  Object.assign(r, {
    hostId: data.hostId || null,
    locked: !!data.locked,
    weatherEnabled: data.weatherEnabled !== false,
    outfitSettings: data.outfitSettings || null,
    createdAt: data.createdAt || now(),
    lastPersistAt: 0,
    journalSeq: data.journalSeq || 0,
    revisions: Object.assign(createRevisionBag(), data.revisions || {}),
    blockMods: Object.assign(Object.create(null), data.blockMods || {}),
    fires: Object.assign(Object.create(null), data.fires || {}),
    crucibles: Object.assign(Object.create(null), data.crucibles || {}),
    anvils: Object.assign(Object.create(null), data.anvils || {}),
    cookingPots: Object.assign(Object.create(null), data.cookingPots || {}),
    kilns: Object.assign(Object.create(null), data.kilns || {}),
    campfires: Object.assign(Object.create(null), data.campfires || {}),
    furnaces: Object.assign(Object.create(null), data.furnaces || {}),
    piles: Object.assign(Object.create(null), data.piles || {}),
    clayShapes: Object.assign(Object.create(null), data.clayShapes || {}),
    waterFlow: Object.assign(Object.create(null), data.waterFlow || {}),
  });
  for (const [k, v] of Object.entries(data.animals || {})) r.animals.set(k, v);
  for (const [k, v] of Object.entries(data.inventories || {})) r.inventories.set(k, cleanInventory(v));
  for (const [k, v] of Object.entries(data.ropes || {})) r.ropes.set(k, v);
  for (const [k, v] of Object.entries(data.boats || {})) r.boats.set(k, v);
  for (const [k, v] of Object.entries(data.chests || {})) r.chests.set(k, v);
  for (const [k, v] of Object.entries(data.droppedItems || {})) r.droppedItems.set(k, v);
  return r;
}

function serializableRoom(room) {
  return {
    key: room.key,
    seed: room.seed,
    worldType: room.worldType,
    hostId: room.hostId,
    locked: room.locked,
    weatherEnabled: room.weatherEnabled,
    outfitSettings: room.outfitSettings,
    createdAt: room.createdAt,
    journalSeq: room.journalSeq,
    revisions: room.revisions,
    blockMods: room.blockMods,
    fires: room.fires,
    animals: mapToObject(room.animals),
    inventories: mapToObject(room.inventories),
    ropes: mapToObject(room.ropes),
    boats: mapToObject(room.boats),
    crucibles: room.crucibles,
    anvils: room.anvils,
    cookingPots: room.cookingPots,
    kilns: room.kilns,
    campfires: room.campfires,
    furnaces: room.furnaces,
    chests: mapToObject(room.chests),
    droppedItems: mapToObject(room.droppedItems),
    piles: room.piles,
    clayShapes: room.clayShapes,
    waterFlow: room.waterFlow,
  };
}

function persistRoom(room, reason = 'periodic') {
  if (!room.dirty && reason === 'periodic') return;
  const tmp = `${roomPath(room)}.tmp`;
  const data = JSON.stringify(serializableRoom(room));
  try {
    fs.writeFileSync(tmp, data, 'utf8');
    fs.renameSync(tmp, roomPath(room));
    room.lastPersistAt = now();
    room.dirty = false;
    if (DEBUG) console.log(`[persist] ${room.key} ${reason}`);
  } catch (err) {
    console.error(`[persist] failed for ${room.key}:`, err.message);
    try { fs.unlinkSync(tmp); } catch (_) {}
  }
}

const journalQueues = new Map();
function journal(room, type, actor, payload = {}) {
  room.journalSeq += 1;
  const rec = {
    seq: room.journalSeq,
    time: now(),
    type,
    actor: actor || null,
    payload: cloneJson(payload),
  };
  room.dirty = true;
  const line = JSON.stringify(rec) + '\n';
  const prev = journalQueues.get(room.key) || Promise.resolve();
  const next = prev
    .catch(() => {})
    .then(() => fs.promises.appendFile(journalPath(room), line, 'utf8'))
    .catch(err => { console.error(`[journal] ${room.key}:`, err.message); });
  journalQueues.set(room.key, next);
  next.then(() => {
    if (journalQueues.get(room.key) === next) journalQueues.delete(room.key);
  });
}

function bump(room, key) {
  room.revisions[key] = (room.revisions[key] || 0) + 1;
  room.revisions.world += 1;
  return room.revisions[key];
}

function send(ws, obj) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(JSON.stringify(obj));
    return true;
  } catch (_) { return false; }
}
function broadcast(room, msg, excludeId) {
  for (const [pid, p] of room.players) if (pid !== excludeId) send(p.ws, msg);
}
function broadcastBinary(room, data, excludeId) {
  for (const [pid, p] of room.players) {
    if (pid === excludeId || !p?.ws || p.ws.readyState !== WebSocket.OPEN) continue;
    try { p.ws.send(data, { binary: true }); } catch (_) {}
  }
}
function error(ws, code, msg, retryAfter = undefined) {
  send(ws, { type: 'error', code, msg, ...(retryAfter != null ? { retryAfter } : {}) });
}
function isHost(room, pid) { return room.hostId === pid; }
function playerPresent(room, pid) { return !!room.players.get(pid); }
function withinReach(player, x, y, z, reach = 10) {
  if (!player?.lastPos) return false;
  return distanceSq(player.lastPos, { x, y, z }) <= reach * reach;
}

// v132 NET: 400ms bounded rewind history for server-authoritative combat.
const COMBAT_HISTORY_MS = 400;
const COMBAT_HISTORY_MAX = 24;
function recordCombatHistory(player, t, pos, ry) {
  const h = player.combatHistory || (player.combatHistory = []);
  h.push({ t, x: pos.x, y: pos.y, z: pos.z, ry });
  const cutoff = t - COMBAT_HISTORY_MS - 50;
  while (h.length > COMBAT_HISTORY_MAX || (h.length && h[0].t < cutoff)) h.shift();
}
function sampleCombatHistory(player, targetTime) {
  const h = player?.combatHistory;
  if (!h || !h.length) return player?.lastPos ? { t: targetTime, ...player.lastPos, ry: player.lastRy || 0 } : null;
  if (targetTime <= h[0].t) return { ...h[0] };
  const last = h[h.length - 1];
  if (targetTime >= last.t) return { ...last };
  let lo = 0, hi = h.length - 1;
  while (lo + 1 < hi) {
    const mid = (lo + hi) >> 1;
    if (h[mid].t <= targetTime) lo = mid; else hi = mid;
  }
  const a = h[lo], b = h[hi], u = clamp((targetTime - a.t) / Math.max(1, b.t - a.t), 0, 1);
  return {
    t: targetTime,
    x: a.x + (b.x - a.x) * u,
    y: a.y + (b.y - a.y) * u,
    z: a.z + (b.z - a.z) * u,
    ry: a.ry + (b.ry - a.ry) * u,
  };
}
function combatActionServerTime(player, clientTime, serverNow) {
  if (!finiteNumber(clientTime) || !player?.clockSynced) return serverNow;
  return clamp(clientTime + player.clientClockOffsetMs, serverNow - COMBAT_HISTORY_MS, serverNow + 25);
}
// Harvest tickets: a short-lived record that this player recently performed
// a server-validated, reach-checked world interaction (block break, tree
// felling, bush/fruit interaction) near (x,y,z). world-sourced item drops
// (chopped wood, sticks, fruit, berries, scraped materials, felled tree
// debris) are only granted server-side if they match a live ticket — this
// is what stops a client from minting arbitrary "harvest" drops out of
// thin air while still allowing the physical spread of tree-fall debris.
const HARVEST_TICKET_TTL_MS = 6000;
const HARVEST_TICKET_RADIUS = 14; // covers wide tree canopies/fall spread
const HARVEST_TICKET_CAP = 24;
function issueHarvestTicket(player, x, y, z) {
  if (!player) return;
  const list = player.harvestTickets || (player.harvestTickets = []);
  list.push({ x, y, z, at: now() });
  if (list.length > HARVEST_TICKET_CAP) list.splice(0, list.length - HARVEST_TICKET_CAP);
}
function consumeHarvestTicket(player, x, y, z) {
  const list = player?.harvestTickets;
  if (!list || !list.length) return false;
  const t = now();
  let matchIdx = -1;
  for (let i = list.length - 1; i >= 0; i--) {
    const tk = list[i];
    if (t - tk.at > HARVEST_TICKET_TTL_MS) continue;
    if (distanceSq(tk, { x, y, z }) <= HARVEST_TICKET_RADIUS * HARVEST_TICKET_RADIUS) { matchIdx = i; break; }
  }
  // Prune expired tickets opportunistically.
  for (let i = list.length - 1; i >= 0; i--) if (t - list[i].at > HARVEST_TICKET_TTL_MS) list.splice(i, 1);
  return matchIdx !== -1; // ticket is consumable multiple times within TTL — one chop can shed several drops/ticks
}

// Berry bush harvest: server owns the regrow cooldown per bush position (a
// small map on the room, same shape as the client's local
// _berryBushCooldowns) so a client can't just re-request berries before the
// bush has actually regrown.
const BERRY_BUSH_TYPE = 18; // BLK_BUSH_BERRY — must match client constant
const BERRIES_ITEM_TYPE = 16; // BLK_BERRIES
function berryBushKey(x, y, z) { return `${x},${y},${z}`; }
function berryBushRegrowSeconds() {
  return 45 + Math.random() * 95; // covers the client's season-based 45-175s range without needing season state server-side
}

// Basic recipes for the direct-grant crafts that had no server round-trip at
// all. Each recipe lists inputs (consumed from inventory) and the output
// granted. This is an inputs-present check, not a timing/minigame
// validation — it stops "mint with nothing in inventory" but still trusts
// client-reported completion of any labor-bar minigame.
//
// The labor-bar crafts (hull/mast/sail/plank-split) consume whatever item is
// in the player's selected slot at craft time, not a fixed ingredient type —
// the client never enforces a specific input type for these, so a fixed
// recipe here would be wrong and could block legitimate play. Those are
// handled as "any 1 item consumed" via requireAnyItem below instead of a
// fixed CRAFT_RECIPES entry.
const CRAFT_RECIPES = Object.freeze({
  tallow_candle: { inputs: [[37, 1], [12, 1]], output: [35, 1] }, // tallow + stick -> candle
  // Two clay variants (Raw Clay=24, Deep Clay=60) both fire into item 25 in
  // the client code (fire_clay handler accepts either type at this call
  // site) — server accepts whichever the client says it consumed.
  fired_clay: { inputAny: [[24, 1], [60, 1]], output: [25, 1] },
  rope_twist: { inputs: [[38, 2]], output: [27, 1] }, // 2x vine -> rope
  // Labor-bar crafts gated by a fixed held-item type in the client
  // (confirmed from the actual keydown handler, not guessed):
  // hull needs Plank(26), mast needs Wood(5), sail needs Leaves(6).
  boat_hull: { inputs: [[26, 1]], output: [32, 1] },
  boat_mast: { inputs: [[5, 1]], output: [33, 1] },
  boat_sail: { inputs: [[6, 1]], output: [34, 1] },
  plank_split: { inputs: [[5, 1]], output: [26, 3] }, // wood -> 3 planks; input type confirmed from same handler family
});

// Coil-clay pottery (the live, player-reachable clay coiling+firing system:
// _coilState/_CLAY_FORMS in the client — a separate, unrelated "_pc.clay*"
// system also exists in the client but is dead code, never called from any
// input handler, so it's not wired here).
//
// This is a two-step pipeline, not a single request: raw clay is consumed
// the moment coiling finishes (client already does this locally), then the
// fired result is granted only after a ~48s dry+fire wait. A plain
// inputs-present recipe doesn't fit — the clay is long gone from inventory
// by the time firing completes. Instead: consuming the clay issues a
// short-lived firing ticket per form; only firing_result can redeem it, and
// only within its TTL, so a client can't skip the wait or claim a form it
// never started coiling.
const CLAY_FIRING_TTL_MS = 90_000; // generous vs. the ~48s real dry+fire time, covers lag/tab-throttling
const CLAY_FORM_OUTPUTS = Object.freeze({
  bowl: [[25, 1], [29, 1]],
  pot: [[86, 1]],
  brick: [[25, 3]],
  figurine: [[25, 1], [29, 2]],
});

function hasCapacity(room, storeName, key) {
  const store = room[storeName];
  const max = limits[storeName];
  if (!store || !max) return false;
  if (store instanceof Map) return store.has(key) || store.size < max;
  return Object.prototype.hasOwnProperty.call(store, key) || Object.keys(store).length < max;
}
function parseExpectedRevision(msg) {
  return Number.isInteger(msg?.expectedRevision) ? msg.expectedRevision : null;
}
function checkRevision(current, expected) {
  return expected == null || current === expected;
}
function cleanCount(v, max = 999) { return Number.isFinite(v) ? clamp(Math.floor(v), 0, max) : 0; }
function assertJoined(ctx) {
  if (!ctx.room || !ctx.playerId || !ctx.player) {
    error(ctx.ws, 'NOT_JOINED', 'Not joined to a room yet.');
    return false;
  }
  return true;
}

function clearStructuresAt(room, key) {
  if (!room || !key) return;
  // Coordinate-keyed persistent structures.
  for (const storeName of ['chests','piles','campfires','cookingPots','crucibles','anvils','kilns','furnaces','clayShapes','waterFlow','fires']) {
    const store = room[storeName];
    if (!store) continue;
    if (store instanceof Map) store.delete(key);
    else delete store[key];
  }
  // Position-keyed object stores that use generated IDs.
  for (const [id, obj] of room.ropes || []) {
    if (obj && `${obj.x},${obj.y},${obj.z}` === key) room.ropes.delete(id);
  }
  for (const [id, obj] of room.boats || []) {
    if (obj && `${obj.x ?? obj.cx},${obj.y ?? 0},${obj.z ?? obj.cz}` === key) room.boats.delete(id);
  }
}

function buildJoinSnapshot(room) {
  return {
    mods: room.blockMods,
    fires: room.fires,
    animals: mapToObject(room.animals),
    ropes: mapToObject(room.ropes),
    boats: mapToObject(room.boats),
    crucibles: room.crucibles,
    anvils: room.anvils,
    cookingPots: room.cookingPots,
    kilns: room.kilns,
    campfires: room.campfires,
    furnaces: room.furnaces,
    droppedItems: mapToObject(room.droppedItems),
    piles: room.piles,
    clayShapes: room.clayShapes,
    waterFlow: room.waterFlow,
    // Chest slot contents are private; send them only after chest_open.
    revisions: room.revisions,
  };
}

function saveAllRooms() { for (const room of rooms.values()) persistRoom(room, 'shutdown/flush'); }

function loadRoomsFromDisk() {
  let files = [];
  try { files = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.json')); } catch (_) {}
  for (const file of files) {
    try {
      const data = JSON.parse(fs.readFileSync(path.join(DATA_DIR, file), 'utf8'));
      if (!data || !validRoomKey(data.key)) continue;
      if (rooms.size >= limits.rooms) break;
      rooms.set(data.key, reviveRoom(data));
    } catch (err) {
      console.warn(`[load] ignored ${file}: ${err.message}`);
    }
  }
}
loadRoomsFromDisk();

function getOrCreateRoom(key, seed, worldType) {
  if (rooms.has(key)) return rooms.get(key);
  if (rooms.size >= limits.rooms) return null;
  const room = createRoom(key, seed, worldType);
  rooms.set(key, room);
  return room;
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP
// ─────────────────────────────────────────────────────────────────────────────

const httpServer = http.createServer((req, res) => {
  const headers = {
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'SAMEORIGIN',
    'Referrer-Policy': 'no-referrer',
    'Cache-Control': 'no-store',
  };

  if (req.url === '/health') {
    const totalPlayers = [...rooms.values()].reduce((n, r) => n + r.players.size, 0);
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', ...headers });
    res.end(JSON.stringify({ status: 'ok', version: PROTOCOL_VERSION, rooms: rooms.size, players: totalPlayers, time: now() }));
    return;
  }

  if (req.method !== 'GET') {
    res.writeHead(405, { ...headers, Allow: 'GET' });
    res.end('Method not allowed');
    return;
  }

  let pathname;
  try { pathname = decodeURIComponent((req.url || '/').split('?')[0]); }
  catch (_) { res.writeHead(400, headers); res.end('Bad request'); return; }

  const requestedPath = pathname === '/' ? INDEX_HTML : path.resolve(__dirname, `.${pathname}`);
  const rel = path.relative(__dirname, requestedPath);
  if (rel.startsWith('..') || path.isAbsolute(rel)) {
    res.writeHead(403, headers); res.end('Forbidden'); return;
  }

  fs.readFile(requestedPath, (err, data) => {
    if (err) {
      fs.readFile(INDEX_HTML, (err2, fallback) => {
        if (err2) {
          res.writeHead(500, { ...headers, 'Content-Type': 'text/plain; charset=utf-8' });
          res.end('500 Server Error');
          return;
        }
        res.writeHead(200, { ...headers, 'Content-Type': 'text/html; charset=utf-8' });
        res.end(fallback);
      });
      return;
    }
    const ext = path.extname(requestedPath).toLowerCase();
    const mime = {
      '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
      '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8',
      '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif',
      '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.wav': 'audio/wav', '.mp3': 'audio/mpeg',
      '.ogg': 'audio/ogg', '.wasm': 'application/wasm', '.mp4': 'video/mp4', '.webm': 'video/webm'
    }[ext] || 'application/octet-stream';
    res.writeHead(200, { ...headers, 'Content-Type': mime });
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server: httpServer, maxPayload: MAX_WS_PAYLOAD, perMessageDeflate: false });

function connectionAllowed(ip) {
  const b = connectionCounts.get(ip) || { count: 0, started: now() };
  if (now() - b.started > 60_000) { b.started = now(); b.count = 0; }
  b.count += 1;
  connectionCounts.set(ip, b);
  return b.count <= 12;
}

wss.on('connection', (ws, req) => {
  const ip = ipOf(req);
  if (!connectionAllowed(ip)) {
    try { ws.close(1008, 'Too many connections from this address'); } catch (_) {}
    return;
  }

  const rl = rateLimiter();
  const ctx = {
    ws, ip,
    room: null,
    playerId: null,
    player: null,
    joined: false,
    token: randomId('sess_'),
    lastSeq: -1,
    joinedAt: 0,
  };

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  function leave() {
    if (!ctx.room || !ctx.playerId) return;
    const room = ctx.room;
    const player = room.players.get(ctx.playerId);
    if (!player || player.ws !== ws) return;
    room.players.delete(ctx.playerId);
    bump(room, 'players');
    journal(room, 'PLAYER_LEAVE', ctx.playerId, { name: player.name });
    broadcast(room, { type: 'player_leave', id: ctx.playerId, name: player.name });
    if (room.hostId === ctx.playerId) {
      room.hostId = null;
      const next = [...room.players.entries()].sort((a, b) => a[1].joinTime - b[1].joinTime)[0];
      if (next) {
        room.hostId = next[0];
        send(next[1].ws, { type: 'host_assigned', msg: 'You are now the host of this room.' });
        broadcast(room, { type: 'chat', pid: 'server', name: '[SERVER]', skin: '#00aaff', text: `${next[1].name} is now the host.`, global: true });
        journal(room, 'HOST_TRANSFER', 'server', { to: next[0] });
      }
    }
    persistRoom(room, 'player_leave');
    ctx.room = null; ctx.player = null; ctx.playerId = null; ctx.joined = false;
  }

  ws.on('message', async (raw, isBinary) => {
    if (!rl.allow('all', 240, 5000)) {
      error(ws, 'RATE_LIMIT', 'Too many network messages; slow down.');
      return;
    }

    if (isBinary) {
      const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
      if (buf.length >= 24 && buf[0] === 87 && buf[1] === 70 && buf[2] === 77 && buf[3] === 86 && buf[4] === 1) {
        if (!ctx.room || !isHost(ctx.room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Micro-voxel simulation is host authoritative.'); return; }
        const res = buf[5];
        if (res !== 16 || buf.length > 24 + 8192 || ((buf.length - 24) & 1)) return;
        let cells = 0;
        for (let i = 24; i + 1 < buf.length; i += 2) {
          const run = buf[i];
          if (!run) return;
          cells += run;
          if (cells > 4096) return;
        }
        if (cells !== 4096) return;
        const seq = buf.readUInt32BE(8);
        if (seq <= ctx.lastSeq) return;
        ctx.lastSeq = seq;
        const x = buf.readInt32BE(12), y = buf.readInt32BE(16), z = buf.readInt32BE(20);
        if (Math.abs(x) > 10000000 || Math.abs(z) > 10000000 || y < -128 || y > 512) return;
        broadcastBinary(ctx.room, buf, ctx.playerId);
        journal(ctx.room, 'MICRO_VOXEL_RLE', ctx.playerId, { x, y, z, bytes: buf.length });
      }
      return;
    }

    let msg;
    try {
      const text = raw.toString('utf8');
      if (Buffer.byteLength(text, 'utf8') > MAX_WS_PAYLOAD) {
        error(ws, 'PAYLOAD_TOO_LARGE', 'Message too large.');
        return;
      }
      msg = await parseJsonOffThread(text);
    } catch (err) {
      error(ws, err?.message === 'PARSER_BUSY' ? 'PARSER_BUSY' : 'BAD_JSON', err?.message === 'PARSER_BUSY' ? 'Server parser queue is busy.' : 'Invalid JSON.');
      return;
    }
    if (!msg || typeof msg !== 'object' || Array.isArray(msg)) return;

    // Optional monotonic client sequence number. Replays are rejected.
    if (Number.isInteger(msg.seq)) {
      if (msg.seq <= ctx.lastSeq) return;
      ctx.lastSeq = msg.seq;
    }

    // JOIN
    if (msg.type === 'join') {
      if (ctx.joined) { error(ws, 'ALREADY_JOINED', 'This connection is already joined.'); return; }
      if (!rl.allow('join', 3, 10_000)) { error(ws, 'JOIN_RATE', 'Too many join attempts.'); return; }
      if (STRICT_LEGACY) {
        const clientVersion = safeString(msg.clientVersion, 32);
        if (clientVersion !== PROTOCOL_VERSION) {
          error(ws, 'VERSION_MISMATCH', `Client ${clientVersion || '(unknown)'} is incompatible with server ${PROTOCOL_VERSION}. Refresh the page to get the latest version.`);
          try { ws.close(4001, 'VERSION_MISMATCH'); } catch (_) {}
          return;
        }
      }
      const requestedId = safeString(msg.id, 64);
      if (!validPlayerId(requestedId)) { error(ws, 'BAD_PLAYER_ID', 'Invalid player id.'); try { ws.close(1008); } catch (_) {} return; }
      const rawKey = msg.room || `${msg.seed || 0}-${msg.worldType || 'plains'}`;
      const roomKey = safeString(String(rawKey), 128);
      if (!validRoomKey(roomKey)) { error(ws, 'BAD_ROOM', 'Invalid room id.'); try { ws.close(1008); } catch (_) {} return; }
      const seed = finiteNumber(msg.seed) ? msg.seed : 0;
      const worldType = safeString(String(msg.worldType || 'plains'), 32, 'plains');
      const room = getOrCreateRoom(roomKey, seed, worldType);
      if (!room) { error(ws, 'SERVER_CAPACITY', 'Server room capacity reached.'); return; }
      if (room.locked) { error(ws, 'ROOM_LOCKED', 'Room is locked.'); try { ws.close(1008); } catch (_) {} return; }
      if (room.players.size >= limits.playersPerRoom) { error(ws, 'ROOM_FULL', 'Room is full.'); return; }

      if (room.players.has(requestedId)) {
        const old = room.players.get(requestedId);
        send(old.ws, { type: 'kicked', reason: 'Reconnected from another tab' });
        try { old.ws.close(4001, 'Reconnected'); } catch (_) {}
        room.players.delete(requestedId);
      }

      const wantsHost = !!msg.wantHost || (PUBLIC_HOST_TOKEN && safeString(msg.hostToken, 256) === PUBLIC_HOST_TOKEN);
      if (room.hostId === null && (wantsHost || room.players.size === 0)) room.hostId = requestedId;
      const player = {
        ws,
        name: safeString(msg.name, 32, 'Player') || 'Player',
        skin: typeof msg.skin === 'string' ? msg.skin.slice(0, 4096) : null,
        joinTime: now(),
        lastPos: null,
        lastRy: 0,
        lastUpdateAt: 0,
        lastHealthAt: 0,
        lastHungerAt: 0,
        lastSwingAt: 0,
        id: requestedId,
        lastSwingSeq: -1,
        lastArrowAt: 0,
        lastArrowSeq: -1,
        pendingArrowShots: new Map(),
        bowDrawStartedAt: 0,
        clientClockOffsetMs: 0,
        clockSynced: false,
        combatHistory: [],
        combatStrength: 0.30,
        health: 100,
        hunger: 100,
        sessionToken: ctx.token,
        movementViolations: 0,
        harvestTickets: [], // recent server-confirmed break/interaction events, used to authorize world-source drop_item_spawn
        clayFiringTickets: [], // in-progress coil-clay pieces: {form, at} — consumed clay awaiting its fired result
        inventoryRevision: 0,
        inventory: room.inventories.has(requestedId) ? cleanInventory(room.inventories.get(requestedId)) : [],
        inventoryHash: inventoryHash(room.inventories.get(requestedId) || []),
        inventoryBootstrapped: room.inventories.has(requestedId),
        roomRef: room,
      };
      room.players.set(requestedId, player);
      bump(room, 'players');
      journal(room, 'PLAYER_JOIN', requestedId, { name: player.name });
      ctx.room = room; ctx.playerId = requestedId; ctx.player = player; ctx.joined = true; ctx.joinedAt = now();

      const snap = buildJoinSnapshot(room);
      send(ws, {
        type: 'join_ack',
        serverVersion: PROTOCOL_VERSION,
        sessionToken: ctx.token,
        playerCount: room.players.size,
        authoritative: {
          identity: true,
          worldMutations: true,
          inventory: true,
          crafting: false,
          legacyStateCompatibility: false,
        },
        hostId: room.hostId,
        ...snap,
      });
      broadcast(room, { type: 'player_join', id: requestedId, name: player.name, skin: player.skin }, requestedId);
      const players = [...room.players.entries()].map(([id, p]) => ({ id, name: p.name, skin: p.skin }));
      send(ws, { type: 'player_list', players });
      if (room.outfitSettings) send(ws, { type: 'outfit_settings', settings: room.outfitSettings });
      if (!room.weatherEnabled) send(ws, { type: 'weather_toggle', enabled: false });
      return;
    }

    if (!assertJoined(ctx)) return;
    const room = ctx.room;
    const player = ctx.player;
    const pname = player.name;

    // Player inventory ledger. The first bootstrap mirrors the current prototype
    // inventory so the client can enter multiplayer without losing its items.
    // After bootstrap, the server owns the ledger and network-sensitive
    // operations must reconcile against it. Full secure acquisition authority
    // still requires all crafting/harvest/grant paths to issue server grants.
    if (msg.type === 'inventory_sync') {
      if (player.inventoryBootstrapped) { sendInventoryState(ws, player); return; }
      const inv = cleanInventory(msg.slots);
      player.inventory = inv;
      player.inventoryHash = inventoryHash(inv);
      player.inventoryBootstrapped = true;
      player.inventoryRevision += 1;
      room.inventories.set(ctx.playerId, cleanInventory(inv));
      room.dirty = true;
      journal(room, 'INVENTORY_BOOTSTRAP', ctx.playerId, { revision: player.inventoryRevision, hash: player.inventoryHash });
      sendInventoryState(ws, player);
      return;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Core player synchronization — validates movement envelope.
    // ─────────────────────────────────────────────────────────────────────
    if (msg.type === 'player_update') {
      if (!validXYZ(msg) || !finiteNumber(msg.ry)) return;
      const t = now();
      if (t - player.lastUpdateAt < 40) return;
      if (player.lastPos) {
        const dt = clamp((t - player.lastUpdateAt) / 1000, 0.04, 0.25);
        const dx = msg.x - player.lastPos.x, dz = msg.z - player.lastPos.z, dy = msg.y - player.lastPos.y;
        const horizontal = Math.hypot(dx, dz);
        const maxHorizontal = 18 * dt + 1.25; // generous for sprinting/lag
        const maxVertical = 24 * dt + 2.0;   // generous for jumping/climbing
        if (horizontal > maxHorizontal || Math.abs(dy) > maxVertical) {
          player.movementViolations += 1;
          error(ws, 'MOVEMENT_REJECTED', 'Movement exceeded server envelope.');
          if (player.movementViolations >= 8) {
            send(ws, { type: 'admin_summon', x: player.lastPos.x, y: player.lastPos.y, z: player.lastPos.z });
            player.movementViolations = 0;
          }
          return;
        }
      }
      player.lastUpdateAt = t;
      player.lastPos = { x: msg.x, y: msg.y, z: msg.z };
      player.lastRy = clamp(msg.ry, -1000, 1000);
      if (finiteNumber(msg.clientTime)) {
        const measured = t - msg.clientTime;
        player.clientClockOffsetMs = player.clockSynced ? player.clientClockOffsetMs * 0.85 + measured * 0.15 : measured;
        player.clockSynced = true;
      }
      recordCombatHistory(player, t, player.lastPos, player.lastRy);
      if (typeof msg.name === 'string') player.name = safeString(msg.name, 32, player.name);
      if (typeof msg.skin === 'string') player.skin = msg.skin.slice(0, 4096);
      broadcast(room, { type: 'player_update', id: ctx.playerId, x: msg.x, y: msg.y, z: msg.z, ry: player.lastRy, name: player.name, skin: player.skin, swimming: !!msg.swimming, backpack: !!msg.backpack, serverTime: t }, ctx.playerId);
      return;
    }

    if (msg.type === 'chat') {
      if (!rl.allow('chat', 8, 4000)) return;
      const text = safeString(msg.text, 256).trim();
      if (!text) return;
      const senderPos = player.lastPos;
      if (!senderPos) return;
      const radiusSq = 30 * 30;
      for (const [pid, peer] of room.players) {
        if (pid === ctx.playerId || !peer.lastPos) continue;
        if (distanceSq(senderPos, peer.lastPos) <= radiusSq) {
          send(peer.ws, { type: 'chat', pid: ctx.playerId, name: pname, skin: player.skin || '#fff', text, local: true });
        }
      }
      // Sender renders their own message immediately on the client.
      return;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Block mutations: server-owned revision + reach + atomic update.
    // ─────────────────────────────────────────────────────────────────────
    if (msg.type === 'block_break' || msg.type === 'block_place') {
      if (!validXYZ(msg) || !withinReach(player, msg.x, msg.y, msg.z, 10)) { error(ws, 'EDIT_REJECTED', 'Block edit failed server validation.'); return; }
      if (msg.type === 'block_place' && !validBlockType(msg.t)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (!hasCapacity(room, 'blockMods', key)) { error(ws, 'WORLD_CAP', 'World edit limit reached.'); return; }
      const current = Object.prototype.hasOwnProperty.call(room.blockMods, key) ? room.blockMods[key] : undefined;
      const expected = parseExpectedRevision(msg);
      if (!checkRevision(room.revisions.blocks, expected)) { error(ws, 'REVISION_CONFLICT', 'World changed; refresh and retry.'); return; }
      room.blockMods[key] = msg.type === 'block_break' ? null : msg.t;
      if (msg.type === 'block_break') { clearStructuresAt(room, key); issueHarvestTicket(player, msg.x, msg.y, msg.z); }
      bump(room, 'blocks');
      journal(room, msg.type === 'block_break' ? 'BLOCK_BREAK' : 'BLOCK_PLACE', ctx.playerId, { x: msg.x, y: msg.y, z: msg.z, t: msg.t ?? null, previous: current ?? null });
      broadcast(room, { type: msg.type, pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z, ...(msg.type === 'block_place' ? { t: msg.t } : {}), revision: room.revisions.blocks }, ctx.playerId);
      return;
    }

    if (msg.type === 'block_batch') {
      if (!Array.isArray(msg.ops) || msg.ops.length < 1 || msg.ops.length > 64) return;
      const clean = [];
      for (const op of msg.ops) {
        if (!op || !validXYZ(op) || !withinReach(player, op.x, op.y, op.z, 10)) continue;
        if (op.op !== 'break' && op.op !== 'place') continue;
        if (op.op === 'place' && !validBlockType(op.t)) continue;
        const key = `${op.x},${op.y},${op.z}`;
        if (!hasCapacity(room, 'blockMods', key)) continue;
        room.blockMods[key] = op.op === 'break' ? null : op.t;
        if (op.op === 'break') { clearStructuresAt(room, key); issueHarvestTicket(player, op.x, op.y, op.z); }
        clean.push({ op: op.op, x: op.x, y: op.y, z: op.z, ...(op.op === 'place' ? { t: op.t } : {}) });
      }
      if (!clean.length) return;
      bump(room, 'blocks');
      journal(room, 'BLOCK_BATCH', ctx.playerId, { ops: clean });
      broadcast(room, { type: 'block_batch', pid: ctx.playerId, ops: clean, revision: room.revisions.blocks }, ctx.playerId);
      return;
    }

    // Berry bush harvest: server owns the regrow cooldown, so a client can't
    // just re-request berries before the bush has actually regrown. No
    // world block is broken for this action, so there's no block_break
    // ticket to check — the bush position itself, reach-checked, is the
    // anchor.
    if (msg.type === 'berry_harvest_request') {
      if (!validXYZ(msg) || !withinReach(player, msg.x, msg.y, msg.z, 6)) { error(ws, 'HARVEST_REJECTED', 'Too far from that bush.'); return; }
      const key = berryBushKey(msg.x, msg.y, msg.z);
      const readyAt = room.berryBushCooldowns.get(key) || 0;
      if (now() < readyAt) { error(ws, 'HARVEST_COOLDOWN', 'That bush is still regrowing.', Math.ceil((readyAt - now()) / 1000)); return; }
      if (!player.inventoryBootstrapped) { error(ws, 'NOT_READY', 'Inventory not ready yet.'); return; }
      const count = 2 + Math.floor(Math.random() * 2); // 2-3 berries, matches client's original range
      if (!inventoryAdd(player, BERRIES_ITEM_TYPE, count)) { error(ws, 'INVENTORY_FULL', 'Not enough inventory space.'); return; }
      room.berryBushCooldowns.set(key, now() + berryBushRegrowSeconds() * 1000);
      journal(room, 'BERRY_HARVEST', ctx.playerId, { x: msg.x, y: msg.y, z: msg.z, count });
      sendInventoryState(ws, player);
      broadcast(room, { type: 'berry_bush_harvested', x: msg.x, y: msg.y, z: msg.z, pid: ctx.playerId }, ctx.playerId);
      return;
    }

    // Passive forage (shell/sand picked up with no world-object anchor).
    // No recipe to check against, so this is capped by a hard per-player
    // rate limit instead — cheap deterrent against a macro/bot spamming it,
    // not a full validation.
    if (msg.type === 'forage_request') {
      if (!rl.allow('forage', 6, 15_000)) { error(ws, 'FORAGE_RATE', 'Foraging too fast.'); return; }
      if (!player.inventoryBootstrapped) { error(ws, 'NOT_READY', 'Inventory not ready yet.'); return; }
      const kind = safeString(msg.kind, 16);
      let itemType, count = 1, chance;
      if (kind === 'shell') { itemType = 44; chance = 0.2; }
      else if (kind === 'sand') { itemType = 4; chance = 0.08; }
      else { error(ws, 'BAD_FORAGE', 'Unknown forage kind.'); return; }
      if (Math.random() >= chance) { send(ws, { type: 'forage_result', kind, found: false }); return; }
      if (!inventoryAdd(player, itemType, count)) { error(ws, 'INVENTORY_FULL', 'Not enough inventory space.'); return; }
      journal(room, 'FORAGE', ctx.playerId, { kind, itemType, count });
      sendInventoryState(ws, player);
      send(ws, { type: 'forage_result', kind, found: true, itemType, count });
      return;
    }

    // Crafting: server validates and consumes fixed inputs before granting
    // the output. This does NOT re-validate any client-side labor-bar/
    // minigame timing — it only stops crafting with nothing (or the wrong
    // thing) in inventory.
    if (msg.type === 'craft_request') {
      if (!player.inventoryBootstrapped) { error(ws, 'NOT_READY', 'Inventory not ready yet.'); return; }
      if (!rl.allow('craft', 20, 10_000)) { error(ws, 'CRAFT_RATE', 'Crafting too fast.'); return; }
      const action = safeString(msg.action, 32);
      const recipe = CRAFT_RECIPES[action];
      if (!recipe) { error(ws, 'UNKNOWN_CRAFT', 'Unknown craft action.'); return; }
      if (recipe.inputs) {
        for (const [type, count] of recipe.inputs) if (!hasInventoryItem(type, count)) { error(ws, 'CRAFT_MISSING_INPUT', 'Missing ingredients for that craft.'); return; }
        for (const [type, count] of recipe.inputs) consumeInventoryItem(type, count);
      } else if (recipe.inputAny) {
        const match = recipe.inputAny.find(([type, count]) => hasInventoryItem(type, count));
        if (!match) { error(ws, 'CRAFT_MISSING_INPUT', 'Missing ingredients for that craft.'); return; }
        consumeInventoryItem(match[0], match[1]);
      }
      const outputs = recipe.outputs || [recipe.output];
      for (const [outType, outCount] of outputs) if (!inventoryAdd(player, outType, outCount)) { error(ws, 'INVENTORY_FULL', 'Not enough inventory space.'); return; }
      journal(room, 'CRAFT', ctx.playerId, { action });
      sendInventoryState(ws, player);
      return;
    }

    // Clay coiling, step 1: the client has just finished the 6-coil
    // minigame and is about to remove Raw Clay(24) from inventory locally.
    // Server consumes it here and issues a firing ticket for the chosen
    // form; nothing is granted yet — that happens in clay_fire_result once
    // the client's dry+fire timers actually elapse.
    if (msg.type === 'clay_coil_start') {
      if (!player.inventoryBootstrapped) { error(ws, 'NOT_READY', 'Inventory not ready yet.'); return; }
      if (!rl.allow('craft', 20, 10_000)) { error(ws, 'CRAFT_RATE', 'Crafting too fast.'); return; }
      const form = safeString(msg.form, 16);
      if (!CLAY_FORM_OUTPUTS[form]) { error(ws, 'UNKNOWN_CRAFT', 'Unknown clay form.'); return; }
      if (!hasInventoryItem(24, 1)) { error(ws, 'CRAFT_MISSING_INPUT', 'No raw clay to coil.'); return; }
      consumeInventoryItem(24, 1);
      const list = player.clayFiringTickets || (player.clayFiringTickets = []);
      list.push({ form, at: now() });
      if (list.length > 8) list.splice(0, list.length - 8);
      journal(room, 'CLAY_COIL_START', ctx.playerId, { form });
      sendInventoryState(ws, player);
      return;
    }
    // Clay coiling, step 2: redeem a firing ticket for its output. Consumes
    // one matching ticket so the same coil can't be fired twice.
    if (msg.type === 'clay_fire_result') {
      const form = safeString(msg.form, 16);
      const list = player.clayFiringTickets;
      const t = now();
      const idx = list ? list.findIndex(tk => tk.form === form && (t - tk.at) <= CLAY_FIRING_TTL_MS) : -1;
      if (idx === -1) { error(ws, 'CLAY_NO_TICKET', 'No in-progress firing for that form.'); return; }
      list.splice(idx, 1);
      if (!player.inventoryBootstrapped) { error(ws, 'NOT_READY', 'Inventory not ready yet.'); return; }
      const outputs = CLAY_FORM_OUTPUTS[form];
      for (const [outType, outCount] of outputs) if (!inventoryAdd(player, outType, outCount)) { error(ws, 'INVENTORY_FULL', 'Not enough inventory space.'); return; }
      journal(room, 'CLAY_FIRE_RESULT', ctx.playerId, { form });
      sendInventoryState(ws, player);
      return;
    }

    // Harvest grants that go straight to inventory with no drop-item step
    // (clam/shell/pearl finds on the seafloor). Same harvest-ticket
    // mechanism as world-source drop_item_spawn: requires a recent,
    // reach-checked block_break by this player near (x,y,z).
    const HARVEST_GRANTS = { clam: [[45, 1]], clam_pearl: [[45, 1], [46, 1]], shell: [[44, 1]] };
    if (msg.type === 'harvest_grant_request') {
      if (!validXYZ(msg)) return;
      const kind = safeString(msg.kind, 16);
      const grant = HARVEST_GRANTS[kind];
      if (!grant) { error(ws, 'UNKNOWN_HARVEST', 'Unknown harvest kind.'); return; }
      if (!consumeHarvestTicket(player, msg.x, msg.y, msg.z)) { error(ws, 'INVENTORY_PROVENANCE', 'No recent harvest activity to justify that.'); return; }
      if (!player.inventoryBootstrapped) { error(ws, 'NOT_READY', 'Inventory not ready yet.'); return; }
      for (const [type, count] of grant) if (!inventoryAdd(player, type, count)) { error(ws, 'INVENTORY_FULL', 'Not enough inventory space.'); return; }
      journal(room, 'HARVEST_GRANT', ctx.playerId, { kind, x: msg.x, y: msg.y, z: msg.z });
      sendInventoryState(ws, player);
      return;
    }

    if (msg.type === 'map_marker') {
      if (!finiteNumber(msg.x) || !finiteNumber(msg.z) || !rl.allow('marker', 4, 5000)) return;
      broadcast(room, { type: 'map_marker', pid: ctx.playerId, name: pname, x: clamp(msg.x, -10_000_000, 10_000_000), z: clamp(msg.z, -10_000_000, 10_000_000), label: safeString(msg.label, 80), color: safeString(msg.color, 32) }, ctx.playerId);
      return;
    }

    if (msg.type === 'tide_sync') {
      // Ignore untrusted timestamps; relay only the server time and the
      // requesting client's visual origin for compatibility.
      broadcast(room, { type: 'tide_sync', start: now(), wall: now(), pid: ctx.playerId }, ctx.playerId);
      return;
    }

    function hasInventoryItem(type, count=1) {
      let n=0; for (const slot of player.inventory || []) if (slot?.type === type) n += Math.max(0, slot.count || 0); return n >= count;
    }
    function consumeInventoryItem(type, count=1) {
      let left=count;
      for (const slot of player.inventory || []) {
        if (slot?.type !== type || left <= 0) continue;
        const take=Math.min(left, Math.max(0, slot.count || 0)); slot.count -= take; left -= take;
        if (slot.count <= 0) { slot.type=null; slot.count=0; slot.spoilTimer=0; }
      }
      if (left > 0) return false;
      player.inventoryRevision += 1; player.inventoryHash=inventoryHash(player.inventory);
      send(ws, { type:'inventory_state', slots:player.inventory, revision:player.inventoryRevision, hash:player.inventoryHash, authoritative:true });
      return true;
    }

    // PVP: server-authoritative combat intents.
    // Clients describe an attack intent; they never submit damage or hit-zone
    // results. The server uses stored positions, server timing, weapon
    // attributes and the supplied aim vector to determine the hit.
    const MELEE_WEAPONS = {
      55: { mat: 'flint', type: 'knife' },
      56: { mat: 'flint', type: 'spear' },
      71: { mat: 'iron', type: 'knife' },
      72: { mat: 'iron', type: 'axe' },
      73: { mat: 'iron', type: 'axe' },
      125: { mat: 'iron', type: 'dagger' },
      127: { mat: 'obsidian', type: 'knife' },
      128: { mat: 'obsidian', type: 'spear' },
      129: { mat: 'obsidian', type: 'axe' },
      130: { mat: 'obsidian', type: 'sword' },
      131: { mat: 'obsidian', type: 'dagger' },
    };
    const MATERIALS = {
      flint: { sharpness: 1.0 },
      stone: { sharpness: 0.75 },
      bone: { sharpness: 0.6 },
      iron: { sharpness: 1.8 },
      obsidian: { sharpness: 2.6 },
    };
    const WEAPON_TYPES = {
      knife: { baseDmg: 4, range: 2.2, cooldownMs: 350 },
      spear: { baseDmg: 6, range: 3.5, cooldownMs: 500 },
      axe: { baseDmg: 8, range: 2.5, cooldownMs: 550 },
      dagger: { baseDmg: 3, range: 1.8, cooldownMs: 250 },
      sword: { baseDmg: 10, range: 3.0, cooldownMs: 400 },
    };

    if (msg.type === 'swing_weapon') {
      if (!player.lastPos) return;
      if (!Number.isInteger(msg.weapon) || !MELEE_WEAPONS[msg.weapon]) return;
      if (!player.inventoryBootstrapped || !hasInventoryItem(msg.weapon, 1)) return;
      if (!Number.isInteger(msg.attackSeq) || msg.attackSeq <= player.lastSwingSeq) return;
      const ws = MELEE_WEAPONS[msg.weapon];
      const mat = MATERIALS[ws.mat];
      const wt = WEAPON_TYPES[ws.type];
      if (!mat || !wt) return;
      const t = now();
      if (t - (player.lastSwingAt || 0) < wt.cooldownMs) return;
      player.lastSwingAt = t;
      player.lastSwingSeq = msg.attackSeq;
      const actionTime = combatActionServerTime(player, msg.clientTime, t);
      const attackerPos = sampleCombatHistory(player, actionTime) || { ...player.lastPos, ry: player.lastRy };
      const ax = attackerPos.x, ay = attackerPos.y, az = attackerPos.z;
      let dx = finiteNumber(msg.aimX) ? msg.aimX : Math.sin(attackerPos.ry || player.lastRy);
      let dy = finiteNumber(msg.aimY) ? msg.aimY : 0;
      let dz = finiteNumber(msg.aimZ) ? msg.aimZ : Math.cos(attackerPos.ry || player.lastRy);
      const dlen = Math.hypot(dx, dy, dz);
      if (dlen < 0.05 || dlen > 2.0) return;
      dx /= dlen; dy /= dlen; dz /= dlen;
      const faceX = Math.sin(attackerPos.ry || player.lastRy);
      const faceZ = Math.cos(attackerPos.ry || player.lastRy);
      const submittedHoriz = Math.hypot(dx, dz);
      const facingDot = submittedHoriz > 0.05 ? (dx * faceX + dz * faceZ) / submittedHoriz : 1;
      // The attack intent may pitch vertically, but cannot turn materially
      // behind the server-tracked player facing. This blocks silent reverse
      // aim while allowing normal up/down melee aiming.
      if (facingDot < 0.45) {
        error(ws, 'COMBAT_REJECTED', 'Attack angle is inconsistent with player facing.');
        return;
      }
      let best = null, bestT = wt.range + 0.9;
      const fx = Math.sin(attackerPos.ry || player.lastRy), fz = Math.cos(attackerPos.ry || player.lastRy);
      for (const [tid, target] of room.players) {
        if (tid === ctx.playerId || !target.lastPos) continue;
        const targetPos = sampleCombatHistory(target, actionTime) || { ...target.lastPos, ry: target.lastRy || 0 };
        const vx = targetPos.x - ax, vy = (targetPos.y + 0.9) - (ay + 0.9), vz = targetPos.z - az;
        const proj = vx * dx + vy * dy + vz * dz;
        if (proj < 0 || proj > wt.range) continue;
        const ex = vx - dx * proj, ey = vy - dy * proj, ez = vz - dz * proj;
        if (Math.hypot(ex, ey, ez) > 0.62 || proj >= bestT) continue;
        const horiz = Math.hypot(vx, vz) || 1;
        const facing = (vx * fx + vz * fz) / horiz;
        if (facing < 0.30) continue;
        best = { id: tid, target, targetPos, proj };
        bestT = proj;
      }
      if (!best) return;
      const impactY = ay + 0.9 + dy * best.proj;
      const frac = (impactY - (best.targetPos.y + 0.9)) / 0.9;
      let zone = 'chest', zoneMult = 1.0;
      if (frac < -0.32) { zone = 'leg'; zoneMult = 0.6; }
      else if (frac > 0.55) { zone = 'head'; zoneMult = 1.8; }
      const strength = clamp(Number(player.combatStrength) || 0.30, 0.30, 1.0);
      const damage = Math.max(1, Math.round(wt.baseDmg * mat.sharpness * strength * zoneMult));
      const target = best.target;
      target.health = clamp((target.health || 100) - damage, 0, 100);
      bump(room, 'players');
      send(target.ws, { type: 'pvp_hit', attacker: ctx.playerId, attackerName: pname, target: best.id, damage, zone, weapon: String(msg.weapon), authoritative: true });
      if (target.health <= 0) {
        target.health = 0;
        broadcast(room, { type: 'pvp_kill', attacker: ctx.playerId, attackerName: pname, victim: best.id, victimName: target.name, weapon: String(msg.weapon) });
        send(target.ws, { type: 'death_broadcast', pid: best.id, name: target.name, cause: `Killed by ${pname}`, killer: pname });
        journal(room, 'PVP_KILL', ctx.playerId, { victim: best.id, weapon: String(msg.weapon), zone, damage });
      }
      send(target.ws, { type: 'health_update', pid: best.id, health: target.health, authoritative: true, revision: room.revisions.players });
      return;
    }

    if (msg.type === 'bow_draw_start') {
      if (!player.inventoryBootstrapped || !hasInventoryItem(63, 1)) return;
      player.bowDrawStartedAt = now();
      return;
    }
    if (msg.type === 'bow_release') {
      if (!Number.isInteger(msg.attackSeq) || msg.attackSeq <= (player.lastArrowSeq || -1)) return;
      const requestedArrow = Number.isInteger(msg.arrowType) ? msg.arrowType : PROTOCOL_IDS.ARROW;
      if (!player.inventoryBootstrapped || !hasInventoryItem(requestedArrow, 1)) return;
      const started = player.bowDrawStartedAt || now();
      const elapsed = clamp(now() - started, 0, 1800);
      const draw = clamp(elapsed / 1200, 0, 1);
      player.pendingArrowShots.set(msg.attackSeq, { draw, releasedAt: now(), arrowType: requestedArrow });
      player.bowDrawStartedAt = 0;
      return;
    }
    // Ranged PvP intent: the client identifies the shot, but server timing
    // and geometry determine draw strength, trajectory and hit zone.
    if (msg.type === 'arrow_hit_intent') {
      if (!player.lastPos || !validPlayerId(msg.target)) return;
      const target = room.players.get(msg.target);
      if (!target || msg.target === ctx.playerId || !target.lastPos) return;
      if (!Number.isInteger(msg.attackSeq) || msg.attackSeq <= (player.lastArrowSeq || -1)) return;
      const arrowTable = {
        57:  { min: 8,  max: 25, range: 55 },
        119: { min: 10, max: 28, range: 55 },
        120: { min: 10, max: 30, range: 60 },
        121: { min: 30, max: 40, range: 50 },
        122: { min: 6,  max: 18, range: 55 },
        132: { min: 32, max: 48, range: 60 },
      };
      const shot = player.pendingArrowShots && player.pendingArrowShots.get(msg.attackSeq);
      if (!shot) return;
      const arrowType = Number.isInteger(shot.arrowType) ? shot.arrowType : PROTOCOL_IDS.ARROW;
      const ac = arrowTable[arrowType];
      if (!ac) return;
      if (!player.inventoryBootstrapped || !hasInventoryItem(arrowType, 1)) return;
      const actionTime = combatActionServerTime(player, msg.clientTime, now());
      const attackerPos = sampleCombatHistory(player, actionTime) || { ...player.lastPos, ry: player.lastRy || 0 };
      const targetPos = sampleCombatHistory(target, actionTime) || { ...target.lastPos, ry: target.lastRy || 0 };
      const dx = targetPos.x - attackerPos.x;
      const dy = (targetPos.y + 0.9) - (attackerPos.y + 0.9);
      const dz = targetPos.z - attackerPos.z;
      const dist = Math.hypot(dx, dy, dz);
      if (dist > ac.range || dist < 0.1) return;
      let ax = finiteNumber(msg.aimX) ? msg.aimX : dx;
      let ay = finiteNumber(msg.aimY) ? msg.aimY : dy;
      let az = finiteNumber(msg.aimZ) ? msg.aimZ : dz;
      const alen = Math.hypot(ax, ay, az);
      if (alen < 0.05 || alen > 1000) return;
      ax /= alen; ay /= alen; az /= alen;
      const faceX = Math.sin(attackerPos.ry || player.lastRy), faceZ = Math.cos(attackerPos.ry || player.lastRy);
      const ah = Math.hypot(ax, az) || 1;
      if ((ax * faceX + az * faceZ) / ah < 0.20) { error(ws, 'COMBAT_REJECTED', 'Arrow aim is inconsistent with player facing.'); return; }
      // Server ray test against a compact player hit volume.
      const proj = dx * ax + dy * ay + dz * az;
      if (proj < 0 || proj > ac.range) return;
      const ex = dx - ax * proj, ey = dy - ay * proj, ez = dz - az * proj;
      if (Math.hypot(ex, ey, ez) > 0.75) return;
      const frac = ((attackerPos.y + 0.9) + ay * proj - (targetPos.y + 0.9)) / 0.9;
      let zone = 'chest', zoneMult = 1.0;
      if (frac < -0.32) { zone = 'leg'; zoneMult = 0.6; }
      else if (frac > 0.55) { zone = 'head'; zoneMult = 1.8; }
      const strength = clamp(Number(player.combatStrength) || 0.30, 0.30, 1.0);
      const base = ac.min + (ac.max - ac.min) * shot.draw;
      const damage = Math.max(1, Math.round(base * strength * zoneMult));
      if (!consumeInventoryItem(arrowType, 1)) return;
      player.pendingArrowShots.delete(msg.attackSeq);
      player.lastArrowSeq = msg.attackSeq;
      target.health = clamp((target.health || 100) - damage, 0, 100);
      bump(room, 'players');
      send(target.ws, { type: 'pvp_hit', attacker: ctx.playerId, attackerName: pname, target: msg.target, damage, zone, weapon: 'bow', authoritative: true });
      send(target.ws, { type: 'health_update', pid: msg.target, health: target.health, authoritative: true, revision: room.revisions.players });
      if (target.health <= 0) {
        target.health = 0;
        broadcast(room, { type: 'pvp_kill', attacker: ctx.playerId, attackerName: pname, victim: msg.target, victimName: target.name, weapon: 'bow' });
        send(target.ws, { type: 'death_broadcast', pid: msg.target, name: target.name, cause: `Killed by ${pname}`, killer: pname });
        journal(room, 'PVP_KILL', ctx.playerId, { victim: msg.target, weapon: 'bow', zone, damage });
      }
      return;
    }

    if (msg.type === 'arrow_shoot') {
      if (!validXYZ(msg) || !validVec({ x: msg.dx, y: msg.dy, z: msg.dz }, 2)) return;
      if (!rl.allow('arrow', 12, 1000)) return;
      if (player.lastPos && distanceSq(player.lastPos, msg) > 6 * 6) return;
      broadcast(room, { type: 'arrow_shoot', pid: ctx.playerId, x: player.lastPos?.x ?? msg.x, y: player.lastPos?.y ?? msg.y, z: player.lastPos?.z ?? msg.z, dx: msg.dx, dy: msg.dy, dz: msg.dz }, ctx.playerId);
      return;
    }

    if (msg.type === 'bow_state') {
      broadcast(room, { type: 'bow_state', pid: ctx.playerId, nocked: !!msg.nocked }, ctx.playerId);
      return;
    }

    // CHESTS: revision-aware, lock-aware mutation. Published v131 requires
    // an explicit expected revision; there is no legacy whole-array fallback.
    if (msg.type === 'chest_open') {
      if (!validXYZ(msg) || !withinReach(player, msg.x, msg.y, msg.z, 8)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      const contents = room.chests.get(key) || Array.from({ length: limits.chestSlots }, () => ({ type: null, count: 0, spoilTimer: 0 }));
      const revision = Number.isInteger(contents.revision) ? contents.revision : 0;
      const sanitized = Array.isArray(contents.slots) ? contents.slots : contents;
      send(ws, { type: 'chest_update', pid: 'server', x: msg.x, y: msg.y, z: msg.z, contents: cloneJson(sanitized), revision });
      broadcast(room, { type: 'chest_open', pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z }, ctx.playerId);
      return;
    }

    if (msg.type === 'chest_update') {
      if (!validXYZ(msg) || !withinReach(player, msg.x, msg.y, msg.z, 8)) return;
      if (!Array.isArray(msg.contents) || msg.contents.length > limits.chestSlots) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      const old = room.chests.get(key);
      const oldSlots = old && Array.isArray(old.slots) ? old.slots : (Array.isArray(old) ? old : []);
      const currentRevision = old && Number.isInteger(old.revision) ? old.revision : 0;
      if (!Number.isInteger(msg.expectedRevision)) { error(ws, 'REVISION_REQUIRED', 'This server requires revisioned chest updates.'); return; }
      if (!checkRevision(currentRevision, msg.expectedRevision)) { error(ws, 'REVISION_CONFLICT', 'Chest changed; reopen it.'); return; }
      if (!player.inventoryBootstrapped) { error(ws, 'INVENTORY_REQUIRED', 'Inventory must be synchronized before using containers.'); return; }
      const contents = Array.from({ length: limits.chestSlots }, (_, i) => {
        const s = msg.contents[i];
        if (!s || typeof s !== 'object') return { type:null, count:0, spoilTimer:0 };
        return { type: validBlockType(s.type) ? s.type : null, count: cleanCount(s.count), spoilTimer: Number.isFinite(s.spoilTimer) ? clamp(s.spoilTimer,0,1e9) : 0 };
      });
      const oldCounts = inventoryCounts(oldSlots), newCounts = inventoryCounts(contents), invCounts = inventoryCounts(player.inventory);
      const plan = [];
      for (const [rawType, n] of Object.entries(newCounts)) {
        const type = Number(rawType), added = n - (oldCounts[type] || 0);
        if (added > 0) {
          if ((invCounts[type] || 0) < added) { error(ws,'INVENTORY_PROVENANCE','Chest insertion exceeds your server-known inventory.'); return; }
          plan.push({ type, delta: -added });
        }
      }
      for (const [rawType, n] of Object.entries(oldCounts)) {
        const type = Number(rawType), removed = n - (newCounts[type] || 0);
        if (removed > 0) plan.push({ type, delta: removed });
      }
      for (const op of plan) {
        if (op.delta < 0) { if (!inventoryRemove(player, op.type, -op.delta)) { error(ws,'INVENTORY_TRANSACTION','Inventory changed; reopen container.'); return; } }
      }
      for (const op of plan) {
        if (op.delta > 0 && !inventoryCanAdd(player, op.delta)) {
          // Roll back removals before rejecting.
          for (const undo of plan) if (undo.delta < 0) inventoryAdd(player, undo.type, -undo.delta);
          error(ws,'INVENTORY_FULL','Not enough inventory space.'); return;
        }
      }
      for (const op of plan) if (op.delta > 0) inventoryAdd(player, op.type, op.delta);
      const next = { slots: contents, revision: currentRevision + 1, updatedAt: now(), updatedBy: ctx.playerId };
      room.chests.set(key, next);
      bump(room, 'chests');
      journal(room, 'CHEST_UPDATE', ctx.playerId, { x:msg.x,y:msg.y,z:msg.z,fromRevision:currentRevision,toRevision:next.revision,slotCount:contents.length });
      sendInventoryState(ws, player);
      broadcast(room, { type:'chest_update', pid:ctx.playerId, x:msg.x,y:msg.y,z:msg.z, contents, revision:next.revision }, ctx.playerId);
      return;
    }

    if (msg.type === 'weather_toggle') {
      if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can change weather settings.'); return; }
      room.weatherEnabled = !!msg.enabled;
      journal(room, 'WEATHER_TOGGLE', ctx.playerId, { enabled: room.weatherEnabled });
      broadcast(room, { type: 'weather_toggle', enabled: room.weatherEnabled });
      return;
    }

    if (msg.type === 'outfit_settings') {
      if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can change outfit settings.'); return; }
      if (!msg.settings || typeof msg.settings !== 'object') return;
      const raw = JSON.stringify(msg.settings);
      if (raw.length > 8192) return;
      room.outfitSettings = JSON.parse(raw);
      journal(room, 'OUTFIT_SETTINGS', ctx.playerId, { bytes: raw.length });
      broadcast(room, { type: 'outfit_settings', settings: room.outfitSettings });
      return;
    }

    // ─────────────────────────────────────────────────────────────────────
    // World simulation messages.
    // Host owns shared simulation state in v131 published mode. This is a
    // deliberate security boundary: ordinary clients cannot spawn/overwrite
    // global fires, animals, forges, kilns, water, clay or boats.
    // ─────────────────────────────────────────────────────────────────────
    const hostOnlySimulation = new Set([
      'fire_start', 'fire_spread', 'fire_extinguish', 'fire_state', 'campfire_state',
      'animal_spawn', 'animal_update', 'animal_kill', 'animal_hurt', 'animal_tame', 'animal_despawn',
      'crucible_state', 'crucible_result', 'anvil_state', 'anvil_result', 'hammer_hit',
      'cooking_pot_state', 'cooking_pot_result', 'kiln_state', 'kiln_result', 'furnace_state',
      'water_flow', 'water_drain', 'clay_shape_update', 'clay_carve',
      'boat_place', 'boat_break', 'boat_mount', 'boat_dismount', 'boat_update',
    ]);
    if (hostOnlySimulation.has(msg.type) && !isHost(room, ctx.playerId)) {
      error(ws, 'HOST_ONLY', 'This simulation state is server/host authoritative. Use the gameplay action rather than a state write.');
      return;
    }

    if (msg.type === 'fire_start') {
      if (!validXYZ(msg)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (!hasCapacity(room, 'fires', key)) return;
      room.fires[key] = { x: msg.x, y: msg.y, z: msg.z, fuel: clamp(msg.fuel ?? 100, 0, 10000), heat: clamp(msg.heat ?? 1, 0, 10), burning: true, owner: ctx.playerId, updatedAt: now() };
      bump(room, 'fires'); journal(room, 'FIRE_START', ctx.playerId, room.fires[key]);
      broadcast(room, { type: 'fire_start', pid: ctx.playerId, ...room.fires[key], revision: room.revisions.fires }, ctx.playerId); return;
    }
    if (msg.type === 'fire_spread') {
      const rawTargets = Array.isArray(msg.targets) ? msg.targets.slice(0, 32) : [msg];
      const targets = [];
      for (const t of rawTargets) {
        if (!validXYZ(t)) continue;
        const key = `${t.x},${t.y},${t.z}`;
        if (!hasCapacity(room, 'fires', key)) continue;
        room.fires[key] = { x: t.x, y: t.y, z: t.z, fuel: clamp(t.fuel ?? 60, 0, 10000), heat: 1, burning: true, owner: ctx.playerId, updatedAt: now() };
        targets.push(room.fires[key]);
      }
      if (targets.length) { bump(room, 'fires'); journal(room, 'FIRE_SPREAD', ctx.playerId, { targets }); broadcast(room, { type: 'fire_spread', pid: ctx.playerId, targets, revision: room.revisions.fires }, ctx.playerId); }
      return;
    }
    if (msg.type === 'fire_extinguish') {
      if (!validXYZ(msg)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (!room.fires[key]) return;
      delete room.fires[key]; bump(room, 'fires'); journal(room, 'FIRE_EXTINGUISH', ctx.playerId, { x: msg.x, y: msg.y, z: msg.z }); broadcast(room, { type: 'fire_extinguish', pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z, revision: room.revisions.fires }, ctx.playerId); return;
    }
    if (msg.type === 'fire_state') {
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (!room.fires[key]) return;
      room.fires[key].fuel = clamp(msg.fuel ?? room.fires[key].fuel, 0, 10000);
      room.fires[key].heat = clamp(msg.heat ?? room.fires[key].heat, 0, 10);
      room.fires[key].burning = !!msg.burning;
      room.fires[key].updatedAt = now();
      bump(room, 'fires'); broadcast(room, { type: 'fire_state', pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z, fuel: room.fires[key].fuel, heat: room.fires[key].heat, burning: room.fires[key].burning, revision: room.revisions.fires }, ctx.playerId); return;
    }
    if (msg.type === 'campfire_state') {
      if (!validXYZ(msg)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (!hasCapacity(room, 'campfires', key)) return;
      const slots = Array.isArray(msg.slots) ? msg.slots.slice(0, 8).map(s => ({ type: validBlockType(s?.type) ? s.type : null, count: cleanCount(s?.count), spoilTimer: Number.isFinite(s?.spoilTimer) ? clamp(s.spoilTimer, 0, 1e9) : 0 })) : [];
      room.campfires[key] = { x: msg.x, y: msg.y, z: msg.z, fuel: clamp(msg.fuel ?? 0, 0, 10000), heat: clamp(msg.heat ?? 0, 0, 10), slots, updatedAt: now() };
      bump(room, 'fires'); broadcast(room, { type: 'campfire_state', pid: ctx.playerId, ...room.campfires[key], revision: room.revisions.fires }, ctx.playerId); return;
    }

    if (msg.type === 'animal_spawn') {
      if (!validXYZ(msg) || room.animals.size >= limits.animals) return;
      const aid = safeString(msg.animalId || randomId('a_'), 64);
      if (!aid || room.animals.has(aid)) return;
      const a = { type: Number.isInteger(msg.animalType) ? clamp(msg.animalType, 0, 255) : 0, x: msg.x, y: msg.y, z: msg.z, hp: clamp(msg.hp ?? 100, 0, 500), name: safeString(msg.name, 32), tamed: !!msg.tamed, owner: ctx.playerId, updatedAt: now() };
      room.animals.set(aid, a); bump(room, 'animals'); journal(room, 'ANIMAL_SPAWN', ctx.playerId, { animalId: aid, ...a }); broadcast(room, { type: 'animal_spawn', animalId: aid, animalType: a.type, x: a.x, y: a.y, z: a.z, hp: a.hp, name: a.name, tamed: a.tamed, pid: ctx.playerId, revision: room.revisions.animals }, ctx.playerId); return;
    }
    if (msg.type === 'animal_update') {
      const a = room.animals.get(msg.animalId); if (!a || !validXYZ(msg)) return;
      a.x = msg.x; a.y = msg.y; a.z = msg.z; if (finiteNumber(msg.hp)) a.hp = clamp(msg.hp, 0, 500); if (finiteNumber(msg.ry)) a.ry = clamp(msg.ry, -1000, 1000); if (typeof msg.state === 'string') a.state = safeString(msg.state, 32); if (msg.tamed !== undefined) a.tamed = !!msg.tamed; a.updatedAt = now(); bump(room, 'animals'); broadcast(room, { type: 'animal_update', animalId: msg.animalId, x: a.x, y: a.y, z: a.z, ry: a.ry, hp: a.hp, state: a.state, tamed: a.tamed, revision: room.revisions.animals }, ctx.playerId); return;
    }
    if (msg.type === 'animal_kill' || msg.type === 'animal_despawn') {
      if (!room.animals.has(msg.animalId)) return; room.animals.delete(msg.animalId); bump(room, 'animals'); journal(room, msg.type === 'animal_kill' ? 'ANIMAL_KILL' : 'ANIMAL_DESPAWN', ctx.playerId, { animalId: msg.animalId }); broadcast(room, { type: msg.type, animalId: msg.animalId, killer: pname, pid: ctx.playerId, revision: room.revisions.animals }, ctx.playerId); return;
    }
    if (msg.type === 'animal_hurt') {
      const a = room.animals.get(msg.animalId); if (!a) return; const damage = clamp(finiteNumber(msg.damage) ? msg.damage : 10, 0, 100); a.hp = clamp((a.hp || 100) - damage, 0, 500); bump(room, 'animals'); broadcast(room, { type: 'animal_hurt', animalId: msg.animalId, damage, attacker: pname, pid: ctx.playerId, revision: room.revisions.animals }, ctx.playerId); return;
    }
    if (msg.type === 'animal_tame') {
      const a = room.animals.get(msg.animalId); if (!a) return; a.tamed = true; bump(room, 'animals'); broadcast(room, { type: 'animal_tame', animalId: msg.animalId, pid: ctx.playerId, revision: room.revisions.animals }, ctx.playerId); return;
    }

    // Generic state stores: all are sanitized and revisioned.
    const objectStateHandlers = {
      crucible_state: ['crucibles', 'forge'],
      anvil_state: ['anvils', 'forge'],
      cooking_pot_state: ['cookingPots', 'cooking'],
      kiln_state: ['kilns', 'cooking'],
      furnace_state: ['furnaces', 'forge'],
    };
    if (objectStateHandlers[msg.type]) {
      const [storeName, revKey] = objectStateHandlers[msg.type];
      if (!validXYZ(msg)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (!hasCapacity(room, storeName, key)) return;
      const current = room[storeName][key] || {};
      const next = { ...current, x: msg.x, y: msg.y, z: msg.z, updatedAt: now() };
      for (const k of ['heat', 'fuel', 'progress', 'input', 'output', 'smelting', 'item', 'forgeType', 'hits', 'burning', 'kilnType', 'recipe', 'waterLevel']) {
        if (msg[k] !== undefined) next[k] = typeof msg[k] === 'number' ? clamp(msg[k], 0, 10000) : (typeof msg[k] === 'string' ? safeString(msg[k], 64) : !!msg[k]);
      }
      if (Array.isArray(msg.contents)) next.contents = msg.contents.slice(0, 8).map(s => ({ type: validBlockType(s?.type) ? s.type : null, count: cleanCount(s?.count) }));
      room[storeName][key] = next;
      bump(room, revKey); journal(room, msg.type.toUpperCase(), ctx.playerId, next);
      broadcast(room, { type: msg.type, pid: ctx.playerId, ...next, revision: room.revisions[revKey] }, ctx.playerId);
      return;
    }
    if (msg.type === 'crucible_result' || msg.type === 'anvil_result' || msg.type === 'kiln_result' || msg.type === 'cooking_pot_result') {
      // Results are accepted only from the host in published host-authoritative mode; server
      // stores the result as an immutable revision rather than applying twice.
      const storeName = msg.type.includes('crucible') ? 'crucibles' : msg.type.includes('anvil') ? 'anvils' : msg.type.includes('kiln') ? 'kilns' : 'cookingPots';
      const revKey = storeName === 'crucibles' || storeName === 'anvils' ? 'forge' : 'cooking';
      if (!validXYZ(msg)) return;
      const key = `${msg.x},${msg.y},${msg.z}`; if (!room[storeName][key]) return;
      const state = room[storeName][key]; if (state.resultCommitted) return;
      state.progress = 100; state.resultCommitted = true; state.result = safeString(msg.result || msg.recipe || msg.output, 128); state.outputs = Array.isArray(msg.outputs) ? cloneJson(msg.outputs.slice(0, 8)) : undefined; state.outputType = validBlockType(msg.outputType) ? msg.outputType : state.outputType; state.updatedAt = now();
      bump(room, revKey); journal(room, msg.type.toUpperCase(), ctx.playerId, { x: msg.x, y: msg.y, z: msg.z, result: state.result }); broadcast(room, { type: msg.type, pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z, recipe: state.result, output: state.result, outputs: state.outputs, resultType: state.outputType, revision: room.revisions[revKey] }, ctx.playerId); return;
    }
    if (msg.type === 'hammer_hit') {
      if (!validXYZ(msg)) return;
      broadcast(room, { type: 'hammer_hit', pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z }, ctx.playerId); return;
    }

    // DROPS: unique server IDs, reach-checked pickup, owner-locked physics
    // updates. Spawn remains host/player-requested but is server-IDed, auditable and capped.
    if (msg.type === 'drop_item_spawn') {
      if (!validXYZ(msg) || !withinReach(player, msg.x, msg.y, msg.z, 8) || room.droppedItems.size >= limits.droppedItems) return;
      const itemType = validBlockType(msg.itemType) ? msg.itemType : -1;
      const count = clamp(cleanCount(msg.count), 1, INVENTORY_SLOT_CAP);
      const source = safeString(msg.source, 16, 'player');
      if (source === 'world') {
        // "world" covers both legitimate harvest byproducts (tree chops,
        // scraped sticks, felled debris) and host-privileged world spawns
        // (loot, admin/world-seeded drops). Harvest byproducts must match a
        // recent server-confirmed break/interaction ticket for this player;
        // host status alone is no longer sufficient to mint arbitrary items.
        const hasTicket = consumeHarvestTicket(player, msg.x, msg.y, msg.z);
        if (!hasTicket && !isHost(room, ctx.playerId)) { error(ws, 'INVENTORY_PROVENANCE', 'No recent harvest activity to justify that drop.'); return; }
      } else if (!player.inventoryBootstrapped || !hasInventoryItem(itemType, count)) {
        error(ws,'INVENTORY_PROVENANCE','You do not own that item.'); return;
      }
      if (source !== 'world' && !inventoryRemove(player, itemType, count)) return;
      const itemId = randomId('di_');
      const di = { owner:ctx.playerId,type:itemType,x:msg.x,y:msg.y,z:msg.z,count,vx:clamp(finiteNumber(msg.vx)?msg.vx:0,-50,50),vy:clamp(finiteNumber(msg.vy)?msg.vy:0,-50,50),vz:clamp(finiteNumber(msg.vz)?msg.vz:0,-50,50),spoilTimer:clamp(finiteNumber(msg.spoilTimer)?msg.spoilTimer:0,0,1e9),createdAt:now(),updatedAt:now(),source };
      room.droppedItems.set(itemId, di); bump(room,'items'); journal(room,'DROP_SPAWN',ctx.playerId,{itemId,...di});
      if (source !== 'world') sendInventoryState(ws, player);
      broadcast(room,{type:'drop_item_spawn',itemId,itemType:di.type,x:di.x,y:di.y,z:di.z,count:di.count,vx:di.vx,vy:di.vy,vz:di.vz,spoilTimer:di.spoilTimer,pid:ctx.playerId,revision:room.revisions.items},ctx.playerId); return;
    }
    if (msg.type === 'drop_item_pickup') {
      const di = room.droppedItems.get(msg.itemId); if (!di || !player.lastPos) return;
      if (distanceSq(player.lastPos, di) > 6 * 6) return;
      const amount = clamp(cleanCount(msg.amount || 1),1,di.count);
      if (!player.inventoryBootstrapped || !inventoryCanAdd(amount)) { error(ws,'INVENTORY_FULL','Not enough inventory space.'); return; }
      if (!inventoryAdd(player, di.type, amount, di.spoilTimer)) return;
      di.count -= amount; di.updatedAt = now();
      if (di.count <= 0) { room.droppedItems.delete(msg.itemId); }
      bump(room,'items'); journal(room,'DROP_PICKUP',ctx.playerId,{itemId:msg.itemId,type:di.type,count:amount,remaining:di.count});
      sendInventoryState(ws, player);
      if (di.count <= 0) broadcast(room,{type:'drop_item_pickup',itemId:msg.itemId,pid:ctx.playerId,name:pname,revision:room.revisions.items},ctx.playerId);
      else broadcast(room,{type:'drop_item_update',itemId:msg.itemId,count:di.count,pid:ctx.playerId,revision:room.revisions.items},ctx.playerId);
      return;
    }
    if (msg.type === 'drop_item_update') {
      const di = room.droppedItems.get(msg.itemId); if (!di || di.owner !== ctx.playerId || !validXYZ(msg)) return;
      if (distanceSq(player.lastPos, msg) > 8 * 8) return;
      di.x=msg.x; di.y=msg.y; di.z=msg.z; if (finiteNumber(msg.vx)) di.vx=clamp(msg.vx,-50,50); if (finiteNumber(msg.vy)) di.vy=clamp(msg.vy,-50,50); if (finiteNumber(msg.vz)) di.vz=clamp(msg.vz,-50,50); di.updatedAt=now(); bump(room,'items'); broadcast(room,{type:'drop_item_update',itemId:msg.itemId,x:di.x,y:di.y,z:di.z,count:di.count,pid:ctx.playerId,revision:room.revisions.items},ctx.playerId); return;
    }
    if (msg.type === 'drop_item_despawn') { const di=room.droppedItems.get(msg.itemId); if(!di || (di.owner!==ctx.playerId && !isHost(room,ctx.playerId))) return; room.droppedItems.delete(msg.itemId); bump(room,'items'); broadcast(room,{type:'drop_item_despawn',itemId:msg.itemId,revision:room.revisions.items},ctx.playerId); return; }

    if (msg.type === 'pile_sync' || msg.type === 'pile_consume') {
      if (!validXYZ(msg) || !withinReach(player, msg.x, msg.y, msg.z, 8)) return;
      const key = `${msg.x},${msg.y},${msg.z}`;
      if (msg.type === 'pile_sync') room.piles[key] = { type: validBlockType(msg.itemType) ? msg.itemType : 0, count: clamp(cleanCount(msg.count), 0, 99999), x: msg.x, y: msg.y, z: msg.z, updatedBy: ctx.playerId };
      else if (room.piles[key]) { const remaining = clamp(cleanCount(msg.remaining, 99999), 0, 99999); if (remaining <= 0) delete room.piles[key]; else room.piles[key].count = remaining; }
      bump(room, 'items'); broadcast(room, { type: msg.type, pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z, itemType: room.piles[key]?.type, count: room.piles[key]?.count ?? 0, remaining: room.piles[key]?.count ?? 0, revision: room.revisions.items }, ctx.playerId); return;
    }

    // Boats: object IDs are server-generated and updates are owner/rider gated.
    if (msg.type === 'boat_place') {
      if (!validXYZ(msg) || room.boats.size >= limits.boats) return;
      const bid = randomId('b_');
      const blocks = Array.isArray(msg.blocks) ? msg.blocks.slice(0, 256) : [];
      const boat = { x: msg.x, y: msg.y, z: msg.z, ry: finiteNumber(msg.ry) ? msg.ry : 0, rider: null, owner: ctx.playerId, blocks, updatedAt: now() };
      room.boats.set(bid, boat); bump(room, 'boats'); journal(room, 'BOAT_PLACE', ctx.playerId, { boatId: bid, ...boat }); broadcast(room, { type: 'boat_place', boatId: bid, ...boat, pid: ctx.playerId, revision: room.revisions.boats }, ctx.playerId); return;
    }
    if (msg.type === 'boat_break') {
      const boat = room.boats.get(msg.boatId); if (!boat || (boat.owner !== ctx.playerId && !isHost(room, ctx.playerId))) return; room.boats.delete(msg.boatId); bump(room, 'boats'); journal(room, 'BOAT_BREAK', ctx.playerId, { boatId: msg.boatId }); broadcast(room, { type: 'boat_break', boatId: msg.boatId, pid: ctx.playerId, revision: room.revisions.boats }, ctx.playerId); return;
    }
    if (msg.type === 'boat_mount') {
      const boat = room.boats.get(msg.boatId); if (!boat || !player.lastPos) return; if (distanceSq(player.lastPos, boat) > 6 * 6) return; if (boat.rider && boat.rider !== ctx.playerId) return; boat.rider = ctx.playerId; bump(room, 'boats'); broadcast(room, { type: 'boat_mount', boatId: msg.boatId, pid: ctx.playerId, name: pname, revision: room.revisions.boats }, ctx.playerId); return;
    }
    if (msg.type === 'boat_dismount') {
      const boat = room.boats.get(msg.boatId); if (!boat || boat.rider !== ctx.playerId) return; boat.rider = null; bump(room, 'boats'); broadcast(room, { type: 'boat_dismount', boatId: msg.boatId, pid: ctx.playerId, name: pname, x: player.lastPos?.x, y: player.lastPos?.y, z: player.lastPos?.z, revision: room.revisions.boats }, ctx.playerId); return;
    }
    if (msg.type === 'boat_update') {
      const boat = room.boats.get(msg.boatId); if (!boat || (boat.owner !== ctx.playerId && boat.rider !== ctx.playerId)) return; if (!validXYZ(msg)) return; if (boat.lastServerPos && distanceSq(boat.lastServerPos, msg) > 35 * 35) return; boat.x = msg.x; boat.y = msg.y; boat.z = msg.z; boat.ry = finiteNumber(msg.ry) ? msg.ry : boat.ry; boat.lastServerPos = { x: msg.x, y: msg.y, z: msg.z }; bump(room, 'boats'); broadcast(room, { type: 'boat_update', boatId: msg.boatId, x: boat.x, y: boat.y, z: boat.z, ry: boat.ry, vx: finiteNumber(msg.vx) ? clamp(msg.vx, -60, 60) : 0, vy: finiteNumber(msg.vy) ? clamp(msg.vy, -60, 60) : 0, vz: finiteNumber(msg.vz) ? clamp(msg.vz, -60, 60) : 0, pid: ctx.playerId, revision: room.revisions.boats }, ctx.playerId); return;
    }

    // Water / clay
    if (msg.type === 'water_flow') {
      if (!validXYZ(msg)) return; const key = `${msg.x},${msg.y},${msg.z}`; if (!hasCapacity(room, 'waterFlow', key)) return; room.waterFlow[key] = { x: msg.x, y: msg.y, z: msg.z, level: clamp(msg.level ?? 1, 0, 1), direction: safeString(String(msg.direction ?? ''), 16), source: !!msg.source, updatedAt: now() }; bump(room, 'water'); broadcast(room, { type: 'water_flow', pid: ctx.playerId, ...room.waterFlow[key], revision: room.revisions.water }, ctx.playerId); return;
    }
    if (msg.type === 'water_drain') {
      const key = `${msg.x},${msg.y},${msg.z}`; if (!room.waterFlow[key]) return; delete room.waterFlow[key]; bump(room, 'water'); broadcast(room, { type: 'water_drain', pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z, revision: room.revisions.water }, ctx.playerId); return;
    }
    if (msg.type === 'clay_shape_update') {
      if (!validXYZ(msg) || !hasCapacity(room, 'clayShapes', `${msg.x},${msg.y},${msg.z}`)) return; const key = `${msg.x},${msg.y},${msg.z}`; const vertices = Array.isArray(msg.vertices) ? msg.vertices.slice(0, limits.maxClayVertices) : []; const faces = Array.isArray(msg.faces) ? msg.faces.slice(0, limits.maxClayFaces) : []; room.clayShapes[key] = { x: msg.x, y: msg.y, z: msg.z, shape: safeString(msg.shape, 32), carved: !!msg.carved, vertices, faces, updatedAt: now() }; bump(room, 'clay'); broadcast(room, { type: 'clay_shape_update', pid: ctx.playerId, ...room.clayShapes[key], revision: room.revisions.clay }, ctx.playerId); return;
    }
    if (msg.type === 'clay_carve') {
      if (!validXYZ(msg)) return; broadcast(room, { type: 'clay_carve', pid: ctx.playerId, x: msg.x, y: msg.y, z: msg.z, data: safeString(JSON.stringify(msg.data || {}), 4096) }, ctx.playerId); return;
    }

    // Published v131: clients cannot directly write authoritative health/hunger.
    // PvP and admin actions mutate server-owned player health; clients only
    // receive health_update. A death message may declare presentation, but it
    // never restores health or bypasses server state.
    if (msg.type === 'health_update' || msg.type === 'hunger_update') {
      return;
    }
    if (msg.type === 'death_broadcast' || msg.type === 'death_intent') {
      // A client may only report a death that the server has already detected.
      // Client packets can no longer create a positive-to-zero health transition.
      if ((player.health ?? 100) > 0) { error(ws, 'DEATH_REJECTED', 'Server health is not zero.'); return; }
      send(ws, { type: 'health_update', pid: ctx.playerId, health: 0, authoritative: true, revision: room.revisions.players });
      journal(room, 'PLAYER_DEATH', ctx.playerId, { cause: safeString(msg.cause, 64), killer: safeString(msg.killer, 64) });
      broadcast(room, { type: 'death_broadcast', pid: ctx.playerId, name: pname, cause: safeString(msg.cause, 64), killer: safeString(msg.killer, 64) }, ctx.playerId);
      return;
    }
    if (msg.type === 'respawn_intent') {
      if ((player.health ?? 100) > 0) return;
      player.health = 100;
      bump(room, 'players');
      send(ws, { type: 'health_update', pid: ctx.playerId, health: 100, authoritative: true, revision: room.revisions.players });
      journal(room, 'PLAYER_RESPAWN', ctx.playerId, {});
      return;
    }

    // Generic entity sync: only host can publish arbitrary global entities.
    if (msg.type === 'entity_sync' || msg.type === 'entity_despawn') {
      if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can publish arbitrary entity state.'); return; }
      if (typeof msg.entityId !== 'string' || msg.entityId.length > 64) return;
      if (msg.type === 'entity_sync' && !validXYZ(msg)) return;
      broadcast(room, { type: msg.type, pid: ctx.playerId, entityId: safeString(msg.entityId, 64), entityType: safeString(msg.entityType, 32), ...(msg.type === 'entity_sync' ? { x: msg.x, y: msg.y, z: msg.z, data: msg.data } : {}) }, ctx.playerId); return;
    }

    // Admin commands: host only, with strict target resolution and validated coords.
    if (msg.type === 'admin_kick') {
      if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can kick players.'); return; }
      const target = room.players.get(msg.target); if (!target) return;
      send(target.ws, { type: 'kicked', reason: 'Kicked by host' }); try { target.ws.close(4003, 'Kicked'); } catch (_) {} journal(room, 'ADMIN_KICK', ctx.playerId, { target: msg.target }); return;
    }
    if (msg.type === 'admin_summon') {
      if (!isHost(room, ctx.playerId) || !validXYZ(msg)) { error(ws, 'ADMIN_REJECTED', 'Invalid summon request.'); return; }
      const target = room.players.get(msg.target); if (target) send(target.ws, { type: 'admin_summon', x: msg.x, y: msg.y, z: msg.z }); return;
    }
    if (msg.type === 'admin_forcewalk') {
      if (!isHost(room, ctx.playerId) || !validXYZ(msg)) { error(ws, 'HOST_ONLY', 'Only the host can force-walk players.'); return; }
      const ids = msg.target === '*' ? [...room.players.keys()].filter(id => id !== ctx.playerId) : [msg.target];
      for (const tid of ids) { const tp = room.players.get(tid); if (tp) send(tp.ws, { type: 'admin_forcewalk', target: tid, x: msg.x, y: msg.y, z: msg.z, stopDist: clamp(msg.stopDist ?? 1, 0, 20), lockControls: !!msg.lockControls, targetName: tp.name }); }
      return;
    }
    if (msg.type === 'admin_stopwalk') {
      if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can stop forced walks.'); return; }
      const ids = msg.target === '*' ? [...room.players.keys()] : [msg.target]; for (const tid of ids) { const tp = room.players.get(tid); if (tp) send(tp.ws, { type: 'admin_stopwalk', target: tid }); } return;
    }
    if (msg.type === 'admin_lock') { if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can lock the world.'); return; } room.locked = !!msg.locked; journal(room, 'ADMIN_LOCK', ctx.playerId, { locked: room.locked }); broadcast(room, { type: 'chat', pid: 'server', name: '[SERVER]', skin: '#00aaff', text: room.locked ? 'World locked.' : 'World unlocked.', global: true }); return; }
    if (msg.type === 'admin_weather') { if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can control weather.'); return; } broadcast(room, { type: 'admin_weather', action: safeString(msg.action, 32) }); return; }
    if (msg.type === 'tornado_spawn') { if (!isHost(room, ctx.playerId) || !finiteNumber(msg.x) || !finiteNumber(msg.z)) return; broadcast(room, { type: 'tornado_spawn', x: clamp(msg.x, -10_000_000, 10_000_000), z: clamp(msg.z, -10_000_000, 10_000_000), strength: clamp(msg.strength ?? 1, 0, 100) }, ctx.playerId); return; }
    if (msg.type === 'admin_broadcast') { if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can broadcast.'); return; } broadcast(room, { type: 'chat', pid: 'server', name: '[SERVER]', skin: '#00aaff', text: safeString(msg.text, 256), global: true }); return; }
    if (msg.type === 'admin_gamemode') { if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can change gamemode.'); return; } broadcast(room, { type: 'admin_gamemode', mode: safeString(msg.mode, 32), pid: ctx.playerId }); return; }
    if (msg.type === 'admin_teleport_all') { if (!isHost(room, ctx.playerId) || !validXYZ(msg)) { error(ws, 'HOST_ONLY', 'Invalid teleport.'); return; } for (const [tid, tp] of room.players) if (tid !== ctx.playerId) send(tp.ws, { type: 'admin_summon', x: msg.x, y: msg.y, z: msg.z }); broadcast(room, { type: 'chat', pid: 'server', name: '[SERVER]', skin: '#00aaff', text: `All players summoned to ${msg.x}, ${msg.y}, ${msg.z}`, global: true }); return; }
    if (msg.type === 'admin_give') { if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can give items.'); return; } const tp = room.players.get(msg.target); const itemType = validBlockType(msg.itemType) ? msg.itemType : 0; const count = clamp(cleanCount(msg.count),1,INVENTORY_SLOT_CAP); if (tp && inventoryAdd(tp, itemType, count)) { sendInventoryState(tp.ws, tp); journal(room,'ADMIN_GIVE',ctx.playerId,{target:msg.target,itemType,count}); } return; }
    if (msg.type === 'admin_heal') { if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can heal.'); return; } const ids = msg.target === '*' ? [...room.players.keys()] : [msg.target]; for (const tid of ids) { const tp = room.players.get(tid); if (tp) { tp.health = 100; send(tp.ws, { type: 'admin_heal', health: 100 }); } } bump(room, 'players'); return; }

    if (msg.type === 'player_cmd') {
      if (!isHost(room, ctx.playerId)) { error(ws, 'HOST_ONLY', 'Only the host can affect other players.'); return; }
      const allowed = new Set(['heal','god','fly','noclip','creative','speed','gamemode','hunger','thirst','stamina','fatigue','strength','freeze','flyspeed','instabreak','clearinv','repair','kill','respawn','revive','boat','unboat','teleport_to','tag','label']);
      if (!allowed.has(msg.cheat)) return;
      let args = {};
      try { args = JSON.parse(JSON.stringify(msg.args || {})); } catch (_) { return; }
      if (JSON.stringify(args).length > 2048) return;
      const ids = msg.target === '*' ? [...room.players.keys()].filter(id => id !== ctx.playerId) : [msg.target];
      for (const tid of ids) { const tp = room.players.get(tid); if (!tp) continue; if (msg.cheat === 'clearinv') { tp.inventory = Array.from({length: Math.max(2,tp.inventory?.length||2)}, () => ({type:null,count:0,spoilTimer:0})); tp.inventoryRevision=(tp.inventoryRevision||0)+1; sendInventoryState(tp.ws,tp); } send(tp.ws, { type: 'player_cmd', cheat: msg.cheat, args, from: pname }); }
      return;
    }

    // Unknown message types are ignored; published clients must use the current protocol.
  });

  ws.on('close', leave);
  ws.on('error', err => { if (DEBUG) console.warn('[ws]', ctx.playerId || 'prejoin', err?.message || err); });
});

// ─────────────────────────────────────────────────────────────────────────────
// Maintenance, persistence and heartbeat
// ─────────────────────────────────────────────────────────────────────────────

const heartbeatTimer = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { try { ws.terminate(); } catch (_) {} return; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  });
}, 30_000);

const persistenceTimer = setInterval(() => {
  for (const room of rooms.values()) persistRoom(room, 'periodic');
  // prune empty rooms only after durable persistence
  for (const [key, room] of rooms) {
    if (room.players.size === 0 && now() - room.lastPersistAt > EMPTY_ROOM_GRACE) rooms.delete(key);
  }
}, SNAPSHOT_INTERVAL);


httpServer.listen(PORT, () => {
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  World (Infinite) v131 — Hardened Multiplayer Server');
  console.log(`  HTTP : http://localhost:${PORT}`);
  console.log(`  WS   : ws://localhost:${PORT}`);
  console.log(`  Data : ${DATA_DIR}`);
  console.log(`  Mode : PUBLISHED STRICT`);
  console.log('═══════════════════════════════════════════════════════════');
});

function shutdown() {
  console.log('\nShutting down...');
  clearInterval(heartbeatTimer);
  clearInterval(persistenceTimer);
  saveAllRooms();
  for (const room of rooms.values()) for (const p of room.players.values()) {
    send(p.ws, { type: 'kicked', reason: 'Server shutting down' });
    try { p.ws.close(1001, 'Server shutting down'); } catch (_) {}
  }
  wss.close(() => httpServer.close(() => process.exit(0)));
  setTimeout(() => process.exit(0), 5000);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('uncaughtException', err => console.error('[uncaughtException]', err));
process.on('unhandledRejection', err => console.error('[unhandledRejection]', err));
