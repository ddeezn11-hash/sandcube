/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  server.js — Survival game WebSocket server with world saving           ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  npm install ws                                                          ║
 * ║  node server.js                                                          ║
 * ║                                                                          ║
 * ║  HOW WORLD SAVING WORKS                                                  ║
 * ║  ─────────────────────────────────────────────────────────────────────  ║
 * ║  The game generates terrain from a seed — every player with the same    ║
 * ║  seed gets identical terrain automatically. The server only needs to    ║
 * ║  store CHANGES on top of that: blocks broken (stored as null) and       ║
 * ║  blocks placed (stored as block type id).                               ║
 * ║                                                                          ║
 * ║  When a new player joins a room the server sends them all stored mods   ║
 * ║  inside join_ack. The client applies them so the world matches          ║
 * ║  everyone else's — even if they joined hours later.                     ║
 * ║                                                                          ║
 * ║  SAME SEED = SAME WORLD. The worldKey the game sends is already         ║
 * ║  "{seed}_{worldType}" so two players on the same seed are automatically ║
 * ║  in the same room with the same terrain.                                 ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

'use strict';

const { WebSocketServer, WebSocket } = require('ws');

const PORT = parseInt(process.env.PORT) || 8080;
const wss  = new WebSocketServer({ port: PORT });

// ─────────────────────────────────────────────────────────────────────────────
// ROOM STRUCTURE
//
// rooms  Map<roomId, Room>
//
// Room {
//   host:      string|null        playerId of current host
//   locked:    boolean            when true, new joins refused
//   seed:      number|null        world seed (set by first joiner)
//   worldType: string|null        world type (set by first joiner)
//   mods:      Map<"x,y,z", t>   world changes: t = block type, or null = broken
//   players:   Map<playerId, Player>
// }
//
// Player {
//   ws, id, name, skin, room,
//   x, y, z, ry,
//   joinTime
// }
// ─────────────────────────────────────────────────────────────────────────────

const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      host:      null,
      locked:    false,
      seed:      null,
      worldType: null,
      mods:      new Map(),   // "x,y,z" → blockType | null
      players:   new Map(),
    });
  }
  return rooms.get(roomId);
}

function pruneRoom(roomId) {
  const room = rooms.get(roomId);
  if (room && room.players.size === 0) {
    // Keep the world data even when empty so the world persists between sessions.
    // Only delete if it was never used (no mods, no seed).
    if (!room.seed && room.mods.size === 0) {
      rooms.delete(roomId);
      console.log(`[room]   "${roomId}" deleted (empty, no world data)`);
    } else {
      console.log(`[room]   "${roomId}" empty but world saved (${room.mods.size} mods)`);
    }
  }
}

// Serialize mods Map → plain object for JSON
function modsToObj(mods) {
  const obj = {};
  for (const [k, v] of mods) obj[k] = v;
  return obj;
}

// ─────────────────────────────────────────────────────────────────────────────
// SEND HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(room, obj, excludeId = null) {
  const data = JSON.stringify(obj);
  for (const [pid, p] of room.players) {
    if (pid === excludeId) continue;
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(data);
  }
}

function sendTo(room, targetId, obj) {
  const p = room.players.get(targetId);
  if (p) send(p.ws, obj);
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTION HANDLER
// ─────────────────────────────────────────────────────────────────────────────

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?')
    .split(',')[0].trim();
  console.log(`[connect] ${ip}`);

  let player = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    // ── JOIN ─────────────────────────────────────────────────────────────────
    if (msg.type === 'join') {
      const { id, room: roomId, name, skin, wantHost, seed, worldType } = msg;

      if (!id || typeof id !== 'string') return;
      if (!roomId || typeof roomId !== 'string') return;

      const room = getRoom(roomId);

      // Reject if locked (unless this is the host reconnecting)
      if (room.locked && room.host !== id) {
        send(ws, { type: 'error', msg: '🔒 World is locked — the host has closed it.' });
        ws.close();
        return;
      }

      // Clean up stale entry for this id (reconnect)
      if (room.players.has(id)) {
        const old = room.players.get(id);
        if (old.ws !== ws) try { old.ws.terminate(); } catch {}
        room.players.delete(id);
      }

      player = {
        ws,
        id,
        name:     (typeof name === 'string' ? name : 'Unknown').slice(0, 32),
        skin:     (typeof skin === 'string' ? skin : '#ffffff').slice(0, 20),
        room:     roomId,
        x: 0, y: 0, z: 0, ry: 0,
        joinTime: Date.now(),
      };
      room.players.set(id, player);

      // ── WORLD SEED REGISTRATION ───────────────────────────────────────────
      // roomId is already "{seed}_{worldType}" so mismatches can't normally
      // happen, but we guard anyway.
      // Rule: first joiner sets the seed and it is NEVER overwritten.
      // If a later joiner sends a different seed we reject them — it means
      // they changed type/seed after seeing the code but before launching.
      if (room.seed === null) {
        if (typeof seed === 'number') {
          room.seed      = seed;
          room.worldType = typeof worldType === 'string' ? worldType : 'default';
          console.log(`[world]  Room "${roomId}" registered seed=${room.seed} type=${room.worldType}`);
        }
      } else if (typeof seed === 'number' && seed !== room.seed) {
        // Seed mismatch — reject with a helpful message
        send(ws, {
          type: 'error',
          msg:  `Seed mismatch: this room is seed ${room.seed} (${room.worldType}). Your client sent seed ${seed}. Please use the correct room code.`
        });
        room.players.delete(id);
        ws.close();
        return;
      }

      // ── HOST ASSIGNMENT ───────────────────────────────────────────────────
      // Only assign host if the room has no host yet.
      // wantHost=true is a request, not a guarantee — first joiner always wins.
      // This prevents a second "Host a World" click from stealing the role.
      if (!room.host) {
        room.host = id;
        send(ws, { type: 'host_assigned', id });
        console.log(`[host]   "${player.name}" is host of "${roomId}"`);
      }

      // ── SEND CURRENT ROSTER ───────────────────────────────────────────────
      const roster = [];
      for (const [pid, p] of room.players) {
        if (pid !== id) roster.push({ id: pid, name: p.name, skin: p.skin });
      }
      send(ws, { type: 'player_list', players: roster });

      // ── JOIN ACK + WORLD MODS ─────────────────────────────────────────────
      // Send the new player all stored world modifications so their world
      // matches what everyone else sees. This is the core of world saving.
      send(ws, {
        type:        'join_ack',
        playerCount: room.players.size,
        seed:        room.seed,
        worldType:   room.worldType,
        mods:        modsToObj(room.mods),   // all blocks broken/placed so far
        modCount:    room.mods.size,
      });

      // Tell everyone else this player joined
      broadcast(room, { type: 'player_join', id, name: player.name, skin: player.skin }, id);

      console.log(`[join]   "${player.name}" → "${roomId}" (${room.players.size} players, ${room.mods.size} world mods)`);
      return;
    }

    // All other messages need an active session
    if (!player) return;
    const room = rooms.get(player.room);
    if (!room) return;

    switch (msg.type) {

      // ── POSITION UPDATE ─────────────────────────────────────────────────
      case 'player_update': {
        player.x  = typeof msg.x  === 'number' ? msg.x  : player.x;
        player.y  = typeof msg.y  === 'number' ? msg.y  : player.y;
        player.z  = typeof msg.z  === 'number' ? msg.z  : player.z;
        player.ry = typeof msg.ry === 'number' ? msg.ry : player.ry;
        broadcast(room, {
          type: 'player_update',
          id:   player.id, name: player.name, skin: player.skin,
          x: player.x, y: player.y, z: player.z, ry: player.ry,
        }, player.id);
        break;
      }

      // ── BLOCK BREAK ─────────────────────────────────────────────────────
      // Save null (= broken) at this position, relay to others.
      case 'block_break': {
        const k = `${msg.x},${msg.y},${msg.z}`;
        room.mods.set(k, null);  // null means "this block was broken"
        broadcast(room, {
          type: 'block_break', pid: player.id,
          x: msg.x, y: msg.y, z: msg.z,
        }, player.id);
        break;
      }

      // ── BLOCK PLACE ─────────────────────────────────────────────────────
      // Save block type at this position, relay to others.
      case 'block_place': {
        const k = `${msg.x},${msg.y},${msg.z}`;
        room.mods.set(k, msg.t);  // t = block type id
        broadcast(room, {
          type: 'block_place', pid: player.id,
          x: msg.x, y: msg.y, z: msg.z, t: msg.t,
        }, player.id);
        break;
      }

      // ── CHAT ────────────────────────────────────────────────────────────
      case 'chat': {
        const text = String(msg.text || '').replace(/<[^>]*>/g, '').trim().slice(0, 200);
        if (!text) break;
        broadcast(room, {
          type: 'chat',
          name: typeof msg.name === 'string' ? msg.name.slice(0, 32) : player.name,
          skin: typeof msg.skin === 'string' ? msg.skin.slice(0, 20) : player.skin,
          text,
        });
        console.log(`[chat]   [${player.room}] ${player.name}: ${text}`);
        break;
      }

      // ── MAP MARKER ──────────────────────────────────────────────────────
      case 'map_marker': {
        broadcast(room, {
          type:  'map_marker',
          x: msg.x, z: msg.z,
          label: String(msg.label || '').slice(0, 64),
          color: String(msg.color || '#ffff00').slice(0, 20),
          pid:   player.id, name: player.name,
        }, player.id);
        break;
      }

      // ── PVP HIT ─────────────────────────────────────────────────────────
      case 'pvp_hit': {
        sendTo(room, msg.target, {
          type:         'pvp_hit',
          attacker:     player.id,
          attackerName: player.name,
          target:       msg.target,
          damage:       Math.min(Math.max(Number(msg.damage) || 0, 0), 100),
        });
        break;
      }

      // ── PVP KILL ────────────────────────────────────────────────────────
      case 'pvp_kill': {
        broadcast(room, {
          type:         'pvp_kill',
          attacker:     player.id, attackerName: player.name,
          target:       msg.target,
          targetName:   room.players.get(msg.target)?.name ?? String(msg.target),
        });
        break;
      }

      // ── ADMIN: KICK ─────────────────────────────────────────────────────
      case 'admin_kick': {
        if (room.host !== player.id) break;
        const t = room.players.get(msg.target);
        if (!t) break;
        send(t.ws, { type: 'kicked', reason: 'You were kicked by the host.' });
        t.ws.close();
        console.log(`[admin]  "${player.name}" kicked "${t.name}"`);
        break;
      }

      // ── ADMIN: LOCK ──────────────────────────────────────────────────────
      case 'admin_lock': {
        if (room.host !== player.id) break;
        room.locked = !!msg.locked;
        broadcast(room, {
          type: 'chat', name: '[SERVER]', skin: '#ff8800',
          text: room.locked
            ? '🔒 Host locked the world — no new players can join.'
            : '🔓 Host unlocked the world.',
        });
        console.log(`[admin]  "${player.name}" ${room.locked ? 'locked' : 'unlocked'} "${player.room}"`);
        break;
      }

      // ── ADMIN: SUMMON ────────────────────────────────────────────────────
      case 'admin_summon': {
        if (room.host !== player.id) break;
        sendTo(room, msg.target, { type: 'admin_summon', x: msg.x, y: msg.y, z: msg.z });
        break;
      }

      // ── ADMIN: FORCE WALK ────────────────────────────────────────────────
      case 'admin_forcewalk': {
        if (room.host !== player.id) break;
        sendTo(room, msg.target, {
          type: 'admin_forcewalk',
          x: msg.x, y: msg.y, z: msg.z,
          stopDist:     msg.stopDist     ?? 1.6,
          lockControls: msg.lockControls ?? true,
        });
        break;
      }

      // ── ADMIN: STOP WALK ─────────────────────────────────────────────────
      case 'admin_stopwalk': {
        if (room.host !== player.id) break;
        sendTo(room, msg.target, { type: 'admin_stopwalk' });
        break;
      }

      // ── ADMIN: WEATHER ───────────────────────────────────────────────────
      case 'admin_weather': {
        if (room.host !== player.id) break;
        broadcast(room, { type: 'admin_weather', action: String(msg.action || '') }, player.id);
        break;
      }

      // ── WEATHER TOGGLE ───────────────────────────────────────────────────
      case 'weather_toggle': {
        if (room.host !== player.id) break;
        broadcast(room, { type: 'weather_toggle', enabled: !!msg.enabled }, player.id);
        break;
      }

      // ── TORNADO SPAWN ────────────────────────────────────────────────────
      case 'tornado_spawn': {
        if (room.host !== player.id) break;
        broadcast(room, {
          type: 'tornado_spawn',
          x: msg.x, z: msg.z, strength: msg.strength ?? 1,
        }, player.id);
        break;
      }

      default: break;
    }
  });

  // ── DISCONNECT ─────────────────────────────────────────────────────────────
  ws.on('close', () => {
    if (!player) return;
    const room = rooms.get(player.room);
    if (!room) return;

    room.players.delete(player.id);
    console.log(`[leave]  "${player.name}" left "${player.room}" (${room.players.size} remaining)`);
    broadcast(room, { type: 'player_leave', id: player.id });

    // Reassign host if it was this player
    if (room.host === player.id) {
      let next = null;
      for (const p of room.players.values()) {
        if (!next || p.joinTime < next.joinTime) next = p;
      }
      if (next) {
        room.host = next.id;
        send(next.ws, { type: 'host_assigned', id: next.id });
        broadcast(room, {
          type: 'chat', name: '[SERVER]', skin: '#aaffcc',
          text: `Host left. ${next.name} is now the host.`,
        });
        console.log(`[host]   Re-assigned to "${next.name}" in "${player.room}"`);
      } else {
        room.host = null;
      }
    }

    pruneRoom(player.room);
  });

  ws.on('error', (err) => console.error(`[ws err] ${err.message}`));
});

// ─────────────────────────────────────────────────────────────────────────────
// HEARTBEAT — drop dead sockets every 30s
// ─────────────────────────────────────────────────────────────────────────────
setInterval(() => {
  for (const [roomId, room] of rooms) {
    for (const [pid, p] of room.players) {
      if (p.ws.readyState !== WebSocket.OPEN) {
        room.players.delete(pid);
        broadcast(room, { type: 'player_leave', id: pid });
        console.log(`[ping]   Dropped dead connection "${p.name}" in "${roomId}"`);
      }
    }
  }
}, 30_000);

// ─────────────────────────────────────────────────────────────────────────────
// STATS — log room summary every 5 minutes
// ─────────────────────────────────────────────────────────────────────────────
setInterval(() => {
  if (rooms.size === 0) return;
  console.log(`[stats]  ${rooms.size} room(s) active:`);
  for (const [id, room] of rooms) {
    console.log(`         "${id}" — ${room.players.size} player(s), ${room.mods.size} world mods, seed=${room.seed}`);
  }
}, 5 * 60_000);

// ─────────────────────────────────────────────────────────────────────────────
// STARTUP
// ─────────────────────────────────────────────────────────────────────────────
console.log('');
console.log('  ╔═══════════════════════════════════════════════════════════╗');
console.log(`  ║  🌍 Game server  →  ws://localhost:${PORT}                   ║`);
console.log('  ╠═══════════════════════════════════════════════════════════╣');
console.log('  ║  World saving  ON  — blocks sync to all players           ║');
console.log('  ║  Worlds persist while server runs (Render: always)        ║');
console.log('  ║  Same seed = same room = same world automatically         ║');
console.log('  ╚═══════════════════════════════════════════════════════════╝');
console.log('');
