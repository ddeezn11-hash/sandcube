/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  server.js — WebSocket multiplayer server for the survival game         ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  QUICK START                                                             ║
 * ║    npm install ws                                                        ║
 * ║    node server.js                                                        ║
 * ║                                                                          ║
 * ║  Then in the game:  Multiplayer → pick "Localhost :8080"                ║
 * ║                                                                          ║
 * ║  DEPLOY (Render / Railway / Fly.io)                                      ║
 * ║    Push this file, set start command: node server.js                    ║
 * ║    Set env var PORT if needed (defaults to 8080)                        ║
 * ║    Use your deployed URL as the "Custom URL" in the game picker         ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 *
 * FULL MESSAGE PROTOCOL (matched to game client):
 *
 *  CLIENT → SERVER
 *  ─────────────────────────────────────────────────────────────────────────
 *  join              Player enters a room (sent on connect)
 *  player_update     Position/rotation tick (~10 Hz)
 *  chat              Chat message
 *  block_break       Player broke a block
 *  block_place       Player placed a block
 *  map_marker        Player dropped a map waypoint
 *  pvp_hit           Player hit another player
 *  pvp_kill          Player killed another player
 *  admin_kick        Host kicks a player
 *  admin_lock        Host locks/unlocks the room to new joins
 *  admin_summon      Host teleports a player to host position
 *  admin_forcewalk   Host forces a player to walk to a position
 *  admin_stopwalk    Host cancels a forced walk
 *  admin_weather     Host sets day/night/calm/storm for all
 *  weather_toggle    Host toggles rain on/off for all
 *  tornado_spawn     Host spawns a tornado for all
 *
 *  SERVER → CLIENT
 *  ─────────────────────────────────────────────────────────────────────────
 *  join_ack          Confirms join, sends player count
 *  player_list       Existing players in the room (sent on join)
 *  player_join       Someone new joined
 *  player_leave      Someone left
 *  player_update     Relayed position update
 *  host_assigned     This client is now the host
 *  kicked            This client was kicked
 *  error             Something went wrong
 *  (+ relay of all admin_*, weather_*, pvp_*, map_marker, block_*, chat)
 */

'use strict';

const { WebSocketServer, WebSocket } = require('ws');

const PORT = parseInt(process.env.PORT) || 8080;
const wss  = new WebSocketServer({ port: PORT });

// ─────────────────────────────────────────────────────────────────────────────
// DATA STRUCTURES
// ─────────────────────────────────────────────────────────────────────────────
//
// rooms  Map<roomId, Room>
//
// Room {
//   host:    string|null   — playerId of the current host
//   locked:  boolean       — when true, new joins are rejected
//   players: Map<playerId, Player>
// }
//
// Player {
//   ws:       WebSocket
//   id:       string
//   name:     string
//   skin:     string   (CSS colour, e.g. "#ff8800")
//   room:     string
//   x,y,z:   number   (last known position)
//   ry:       number   (last known yaw)
//   joinTime: number   (Date.now())
// }
//
// ─────────────────────────────────────────────────────────────────────────────

const rooms = new Map(); // roomId → Room

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, { host: null, locked: false, players: new Map() });
  }
  return rooms.get(roomId);
}

function pruneRoom(roomId) {
  const room = rooms.get(roomId);
  if (room && room.players.size === 0) {
    rooms.delete(roomId);
    console.log(`[room]   ${roomId} deleted (empty)`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SEND HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/** Send a JSON object to one WebSocket. */
function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

/** Broadcast to every player in a room, optionally skipping one sender. */
function broadcast(room, obj, excludeId = null) {
  const data = JSON.stringify(obj);
  for (const [pid, p] of room.players) {
    if (pid === excludeId) continue;
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(data);
  }
}

/** Send only to a specific player in a room. */
function sendTo(room, targetId, obj) {
  const p = room.players.get(targetId);
  if (p) send(p.ws, obj);
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTION HANDLER
// ─────────────────────────────────────────────────────────────────────────────

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?').split(',')[0].trim();
  console.log(`[connect] ${ip}`);

  /** The Player object for this connection. Set on 'join', null before. */
  let player = null;

  // ── INCOMING MESSAGES ──────────────────────────────────────────────────────
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    // ── JOIN ─────────────────────────────────────────────────────────────────
    if (msg.type === 'join') {
      const { id, room: roomId, name, skin, wantHost } = msg;

      // Basic validation
      if (!id || typeof id !== 'string') return;
      if (!roomId || typeof roomId !== 'string') return;

      const room = getRoom(roomId);

      // Reject if world is locked (unless this player is (re)joining as host)
      if (room.locked && room.host !== id) {
        send(ws, { type: 'error', msg: '🔒 This world is locked — the host has closed it to new players.' });
        ws.close();
        return;
      }

      // Handle reconnect: clean up any stale entry for this id
      if (room.players.has(id)) {
        const stale = room.players.get(id);
        if (stale.ws !== ws) {
          try { stale.ws.terminate(); } catch {}
        }
        room.players.delete(id);
      }

      // Create the player record
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

      // Assign host:
      //   • Room has no host yet  → this player becomes host automatically
      //   • wantHost flag is set  → this player requested to be host (clicked "Host a World")
      if (!room.host || wantHost) {
        room.host = id;
        send(ws, { type: 'host_assigned', id });
        console.log(`[host]   "${player.name}" is host of room "${roomId}"`);
      }

      // Send this player the current roster so they can see existing players
      const roster = [];
      for (const [pid, p] of room.players) {
        if (pid !== id) roster.push({ id: pid, name: p.name, skin: p.skin });
      }
      send(ws, { type: 'player_list', players: roster });

      // Acknowledge the join with a player count
      send(ws, { type: 'join_ack', playerCount: room.players.size });

      // Tell everyone else this player just arrived
      broadcast(room, { type: 'player_join', id, name: player.name, skin: player.skin }, id);

      console.log(`[join]   "${player.name}" → room "${roomId}" (${room.players.size} players)`);
      return;
    }

    // All remaining message types require an active session
    if (!player) return;
    const room = rooms.get(player.room);
    if (!room) return;

    switch (msg.type) {

      // ── POSITION / ROTATION UPDATE ──────────────────────────────────────
      // The game sends this ~10 times per second while moving.
      // Server stores the last known position and relays to everyone else.
      case 'player_update': {
        player.x  = typeof msg.x  === 'number' ? msg.x  : player.x;
        player.y  = typeof msg.y  === 'number' ? msg.y  : player.y;
        player.z  = typeof msg.z  === 'number' ? msg.z  : player.z;
        player.ry = typeof msg.ry === 'number' ? msg.ry : player.ry;
        broadcast(room, {
          type: 'player_update',
          id:   player.id,
          name: player.name,
          skin: player.skin,
          x:    player.x,
          y:    player.y,
          z:    player.z,
          ry:   player.ry,
        }, player.id);
        break;
      }

      // ── CHAT ──────────────────────────────────────────────────────────────
      // Strip HTML, cap length, then broadcast to whole room (including sender
      // so they see their own message appear for everyone else).
      case 'chat': {
        const text = String(msg.text || '')
          .replace(/<[^>]*>/g, '')   // strip HTML tags
          .trim()
          .slice(0, 200);
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

      // ── BLOCK BREAK ───────────────────────────────────────────────────────
      // A player broke a block. Relay to everyone else so their world updates.
      case 'block_break': {
        broadcast(room, {
          type: 'block_break',
          pid:  player.id,
          x:    msg.x,
          y:    msg.y,
          z:    msg.z,
        }, player.id);
        break;
      }

      // ── BLOCK PLACE ───────────────────────────────────────────────────────
      // A player placed a block. Relay to everyone else.
      case 'block_place': {
        broadcast(room, {
          type: 'block_place',
          pid:  player.id,
          x:    msg.x,
          y:    msg.y,
          z:    msg.z,
          t:    msg.t,   // block type id
        }, player.id);
        break;
      }

      // ── MAP MARKER ────────────────────────────────────────────────────────
      // A player dropped a waypoint on their map. Relay to everyone else.
      case 'map_marker': {
        broadcast(room, {
          type:  'map_marker',
          x:     msg.x,
          z:     msg.z,
          label: String(msg.label || '').slice(0, 64),
          color: String(msg.color || '#ffff00').slice(0, 20),
          pid:   player.id,
          name:  player.name,
        }, player.id);
        break;
      }

      // ── PVP HIT ───────────────────────────────────────────────────────────
      // Player A punched player B. Send damage only to the target.
      case 'pvp_hit': {
        const dmg = Math.min(Math.max(Number(msg.damage) || 0, 0), 100);
        sendTo(room, msg.target, {
          type:         'pvp_hit',
          attacker:     player.id,
          attackerName: player.name,
          target:       msg.target,
          damage:       dmg,
        });
        break;
      }

      // ── PVP KILL ──────────────────────────────────────────────────────────
      // Player A killed player B. Broadcast the death message to the whole room.
      case 'pvp_kill': {
        broadcast(room, {
          type:         'pvp_kill',
          attacker:     player.id,
          attackerName: player.name,
          target:       msg.target,
          targetName:   room.players.get(msg.target)?.name ?? String(msg.target),
        });
        break;
      }

      // ══════════════════════════════════════════════════════════════════════
      // ADMIN MESSAGES — all require sender to be the room host.
      // If a non-host sends these they are silently dropped.
      // ══════════════════════════════════════════════════════════════════════

      // ── ADMIN: KICK ───────────────────────────────────────────────────────
      // Host removes a player from the room.
      case 'admin_kick': {
        if (room.host !== player.id) break;
        const target = room.players.get(msg.target);
        if (!target) break;
        send(target.ws, { type: 'kicked', reason: 'You were kicked by the host.' });
        target.ws.close();
        console.log(`[admin]  "${player.name}" kicked "${target.name}" from "${player.room}"`);
        break;
      }

      // ── ADMIN: LOCK / UNLOCK ──────────────────────────────────────────────
      // Host closes or opens the room to new joins.
      case 'admin_lock': {
        if (room.host !== player.id) break;
        room.locked = !!msg.locked;
        broadcast(room, {
          type: 'chat',
          name: '[SERVER]',
          skin: '#ff8800',
          text: room.locked
            ? '🔒 Host locked the world — no new players can join.'
            : '🔓 Host unlocked the world — new players can join.',
        });
        console.log(`[admin]  "${player.name}" ${room.locked ? 'locked' : 'unlocked'} "${player.room}"`);
        break;
      }

      // ── ADMIN: SUMMON ─────────────────────────────────────────────────────
      // Host teleports a target player to the host's position.
      case 'admin_summon': {
        if (room.host !== player.id) break;
        sendTo(room, msg.target, {
          type: 'admin_summon',
          x:    msg.x,
          y:    msg.y,
          z:    msg.z,
        });
        break;
      }

      // ── ADMIN: FORCE WALK ─────────────────────────────────────────────────
      // Host makes a target player walk to a specific position.
      case 'admin_forcewalk': {
        if (room.host !== player.id) break;
        sendTo(room, msg.target, {
          type:         'admin_forcewalk',
          x:            msg.x,
          y:            msg.y,
          z:            msg.z,
          stopDist:     msg.stopDist     ?? 1.6,
          lockControls: msg.lockControls ?? true,
        });
        break;
      }

      // ── ADMIN: STOP WALK ──────────────────────────────────────────────────
      // Host cancels any active forced walk on a target.
      case 'admin_stopwalk': {
        if (room.host !== player.id) break;
        sendTo(room, msg.target, { type: 'admin_stopwalk' });
        break;
      }

      // ── ADMIN: WEATHER ────────────────────────────────────────────────────
      // Host forces a weather/time change for everyone.
      // action: 'day' | 'night' | 'calm' | 'storm'
      case 'admin_weather': {
        if (room.host !== player.id) break;
        broadcast(room, {
          type:   'admin_weather',
          action: String(msg.action || ''),
        }, player.id);
        break;
      }

      // ── WEATHER TOGGLE ────────────────────────────────────────────────────
      // Host enables or disables rain/weather for everyone.
      case 'weather_toggle': {
        if (room.host !== player.id) break;
        broadcast(room, {
          type:    'weather_toggle',
          enabled: !!msg.enabled,
        }, player.id);
        break;
      }

      // ── TORNADO SPAWN ─────────────────────────────────────────────────────
      // Host spawned a tornado; sync it to all other clients.
      case 'tornado_spawn': {
        if (room.host !== player.id) break;
        broadcast(room, {
          type:     'tornado_spawn',
          x:        msg.x,
          z:        msg.z,
          strength: msg.strength ?? 1,
        }, player.id);
        break;
      }

      default:
        // Unknown or future message type — ignore safely
        break;
    }
  }); // end ws.on('message')

  // ── DISCONNECT ─────────────────────────────────────────────────────────────
  ws.on('close', () => {
    if (!player) return;

    const room = rooms.get(player.room);
    if (!room) return;

    room.players.delete(player.id);
    console.log(`[leave]  "${player.name}" left "${player.room}" (${room.players.size} remaining)`);

    // Tell everyone still in the room this player is gone
    broadcast(room, { type: 'player_leave', id: player.id });

    // If the host left, promote the next player alphabetically by join time
    if (room.host === player.id) {
      // Find the player who joined earliest
      let nextPlayer = null;
      for (const p of room.players.values()) {
        if (!nextPlayer || p.joinTime < nextPlayer.joinTime) nextPlayer = p;
      }

      if (nextPlayer) {
        room.host = nextPlayer.id;
        send(nextPlayer.ws, { type: 'host_assigned', id: nextPlayer.id });
        broadcast(room, {
          type: 'chat',
          name: '[SERVER]',
          skin: '#aaffcc',
          text: `Host left. ${nextPlayer.name} is now the host.`,
        });
        console.log(`[host]   Re-assigned to "${nextPlayer.name}" in "${player.room}"`);
      } else {
        pruneRoom(player.room);
      }
    }

    if (room.players.size === 0) pruneRoom(player.room);
  });

  ws.on('error', (err) => {
    console.error(`[ws err] ${err.message}`);
  });

}); // end wss.on('connection')

// ─────────────────────────────────────────────────────────────────────────────
// HEARTBEAT — drop dead connections every 30 seconds
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
    pruneRoom(roomId);
  }
}, 30_000);

// ─────────────────────────────────────────────────────────────────────────────
// STARTUP
// ─────────────────────────────────────────────────────────────────────────────
console.log('');
console.log('  ╔══════════════════════════════════════════════════════╗');
console.log(`  ║  🌍 Game server listening on ws://localhost:${PORT}    ║`);
console.log('  ╠══════════════════════════════════════════════════════╣');
console.log('  ║  In-game:  Multiplayer → Localhost :8080             ║');
console.log('  ║  Or use Custom URL for a deployed server             ║');
console.log('  ╚══════════════════════════════════════════════════════╝');
console.log('');
