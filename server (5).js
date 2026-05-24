/**
 * server.js — World (Infinite) v12/v13 Multiplayer Server
 *
 * Full message coverage:
 *   Client → Server:
 *     join, player_update, block_break, block_place, chat,
 *     map_marker, pvp_hit, admin_kick, admin_summon,
 *     admin_forcewalk, admin_stopwalk, admin_weather,
 *     admin_lock, weather_toggle, tornado_spawn
 *
 *   Server → Client:
 *     join_ack, player_list, player_join, player_leave,
 *     player_update, block_break, block_place, chat,
 *     map_marker, pvp_hit, pvp_kill, kicked, host_assigned,
 *     admin_summon, admin_forcewalk, admin_stopwalk,
 *     admin_weather, weather_toggle, tornado_spawn, error
 *
 * Usage:
 *   npm install ws
 *   node server.js
 *   PORT=8080 node server.js
 */

'use strict';

const WebSocket = require('ws');
const PORT = parseInt(process.env.PORT || '8080', 10);

// ── Room state ────────────────────────────────────────────────────────────────
// rooms: Map<roomKey, Room>
const rooms = new Map();

function getRoom(key) {
  if (!rooms.has(key)) {
    rooms.set(key, {
      key,
      hostId: null,
      locked: false,
      players: new Map(),   // playerId → PlayerState
      mods: new Map(),      // "x,y,z" → blockType|-1
    });
  }
  return rooms.get(key);
}

function deleteEmptyRoom(room) {
  if (room.players.size === 0) rooms.delete(room.key);
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(room, obj, excludeId = null) {
  for (const [pid, p] of room.players) {
    if (pid !== excludeId) send(p.ws, obj);
  }
}

function broadcastAll(room, obj) {
  broadcast(room, obj, null);
}

function assignNewHost(room) {
  if (room.players.size === 0) return;
  const [newHostId, newHost] = room.players.entries().next().value;
  room.hostId = newHostId;
  send(newHost.ws, {
    type: 'host_assigned',
    msg: `You are now the host of room ${room.key}`,
  });
  console.log(`[room:${room.key}] New host: ${newHost.name} (${newHostId})`);
}

function isHost(room, senderId) {
  return room.hostId === senderId;
}

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ port: PORT });

wss.on('listening', () => {
  console.log(`[server] Listening on ws://0.0.0.0:${PORT}`);
});

wss.on('connection', (ws) => {
  let currentRoom = null;
  let currentPlayerId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const type = msg.type;

    // ── JOIN ──────────────────────────────────────────────────────────────
    if (type === 'join') {
      const { id, room: roomKey, name, skin, wantHost } = msg;
      if (!id || !roomKey) return;

      const room = getRoom(roomKey);

      if (room.locked && !wantHost) {
        send(ws, { type: 'error', msg: 'This world is locked.' });
        ws.close();
        return;
      }

      // Replace stale same-id connection
      if (room.players.has(id)) {
        try { room.players.get(id).ws.close(); } catch {}
      }

      const player = {
        id, name: name || id, skin: skin || '#ffffff',
        ws, x: 0, y: 0, z: 0, ry: 0, hp: 100,
      };
      room.players.set(id, player);
      currentRoom = room;
      currentPlayerId = id;

      // Host assignment: explicit request OR first player in empty room
      if (wantHost || room.hostId === null) {
        room.hostId = id;
        send(ws, {
          type: 'host_assigned',
          msg: `You are the host of room ${roomKey}`,
        });
        console.log(`[room:${roomKey}] Host: ${player.name} (${id})`);
      }

      // Send world state + player list to joiner
      const modsObj = {};
      for (const [k, t] of room.mods) modsObj[k] = t;

      send(ws, {
        type: 'join_ack',
        playerId: id,
        playerCount: room.players.size,
        mods: modsObj,
      });

      const playerList = [];
      for (const [pid, p] of room.players) {
        if (pid !== id) playerList.push({ id: pid, name: p.name, skin: p.skin, x: p.x, y: p.y, z: p.z });
      }
      if (playerList.length > 0) send(ws, { type: 'player_list', players: playerList });

      broadcast(room, { type: 'player_join', id, name: player.name, skin: player.skin }, id);
      console.log(`[room:${roomKey}] ${player.name} joined. Players: ${room.players.size}`);
      return;
    }

    if (!currentRoom || !currentPlayerId) return;
    const room = currentRoom;
    const senderId = currentPlayerId;
    const senderPlayer = room.players.get(senderId);
    if (!senderPlayer) return;

    switch (type) {

      // ── POSITION UPDATE ────────────────────────────────────────────────
      case 'player_update': {
        const { x, y, z, ry, name, skin } = msg;
        if (Number.isFinite(x)) senderPlayer.x = x;
        if (Number.isFinite(y)) senderPlayer.y = y;
        if (Number.isFinite(z)) senderPlayer.z = z;
        if (Number.isFinite(ry)) senderPlayer.ry = ry;
        if (name) senderPlayer.name = name;
        if (skin) senderPlayer.skin = skin;
        broadcast(room, { type: 'player_update', id: senderId, name: senderPlayer.name, skin: senderPlayer.skin, x, y, z, ry }, senderId);
        break;
      }

      // ── BLOCK BREAK ───────────────────────────────────────────────────
      case 'block_break': {
        const { x, y, z } = msg;
        if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) break;
        room.mods.set(`${Math.round(x)},${Math.round(y)},${Math.round(z)}`, -1);
        broadcast(room, { type: 'block_break', pid: senderId, x, y, z }, senderId);
        break;
      }

      // ── BLOCK PLACE ───────────────────────────────────────────────────
      case 'block_place': {
        const { x, y, z, t } = msg;
        if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) break;
        room.mods.set(`${Math.round(x)},${Math.round(y)},${Math.round(z)}`, t);
        broadcast(room, { type: 'block_place', pid: senderId, x, y, z, t }, senderId);
        break;
      }

      // ── CHAT ──────────────────────────────────────────────────────────
      case 'chat': {
        if (!msg.text) break;
        broadcastAll(room, {
          type: 'chat',
          name: msg.name || senderPlayer.name,
          skin: msg.skin || senderPlayer.skin,
          text: msg.text,
        });
        console.log(`[room:${room.key}] <${msg.name || senderPlayer.name}>: ${msg.text}`);
        break;
      }

      // ── MAP MARKER ────────────────────────────────────────────────────
      case 'map_marker': {
        broadcast(room, {
          type: 'map_marker',
          x: msg.x, z: msg.z,
          label: msg.label || `${msg.x},${msg.z}`,
          color: msg.color || '#ff88ff',
          pid: senderId,
          name: msg.name || senderPlayer.name,
        }, senderId);
        break;
      }

      // ── PVP HIT ───────────────────────────────────────────────────────
      case 'pvp_hit': {
        const { attacker, attackerName, target, damage } = msg;
        const victim = room.players.get(target);
        if (!victim) break;
        const dmg = Math.max(1, Math.min(100, damage || 15));
        send(victim.ws, {
          type: 'pvp_hit',
          attacker,
          attackerName: attackerName || senderPlayer.name,
          target,
          damage: dmg,
        });
        victim.hp = Math.max(0, (victim.hp || 100) - dmg);
        if (victim.hp <= 0) {
          broadcastAll(room, {
            type: 'pvp_kill',
            attacker: senderId,
            attackerName: senderPlayer.name,
            victim: target,
            victimName: victim.name,
          });
          victim.hp = 100;
        }
        break;
      }

      // ── ADMIN: KICK ───────────────────────────────────────────────────
      case 'admin_kick': {
        if (!isHost(room, senderId)) break;
        const victim = room.players.get(msg.target);
        if (!victim) break;
        send(victim.ws, { type: 'kicked', reason: 'You were kicked by the host.' });
        try { victim.ws.close(); } catch {}
        console.log(`[room:${room.key}] Host kicked ${victim.name}`);
        break;
      }

      // ── ADMIN: SUMMON ─────────────────────────────────────────────────
      case 'admin_summon': {
        if (!isHost(room, senderId)) break;
        const victim = room.players.get(msg.target);
        if (!victim) break;
        send(victim.ws, { type: 'admin_summon', x: msg.x, y: msg.y, z: msg.z });
        break;
      }

      // ── ADMIN: FORCE WALK ─────────────────────────────────────────────
      // Host sends this to move a specific player (or all players) to a
      // destination using the client-side A* pathfinder.
      // msg.target = playerId string, a username, or '*' for everyone.
      case 'admin_forcewalk': {
        if (!isHost(room, senderId)) break;
        const { target, x, y, z, stopDist, targetName } = msg;

        if (!target || target === '*') {
          // Broadcast to everyone except the host
          broadcast(room, {
            type: 'admin_forcewalk',
            target: '*', x, y, z,
            stopDist: stopDist ?? 1.4,
            lockControls: false,
            targetName,
          }, senderId);
          console.log(`[room:${room.key}] Host force-walking all players to ${x},${y},${z}`);
        } else {
          // Try by playerId first, then by name
          let victim = room.players.get(target);
          if (!victim) {
            const lc = String(target).toLowerCase();
            for (const [, p] of room.players) {
              if ((p.name || '').toLowerCase() === lc) { victim = p; break; }
            }
          }
          if (!victim) {
            send(senderPlayer.ws, { type: 'error', msg: `Player "${target}" not found` });
            break;
          }
          send(victim.ws, {
            type: 'admin_forcewalk',
            target: victim.id,
            x, y, z,
            stopDist: stopDist ?? 1.4,
            lockControls: false,
            targetName: targetName || senderPlayer.name,
          });
          console.log(`[room:${room.key}] Host force-walking ${victim.name} to ${x},${y},${z}`);
        }
        break;
      }

      // ── ADMIN: STOP WALK ──────────────────────────────────────────────
      case 'admin_stopwalk': {
        const { target } = msg;
        // Anyone can stop their own walk; only host can stop others
        if (target !== senderId && !isHost(room, senderId)) break;

        if (!target || target === '*') {
          broadcast(room, { type: 'admin_stopwalk', target: '*' }, senderId);
        } else {
          // Target may be a playerId or a username
          let victim = room.players.get(target);
          if (!victim) {
            const lc = String(target).toLowerCase();
            for (const [, p] of room.players) {
              if ((p.name || '').toLowerCase() === lc) { victim = p; break; }
            }
          }
          if (victim) send(victim.ws, { type: 'admin_stopwalk', target: victim.id });
        }
        break;
      }

      // ── ADMIN: WEATHER ────────────────────────────────────────────────
      case 'admin_weather': {
        if (!isHost(room, senderId)) break;
        broadcast(room, { type: 'admin_weather', action: msg.action }, senderId);
        console.log(`[room:${room.key}] Weather: ${msg.action}`);
        break;
      }

      // ── ADMIN: LOCK WORLD ─────────────────────────────────────────────
      case 'admin_lock': {
        if (!isHost(room, senderId)) break;
        room.locked = !!msg.locked;
        console.log(`[room:${room.key}] World ${room.locked ? 'locked' : 'unlocked'}`);
        break;
      }

      // ── WEATHER TOGGLE ────────────────────────────────────────────────
      case 'weather_toggle': {
        if (!isHost(room, senderId)) break;
        broadcast(room, { type: 'weather_toggle', enabled: !!msg.enabled }, senderId);
        break;
      }

      // ── TORNADO SPAWN ─────────────────────────────────────────────────
      case 'tornado_spawn': {
        if (!isHost(room, senderId)) break;
        broadcast(room, {
          type: 'tornado_spawn',
          x: msg.x, z: msg.z,
          strength: msg.strength || 70,
        }, senderId);
        break;
      }

      default: break;
    }
  });

  ws.on('close', () => {
    if (!currentRoom || !currentPlayerId) return;
    const room = currentRoom;
    const id = currentPlayerId;
    const player = room.players.get(id);
    if (!player) return;

    room.players.delete(id);
    broadcast(room, { type: 'player_leave', id, name: player.name });
    console.log(`[room:${room.key}] ${player.name} left. Players: ${room.players.size}`);

    if (room.hostId === id) {
      room.hostId = null;
      if (room.players.size > 0) assignNewHost(room);
    }

    deleteEmptyRoom(room);
  });

  ws.on('error', () => {});
});

process.on('SIGINT', () => { wss.close(() => process.exit(0)); });
process.on('SIGTERM', () => { wss.close(() => process.exit(0)); });
