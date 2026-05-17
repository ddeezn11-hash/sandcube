// sandcube multiplayer server
// Handles rooms, player positions, block edits, chat, map markers, and admin actions.

const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;

// rooms[roomKey] = Map<playerId, { ws, name, skin, x, y, z, ry, joinTime, isHost, roomLocked, chatMuted }>
const rooms = new Map();

// ── Helpers ───────────────────────────────────────────────────────────────────

function getRoom(roomKey) {
  if (!rooms.has(roomKey)) rooms.set(roomKey, new Map());
  return rooms.get(roomKey);
}

function broadcast(roomKey, msg, excludeId = null) {
  const players = rooms.get(roomKey);
  if (!players) return;
  const data = JSON.stringify(msg);
  for (const [id, p] of players) {
    if (id === excludeId) continue;
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(data);
  }
}

function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function roomHost(roomKey) {
  const players = rooms.get(roomKey);
  if (!players) return null;
  for (const [, p] of players) if (p.isHost) return p;
  return null;
}

function playerList(roomKey, excludeId) {
  const players = rooms.get(roomKey);
  if (!players) return [];
  return [...players.entries()]
    .filter(([id]) => id !== excludeId)
    .map(([id, p]) => ({ id, name: p.name, skin: p.skin, x: p.x, y: p.y, z: p.z }));
}

// Promote the longest-connected player to host (called when host disconnects).
function promoteNewHost(roomKey) {
  const players = rooms.get(roomKey);
  if (!players || players.size === 0) return;
  let oldest = null;
  for (const [id, p] of players) {
    if (!oldest || p.joinTime < oldest.p.joinTime) oldest = { id, p };
  }
  if (!oldest) return;
  oldest.p.isHost = true;
  send(oldest.p.ws, {
    type: 'host_assigned',
    msg: 'The host left — you are now the host of this room.',
  });
  console.log(`[host]  ${oldest.p.name} (${oldest.id}) promoted to host in room ${roomKey}`);
}

// ── Server ────────────────────────────────────────────────────────────────────

const wss = new WebSocketServer({ port: PORT });

wss.on('listening', () => {
  console.log(`Sandcube server listening on port ${PORT}`);
});

wss.on('connection', (ws) => {
  let playerId = null;
  let roomKey  = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // ── Join ───────────────────────────────────────────────────────────────
      case 'join': {
        playerId = String(msg.id   || '').slice(0, 32);
        roomKey  = String(msg.room || '').slice(0, 64);

        if (!playerId || !roomKey) {
          send(ws, { type: 'error', msg: 'Missing id or room.' });
          return;
        }

        const players = getRoom(roomKey);

        // Reject if world is locked
        const host = roomHost(roomKey);
        if (host?.roomLocked) {
          send(ws, { type: 'kicked', reason: 'World is locked — not accepting new players.' });
          ws.close();
          return;
        }

        // Clean up stale duplicate connection for same playerId
        if (players.has(playerId)) {
          const old = players.get(playerId);
          try { old.ws.close(); } catch {}
          players.delete(playerId);
        }

        // Host grant rules:
        //   - Client sends wantHost:true when the player clicked "Host World"
        //   - Only granted if no one else in the room is already host
        const noHostYet  = !roomHost(roomKey);
        const wantsHost  = !!msg.wantHost;
        const becomeHost = wantsHost && noHostYet;

        players.set(playerId, {
          ws,
          name:       String(msg.name || 'Player').slice(0, 32),
          skin:       String(msg.skin || '#ffffff').slice(0, 16),
          x: 0, y: 64, z: 0, ry: 0,
          joinTime:   Date.now(),
          isHost:     becomeHost,
          roomLocked: false,
          chatMuted:  false,
        });

        // Send existing players to newcomer
        send(ws, { type: 'player_list', players: playerList(roomKey, playerId) });

        // Join ack
        send(ws, { type: 'join_ack', playerCount: players.size });

        // Notify others
        broadcast(roomKey, {
          type: 'player_join',
          id:   playerId,
          name: msg.name,
          skin: msg.skin,
        }, playerId);

        // Confirm host to this socket only
        if (becomeHost) {
          send(ws, { type: 'host_assigned', msg: 'You are the host of this room.' });
        }

        console.log(`[join]  ${msg.name} (${playerId}) → room ${roomKey}  host=${becomeHost}  (${players.size} players)`);
        break;
      }

      // ── Position update ────────────────────────────────────────────────────
      case 'player_update': {
        if (!playerId || !roomKey) return;
        const p = rooms.get(roomKey)?.get(playerId);
        if (!p) return;
        p.x    = msg.x    ?? p.x;
        p.y    = msg.y    ?? p.y;
        p.z    = msg.z    ?? p.z;
        p.ry   = msg.ry   ?? p.ry;
        p.name = msg.name ?? p.name;
        p.skin = msg.skin ?? p.skin;
        broadcast(roomKey, {
          type: 'player_update',
          id: playerId,
          name: p.name, skin: p.skin,
          x: p.x, y: p.y, z: p.z, ry: p.ry,
        }, playerId);
        break;
      }

      // ── Block break ────────────────────────────────────────────────────────
      case 'block_break': {
        if (!roomKey) return;
        broadcast(roomKey, {
          type: 'block_break',
          pid: playerId,
          x: msg.x, y: msg.y, z: msg.z,
        }, playerId);
        break;
      }

      // ── Block place ────────────────────────────────────────────────────────
      case 'block_place': {
        if (!roomKey) return;
        broadcast(roomKey, {
          type: 'block_place',
          pid: playerId,
          x: msg.x, y: msg.y, z: msg.z, t: msg.t,
        }, playerId);
        break;
      }

      // ── Chat ───────────────────────────────────────────────────────────────
      case 'chat': {
        if (!roomKey) return;
        const players = rooms.get(roomKey);
        const sender  = players?.get(playerId);
        if (!sender) return;
        const h = roomHost(roomKey);
        if (h?.chatMuted && !sender.isHost) return;
        broadcast(roomKey, {
          type: 'chat',
          name: msg.name ?? sender.name,
          skin: msg.skin ?? sender.skin,
          text: String(msg.text || '').slice(0, 256),
        });
        break;
      }

      // ── Map marker ─────────────────────────────────────────────────────────
      case 'map_marker': {
        if (!roomKey) return;
        broadcast(roomKey, {
          type: 'map_marker',
          x: msg.x, z: msg.z,
          label: msg.label,
          color: msg.color,
          pid:  playerId,
          name: msg.name,
        }, playerId);
        break;
      }

      // ── Admin: kick player ─────────────────────────────────────────────────
      case 'admin_kick': {
        if (!roomKey) return;
        const kicker = rooms.get(roomKey)?.get(playerId);
        if (!kicker?.isHost) return;
        const players = rooms.get(roomKey);
        const target  = players?.get(msg.target);
        if (target) {
          send(target.ws, { type: 'kicked', reason: msg.reason || 'Kicked by host.' });
          target.ws.close();
          players.delete(msg.target);
          broadcast(roomKey, { type: 'player_leave', id: msg.target });
          console.log(`[kick]  ${msg.target} kicked in room ${roomKey}`);
        }
        break;
      }

      // ── Admin: summon player to host ───────────────────────────────────────
      case 'admin_summon': {
        if (!roomKey) return;
        const summoner = rooms.get(roomKey)?.get(playerId);
        if (!summoner?.isHost) return;
        const target = rooms.get(roomKey)?.get(msg.target);
        if (target) {
          send(target.ws, {
            type: 'admin_summon',
            x: summoner.x, y: summoner.y, z: summoner.z,
          });
        }
        break;
      }

      // ── Admin: lock / unlock world ─────────────────────────────────────────
      case 'admin_lock': {
        if (!roomKey) return;
        const p = rooms.get(roomKey)?.get(playerId);
        if (!p?.isHost) return;
        p.roomLocked = !!msg.locked;
        console.log(`[lock]  Room ${roomKey} locked=${p.roomLocked}`);
        break;
      }

      // ── Admin: mute / unmute chat ──────────────────────────────────────────
      case 'admin_mute': {
        if (!roomKey) return;
        const p = rooms.get(roomKey)?.get(playerId);
        if (!p?.isHost) return;
        p.chatMuted = !!msg.muted;
        console.log(`[mute]  Room ${roomKey} chatMuted=${p.chatMuted}`);
        break;
      }

      // ── Weather toggle (host only → all clients) ───────────────────────────
      case 'weather_toggle': {
        if (!roomKey) return;
        const p = rooms.get(roomKey)?.get(playerId);
        if (!p?.isHost) return;
        broadcast(roomKey, { type: 'weather_toggle', enabled: !!msg.enabled }, playerId);
        console.log(`[weather] Room ${roomKey} enabled=${msg.enabled}`);
        break;
      }

      // ── Admin weather: day / night / calm / storm ──────────────────────────
      case 'admin_weather': {
        if (!roomKey) return;
        const p = rooms.get(roomKey)?.get(playerId);
        if (!p?.isHost) return;
        broadcast(roomKey, { type: 'admin_weather', action: msg.action }, playerId);
        break;
      }

      // ── Tornado spawn (host only → all clients) ────────────────────────────
      case 'tornado_spawn': {
        if (!roomKey) return;
        const p = rooms.get(roomKey)?.get(playerId);
        if (!p?.isHost) return;
        broadcast(roomKey, {
          type: 'tornado_spawn',
          x: msg.x, z: msg.z, strength: msg.strength,
        }, playerId);
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    if (!playerId || !roomKey) return;
    const players = rooms.get(roomKey);
    if (!players) return;
    const leaving = players.get(playerId);
    const name    = leaving?.name ?? playerId;
    const wasHost = leaving?.isHost ?? false;
    players.delete(playerId);
    broadcast(roomKey, { type: 'player_leave', id: playerId });
    if (players.size === 0) {
      rooms.delete(roomKey);
      console.log(`[room]  ${roomKey} empty — removed`);
    } else if (wasHost) {
      promoteNewHost(roomKey);
    }
    console.log(`[leave] ${name} (${playerId}) ← room ${roomKey}  (${players?.size ?? 0} remaining)`);
  });

  ws.on('error', (err) => {
    console.error(`[ws error] ${err.message}`);
  });
});

// ── Stale connection cleanup every 30s ────────────────────────────────────────
setInterval(() => {
  for (const [roomKey, players] of rooms) {
    for (const [id, p] of players) {
      if (p.ws.readyState !== WebSocket.OPEN) {
        const wasHost = p.isHost;
        players.delete(id);
        broadcast(roomKey, { type: 'player_leave', id });
        if (wasHost && players.size > 0) promoteNewHost(roomKey);
      }
    }
    if (players.size === 0) rooms.delete(roomKey);
  }
}, 30_000);
