// sandcube multiplayer server
// Handles rooms, player positions, block edits, chat, map markers, and admin actions.

const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;

// rooms[roomKey] = Map<playerId, { ws, name, skin, x, y, z, ry, joinTime, isHost }>
const rooms = new Map();

// ── Helpers ──────────────────────────────────────────────────────────────────

function getRoomPlayers(roomKey) {
  if (!rooms.has(roomKey)) rooms.set(roomKey, new Map());
  return rooms.get(roomKey);
}

function broadcast(roomKey, msg, excludeId = null) {
  const players = rooms.get(roomKey);
  if (!players) return;
  const data = JSON.stringify(msg);
  for (const [id, player] of players) {
    if (id === excludeId) continue;
    if (player.ws.readyState === WebSocket.OPEN) {
      player.ws.send(data);
    }
  }
}

function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function playerList(roomKey) {
  const players = rooms.get(roomKey);
  if (!players) return [];
  return [...players.entries()].map(([id, p]) => ({
    id,
    name: p.name,
    skin: p.skin,
    x: p.x,
    y: p.y,
    z: p.z,
  }));
}

function assignHost(roomKey) {
  const players = rooms.get(roomKey);
  if (!players || players.size === 0) return;
  // Pick the longest-connected player as host
  let oldest = null;
  for (const [id, p] of players) {
    if (!oldest || p.joinTime < oldest.joinTime) {
      oldest = { id, p };
    }
  }
  if (oldest && !oldest.p.isHost) {
    oldest.p.isHost = true;
    send(oldest.p.ws, {
      type: 'host_assigned',
      msg: 'You are now the host of this room.',
    });
  }
}

// ── Server ───────────────────────────────────────────────────────────────────

const wss = new WebSocketServer({ port: PORT });

wss.on('listening', () => {
  console.log(`Sandcube server listening on port ${PORT}`);
});

wss.on('connection', (ws) => {
  let playerId = null;
  let roomKey = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {

      // ── Join ──────────────────────────────────────────────────────────────
      case 'join': {
        playerId = msg.id;
        roomKey  = msg.room;

        if (!playerId || !roomKey) {
          send(ws, { type: 'error', msg: 'Missing id or room.' });
          return;
        }

        const players = getRoomPlayers(roomKey);

        // Reject if world is locked
        const anyHost = [...players.values()].find(p => p.isHost);
        if (anyHost?.roomLocked) {
          send(ws, { type: 'kicked', reason: 'World is locked — not accepting new players.' });
          ws.close();
          return;
        }

        const isFirstPlayer = players.size === 0;

        players.set(playerId, {
          ws,
          name: msg.name  || 'Player',
          skin: msg.skin  || '#ffffff',
          x: 0, y: 64, z: 0, ry: 0,
          joinTime: Date.now(),
          isHost: isFirstPlayer,
          roomLocked: false,
        });

        // Send current player list to the newcomer
        send(ws, {
          type: 'player_list',
          players: playerList(roomKey).filter(p => p.id !== playerId),
        });

        // Ack with online count
        send(ws, { type: 'join_ack', playerCount: players.size });

        // Notify others
        broadcast(roomKey, {
          type: 'player_join',
          id: playerId,
          name: msg.name,
          skin: msg.skin,
        }, playerId);

        if (isFirstPlayer) {
          send(ws, { type: 'host_assigned', msg: 'You are the host of this room.' });
        }

        console.log(`[join]  ${msg.name} (${playerId}) → room ${roomKey}  (${players.size} players)`);
        break;
      }

      // ── Position update ───────────────────────────────────────────────────
      case 'player_update': {
        if (!playerId || !roomKey) return;
        const players = rooms.get(roomKey);
        const p = players?.get(playerId);
        if (!p) return;
        p.x = msg.x ?? p.x;
        p.y = msg.y ?? p.y;
        p.z = msg.z ?? p.z;
        p.ry = msg.ry ?? p.ry;
        p.name = msg.name ?? p.name;
        p.skin = msg.skin ?? p.skin;

        broadcast(roomKey, {
          type: 'player_update',
          id: playerId,
          name: p.name,
          skin: p.skin,
          x: p.x, y: p.y, z: p.z, ry: p.ry,
        }, playerId);
        break;
      }

      // ── Block break ───────────────────────────────────────────────────────
      case 'block_break': {
        if (!roomKey) return;
        broadcast(roomKey, {
          type: 'block_break',
          pid: msg.pid,
          x: msg.x, y: msg.y, z: msg.z,
        }, playerId);
        break;
      }

      // ── Block place ───────────────────────────────────────────────────────
      case 'block_place': {
        if (!roomKey) return;
        broadcast(roomKey, {
          type: 'block_place',
          pid: msg.pid,
          x: msg.x, y: msg.y, z: msg.z, t: msg.t,
        }, playerId);
        break;
      }

      // ── Chat ──────────────────────────────────────────────────────────────
      case 'chat': {
        if (!roomKey) return;
        const players = rooms.get(roomKey);
        const sender = players?.get(playerId);
        if (!sender) return;

        // Check mute (host can set chatMuted on the room)
        const host = [...(players?.values() ?? [])].find(p => p.isHost);
        if (host?.chatMuted && !sender.isHost) return;

        broadcast(roomKey, {
          type: 'chat',
          name: msg.name ?? sender.name,
          skin: msg.skin ?? sender.skin,
          text: msg.text,
        });
        break;
      }

      // ── Map marker ────────────────────────────────────────────────────────
      case 'map_marker': {
        if (!roomKey) return;
        broadcast(roomKey, {
          type: 'map_marker',
          x: msg.x, z: msg.z,
          label: msg.label,
          color: msg.color,
          pid: msg.pid,
          name: msg.name,
        }, playerId);
        break;
      }

      // ── Admin: kick ───────────────────────────────────────────────────────
      case 'admin_kick': {
        if (!roomKey) return;
        const players = rooms.get(roomKey);
        const kicker = players?.get(msg.pid);
        if (!kicker?.isHost) return;  // only host may kick
        const target = players?.get(msg.target);
        if (target) {
          send(target.ws, { type: 'kicked', reason: msg.reason || 'Kicked by host.' });
          target.ws.close();
          players.delete(msg.target);
          broadcast(roomKey, { type: 'player_leave', id: msg.target }, null);
          console.log(`[kick]  ${msg.target} kicked by host in room ${roomKey}`);
        }
        break;
      }

      // ── Admin: summon player here ─────────────────────────────────────────
      case 'admin_summon': {
        if (!roomKey) return;
        const players = rooms.get(roomKey);
        const summoner = players?.get(msg.pid);
        if (!summoner?.isHost) return;
        const target = players?.get(msg.target);
        if (target) {
          send(target.ws, {
            type: 'admin_summon',
            x: summoner.x, y: summoner.y, z: summoner.z,
          });
        }
        break;
      }

      // ── Admin: lock/unlock world ──────────────────────────────────────────
      case 'admin_lock': {
        if (!roomKey) return;
        const players = rooms.get(roomKey);
        const locker = players?.get(msg.pid);
        if (!locker?.isHost) return;
        locker.roomLocked = !!msg.locked;
        console.log(`[lock]  Room ${roomKey} locked=${locker.roomLocked}`);
        break;
      }

      // ── Weather toggle (host → all clients) ──────────────────────────────
      case 'weather_toggle': {
        if (!roomKey) return;
        const players = rooms.get(roomKey);
        const sender = players?.get(msg.pid);
        if (!sender?.isHost) return;  // only host may sync weather
        broadcast(roomKey, {
          type: 'weather_toggle',
          enabled: !!msg.enabled,
        }, msg.pid);
        console.log(`[weather] Room ${roomKey} weather=${msg.enabled}`);
        break;
      }

      // ── Tornado spawn broadcast (host → all clients) ──────────────────────
      case 'tornado_spawn': {
        if (!roomKey) return;
        broadcast(roomKey, {
          type: 'tornado_spawn',
          x: msg.x, z: msg.z,
          strength: msg.strength,
        }, playerId);
        break;
      }

      // ── Admin weather actions broadcast (day/night/calm/storm) ───────────
      case 'admin_weather': {
        if (!roomKey) return;
        const players = rooms.get(roomKey);
        const sender = players?.get(msg.pid);
        if (!sender?.isHost) return;
        broadcast(roomKey, {
          type: 'admin_weather',
          action: msg.action,   // 'day' | 'night' | 'calm' | 'storm'
        }, msg.pid);
        break;
      }

      default:
        // Unknown message type — silently ignore
        break;
    }
  });

  ws.on('close', () => {
    if (!playerId || !roomKey) return;
    const players = rooms.get(roomKey);
    if (!players) return;

    const leaving = players.get(playerId);
    const name = leaving?.name ?? playerId;
    players.delete(playerId);

    broadcast(roomKey, { type: 'player_leave', id: playerId });

    if (players.size === 0) {
      rooms.delete(roomKey);
      console.log(`[room]  ${roomKey} is now empty — cleaned up`);
    } else {
      // Re-assign host if the host left
      if (leaving?.isHost) assignHost(roomKey);
    }

    console.log(`[leave] ${name} (${playerId}) ← room ${roomKey}  (${players?.size ?? 0} players)`);
  });

  ws.on('error', (err) => {
    console.error(`[ws error] ${err.message}`);
  });
});

// ── Periodic stale-player cleanup (every 30 s) ─────────────────────────────
setInterval(() => {
  for (const [roomKey, players] of rooms) {
    for (const [id, p] of players) {
      if (p.ws.readyState !== WebSocket.OPEN) {
        players.delete(id);
        broadcast(roomKey, { type: 'player_leave', id });
      }
    }
    if (players.size === 0) rooms.delete(roomKey);
  }
}, 30_000);
