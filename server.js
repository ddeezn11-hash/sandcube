const express = require('express');
const http = require('http');
const path = require('path');
const WebSocket = require('ws');

const app = express();
const publicDir = path.resolve(__dirname);
app.use(express.static(publicDir));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const rooms = new Map();
const clients = new Map();

function safeSend(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(roomKey, payload, excludeWs = null) {
  const room = rooms.get(roomKey);
  if (!room) return;
  const data = JSON.stringify(payload);
  for (const client of room) {
    if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    let msg;
    try {
      msg = JSON.parse(message.toString());
    } catch (err) {
      return;
    }

    if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') {
      return;
    }

    if (msg.type === 'join') {
      const { room, id, name, skin } = msg;
      if (typeof room !== 'string' || typeof id !== 'string') {
        return;
      }

      const roomKey = room;
      const roomSet = rooms.get(roomKey) || new Set();
      rooms.set(roomKey, roomSet);

      const existingPlayers = [];
      for (const client of roomSet) {
        const data = clients.get(client);
        if (data) {
          existingPlayers.push({ id: data.id, name: data.name, skin: data.skin });
        }
      }

      clients.set(ws, { room: roomKey, id, name, skin });
      roomSet.add(ws);

      safeSend(ws, { type: 'player_list', players: existingPlayers });
      broadcast(roomKey, { type: 'player_join', id, name, skin }, ws);
      return;
    }

    const client = clients.get(ws);
    if (!client || !client.room) {
      return;
    }

    const allowedForward = new Set([
      'player_update',
      'block_break',
      'block_place',
      'chat',
      'admin_weather',
      'admin_calm',
      'admin_storm',
      'admin_day',
      'admin_night',
      'admin_heal',
      'admin_kill',
      'admin_kick'
    ]);

    if (allowedForward.has(msg.type)) {
      broadcast(client.room, msg, ws);
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    if (!client || !client.room) return;

    const roomKey = client.room;
    const room = rooms.get(roomKey);
    if (room) {
      room.delete(ws);
      if (room.size === 0) {
        rooms.delete(roomKey);
      }
    }

    broadcast(roomKey, { type: 'player_leave', id: client.id }, ws);
    clients.delete(ws);
  });

  ws.on('error', () => {});
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
