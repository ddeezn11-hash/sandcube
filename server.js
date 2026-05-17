const express = require("express");const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");

const PORT = process.env.PORT || 3000;

const app = express();
app.use(cors());
app.use(express.json());

app.get("/", (req, res) => {
  res.send("World Multiplayer Server Running");
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

/*
ROOM STRUCTURE
{
  code: {
    clients: Set<WebSocket>,
    hostId: string|null,
    locked: boolean,
    players: Map<socketId, playerData>
  }
}
*/

const rooms = new Map();

function makeId() {
  return Math.random().toString(36).slice(2, 10);
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(roomCode, data, except = null) {
  const room = rooms.get(roomCode);
  if (!room) return;

  for (const client of room.clients) {
    if (client !== except && client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  }
}

function removeClient(ws) {
  const roomCode = ws.roomCode;
  if (!roomCode) return;

  const room = rooms.get(roomCode);
  if (!room) return;

  room.clients.delete(ws);
  room.players.delete(ws.id);

  broadcast(roomCode, {
    type: "player_leave",
    id: ws.id
  });

  if (room.hostId === ws.id) {
    room.hostId = null;

    const next = [...room.clients][0];
    if (next) {
      room.hostId = next.id;

      broadcast(roomCode, {
        type: "host_change",
        hostId: next.id
      });
    }
  }

  if (room.clients.size === 0) {
    rooms.delete(roomCode);
  }
}

wss.on("connection", (ws) => {
  ws.id = makeId();

  send(ws, {
    type: "connected",
    id: ws.id
  });

  ws.on("message", (msg) => {
    try {
      const data = JSON.parse(msg.toString());

      switch (data.type) {
        case "join": {
          const roomCode = data.roomCode;

          if (!rooms.has(roomCode)) {
            rooms.set(roomCode, {
              clients: new Set(),
              hostId: ws.id,
              locked: false,
              players: new Map()
            });
          }

          const room = rooms.get(roomCode);

          if (room.locked) {
            send(ws, {
              type: "error",
              message: "World is locked"
            });
            return;
          }

          ws.roomCode = roomCode;
          room.clients.add(ws);

          room.players.set(ws.id, {
            id: ws.id,
            username: data.username || "Player",
            skin: data.skin || "#ffffff"
          });

          send(ws, {
            type: "joined",
            roomCode,
            hostId: room.hostId,
            players: [...room.players.values()]
          });

          broadcast(roomCode, {
            type: "player_join",
            player: room.players.get(ws.id)
          }, ws);

          break;
        }

        case "player_update":
        case "chat":
        case "block_place":
        case "block_break":
        case "map_marker":
        case "damage":
        case "heal":
        case "drop_item":
        case "pickup_item":
        case "spawn_animal":
        case "kill_animal":
        case "weather":
        case "time_update":
        case "explosion":
        case "sound":
        case "animation":
        case "projectile":
        case "effect":
        case "inventory_update":
        case "craft":
        case "xp_update":
        case "death":
        case "respawn":
        case "admin_broadcast":
        case "admin_spawn":
        case "admin_clear_drops":
        case "admin_day":
        case "admin_night":
        case "admin_rain":
        case "admin_tornado":
        case "admin_peace":
        case "admin_stats":
        case "admin_coords":
        case "admin_nightvision":
        case "admin_save":
        case "admin_fill_inventory":
        case "admin_heal":
        case "admin_tp_spawn":
        case "admin_toggle_god":
        case "admin_toggle_fly":
        case "admin_toggle_creative":
        case "admin_toggle_noclip":
        case "admin_toggle_speed": {
          if (!ws.roomCode) return;

          data.sender = ws.id;

          broadcast(ws.roomCode, data, ws);
          break;
        }

        case "admin_lock": {
          const room = rooms.get(ws.roomCode);
          if (!room) return;

          if (room.hostId !== ws.id) return;

          room.locked = !!data.locked;

          broadcast(ws.roomCode, {
            type: "world_locked",
            locked: room.locked
          });

          break;
        }

        case "admin_kick": {
          const room = rooms.get(ws.roomCode);
          if (!room) return;

          if (room.hostId !== ws.id) return;

          for (const client of room.clients) {
            if (client.id === data.targetId) {
              send(client, {
                type: "kicked",
                reason: data.reason || "Kicked by host"
              });

              client.close();
              break;
            }
          }

          break;
        }

        case "admin_summon": {
          const room = rooms.get(ws.roomCode);
          if (!room) return;

          if (room.hostId !== ws.id) return;

          broadcast(ws.roomCode, {
            type: "summon_player",
            targetId: data.targetId,
            x: data.x,
            y: data.y,
            z: data.z
          });

          break;
        }

        case "ping": {
          send(ws, {
            type: "pong",
            time: Date.now()
          });
          break;
        }

        default:
          console.log("Unknown packet:", data.type);
      }
    } catch (err) {
      console.error("WS message error:", err);
    }
  });

  ws.on("close", () => {
    removeClient(ws);
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err);
    removeClient(ws);
  });
});

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
