import { WebSocketServer } from "ws";
import WebSocket from "ws";
import redis from "redis";

const redisClient = redis.createClient();

const wss = new WebSocketServer({ port: 8080 });

const subscriber = redisClient.duplicate();
await subscriber.connect();
console.log("Connected to Redis");

const clients: Set<WebSocket> = new Set();
wss.on("connection", (ws) => {
  console.log("New client connected");
  clients.add(ws);

  ws.on("close", () => {
    console.log("Client disconnected");
    clients.delete(ws);
  });
});

await subscriber.subscribe("price_updates", (message) => {
  const trade = JSON.parse(message);

  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(trade));
    }
  });
});
