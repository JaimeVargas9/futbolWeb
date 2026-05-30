require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  transports: ['websocket', 'polling'],
  // LAN: detect silent disconnects faster than the 25s/20s defaults
  pingInterval: 10000,
  pingTimeout:   5000,
});

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/watch', (req, res) => res.sendFile(path.join(__dirname, 'public', 'watch.html')));

// Viewers can query current state on load
app.get('/api/status', (req, res) => {
  res.json({ broadcasting: isBroadcasting, viewers: viewers.size });
});

// ── State ────────────────────────────────────────────────────────────────────
let broadcasterId = null;
let isBroadcasting = false;
const viewers = new Set();

function emitViewerCount() {
  // Only the broadcaster cares about viewer count — don't broadcast to all sockets
  io.to('broadcaster-room').emit('viewer-count', viewers.size);
}

// ── Socket ───────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[+] ${socket.id}`);

  // ── BROADCASTER ─────────────────────────────────────────────────────────────

  socket.on('register-broadcaster', () => {
    // If someone else is already broadcasting, tell this client to go watch
    if (broadcasterId && isBroadcasting && broadcasterId !== socket.id) {
      console.log(`[BROADCAST] Active broadcast exists — redirecting ${socket.id} to /watch`);
      socket.emit('redirect-to-viewer');
      return;
    }

    broadcasterId = socket.id;
    socket.join('broadcaster-room');
    console.log(`[BROADCAST] Registered: ${socket.id}`);
    socket.emit('broadcaster-registered');
    emitViewerCount();
  });

  // Broadcaster has a real stream and is ready to send offers
  socket.on('start-broadcast', () => {
    if (socket.id !== broadcasterId) return;
    isBroadcasting = true;
    console.log(`[BROADCAST] Streaming started`);

    // Notify all waiting viewers the stream is live
    socket.to('viewers-room').emit('broadcaster-available');

    // Tell broadcaster about every viewer already waiting so it creates offers
    viewers.forEach(viewerId => {
      socket.emit('viewer-joined', { viewerId });
    });

    emitViewerCount();
  });

  // Broadcaster explicitly stopped
  socket.on('stop-broadcast', () => {
    if (socket.id !== broadcasterId) return;
    isBroadcasting = false;
    console.log(`[BROADCAST] Streaming stopped`);
    io.to('viewers-room').emit('broadcaster-left');
    emitViewerCount();
  });

  // ── VIEWER ──────────────────────────────────────────────────────────────────

  socket.on('register-viewer', () => {
    viewers.add(socket.id);
    socket.join('viewers-room');
    console.log(`[VIEW] +${socket.id}  (${viewers.size} total)`);
    emitViewerCount();

    if (broadcasterId && isBroadcasting) {
      // Stream is live: tell viewer to get ready and tell broadcaster a new viewer arrived
      socket.emit('broadcaster-available');
      io.to(broadcasterId).emit('viewer-joined', { viewerId: socket.id });
    } else {
      socket.emit('no-broadcaster');
    }
  });

  // ── SIGNALING RELAY ──────────────────────────────────────────────────────────

  socket.on('offer', ({ targetId, sdp }) => {
    io.to(targetId).emit('offer', { fromId: socket.id, sdp });
  });

  socket.on('answer', ({ targetId, sdp }) => {
    io.to(targetId).emit('answer', { fromId: socket.id, sdp });
  });

  socket.on('ice-candidate', ({ targetId, candidate }) => {
    io.to(targetId).emit('ice-candidate', { fromId: socket.id, candidate });
  });

  // ── DISCONNECT ───────────────────────────────────────────────────────────────

  socket.on('disconnect', () => {
    console.log(`[-] ${socket.id}`);

    if (socket.id === broadcasterId) {
      broadcasterId = null;
      isBroadcasting = false;
      console.log('[BROADCAST] Broadcaster disconnected');
      io.to('viewers-room').emit('broadcaster-left');
      emitViewerCount();
    }

    if (viewers.has(socket.id)) {
      viewers.delete(socket.id);
      console.log(`[VIEW] -${socket.id}  (${viewers.size} total)`);
      if (broadcasterId) {
        io.to(broadcasterId).emit('viewer-left', { viewerId: socket.id });
      }
      emitViewerCount();
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n✅  LAN Streamer listo`);
  console.log(`    Emisor:     http://localhost:${PORT}`);
  console.log(`    Receptor:   http://<TU-IP-LAN>:${PORT}/watch\n`);
});
