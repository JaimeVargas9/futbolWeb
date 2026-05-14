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
});

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/watch', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'watch.html'));
});

// State
let broadcasterId = null;
const viewers = new Set();

function viewerCount() {
  return viewers.size;
}

function broadcastViewerCount() {
  io.emit('viewer-count', viewerCount());
}

io.on('connection', (socket) => {
  console.log(`[+] Connected: ${socket.id}`);

  // --- Broadcaster registers ---
  socket.on('register-broadcaster', () => {
    broadcasterId = socket.id;
    socket.join('broadcaster');
    console.log(`[BROADCAST] Broadcaster registered: ${socket.id}`);
    socket.emit('broadcaster-ready');
    // Notify existing viewers that a broadcaster is now available
    socket.to('viewers').emit('broadcaster-available');
    broadcastViewerCount();
  });

  // --- Viewer joins ---
  socket.on('register-viewer', () => {
    viewers.add(socket.id);
    socket.join('viewers');
    console.log(`[VIEW] Viewer joined: ${socket.id} (total: ${viewerCount()})`);
    broadcastViewerCount();

    if (broadcasterId) {
      // Tell viewer a broadcaster exists
      socket.emit('broadcaster-available');
      // Tell broadcaster a new viewer wants a stream
      io.to(broadcasterId).emit('viewer-joined', { viewerId: socket.id });
    } else {
      socket.emit('no-broadcaster');
    }
  });

  // --- WebRTC signaling relay ---
  socket.on('offer', ({ targetId, sdp }) => {
    io.to(targetId).emit('offer', { fromId: socket.id, sdp });
  });

  socket.on('answer', ({ targetId, sdp }) => {
    io.to(targetId).emit('answer', { fromId: socket.id, sdp });
  });

  socket.on('ice-candidate', ({ targetId, candidate }) => {
    io.to(targetId).emit('ice-candidate', { fromId: socket.id, candidate });
  });

  // --- Disconnect ---
  socket.on('disconnect', () => {
    console.log(`[-] Disconnected: ${socket.id}`);

    if (socket.id === broadcasterId) {
      broadcasterId = null;
      console.log('[BROADCAST] Broadcaster left');
      io.to('viewers').emit('broadcaster-left');
      broadcastViewerCount();
    }

    if (viewers.has(socket.id)) {
      viewers.delete(socket.id);
      console.log(`[VIEW] Viewer left: ${socket.id} (total: ${viewerCount()})`);
      if (broadcasterId) {
        io.to(broadcasterId).emit('viewer-left', { viewerId: socket.id });
      }
      broadcastViewerCount();
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n✅ LAN Streamer running`);
  console.log(`   Local:   http://localhost:${PORT}`);
  console.log(`   Network: http://<YOUR-LAN-IP>:${PORT}`);
  console.log(`   Watch:   http://<YOUR-LAN-IP>:${PORT}/watch\n`);
});
