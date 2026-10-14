// Fake-price live-connection test service for the Muse dashboard client test.
// NO market data, NO vendor credentials. Every payload is labeled FAKE.
// Endpoints:
//   GET /         -> small self-test page (same code the temp Muse view uses)
//   GET /health   -> { ok: true }
//   GET /stream   -> Server-Sent Events, one message per second
//   WS  /ws       -> WebSocket, one message per second
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;
const TICK_MS = 1000;

const symbols = [
  { symbol: 'TEST-QQQ', price: 500.0 },
  { symbol: 'TEST-SPY', price: 600.0 },
  { symbol: 'TEST-SOXX', price: 250.0 },
];
let seq = 0;

function nextMessage() {
  seq += 1;
  const serverTimeMs = Date.now();
  for (const s of symbols) {
    const step = (Math.random() - 0.5) * 0.4; // +- $0.20 random walk
    s.price = Math.max(1, Math.round((s.price + step) * 100) / 100);
  }
  return {
    type: 'fake_price_tick',
    fake: true,
    label: 'FAKE TEST DATA - NOT REAL MARKET DATA',
    seq,
    serverTimeMs,
    prices: symbols.map((s) => ({ symbol: s.symbol, price: s.price })),
  };
}

// ---- WebSocket plumbing (minimal RFC 6455, text frames only) ----
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function wsAccept(key) {
  return crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
}
function wsEncode(text) {
  const payload = Buffer.from(text, 'utf8');
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.from([0x81, len]);
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81; header[1] = 126; header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}
const wsClients = new Set();
const sseClients = new Set();

setInterval(() => {
  const msg = JSON.stringify(nextMessage());
  for (const res of sseClients) res.write(`data: ${msg}\n\n`);
  for (const sock of wsClients) {
    try { sock.write(wsEncode(msg)); } catch { wsClients.delete(sock); }
  }
}, TICK_MS);

const page = `<!doctype html>
<html><head><meta charset="utf-8"><title>FAKE price connection test</title>
<style>body{font-family:system-ui;background:#111;color:#eee;padding:16px}
.fake{color:#ffb020;font-weight:700;letter-spacing:.05em}
.card{display:inline-block;border:1px solid #444;border-radius:8px;padding:12px 16px;margin:6px;min-width:150px}
.price{font-size:28px;font-variant-numeric:tabular-nums}
.delay{color:#9ad}</style></head><body>
<div class="fake">FAKE TEST DATA - NOT REAL MARKET DATA</div>
<h2>Live connection test (SSE + WebSocket)</h2>
<div id="cards"></div>
<p>SSE: <span id="sse-status">connecting</span> &nbsp; WS: <span id="ws-status">connecting</span></p>
<p>Last seq: <span id="seq">-</span> &nbsp; Observed delay (server stamp to render): <span class="delay" id="delay">-</span> ms</p>
<script>
const cards = {};
function render(msg, transport) {
  const delay = Date.now() - msg.serverTimeMs;
  document.getElementById('delay').textContent = delay + ' (' + transport + ')';
  document.getElementById('seq').textContent = msg.seq;
  const wrap = document.getElementById('cards');
  for (const p of msg.prices) {
    if (!cards[p.symbol]) {
      const d = document.createElement('div'); d.className = 'card';
      d.innerHTML = '<div>' + p.symbol + '</div><div class="price"></div>';
      wrap.appendChild(d); cards[p.symbol] = d.querySelector('.price');
    }
    cards[p.symbol].textContent = p.price.toFixed(2);
  }
}
const es = new EventSource('/stream');
es.onopen = () => document.getElementById('sse-status').textContent = 'connected';
es.onmessage = (e) => render(JSON.parse(e.data), 'sse');
es.onerror = () => document.getElementById('sse-status').textContent = 'error/reconnecting';
const wsProto = location.protocol === 'https:' ? 'wss://' : 'ws://';
const ws = new WebSocket(wsProto + location.host + '/ws');
ws.onopen = () => document.getElementById('ws-status').textContent = 'connected';
ws.onmessage = (e) => render(JSON.parse(e.data), 'ws');
ws.onerror = () => document.getElementById('ws-status').textContent = 'error';
</script></body></html>`;

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, fake: true }));
    return;
  }
  if (req.url === '/stream') {
    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
      'access-control-allow-origin': '*',
    });
    res.write(': connected\n\n');
    sseClients.add(res);
    req.on('close', () => sseClients.delete(res));
    return;
  }
  if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(page);
    return;
  }
  res.writeHead(404); res.end('not found');
});

server.on('upgrade', (req, socket) => {
  if (req.url !== '/ws' || !req.headers['sec-websocket-key']) { socket.destroy(); return; }
  const accept = wsAccept(req.headers['sec-websocket-key']);
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  wsClients.add(socket);
  socket.on('data', (buf) => {
    // Minimal client-frame handling: reply to ping (0x9) with pong (0xA), drop on close (0x8).
    if (!buf.length) return;
    const opcode = buf[0] & 0x0f;
    if (opcode === 0x8) { socket.end(); wsClients.delete(socket); }
    else if (opcode === 0x9) { socket.write(Buffer.from([0x8a, 0])); }
  });
  socket.on('close', () => wsClients.delete(socket));
  socket.on('error', () => wsClients.delete(socket));
});

server.listen(PORT, () => console.log(`fake-price test service listening on ${PORT}`));
