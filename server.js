// 20 Gang 3D — online server
// Serves the game (public/) and runs one shared room: a lobby with a countdown,
// then a match of 10 teams x 5 slots. Real players take slots, bots fill the rest.
// One player in the match ("host") runs the bots; everyone else follows the network.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const LOBBY_SECONDS = +process.env.LOBBY_SECONDS || 60;    // countdown before a match
const MATCH_SECONDS = +process.env.MATCH_SECONDS || 600;   // hard time limit for a match
const END_SECONDS = 8;                                     // results screen before the next lobby
const TEAMS = 10, PER_TEAM = 5, MAX_PLAYERS = TEAMS * PER_TEAM, MAX_CONN = 200;
const ROOT = path.join(__dirname, 'public');
const BOT_NAMES = ['Ahmet','Mehmet','Emre','Burak','Can','Mert','Kerem','Ali','Hasan','Murat','Serkan','Onur','Yusuf','Ömer','Kaan',
  'Arda','Efe','Tolga','Cem','Deniz','Barış','Uğur','Selim','Oğuz','Furkan','Eren','Kemal','Volkan','Taner','Sinan'];

// ---------- static files ----------
const MIME = { '.html':'text/html; charset=utf-8', '.js':'text/javascript; charset=utf-8', '.css':'text/css; charset=utf-8',
  '.png':'image/png', '.jpg':'image/jpeg', '.jpeg':'image/jpeg', '.ico':'image/x-icon', '.svg':'image/svg+xml', '.txt':'text/plain; charset=utf-8',
  '.json':'application/json', '.xml':'application/xml', '.webmanifest':'application/manifest+json' };
const server = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/health'){ res.writeHead(200, {'content-type':'text/plain'}); res.end('ok'); return; }
  if (p.endsWith('/')) p += 'index.html';
  const file = path.normalize(path.join(ROOT, p));
  if (!file.startsWith(ROOT)){ res.writeHead(403); res.end(); return; }
  fs.readFile(file, (err, data) => {
    if (err){ res.writeHead(404, {'content-type':'text/plain'}); res.end('Not found'); return; }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, { 'content-type': MIME[ext] || 'application/octet-stream',
      'cache-control': ext === '.html' ? 'no-cache' : 'public, max-age=86400' });
    res.end(data);
  });
});

// ---------- room ----------
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 32 * 1024 });
const clients = new Map();          // ws -> player
let nextId = 1;
let phase = 'lobby';                // lobby | playing | ended
let countdownEnd = 0, matchEnd = 0;
let slots = [], hostId = null, alive = [], lastPos = {};

const send = (ws, o) => { if (ws.readyState === 1) ws.send(typeof o === 'string' ? o : JSON.stringify(o)); };
const inMatch = () => [...clients.values()].filter(c => c.inMatch);
const waiting = () => [...clients.values()].filter(c => !c.inMatch && c.joined);
function toMatch(o, except){ const s = JSON.stringify(o); for (const c of clients.values()) if (c.inMatch && c !== except) send(c.ws, s); }
function byId(id){ for (const c of clients.values()) if (c.id === id) return c; return null; }

function cleanName(n){ n = String(n || '').replace(/[<>&"'`]/g, '').replace(/\s+/g, ' ').trim().slice(0, 12); return n || 'Oyuncu'; }
function cleanLook(l){
  const out = {};
  if (!l || typeof l !== 'object') return out;
  for (const [k, v] of Object.entries(l).slice(0, 40)){
    if (!/^[a-zA-Z]{1,16}$/.test(k)) continue;
    if (typeof v === 'string' && v.length <= 32) out[k] = v;
    else if (typeof v === 'number' && isFinite(v)) out[k] = v;
    else if (Array.isArray(v)) out[k] = v.filter(x => typeof x === 'string' && x.length <= 24).slice(0, 12);
  }
  return out;
}

function lobbyState(){
  const w = waiting();
  return { t:'lobby', phase, left: phase === 'lobby' && countdownEnd ? (countdownEnd - Date.now())/1000 : LOBBY_SECONDS,
    count: w.length, inMatch: inMatch().length, max: MAX_PLAYERS, players: w.slice(0, MAX_PLAYERS).map(c => ({ id:c.id, name:c.name })) };
}
function broadcastLobby(){ const s = JSON.stringify(lobbyState()); for (const c of clients.values()) if (!c.inMatch && c.joined) send(c.ws, s); }

function startMatch(){
  const players = waiting().slice(0, MAX_PLAYERS);
  if (!players.length){ countdownEnd = 0; return; }
  slots = [];
  const used = new Set();
  for (let i = 0; i < MAX_PLAYERS; i++){
    const team = Math.floor(i / PER_TEAM), k = i % PER_TEAM;
    let num; do { num = 2 + Math.floor(Math.random()*97); } while (used.has(team*100 + num)); used.add(team*100 + num);
    slots.push({ i, team, k, owner: null, name: BOT_NAMES[Math.floor(Math.random()*BOT_NAMES.length)], num });
  }
  // spread people over the teams: 1st player -> team 0, 2nd -> team 1 ... 11th -> team 0 again
  players.forEach((c, n) => {
    const team = n % TEAMS, k = [2, 1, 3, 0, 4][Math.floor(n / TEAMS)];
    const sl = slots[team*PER_TEAM + k];
    sl.owner = c.id; sl.name = c.name; sl.num = c.num; sl.look = c.look;
    c.inMatch = true; c.slot = sl.i;
  });
  hostId = players[0].id;
  alive = slots.map(() => true); lastPos = {};
  phase = 'playing'; matchEnd = Date.now() + MATCH_SECONDS*1000; countdownEnd = 0;
  for (const c of players) send(c.ws, { t:'start', you: c.id, host: hostId, map: 'sehir', slots });
  broadcastLobby();
  log(`match started: ${players.length} players, ${MAX_PLAYERS - players.length} bots`);
}
// someone arrives while a match is running: they take over a living bot right away
function lateJoin(c){
  if (phase !== 'playing' || c.inMatch) return false;
  const humans = new Array(TEAMS).fill(0), bots = new Array(TEAMS).fill(0);
  for (const sl of slots){ if (sl.owner) humans[sl.team]++; else if (alive[sl.i]) bots[sl.team]++; }
  let best = -1, score = -1e9;
  for (let t = 0; t < TEAMS; t++){
    if (!bots[t]) continue;
    const sc = (humans[t] ? 0 : 100) - humans[t]*10 + bots[t];    // a team of bots first: you lead it
    if (sc > score){ score = sc; best = t; }
  }
  if (best < 0) return false;
  const sl = slots.find(x => x.team === best && !x.owner && alive[x.i]);
  if (!sl) return false;
  sl.owner = c.id; sl.name = c.name; sl.num = c.num; sl.look = c.look;
  c.inMatch = true; c.slot = sl.i;
  send(c.ws, { t:'start', you: c.id, host: hostId, map: 'sehir', slots, late: true, alive, pos: lastPos });
  toMatch({ t:'joined', i: sl.i, name: c.name, num: c.num, look: c.look }, c);
  log(`late join: ${c.name} -> slot ${sl.i} (team ${sl.team})`);
  return true;
}
function teamsAlive(){ const t = new Set(); slots.forEach((s, i) => { if (alive[i]) t.add(s.team); }); return t; }
function endMatch(w){
  if (phase !== 'playing') return;
  phase = 'ended';
  toMatch({ t:'end', w });
  log(`match ended, winner team ${w}`);
  setTimeout(() => {
    for (const c of clients.values()){ c.inMatch = false; c.slot = -1; }
    slots = []; hostId = null; phase = 'lobby'; countdownEnd = 0;
    broadcastLobby();
  }, END_SECONDS*1000);
}
function pickHost(){
  const m = inMatch();
  hostId = m.length ? m[0].id : null;
  if (hostId) toMatch({ t:'host', id: hostId });
}

setInterval(() => {
  const now = Date.now();
  if (phase === 'lobby'){
    const w = waiting().length;
    if (!w) countdownEnd = 0;
    else {
      if (!countdownEnd) countdownEnd = now + LOBBY_SECONDS*1000;
      if (w >= MAX_PLAYERS && countdownEnd - now > 5000) countdownEnd = now + 5000;   // full lobby: go right away
      if (now >= countdownEnd) startMatch();
    }
    broadcastLobby();
  } else if (phase === 'playing'){
    if (!inMatch().length){ endMatch(-1); return; }
    if (now >= matchEnd){
      // time is up: the team with the most soldiers left wins
      const cnt = new Array(TEAMS).fill(0); slots.forEach((s, i) => { if (alive[i]) cnt[s.team]++; });
      endMatch(cnt.indexOf(Math.max(...cnt)));
    }
    for (const c of waiting()) if (!lateJoin(c)) break;
    if (now % 5000 < 1000) broadcastLobby();
  }
}, 1000);

wss.on('connection', ws => {
  if (clients.size >= MAX_CONN){ send(ws, { t:'full' }); ws.close(); return; }
  const c = { ws, id: nextId++, name: 'Oyuncu', num: 10, look: {}, joined: false, inMatch: false, slot: -1, msgs: 0 };
  clients.set(ws, c);
  send(ws, { t:'welcome', id: c.id });
  ws.on('message', raw => {
    if (++c.msgs > 240) return;          // flood guard (reset every second)
    let m; try { m = JSON.parse(raw); } catch(e){ return; }
    if (!m || typeof m.t !== 'string') return;
    switch (m.t){
      case 'join':
        if (c.joined) return;
        c.name = cleanName(m.name); c.num = Math.max(1, Math.min(99, m.num | 0 || 10)); c.look = cleanLook(m.look); c.joined = true;
        if (phase === 'playing' && lateJoin(c)) return;
        send(ws, lobbyState()); broadcastLobby();
        break;
      case 's':     // my own state (+ my shots) -> everyone else in the match
        if (!c.inMatch || !Array.isArray(m.d) || m.d.length > 10) return;
        lastPos[c.slot] = m.d;
        toMatch({ t:'s', i: c.slot, d: m.d, f: Array.isArray(m.f) ? m.f.slice(0, 12) : undefined }, c);
        break;
      case 'b':     // bot states from the host
        if (!c.inMatch || c.id !== hostId || !Array.isArray(m.d)) return;
        for (const d of m.d) if (Array.isArray(d) && slots[d[0]]) lastPos[d[0]] = d.slice(1);
        toMatch({ t:'b', d: m.d.slice(0, MAX_PLAYERS), f: Array.isArray(m.f) ? m.f.slice(0, 200) : undefined }, c);
        break;
      case 'hit': { // route damage to whoever owns the victim: its player, or the host for bots
        if (!c.inMatch || phase !== 'playing') return;
        const sl = slots[m.i | 0]; if (!sl || !alive[sl.i]) return;
        const owner = sl.owner ? byId(sl.owner) : byId(hostId);
        if (owner && owner !== c) send(owner.ws, { t:'hit', i: sl.i, s: m.s | 0, d: Math.max(0, Math.min(100, +m.d || 0)), h: typeof m.h === 'string' ? m.h.slice(0, 8) : '' });
        break;
      }
      case 'kill': {
        if (!c.inMatch || phase !== 'playing') return;
        const sl = slots[m.i | 0]; if (!sl || !alive[sl.i]) return;
        const authority = sl.owner ? sl.owner : hostId;
        if (c.id !== authority) return;
        alive[sl.i] = false;
        toMatch({ t:'kill', i: sl.i, s: m.s | 0, h: typeof m.h === 'string' ? m.h.slice(0, 8) : '' }, c);
        const ta = teamsAlive();
        if (ta.size <= 1) setTimeout(() => endMatch(ta.size ? [...ta][0] : -1), 1500);
        break;
      }
      case 'order': {
        if (!c.inMatch) return;
        const sl = slots[c.slot]; if (!sl) return;
        const o = m.o === 'attack' ? 'attack' : 'follow';
        toMatch({ t:'order', team: sl.team, o }, c);
        break;
      }
    }
  });
  ws.on('close', () => {
    clients.delete(ws);
    if (c.inMatch && phase === 'playing'){
      const sl = slots[c.slot];
      if (sl){ sl.owner = null; toMatch({ t:'left', i: sl.i }); }
      if (hostId === c.id) pickHost();
    }
    broadcastLobby();
  });
});
setInterval(() => { for (const c of clients.values()) c.msgs = 0; }, 1000);
// drop dead connections
setInterval(() => { for (const c of clients.values()){ if (c.ws.isAlive === false){ c.ws.terminate(); continue; } c.ws.isAlive = false; try { c.ws.ping(); } catch(e){} } }, 30000);
wss.on('connection', ws => { ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; }); });

function log(t){ console.log(new Date().toISOString().slice(11, 19), t); }
server.listen(PORT, () => log(`20 Gang 3D server on :${PORT}  (lobby ${LOBBY_SECONDS}s)`));
