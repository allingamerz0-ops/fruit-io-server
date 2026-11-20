'use strict';
/**
 * FRUIT.IO multiplayer backend (Node.js + Socket.IO), server-authoritative.
 *
 * The client only sends: a desired heading, a boost request and an emoji id.
 * Movement, speed, collisions, kills, food, growth and bots are all simulated here.
 * The wire format of `state` / `dead` / `kill` / `init` mirrors the legacy WebSocket
 * protocol of fruit-io.html, so the existing client `onMsg()` can be reused (see README).
 */

const http = require('node:http');
const crypto = require('node:crypto');
const { Server } = require('socket.io');
const pkg = require('./package.json');

/* ------------------------------------------------------------------ config */
const num = (name, def, min, max) => {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const v = Number(raw);
  return Number.isFinite(v) ? Math.min(max, Math.max(min, v)) : def;
};
const bool = (name, def) => {
  const raw = process.env[name];
  return raw === undefined || raw === '' ? def : /^(1|true|yes|on)$/i.test(raw);
};
const PROD = process.env.NODE_ENV === 'production';

const CFG = Object.freeze({
  PORT: num('PORT', 3000, 1, 65535),
  HOST: '0.0.0.0',
  TICK_RATE: num('TICK_RATE', 30, 10, 60), // simulation steps per second
  SNAPSHOT_RATE: num('SNAPSHOT_RATE', 15, 5, 30), // state messages per second per client
  MAX_PLAYERS_PER_ROOM: num('MAX_PLAYERS_PER_ROOM', 20, 1, 20),
  MAX_ROOMS: num('MAX_ROOMS', 2, 1, 50),
  TARGET_POPULATION: num('TARGET_POPULATION', 20, 0, 40), // humans + bots; bots fill the gap
  FOOD_TARGET: num('FOOD_TARGET', 1400, 50, 4000),
  BIG_FOOD_MAX: num('BIG_FOOD_MAX', 36, 0, 100),
  FOOD_VIEW_X: num('FOOD_VIEW_X', 1300, 400, 4000), // half-width of the food window sent to each player
  FOOD_VIEW_Y: num('FOOD_VIEW_Y', 900, 300, 4000),
  ROOM_IDLE_MS: num('ROOM_IDLE_MS', 60000, 1000, 3600000),
  RECONNECT_GRACE_MS: num('RECONNECT_GRACE_MS', 15000, 0, 120000),
  AFK_KICK_MS: num('AFK_KICK_MS', 60000, 0, 3600000), // 0 disables
  MAX_CONN_PER_IP: num('MAX_CONN_PER_IP', 6, 1, 100),
  ALLOW_NO_ORIGIN: bool('ALLOW_NO_ORIGIN', !PROD), // non-browser clients (tests, curl)
  ALLOW_LOCALHOST: bool('ALLOW_LOCALHOST', !PROD),
});
const SNAP_EVERY = Math.max(1, Math.round(CFG.TICK_RATE / CFG.SNAPSHOT_RATE));

/* ----------------------------------------------------------------- logging */
const log = (lvl, msg, meta) =>
  console.log(JSON.stringify({ t: new Date().toISOString(), lvl, msg, ...meta }));

/* ------------------------------------------------------------ CORS / origins */
// CrazyGames serves games from https://<slug>.game-files.crazygames.com (see docs.crazygames.com sitelock).
const DEFAULT_ORIGINS = ['https://*.crazygames.com', 'https://crazygames.com', 'https://*.1001juegos.com', 'https://1001juegos.com'];
const CG_REGIONAL_RE = /^https:\/\/(?:[a-z0-9-]+\.)*crazygames\.(?:com|[a-z]{2}|co\.[a-z]{2}|com\.[a-z]{2})$/i;
const LOCAL_RE = /^https?:\/\/(?:localhost|127\.0\.0\.1)(?::\d{1,5})?$/i;
const escRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

let ANY_ORIGIN = false;
const ORIGIN_RES = [];
for (const raw of [...DEFAULT_ORIGINS, ...(process.env.ALLOWED_ORIGINS || '').split(',')]) {
  const entry = raw.trim().toLowerCase().replace(/\/+$/, '');
  if (!entry) continue;
  if (entry === '*') { ANY_ORIGIN = true; log('warn', 'ALLOWED_ORIGINS contains * : every website can connect'); continue; }
  if (entry.includes('*')) {
    const m = /^([a-z][a-z0-9+.-]*:\/\/)\*\.([^*]+)$/.exec(entry); // only "scheme://*.domain" is supported
    if (!m) { log('warn', 'ignoring unsupported origin pattern', { entry }); continue; }
    ORIGIN_RES.push(new RegExp(`^${escRe(m[1])}(?:[a-z0-9-]+\\.)+${escRe(m[2])}$`));
  } else ORIGIN_RES.push(new RegExp(`^${escRe(entry)}$`));
}
function originAllowed(origin) {
  if (!origin) return CFG.ALLOW_NO_ORIGIN;
  const o = String(origin).toLowerCase();
  return ANY_ORIGIN || ORIGIN_RES.some((re) => re.test(o)) || CG_REGIONAL_RE.test(o) || (CFG.ALLOW_LOCALHOST && LOCAL_RE.test(o));
}
const rejected = new Set();
const logRejected = (origin) => {
  const k = String(origin);
  if (rejected.size < 200 && !rejected.has(k)) { rejected.add(k); log('warn', 'origin rejected', { origin: k }); }
};

/* ----------------------------------------------------------------- helpers */
const TAU = Math.PI * 2;
const rnd = (a, b) => a + Math.random() * (b - a);
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const normAngle = (a) => { a %= TAU; return a > Math.PI ? a - TAU : a < -Math.PI ? a + TAU : a; };
const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const reply = (ack, obj) => { if (typeof ack === 'function') { try { ack(obj); } catch (e) { /* ignore ack errors */ } } };

class Bucket { // token bucket rate limiter
  constructor(rate, burst) { this.rate = rate; this.burst = burst; this.tokens = burst; this.t = Date.now(); }
  take() {
    const n = Date.now();
    this.tokens = Math.min(this.burst, this.tokens + ((n - this.t) / 1000) * this.rate);
    this.t = n;
    if (this.tokens >= 1) { this.tokens -= 1; return true; }
    return false;
  }
}

function cleanName(v) {
  if (typeof v !== 'string') return '';
  const s = v.normalize('NFKC').replace(/[^\p{L}\p{N} _.\-]/gu, '').replace(/\s+/g, ' ').trim();
  return Array.from(s).slice(0, 12).join('');
}

/* --------------------------------------------------------------- game data */
const WORLD = 6000;
const FRUIT_D = [1.05, 1, 1.05, 1.1, 1.02, 1.05, 1.08, 1.12, 1.12]; // weak-point distance per fruit (Apple..Dragon Fruit)
const FRUIT_COUNT = FRUIT_D.length;
const FOOD_COLORS = 7; // client food palette size; ids >=10 mean "fruit colour 10+type"
const BIG_R = [0, 20, 21, 19, 22]; // power-up radius by kind: 1 magnet, 2 turbo, 3 star, 4 heart
const BIG_V = [0, 6, 8, 10, 12]; // growth value by kind
const MAGNET_SEC = 7, TURBO_SEC = 3, BOOST_SEC = 0.55, BOOST_CD = 3;
const NAMES = ['Liam', 'Sofia', 'Noah', 'Aarav', 'Mia', 'Kenji', 'Zara', 'Mateo', 'Elena', 'Omar', 'Chloe', 'Yuki', 'Lucas', 'Nadia', 'Arjun', 'Hana', 'Jake', 'Priya', 'Leo', 'Isla', 'Tariq', 'Ava', 'Diego', 'Maya', 'Ethan', 'Lina', 'Ravi', 'Nora'];
const rad = (g) => Math.sqrt(484 + 70 * g); // area grows with what you eat: no cap

/* ------------------------------------------------------------------- state */
const rooms = new Map(); // roomId -> Room
const clients = new Map(); // socket.id -> client
const sessions = new Map(); // reconnect token -> human entity
const ipCount = new Map();
let nextEntId = 1, arenaSeq = 0, shuttingDown = false, tickMs = 0;

/* -------------------------------------------------------------------- room */
class Room {
  constructor(id, priv) {
    this.id = id; this.priv = priv;
    this.ents = new Map(); this.foods = []; this.fx = []; this.respawns = [];
    this.nextFood = 1; this.nbig = 0; this.nh = 0; this.nb = 0; this.tick = 0;
    this.idleSince = Date.now();
    while (this.foods.length < CFG.FOOD_TARGET) this.addPellet();
    for (let i = 0; i < CFG.BIG_FOOD_MAX; i++) this.addBig();
  }

  /* ---- entities ---- */
  spawnPos() { // best of 8 random points = farthest from everyone
    let best = { x: WORLD / 2, y: WORLD / 2 }, bd = -1;
    for (let i = 0; i < 8; i++) {
      const x = rnd(300, WORLD - 300), y = rnd(300, WORLD - 300);
      let md = 1e9;
      for (const e of this.ents.values()) md = Math.min(md, Math.hypot(e.x - x, e.y - y));
      if (md > bd) { bd = md; best = { x, y }; }
    }
    return best;
  }
  newEnt(human, name, type) {
    const p = this.spawnPos(), a = rnd(-Math.PI, Math.PI);
    return {
      id: nextEntId++, room: this, x: p.x, y: p.y, a, ta: a, sp: 0, vx: 0, vy: 0, g: 0, r: 22, type, name, human,
      bt: 0, bc: 0, mg: 0, k: 0, food: 0, alive: true, th: 0, bb: false,
      sd: Math.random() < 0.5 ? 1 : -1, ag: Math.random() < 0.7,
      client: null, token: null, dcTimer: null, lastInput: Date.now(), lastEmoji: 0,
    };
  }
  addHuman(client, name, type) {
    const e = this.newEnt(true, name, type);
    e.client = client; e.token = crypto.randomBytes(24).toString('hex');
    sessions.set(e.token, e);
    this.ents.set(e.id, e); this.nh++;
    return e;
  }
  addBot() {
    const e = this.newEnt(false, NAMES[(Math.random() * NAMES.length) | 0] + (Math.random() < 0.35 ? (10 + Math.random() * 89) | 0 : ''), (Math.random() * FRUIT_COUNT) | 0);
    e.g = rnd(0, 30); e.r = rad(e.g);
    this.ents.set(e.id, e); this.nb++;
  }
  removeEnt(e) {
    e.alive = false;
    if (!this.ents.delete(e.id)) return;
    if (e.human) {
      this.nh--;
      if (e.token) { sessions.delete(e.token); e.token = null; }
      if (e.dcTimer) { clearTimeout(e.dcTimer); e.dcTimer = null; }
      if (this.nh === 0) this.idleSince = Date.now();
    } else this.nb--;
  }
  kick(e, code) {
    const c = e.client;
    if (c) { c.ent = null; c.socket.emit('kicked', { code }); }
    e.client = null;
    this.removeEnt(e);
  }

  /* ---- food ---- */
  addFood(x, y, v, c, vx = 0, vy = 0, k = 0) {
    const r = k ? BIG_R[k] : Math.min(15, Math.round(4 + v * 3.4));
    this.foods.push({ id: this.nextFood++, x, y, v, c, r, vx, vy, k });
    if (k) this.nbig++;
  }
  addPellet() { this.addFood(rnd(30, WORLD - 30), rnd(30, WORLD - 30), [0.5, 1, 1, 1.5][(Math.random() * 4) | 0], (Math.random() * FOOD_COLORS) | 0); }
  addBig() {
    const k = 1 + ((Math.random() * 4) | 0);
    this.addFood(rnd(80, WORLD - 80), rnd(80, WORLD - 80), BIG_V[k], 7, 0, 0, k);
  }
  collect(e, dt) { // eat + pull nearby food (wider and faster while the magnet is active)
    const foods = this.foods, mag = e.mg > 0, pr = mag ? e.r * 4.5 : e.r * 2.2, pr2 = pr * pr, pull = mag ? 9 : 4;
    for (let i = foods.length - 1; i >= 0; i--) {
      const f = foods[i], dx = f.x - e.x, dy = f.y - e.y, R = e.r * 0.95 + f.r, q = dx * dx + dy * dy;
      if (q < R * R) {
        e.g += f.v; e.food++;
        if (f.k) { this.nbig--; this.powerUp(e, f.k); }
        foods[i] = foods[foods.length - 1]; foods.pop();
      } else if (q < pr2 && !f.k) { f.x -= dx * dt * pull; f.y -= dy * dt * pull; }
    }
  }
  powerUp(e, k) {
    if (k === 1) e.mg = MAGNET_SEC;
    else if (k === 2) { e.bt = TURBO_SEC; e.bc = Math.max(e.bc, BOOST_CD); }
    else return;
    if (e.client) e.client.socket.emit('power', { t: 'power', k, s: k === 1 ? MAGNET_SEC : TURBO_SEC });
  }

  /* ---- combat ---- */
  boost(e) { if (e.alive && e.bc <= 0) { e.bt = BOOST_SEC; e.bc = BOOST_CD; } }
  hit(a, b) { // a's front tip against b's front weak point; returns closing speed or 0
    const ta = a.r * 0.9, tx = a.x + Math.cos(a.a) * ta, ty = a.y + Math.sin(a.a) * ta;
    const w = b.r * FRUIT_D[b.type], wx = b.x + Math.cos(b.a) * w, wy = b.y + Math.sin(b.a) * w;
    const dx = wx - tx, dy = wy - ty, R = a.r * 0.2 + b.r * 0.26, q = dx * dx + dy * dy;
    if (q > R * R) return 0;
    const L = Math.sqrt(q) || 1, c = ((a.vx - b.vx) * dx + (a.vy - b.vy) * dy) / L;
    return c > 30 ? c : 0;
  }
  rear(att, def, d) { // attacker driving into defender's rear cone -> the ATTACKER is eliminated
    const ux = (att.x - def.x) / d, uy = (att.y - def.y) / d;
    if (-(Math.cos(def.a) * ux + Math.sin(def.a) * uy) < 0.57) return false;
    if ((att.vx - def.vx) * -ux + (att.vy - def.vy) * -uy < 45) return false;
    if (Math.cos(att.a) * -ux + Math.sin(att.a) * -uy < 0.5) return false;
    this.kill(att, def, 'rear');
    return true;
  }
  clash(a, b) {
    const dx = b.x - a.x, dy = b.y - a.y, d = Math.hypot(dx, dy) || 1;
    a.x = clamp(a.x - (dx / d) * 16, a.r, WORLD - a.r); a.y = clamp(a.y - (dy / d) * 16, a.r, WORLD - a.r);
    b.x = clamp(b.x + (dx / d) * 16, b.r, WORLD - b.r); b.y = clamp(b.y + (dy / d) * 16, b.r, WORLD - b.r);
    a.sp *= 0.3; b.sp *= 0.3;
    this.pushFx((a.x + b.x) / 2, (a.y + b.y) / 2, -1);
  }
  pushFx(x, y, t) { if (this.fx.length < 64) this.fx.push([Math.round(x), Math.round(y), t]); }
  kill(v, k, why) { // v is eliminated by k
    if (!v.alive) return;
    v.alive = false; k.k++;
    this.pushFx(v.x, v.y, v.type);
    const amt = v.g * rnd(0.7, 0.8) + 4, n = clamp(Math.floor(amt / 1.5), 6, 30);
    for (let i = 0; i < n; i++) {
      const a = rnd(0, TAU), d = Math.sqrt(Math.random()) * (v.r * 1.3 + 8), s = rnd(25, 70);
      this.addFood(clamp(v.x + Math.cos(a) * d, 12, WORLD - 12), clamp(v.y + Math.sin(a) * d, 12, WORLD - 12), amt / n, 10 + v.type, Math.cos(a) * s, Math.sin(a) * s);
    }
    if (v.human && v.client) {
      const c = v.client; c.ent = null; v.client = null;
      c.socket.emit('dead', { t: 'dead', g: Math.round(v.g * 100) / 100, k: v.k, f: v.food, why, by: k.name });
    } else if (!v.human) this.respawns.push(2.5);
    if (k.human && k.client) k.client.socket.emit('kill', { t: 'kill', n: v.name });
  }

  /* ---- bots (same behaviour as the offline client) ---- */
  think(b) {
    let best = null, bd = 1e9, tx, ty, m = false;
    for (const o of this.ents.values()) if (o !== b && o.alive) { const d = Math.hypot(o.x - b.x, o.y - b.y); if (d < bd) { bd = d; best = o; } }
    if (best && bd < 380) {
      const dd = bd || 1, ux = (b.x - best.x) / dd, uy = (b.y - best.y) / dd, back = -(Math.cos(best.a) * ux + Math.sin(best.a) * uy);
      if (back > 0.35 && bd < best.r * 4 + b.r * 3) { b.ta = Math.atan2(uy, ux) + b.sd * 1.3; return; }
      if (b.ag) { const w = best.r * FRUIT_D[best.type]; tx = best.x + Math.cos(best.a) * w + best.vx * 0.25; ty = best.y + Math.sin(best.a) * w + best.vy * 0.25; m = true; }
    }
    if (!m) {
      let fd = 1e12;
      for (const f of this.foods) { const d = (f.x - b.x) ** 2 + (f.y - b.y) ** 2; if (d < fd) { fd = d; tx = f.x; ty = f.y; } }
      if (tx === undefined) { tx = WORLD / 2; ty = WORLD / 2; }
    }
    if (b.x < 160 || b.x > WORLD - 160 || b.y < 160 || b.y > WORLD - 160) { tx = WORLD / 2; ty = WORLD / 2; }
    b.ta = Math.atan2(ty - b.y, tx - b.x) + rnd(-0.12, 0.12);
    if (m && bd < 260 && Math.random() < 0.3) b.bb = true;
  }
  manageBots(dt) {
    const want = Math.max(0, CFG.TARGET_POPULATION - this.nh);
    for (let i = this.respawns.length - 1; i >= 0; i--) {
      this.respawns[i] -= dt;
      if (this.respawns[i] <= 0) { this.respawns.splice(i, 1); if (this.nb < want) this.addBot(); }
    }
    while (this.nb + this.respawns.length < want) this.addBot();
    if (this.nb > want && this.tick % CFG.TICK_RATE === 0) { // humans joined: retire the weakest bot
      let weakest = null;
      for (const e of this.ents.values()) if (!e.human && (!weakest || e.g < weakest.g)) weakest = e;
      if (weakest) this.removeEnt(weakest);
    }
  }

  /* ---- simulation step ---- */
  step(dt) {
    this.tick++;
    const now = Date.now(), list = Array.from(this.ents.values());
    for (const e of list) {
      if (e.human) {
        if (CFG.AFK_KICK_MS && e.client && now - e.lastInput > CFG.AFK_KICK_MS) { this.kick(e, 'afk'); continue; }
      } else { e.th -= dt; if (e.th <= 0) { e.th = rnd(0.15, 0.35); this.think(e); } }
      const d = ((e.ta - e.a + Math.PI * 3) % TAU) - Math.PI, tr = Math.max(1.6, 5.2 - e.r / 40) * dt;
      e.a = normAngle(e.a + clamp(d, -tr, tr));
      const base = Math.max(95, 215 - (e.r - 22) * 0.9), tg = e.bt > 0 ? base * 1.9 : base;
      e.sp += (tg - e.sp) * Math.min(1, dt * 5);
      e.vx = Math.cos(e.a) * e.sp; e.vy = Math.sin(e.a) * e.sp;
      e.x = clamp(e.x + e.vx * dt, e.r, WORLD - e.r); e.y = clamp(e.y + e.vy * dt, e.r, WORLD - e.r);
      e.bt -= dt; e.bc -= dt; e.mg -= dt;
      if (e.bb) { this.boost(e); e.bb = false; }
      this.collect(e, dt);
      e.r = rad(e.g);
    }
    const live = list.filter((e) => e.alive);
    for (let i = 0; i < live.length; i++) {
      for (let j = i + 1; j < live.length; j++) {
        const a = live[i], b = live[j];
        if (!a.alive || !b.alive) continue;
        const dx = b.x - a.x, dy = b.y - a.y, d = Math.hypot(dx, dy) || 1, R = a.r + b.r;
        if (d > R * 1.45) continue;
        const h1 = this.hit(a, b), h2 = this.hit(b, a);
        if (h1 || h2) {
          if (h1 && h2 && Math.abs(h1 - h2) < 0.2 * Math.max(h1, h2)) this.clash(a, b);
          else if (h1 > h2) this.kill(b, a, 'front');
          else this.kill(a, b, 'front');
          continue;
        }
        if (d < R) { // body bump: harmless, just separate (unless it is a rear hit)
          if (this.rear(a, b, d) || this.rear(b, a, d)) continue;
          const o = (R - d) / 2;
          a.x -= (dx / d) * o; a.y -= (dy / d) * o; b.x += (dx / d) * o; b.y += (dy / d) * o;
        }
      }
    }
    for (const e of list) if (!e.alive) this.removeEnt(e);
    for (const f of this.foods) if (f.vx || f.vy) {
      f.x = clamp(f.x + f.vx * dt, 10, WORLD - 10); f.y = clamp(f.y + f.vy * dt, 10, WORLD - 10);
      const k = Math.pow(0.02, dt); f.vx *= k; f.vy *= k;
      if (Math.abs(f.vx) + Math.abs(f.vy) < 4) f.vx = f.vy = 0;
    }
    for (let n = dt * 60; n > 0; n--) if (this.foods.length < CFG.FOOD_TARGET && Math.random() < Math.min(1, n)) this.addPellet();
    if (Math.random() < dt * 0.5 && this.nbig < CFG.BIG_FOOD_MAX) this.addBig();
    this.manageBots(dt);
  }

  /* ---- networking ---- */
  broadcastState() {
    const p = [];
    for (const e of this.ents.values()) if (e.alive) p.push([e.id, Math.round(e.x), Math.round(e.y), Math.round(e.a * 100) / 100, Math.round(e.r * 10) / 10, e.type, Math.round(e.g * 10), e.bt > 0 ? 1 : 0, e.name, e.mg > 0 ? Math.round(e.mg * 10) / 10 : 0]);
    const ev = this.fx; this.fx = [];
    for (const e of this.ents.values()) {
      if (!e.human || !e.client) continue;
      const f = [], vk = Math.max(1, Math.pow(e.r / 60, 0.42)), vx = CFG.FOOD_VIEW_X * vk, vy = CFG.FOOD_VIEW_Y * vk; // bigger fruit = more zoomed out
      for (const fd of this.foods) if (Math.abs(fd.x - e.x) <= vx && Math.abs(fd.y - e.y) <= vy) f.push([fd.id, Math.round(fd.x), Math.round(fd.y), fd.r, fd.c, fd.k]);
      e.client.socket.volatile.emit('state', { t: 's', p, f, e: ev });
    }
  }
  broadcast(event, payload) {
    for (const e of this.ents.values()) if (e.human && e.client) e.client.socket.emit(event, payload);
  }
  destroy(code) {
    for (const e of Array.from(this.ents.values())) if (e.human) this.kick(e, code);
    rooms.delete(this.id);
  }
}

/* ------------------------------------------------------------ room selection */
function pickRoom(code, prefer) {
  const cap = CFG.MAX_PLAYERS_PER_ROOM;
  if (code) { // private room (created on demand)
    const id = `p-${code}`;
    let r = rooms.get(id);
    if (!r) {
      if (rooms.size >= CFG.MAX_ROOMS) return { err: 'server_full' };
      r = new Room(id, true); rooms.set(id, r);
    }
    return r.nh >= cap ? { err: 'room_full' } : { room: r };
  }
  if (prefer && !prefer.priv && rooms.get(prefer.id) === prefer && prefer.nh < cap) return { room: prefer };
  let best = null;
  for (const r of rooms.values()) if (!r.priv && r.nh < cap && (!best || r.nh > best.nh)) best = r; // fill busiest first
  if (best) return { room: best };
  if (rooms.size >= CFG.MAX_ROOMS) return { err: 'server_full' };
  const r = new Room(`arena-${++arenaSeq}`, false); rooms.set(r.id, r);
  return { room: r };
}

/* ----------------------------------------------------------- socket handlers */
const ERR = {
  invalid_payload: 'Payload must be an object.', invalid_name: 'name must be a string.', invalid_fruit: `fruit must be an integer 0-${FRUIT_COUNT - 1}.`,
  invalid_room: 'room must match [A-Za-z0-9_-]{1,16}.', invalid_token: 'token must be a 48-character hex string.', rate_limited: 'Too many requests.',
  already_playing: 'You are already in a match.', session_in_use: 'That session is connected elsewhere.', room_full: 'That room is full.',
  server_full: 'All arenas are full. Try again shortly.', too_many_connections: 'Too many connections from your network.', shutting_down: 'Server is restarting.',
};

function sendInit(c, e, resumed) {
  const init = {
    t: 'init', id: e.id, x: Math.round(e.x), y: Math.round(e.y), a: Math.round(e.a * 100) / 100, type: e.type, name: e.name,
    r: Math.round(e.r * 10) / 10, g: Math.round(e.g * 100) / 100, room: e.room.id, token: e.token, resumed,
    world: WORLD, maxPlayers: CFG.MAX_PLAYERS_PER_ROOM, players: e.room.nh, tickRate: CFG.TICK_RATE, snapshotRate: CFG.SNAPSHOT_RATE,
  };
  c.socket.emit('init', init);
  return init;
}

function onJoin(c, p, ack) {
  const fail = (code) => { const o = { ok: false, code, message: ERR[code] }; c.socket.emit('join_error', o); reply(ack, o); };
  if (shuttingDown) return fail('shutting_down');
  if (p === undefined || p === null) p = {};
  if (!isObj(p)) return fail('invalid_payload');
  if (c.ent && c.ent.alive) return fail('already_playing');

  let token = null;
  if (p.token !== undefined) { if (typeof p.token !== 'string' || !/^[a-f0-9]{48}$/.test(p.token)) return fail('invalid_token'); token = p.token; }
  let fruit = 0;
  if (p.fruit !== undefined) { if (!Number.isInteger(p.fruit) || p.fruit < 0 || p.fruit >= FRUIT_COUNT) return fail('invalid_fruit'); fruit = p.fruit; }
  if (p.name !== undefined && typeof p.name !== 'string') return fail('invalid_name');
  const name = cleanName(p.name) || `Player${100 + ((Math.random() * 900) | 0)}`;
  let code = null;
  if (p.room !== undefined && p.room !== null && p.room !== '') { if (typeof p.room !== 'string' || !/^[A-Za-z0-9_-]{1,16}$/.test(p.room)) return fail('invalid_room'); code = p.room.toLowerCase(); }

  const old = token && sessions.get(token); // reconnect: re-attach to the still-alive entity
  if (old && old.alive) {
    if (old.client && old.client.socket.connected) return fail('session_in_use');
    if (old.dcTimer) { clearTimeout(old.dcTimer); old.dcTimer = null; }
    old.client = c; old.lastInput = Date.now(); c.ent = old; c.room = old.room;
    const init = sendInit(c, old, true);
    return reply(ack, { ok: true, ...init });
  }

  const sel = pickRoom(code, c.room);
  if (sel.err) return fail(sel.err);
  const e = sel.room.addHuman(c, name, fruit);
  c.ent = e; c.room = sel.room;
  log('info', 'join', { room: sel.room.id, players: sel.room.nh, id: e.id });
  reply(ack, { ok: true, ...sendInit(c, e, false) });
}

function onDisconnect(c, reason) {
  clients.delete(c.socket.id);
  const n = (ipCount.get(c.ip) || 1) - 1;
  if (n <= 0) ipCount.delete(c.ip); else ipCount.set(c.ip, n);
  const e = c.ent; c.ent = null;
  if (!e || !e.alive) return;
  e.client = null;
  const deliberate = reason === 'client namespace disconnect' || reason === 'server namespace disconnect';
  if (deliberate || CFG.RECONNECT_GRACE_MS <= 0) return e.room.removeEnt(e);
  e.dcTimer = setTimeout(() => { e.dcTimer = null; if (!e.client) e.room.removeEnt(e); }, CFG.RECONNECT_GRACE_MS); // keeps heading straight, stays vulnerable
}

function clientIp(socket) {
  const xff = socket.handshake.headers['x-forwarded-for']; // soft limit only: this header can be spoofed
  if (typeof xff === 'string' && xff) return xff.split(',')[0].trim().slice(0, 64);
  return socket.handshake.address || 'unknown';
}

/* --------------------------------------------------------------- http + io */
const origin = (req) => req.headers.origin;
function sendJson(req, res, code, obj) {
  const body = JSON.stringify(obj), o = origin(req);
  const h = { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Length': Buffer.byteLength(body) };
  if (o && originAllowed(o)) { h['Access-Control-Allow-Origin'] = o; h.Vary = 'Origin'; } // lets the game fetch /health to wake the instance
  res.writeHead(code, h);
  res.end(req.method === 'HEAD' ? undefined : body);
}
function stats() {
  let players = 0, bots = 0;
  for (const r of rooms.values()) { players += r.nh; bots += r.nb; }
  return { status: 'ok', version: pkg.version, uptimeSec: Math.round(process.uptime()), rooms: rooms.size, players, bots, maxPlayersPerRoom: CFG.MAX_PLAYERS_PER_ROOM, maxRooms: CFG.MAX_ROOMS, tickMsAvg: Math.round(tickMs * 100) / 100, tickBudgetMs: Math.round(1000 / CFG.TICK_RATE), rssMB: Math.round(process.memoryUsage().rss / 1048576) };
}

const httpServer = http.createServer((req, res) => {
  try {
    const path = (req.url || '/').split('?')[0];
    if (req.method !== 'GET' && req.method !== 'HEAD') return sendJson(req, res, 405, { error: 'method_not_allowed' });
    if (path === '/health' || path === '/healthz') return sendJson(req, res, 200, stats());
    if (path === '/') return sendJson(req, res, 200, { name: 'fruit-io-server', status: 'ok', socketPath: '/socket.io' });
    sendJson(req, res, 404, { error: 'not_found' });
  } catch (err) {
    log('error', 'http handler failed', { err: String(err && err.stack || err) });
    if (!res.headersSent) { res.writeHead(500); res.end(); }
  }
});
httpServer.keepAliveTimeout = 65000; // longer than typical proxy idle timeouts, avoids sporadic 502s
httpServer.headersTimeout = 66000;

const io = new Server(httpServer, {
  path: '/socket.io', serveClient: false, maxHttpBufferSize: 4096, connectTimeout: 10000, pingInterval: 10000, pingTimeout: 10000,
  transports: ['websocket', 'polling'],
  cors: { origin: (o, cb) => cb(null, originAllowed(o)), methods: ['GET', 'POST'] },
  // CORS only protects browsers reading responses; this also blocks the WebSocket upgrade from other sites.
  allowRequest: (req, cb) => { const ok = originAllowed(req.headers.origin); if (!ok) logRejected(req.headers.origin); cb(null, ok); },
});
let engineErrors = 0;
io.engine.on('connection_error', (err) => { if (engineErrors++ < 50) log('warn', 'engine connection_error', { code: err.code, message: err.message }); });

io.on('connection', (socket) => {
  const ip = clientIp(socket);
  if ((ipCount.get(ip) || 0) >= CFG.MAX_CONN_PER_IP) {
    socket.emit('join_error', { ok: false, code: 'too_many_connections', message: ERR.too_many_connections });
    return socket.disconnect(true);
  }
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);
  const c = { socket, ip, ent: null, room: null, strikes: 0, b: { join: new Bucket(0.5, 3), input: new Bucket(40, 40), boost: new Bucket(6, 6), emoji: new Bucket(3, 3), leave: new Bucket(1, 3) } };
  clients.set(socket.id, c);

  const on = (event, bucket, fn) => socket.on(event, (...args) => {
    try {
      const ack = typeof args[args.length - 1] === 'function' ? args.pop() : undefined;
      if (!c.b[bucket].take()) {
        if (++c.strikes > 300) socket.disconnect(true);
        if (event === 'join') { const o = { ok: false, code: 'rate_limited', message: ERR.rate_limited }; socket.emit('join_error', o); reply(ack, o); }
        return;
      }
      fn(args[0], ack);
    } catch (err) { log('error', `handler ${event} failed`, { err: String(err && err.stack || err) }); }
  });

  on('join', 'join', (p, ack) => onJoin(c, p, ack));
  on('input', 'input', (p) => {
    const e = c.ent;
    if (!e || !e.alive || !isObj(p) || typeof p.a !== 'number' || !Number.isFinite(p.a) || Math.abs(p.a) > 1000) return;
    e.ta = normAngle(p.a); e.lastInput = Date.now();
  });
  on('boost', 'boost', () => { const e = c.ent; if (e && e.alive) { e.room.boost(e); e.lastInput = Date.now(); } });
  on('emoji', 'emoji', (p) => {
    const e = c.ent, now = Date.now();
    if (!e || !e.alive || !isObj(p) || !Number.isInteger(p.i) || p.i < 0 || p.i > 9 || now - e.lastEmoji < 1000) return;
    e.lastEmoji = now; e.room.broadcast('emoji', { t: 'em', id: e.id, i: p.i });
  });
  on('leave', 'leave', () => { const e = c.ent; c.ent = null; if (e && e.alive) { e.client = null; e.room.removeEnt(e); } });
  socket.on('disconnect', (reason) => { try { onDisconnect(c, reason); } catch (err) { log('error', 'disconnect failed', { err: String(err && err.stack || err) }); } });
  socket.on('error', (err) => log('warn', 'socket error', { err: String(err) }));
});

/* --------------------------------------------------------------- main loop */
let lastTick = Date.now(), loopTimer = null, strikeTimer = null;
function loop() {
  const t0 = process.hrtime.bigint(), now = Date.now(), dt = Math.min(0.05, (now - lastTick) / 1000);
  lastTick = now;
  for (const room of Array.from(rooms.values())) {
    try {
      if (room.nh > 0) { room.step(dt); if (room.tick % SNAP_EVERY === 0) room.broadcastState(); }
      else if (now - room.idleSince > CFG.ROOM_IDLE_MS) { rooms.delete(room.id); log('info', 'room closed (idle)', { room: room.id }); }
    } catch (err) {
      log('error', 'room crashed', { room: room.id, err: String(err && err.stack || err) });
      room.destroy('room_error');
    }
  }
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  tickMs = tickMs * 0.95 + ms * 0.05;
}

function start() {
  httpServer.on('error', (err) => { log('error', 'http server error', { err: String(err) }); process.exit(1); });
  httpServer.listen(CFG.PORT, CFG.HOST, () => log('info', 'listening', { host: CFG.HOST, port: CFG.PORT, tickRate: CFG.TICK_RATE, snapshotRate: CFG.SNAPSHOT_RATE, maxRooms: CFG.MAX_ROOMS, maxPlayersPerRoom: CFG.MAX_PLAYERS_PER_ROOM }));
  loopTimer = setInterval(loop, 1000 / CFG.TICK_RATE);
  strikeTimer = setInterval(() => { for (const c of clients.values()) c.strikes = 0; }, 10000);
}
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log('info', 'shutting down', { signal });
  clearInterval(loopTimer); clearInterval(strikeTimer);
  io.emit('server_notice', { code: 'restarting', message: ERR.shutting_down });
  io.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 8000).unref();
}

if (require.main === module) {
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('unhandledRejection', (err) => log('error', 'unhandledRejection', { err: String(err && err.stack || err) }));
  process.on('uncaughtException', (err) => { log('error', 'uncaughtException', { err: String(err && err.stack || err) }); process.exit(1); });
  start();
}

module.exports = { CFG, Room, rooms, sessions, originAllowed, cleanName, stats, onJoin, onDisconnect, pickRoom, io, httpServer };
