import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
const STORAGE_PATH = path.join(__dirname, 'data', 'server_storage.json');
const CONFIG_PATH = path.join(__dirname, 'config.yaml');
const START_TIME = Date.now();

function readConfigDefaults() {
  let adminUser = 'admin';
  let adminPass = 'admin123';
  let jwtSecret = 'iptv-p2p-super-secret-jwt-key-2025';
  try {
    const raw = fs.readFileSync(CONFIG_PATH, 'utf8');
    const userMatch = raw.match(/^\s*admin_user:\s*"?([^"\r\n#]+)"?/m);
    const passMatch = raw.match(/^\s*admin_pass:\s*"?([^"\r\n#]+)"?/m);
    const secretMatch = raw.match(/^\s*jwt_secret:\s*"?([^"\r\n#]+)"?/m);
    if (userMatch) adminUser = userMatch[1].trim();
    if (passMatch) adminPass = passMatch[1].trim();
    if (secretMatch) jwtSecret = secretMatch[1].trim();
  } catch {
    // Use defaults if config.yaml is absent
  }
  return { adminUser, adminPass, jwtSecret };
}

function readTailscalePreviewStatus() {
  const out = {
    enabled: false,
    connected: false,
    backend_state: 'GoServerRequired',
    auth_mode: 'none',
    auth_configured: false,
    hostname: 'iptv-p2p-node',
    mesh_role: 'hybrid',
    mesh_port: 8448,
    discovery_tag: 'tag:iptv-mesh',
    prefer_tailscale_ice: true,
    bytes_pulled: 0,
    bytes_served: 0,
    peers: []
  };
  try {
    const raw = fs.readFileSync(CONFIG_PATH, 'utf8');
    const idx = raw.search(/^\s*tailscale:\s*$/m);
    if (idx >= 0) {
      const sec = raw.slice(idx);
      const nextTop = sec.slice(sec.indexOf('\n') + 1).search(/^[a-zA-Z0-9_]+:/m);
      const block = nextTop >= 0 ? sec.slice(0, sec.indexOf('\n') + 1 + nextTop) : sec;
      const en = block.match(/^\s*enabled:\s*(true|false)/m);
      const hn = block.match(/^\s*hostname:\s*"?([^"\r\n#]+)"?/m);
      const role = block.match(/^\s*mesh_role:\s*"?([^"\r\n#]+)"?/m);
      const port = block.match(/^\s*mesh_port:\s*(\d+)/m);
      const tag = block.match(/^\s*discovery_tag:\s*"?([^"\r\n#]+)"?/m);
      const pref = block.match(/^\s*prefer_tailscale_ice:\s*(true|false)/m);
      const ak = block.match(/^\s*auth_key:\s*"?([^"\r\n#]*)"?/m);
      const cs = block.match(/^\s*oauth_client_secret:\s*"?([^"\r\n#]*)"?/m);
      if (en) out.enabled = en[1] === 'true';
      if (hn && hn[1].trim()) out.hostname = hn[1].trim();
      if (role && role[1].trim()) out.mesh_role = role[1].trim();
      if (port) out.mesh_port = Number(port[1]) || 8448;
      if (tag && tag[1].trim()) out.discovery_tag = tag[1].trim();
      if (pref) out.prefer_tailscale_ice = pref[1] === 'true';
      const hasAuthKey = Boolean((ak && ak[1].trim()) || (process.env.TS_AUTHKEY && process.env.TS_AUTHKEY.trim()));
      const hasOAuth = Boolean((cs && cs[1].trim()) || (process.env.TS_CLIENT_SECRET && process.env.TS_CLIENT_SECRET.trim()));
      out.auth_configured = hasAuthKey || hasOAuth;
      out.auth_mode = hasOAuth ? 'oauth_client_secret' : (hasAuthKey ? 'static_auth_key' : (out.enabled ? 'persisted_state' : 'none'));
    }
  } catch {
    // Use defaults
  }
  return out;
}

function isTailscaleAddress(rawAddr) {
  const s = String(rawAddr || '').replace(/^::ffff:/i, '').trim();
  return /^100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\./.test(s) || /^fd7a:115c:a1e0:/i.test(s);
}

function readStorage() {
  try {
    const raw = fs.readFileSync(STORAGE_PATH, 'utf8');
    return JSON.parse(raw);
  } catch {
    return {
      streams: {},
      epg_sources: {},
      epg_mappings: {},
      m3u_sources: [],
      deleted_channel_ids: {},
      users: {},
      billing: {
        usdt_address: '',
        usdt_network: 'TRC20',
        monthly_price_usdt: 10,
        quarterly_price_usdt: 25,
        yearly_price_usdt: 80,
        auto_verify_enabled: true
      },
      payment_orders: {}
    };
  }
}

function writeStorage(data) {
  try {
    fs.mkdirSync(path.dirname(STORAGE_PATH), { recursive: true });
    fs.writeFileSync(STORAGE_PATH, JSON.stringify(data, null, 2));
  } catch {}
}

function hashPasswordScrypt(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derived = crypto.scryptSync(password, salt, 64).toString('hex');
  return `scrypt:${salt}:${derived}`;
}

function verifyPasswordScrypt(stored, password) {
  if (!stored || !password || !stored.startsWith('scrypt:')) return false;
  const parts = stored.split(':');
  if (parts.length !== 3) return false;
  const derived = crypto.scryptSync(password, parts[1], 64);
  const expected = Buffer.from(parts[2], 'hex');
  return derived.length === expected.length && crypto.timingSafeEqual(derived, expected);
}

function parseCookies(req) {
  const out = {};
  const hdr = req.headers.cookie || '';
  for (const part of hdr.split(';')) {
    const idx = part.indexOf('=');
    if (idx > 0) {
      out[part.slice(0, idx).trim()] = decodeURIComponent(part.slice(idx + 1).trim());
    }
  }
  return out;
}

function setSessionCookie(res, req, token, maxAgeSec) {
  const isHttps = req.socket?.encrypted || req.headers['x-forwarded-proto'] === 'https';
  const attrs = isHttps
    ? `Path=/; HttpOnly; Max-Age=${maxAgeSec}; SameSite=None; Secure; Partitioned`
    : `Path=/; HttpOnly; Max-Age=${maxAgeSec}; SameSite=Lax`;
  res.setHeader('Set-Cookie', `iptv_session=${encodeURIComponent(token)}; ${attrs}`);
}

function extractUserToken(req, urlObj) {
  const authHeader = req.headers.authorization || '';
  if (authHeader.startsWith('Bearer ')) return authHeader.slice(7).trim();
  const cookies = parseCookies(req);
  if (cookies.iptv_session) return cookies.iptv_session;
  return urlObj.searchParams.get('token') || '';
}

function checkSameOriginOrHeader(req) {
  if (String(req.headers['x-requested-with'] || '').toLowerCase() === 'xmlhttprequest') return true;
  if (String(req.headers.authorization || '').startsWith('Bearer ')) return true;
  const origin = String(req.headers.origin || '').trim();
  if (origin) {
    const host = req.headers['x-forwarded-host'] || req.headers.host || '';
    if (host && origin.endsWith('://' + host)) return true;
  }
  return false;
}

const previewViewers = new Map(); // channelId -> Map(viewerKey -> lastSeenMs)

function recordPreviewViewer(chId, vKey) {
  if (!chId || !vKey) return;
  if (!previewViewers.has(chId)) previewViewers.set(chId, new Map());
  previewViewers.get(chId).set(vKey, Date.now());
}

function removePreviewViewer(chId, vKey) {
  const m = previewViewers.get(chId);
  if (m) {
    m.delete(vKey);
    if (m.size === 0) previewViewers.delete(chId);
  }
}

function getPreviewViewerCounts() {
  const cutoff = Date.now() - 25000;
  const out = {};
  for (const [chId, m] of previewViewers.entries()) {
    for (const [vk, ts] of m.entries()) {
      if (ts < cutoff) m.delete(vk);
    }
    if (m.size === 0) previewViewers.delete(chId);
    else out[chId] = m.size;
  }
  return out;
}

function signToken(payload, secret) {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
  return `${header}.${body}.${sig}`;
}

function verifyToken(token, secret) {
  if (!token) return null;
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  const [header, body, sig] = parts;
  const expected = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
  const sigBuf = Buffer.from(sig);
  const expBuf = Buffer.from(expected);
  if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) return null;
  try {
    const claims = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
    if (claims.exp && claims.exp * 1000 < Date.now()) return null;
    return claims;
  } catch {
    return null;
  }
}

function sendJSON(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(data));
}

function serveFile(res, filePath, contentType, extraHeaders) {
  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not Found');
      return;
    }
    res.writeHead(200, Object.assign({ 'Content-Type': contentType }, extraHeaders || {}));
    res.end(content);
  });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > 8 * 1024 * 1024) {
        reject(new Error('Body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const urlObj = new URL(req.url || '/', `http://localhost:${PORT}`);
  const pathname = urlObj.pathname;
  const { adminUser, adminPass, jwtSecret } = readConfigDefaults();

  if (pathname === '/' || pathname === '/index.html') {
    return serveFile(res, path.join(__dirname, 'web', 'public', 'index.html'), 'text/html; charset=utf-8');
  }
  if (pathname === '/player.html') {
    return serveFile(res, path.join(__dirname, 'web', 'public', 'player.html'), 'text/html; charset=utf-8');
  }
  if (pathname === '/multiview.html') {
    return serveFile(res, path.join(__dirname, 'web', 'public', 'multiview.html'), 'text/html; charset=utf-8');
  }
  if (pathname === '/p2p-engine.js') {
    return serveFile(res, path.join(__dirname, 'web', 'public', 'p2p-engine.js'), 'application/javascript; charset=utf-8');
  }
  if (pathname === '/admin') {
    res.writeHead(302, { Location: '/admin/' });
    return res.end();
  }
  if (pathname === '/admin/' || pathname === '/admin/index.html') {
    return serveFile(res, path.join(__dirname, 'web', 'admin', 'index.html'), 'text/html; charset=utf-8');
  }
  if (pathname === '/pc' || pathname === '/tv') {
    const qs = urlObj.search || '';
    res.writeHead(302, { Location: pathname + '/' + qs });
    return res.end();
  }
  if (pathname.startsWith('/pc/') || pathname.startsWith('/tv/')) {
    const isPc = pathname.startsWith('/pc/');
    const baseDir = path.resolve(__dirname, isPc ? 'pc-client' : 'tv-client');
    const rel = pathname.slice(4) || 'index.html';
    const targetPath = path.resolve(baseDir, rel);
    if (!targetPath.startsWith(baseDir + path.sep) && targetPath !== baseDir) {
      res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
      return res.end('Forbidden');
    }
    const ext = path.extname(targetPath).toLowerCase();
    const mimeMap = {
      '.html': 'text/html; charset=utf-8',
      '.js': 'application/javascript; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.webmanifest': 'application/manifest+json; charset=utf-8',
      '.json': 'application/json; charset=utf-8',
      '.svg': 'image/svg+xml',
      '.png': 'image/png'
    };
    return serveFile(res, targetPath, mimeMap[ext] || 'application/octet-stream', {
      'Cache-Control': 'no-store, no-cache, must-revalidate',
      'Pragma': 'no-cache'
    });
  }

  if (pathname === '/api/channels') {
    const storage = readStorage();
    const viewers = getPreviewViewerCounts();
    const list = Object.values(storage.streams || {}).map((s) => ({
      id: s.id,
      name: s.name,
      tier: s.tier || 'free',
      logo_url: s.logo_url || '',
      epg_id: s.epg_id || '',
      is_acestream: typeof s.source_url === 'string' && s.source_url.toLowerCase().startsWith('acestream://'),
      viewer_count: viewers[s.id] || 0
    }));
    return sendJSON(res, 200, list);
  }

  if (pathname === '/api/channels/viewers') {
    return sendJSON(res, 200, getPreviewViewerCounts());
  }

  if (pathname === '/api/channels/heartbeat' && req.method === 'POST') {
    try {
      const body = JSON.parse(await readBody(req));
      const chId = String(body.channel_id || '').trim();
      const vKey = String(body.viewer_id || req.socket.remoteAddress || 'anon').trim();
      if (body.action === 'leave') removePreviewViewer(chId, vKey);
      else recordPreviewViewer(chId, vKey);
      return sendJSON(res, 200, { viewer_count: getPreviewViewerCounts()[chId] || 0 });
    } catch {
      return sendJSON(res, 400, { error: 'Invalid payload' });
    }
  }

  if (pathname === '/api/epg/json') {
    return sendJSON(res, 200, {});
  }

  if (pathname === '/api/billing/plans') {
    const st = readStorage();
    const b = st.billing || { usdt_address: '', usdt_network: 'TRC20', monthly_price_usdt: 10, quarterly_price_usdt: 25, yearly_price_usdt: 80, auto_verify_enabled: true };
    const configured = Boolean(b.usdt_address && b.usdt_address.trim()) && (b.monthly_price_usdt > 0 || b.quarterly_price_usdt > 0 || b.yearly_price_usdt > 0);
    return sendJSON(res, 200, {
      configured,
      usdt_network: b.usdt_network || 'TRC20',
      monthly_price_usdt: b.monthly_price_usdt ?? 10,
      quarterly_price_usdt: b.quarterly_price_usdt ?? 25,
      yearly_price_usdt: b.yearly_price_usdt ?? 80,
      auto_verify_enabled: b.auto_verify_enabled !== false
    });
  }

  if (pathname === '/api/auth/register' && req.method === 'POST') {
    try {
      const body = JSON.parse(await readBody(req));
      const username = String(body.username || '').trim();
      const password = String(body.password || '');
      const plan = String(body.plan || 'free').toLowerCase();
      if (!/^[a-zA-Z0-9_.-]{3,32}$/.test(username) || username.toLowerCase() === 'admin') {
        return sendJSON(res, 400, { error: 'Kullanıcı adı 3-32 karakter olmalıdır' });
      }
      if (password.length < 6) {
        return sendJSON(res, 400, { error: 'Şifre en az 6 karakter olmalıdır' });
      }
      const st = readStorage();
      st.users = st.users || {};
      if (Object.keys(st.users).some((k) => k.toLowerCase() === username.toLowerCase())) {
        return sendJSON(res, 409, { error: 'Bu kullanıcı adı zaten kayıtlı' });
      }
      if (plan === 'free') {
        const token = signToken({ user_id: username, tier: 'free', sub: username, exp: Math.floor(Date.now() / 1000) + 30 * 86400 }, jwtSecret);
        st.users[username] = {
          user_id: username,
          tier: 'free',
          token,
          password_hash: hashPasswordScrypt(password),
          favorites: [],
          created_at: new Date().toISOString()
        };
        writeStorage(st);
        setSessionCookie(res, req, token, 30 * 86400);
        return sendJSON(res, 200, { status: 'active', token, user: { user_id: username, tier: 'free', favorites: [] } });
      }
      const b = st.billing || {};
      if (!b.usdt_address) return sendJSON(res, 503, { error: 'Admin henüz USDT cüzdan adresi tanımlamamış' });
      const priceMap = { monthly: [b.monthly_price_usdt, 30], quarterly: [b.quarterly_price_usdt, 90], yearly: [b.yearly_price_usdt, 365] };
      const picked = priceMap[plan];
      if (!picked || !(picked[0] > 0)) return sendJSON(res, 400, { error: 'Geçersiz paket' });
      const orderId = 'ord_' + crypto.randomBytes(6).toString('hex');
      const suffix = (crypto.randomInt(1, 999) / 10000).toFixed(4).slice(1);
      const exactAmount = (Math.floor(picked[0]) + Number(suffix)).toFixed(4);
      const order = {
        order_id: orderId,
        username,
        password_hash: hashPasswordScrypt(password),
        plan_id: plan,
        duration_days: picked[1],
        base_amount_usdt: picked[0],
        exact_amount_usdt: exactAmount,
        usdt_address: b.usdt_address,
        usdt_network: b.usdt_network || 'TRC20',
        status: 'pending',
        created_at_unix: Math.floor(Date.now() / 1000),
        expires_at_unix: Math.floor(Date.now() / 1000) + 2700
      };
      st.payment_orders = st.payment_orders || {};
      st.payment_orders[orderId] = order;
      writeStorage(st);
      const { password_hash: _ignored, ...publicOrder } = order;
      return sendJSON(res, 200, { status: 'payment_required', order: publicOrder });
    } catch {
      return sendJSON(res, 400, { error: 'Geçersiz istek' });
    }
  }

  if (pathname === '/api/auth/login' && req.method === 'POST') {
    try {
      const body = JSON.parse(await readBody(req));
      const username = String(body.username || '').trim();
      const password = String(body.password || '');
      const st = readStorage();
      if (username === adminUser && password === adminPass) {
        const token = signToken({ user_id: username, tier: 'admin', sub: username, exp: Math.floor(Date.now() / 1000) + 30 * 86400 }, jwtSecret);
        const admAcc = (st.users || {})[username];
        setSessionCookie(res, req, token, 30 * 86400);
        return sendJSON(res, 200, {
          status: 'success',
          token,
          user: { user_id: username, tier: 'admin', favorites: (admAcc && admAcc.favorites) || [] }
        });
      }
      const key = Object.keys(st.users || {}).find((k) => k.toLowerCase() === username.toLowerCase());
      const acc = key ? st.users[key] : null;
      if (!acc || !verifyPasswordScrypt(acc.password_hash, password)) {
        return sendJSON(res, 401, { error: 'Kullanıcı adı veya şifre hatalı' });
      }
      const token = signToken({ user_id: acc.user_id, tier: acc.tier || 'free', sub: acc.user_id, exp: Math.floor(Date.now() / 1000) + 30 * 86400 }, jwtSecret);
      setSessionCookie(res, req, token, 30 * 86400);
      return sendJSON(res, 200, {
        status: 'success',
        token,
        user: { user_id: acc.user_id, tier: acc.tier || 'free', paid_until: acc.paid_until || '', favorites: acc.favorites || [] }
      });
    } catch {
      return sendJSON(res, 400, { error: 'Geçersiz istek' });
    }
  }

  if (pathname === '/api/auth/logout' && req.method === 'POST') {
    if (!checkSameOriginOrHeader(req)) {
      return sendJSON(res, 403, { error: 'Forbidden' });
    }
    setSessionCookie(res, req, '', 0);
    return sendJSON(res, 200, { status: 'success' });
  }

  if (pathname === '/api/auth/me') {
    const tok = extractUserToken(req, urlObj);
    const claims = verifyToken(tok, jwtSecret);
    if (!claims || claims.tier === 'guest' || !claims.user_id) {
      return sendJSON(res, 200, { authenticated: false, tier: 'guest', favorites: [] });
    }
    const st = readStorage();
    if (claims.tier === 'admin') {
      const admAcc = (st.users || {})[claims.user_id];
      return sendJSON(res, 200, {
        authenticated: true,
        token: tok,
        user: { user_id: claims.user_id, tier: 'admin', favorites: (admAcc && admAcc.favorites) || [] }
      });
    }
    const acc = (st.users || {})[claims.user_id];
    if (!acc) return sendJSON(res, 200, { authenticated: false, tier: 'guest', favorites: [] });
    return sendJSON(res, 200, {
      authenticated: true,
      token: tok,
      user: { user_id: acc.user_id, tier: acc.tier || 'free', paid_until: acc.paid_until || '', favorites: acc.favorites || [] }
    });
  }

  if (pathname === '/api/auth/upgrade' && req.method === 'POST') {
    if (!checkSameOriginOrHeader(req)) {
      return sendJSON(res, 403, { error: 'Forbidden' });
    }
    const tok = extractUserToken(req, urlObj);
    const claims = verifyToken(tok, jwtSecret);
    if (!claims || claims.tier === 'guest' || !claims.user_id) {
      return sendJSON(res, 401, { error: 'Paket yükseltmek için önce giriş yapmalısınız' });
    }
    try {
      const body = JSON.parse(await readBody(req));
      const plan = String(body.plan || 'monthly').toLowerCase();
      const st = readStorage();
      const b = st.billing || {};
      if (!b.usdt_address) return sendJSON(res, 503, { error: 'Admin henüz USDT cüzdan adresi tanımlamamış' });
      const priceMap = { monthly: [b.monthly_price_usdt, 30], quarterly: [b.quarterly_price_usdt, 90], yearly: [b.yearly_price_usdt, 365] };
      const picked = priceMap[plan];
      if (!picked || !(picked[0] > 0)) return sendJSON(res, 400, { error: 'Geçersiz ücretli paket' });
      const orderId = 'ord_' + crypto.randomBytes(6).toString('hex');
      const suffix = (crypto.randomInt(1, 999) / 10000).toFixed(4).slice(1);
      const exactAmount = (Math.floor(picked[0]) + Number(suffix)).toFixed(4);
      const order = {
        order_id: orderId,
        username: claims.user_id,
        plan_id: plan,
        duration_days: picked[1],
        base_amount_usdt: picked[0],
        exact_amount_usdt: exactAmount,
        usdt_address: b.usdt_address,
        usdt_network: b.usdt_network || 'TRC20',
        status: 'pending',
        upgrade_only: true,
        created_at_unix: Math.floor(Date.now() / 1000),
        expires_at_unix: Math.floor(Date.now() / 1000) + 2700
      };
      st.payment_orders = st.payment_orders || {};
      st.payment_orders[orderId] = order;
      writeStorage(st);
      return sendJSON(res, 200, { status: 'payment_required', order });
    } catch {
      return sendJSON(res, 400, { error: 'Geçersiz istek' });
    }
  }

  if (pathname === '/api/auth/payment-status') {
    const orderId = urlObj.searchParams.get('order_id') || '';
    const st = readStorage();
    const ord = (st.payment_orders || {})[orderId];
    if (!ord) return sendJSON(res, 404, { error: 'Sipariş bulunamadı' });
    const { password_hash: _ph, ...publicOrder } = ord;
    return sendJSON(res, 200, { status: ord.status, order: publicOrder });
  }

  if (pathname === '/api/user/favorites/toggle' && req.method === 'POST') {
    if (!checkSameOriginOrHeader(req)) {
      return sendJSON(res, 403, { error: 'Forbidden' });
    }
    const tok = extractUserToken(req, urlObj);
    const claims = verifyToken(tok, jwtSecret);
    if (!claims || claims.tier === 'guest' || !claims.user_id) {
      return sendJSON(res, 401, { error: 'Favoriler için giriş yapmalısınız' });
    }
    try {
      const body = JSON.parse(await readBody(req));
      const chId = String(body.channel_id || '').trim();
      const st = readStorage();
      st.users = st.users || {};
      let acc = st.users[claims.user_id];
      if (!acc) {
        if (claims.tier === 'admin') {
          acc = { user_id: claims.user_id, tier: 'admin', favorites: [] };
        } else {
          return sendJSON(res, 401, { error: 'Kullanıcı bulunamadı' });
        }
      }
      const favs = Array.isArray(acc.favorites) ? [...acc.favorites] : [];
      const idx = favs.indexOf(chId);
      let isFav = false;
      if (idx >= 0) favs.splice(idx, 1);
      else {
        favs.push(chId);
        isFav = true;
      }
      acc.favorites = favs;
      st.users[claims.user_id] = acc;
      writeStorage(st);
      return sendJSON(res, 200, { status: 'success', is_favorite: isFav, favorites: favs });
    } catch {
      return sendJSON(res, 400, { error: 'Geçersiz istek' });
    }
  }

  if (pathname === '/api/admin/login' && req.method === 'POST') {
    try {
      const body = JSON.parse(await readBody(req));
      if (body.username !== adminUser || body.password !== adminPass) {
        return sendJSON(res, 401, { error: 'Geçersiz kullanıcı adı veya şifre' });
      }
      const token = signToken(
        {
          user_id: body.username,
          tier: 'admin',
          exp: Math.floor(Date.now() / 1000) + 24 * 3600
        },
        jwtSecret
      );
      return sendJSON(res, 200, { status: 'success', token, user: body.username });
    } catch {
      return sendJSON(res, 400, { error: 'Invalid JSON payload' });
    }
  }

  if (pathname.startsWith('/api/admin/')) {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : '';
    const claims = verifyToken(token, jwtSecret);
    if (!claims || claims.tier !== 'admin') {
      return sendJSON(res, 401, { error: 'Admin authentication required' });
    }

    const storage = readStorage();
    if (pathname === '/api/admin/metrics') {
      const totalMemMB = Math.round(os.totalmem() / 1024 / 1024);
      const usedMemMB = Math.round((os.totalmem() - os.freemem()) / 1024 / 1024);
      const load = os.loadavg()[0] || 0;
      const cpuPct = Math.min(100, Math.max(0, (load / Math.max(1, os.cpus().length)) * 100));
      return sendJSON(res, 200, {
        cpu_usage_pct: cpuPct,
        ram_used_mb: usedMemMB,
        ram_total_mb: totalMemMB,
        gpu_available: false,
        gpu_encoder_pct: 0,
        gpu_memory_used_mb: 0,
        gpu_temp_c: 0,
        goroutines_count: 0,
        uptime_seconds: Math.floor((Date.now() - START_TIME) / 1000),
        active_streams: 0
      });
    }
    if (pathname === '/api/admin/streams') {
      return sendJSON(res, 200, storage.streams || {});
    }
    if (pathname === '/api/admin/streams/merge' && req.method === 'POST') {
      try {
        const body = JSON.parse(await readBody(req));
        const targetId = String(body.target_id || '').trim();
        const sourceIds = Array.isArray(body.source_ids) ? body.source_ids : [];
        const streams = storage.streams || {};
        const target = streams[targetId];
        if (!target || sourceIds.length === 0) {
          return sendJSON(res, 400, { error: 'Geçersiz birleştirme isteği' });
        }
        if (body.new_name && String(body.new_name).trim()) {
          target.name = String(body.new_name).trim();
        }
        const urlSet = new Set([target.source_url, ...(target.source_urls || [])].filter(Boolean));
        let mergedCount = 0;
        for (const sid of sourceIds) {
          const src = streams[sid];
          if (!src || sid === targetId) continue;
          [src.source_url, ...(src.source_urls || [])].filter(Boolean).forEach((u) => urlSet.add(u));
          if (!target.logo_url && src.logo_url) target.logo_url = src.logo_url;
          if (!target.epg_id && src.epg_id) target.epg_id = src.epg_id;
          delete streams[sid];
          mergedCount++;
        }
        target.source_urls = Array.from(urlSet);
        if (target.source_urls.length > 0) target.source_url = target.source_urls[0];
        writeStorage(storage);
        return sendJSON(res, 200, { status: 'success', merged_count: mergedCount, target_channel: target });
      } catch {
        return sendJSON(res, 400, { error: 'Invalid JSON payload' });
      }
    }
    if (pathname === '/api/admin/streams/dedupe' && req.method === 'POST') {
      const streams = storage.streams || {};
      const urlOwner = new Map();
      let mergedCount = 0;
      for (const id of Object.keys(streams).sort()) {
        const s = streams[id];
        if (!s) continue;
        const urls = [s.source_url, ...(s.source_urls || [])].filter(Boolean);
        const ownerId = urls.map((u) => urlOwner.get(u.trim())).find((o) => o && o !== id);
        if (ownerId && streams[ownerId]) {
          const t = streams[ownerId];
          const set = new Set([t.source_url, ...(t.source_urls || []), ...urls].filter(Boolean));
          t.source_urls = Array.from(set);
          t.source_url = t.source_urls[0] || t.source_url;
          if (!t.epg_id && s.epg_id) t.epg_id = s.epg_id;
          if (!t.logo_url && s.logo_url) t.logo_url = s.logo_url;
          delete streams[id];
          mergedCount++;
        } else {
          urls.forEach((u) => urlOwner.set(u.trim(), id));
        }
      }
      if (mergedCount > 0) writeStorage(storage);
      return sendJSON(res, 200, { status: 'success', merged_count: mergedCount });
    }
    if (pathname === '/api/admin/swarms') {
      return sendJSON(res, 200, getPreviewSwarmStats());
    }
    if (pathname === '/api/admin/m3u/sources') {
      return sendJSON(res, 200, storage.m3u_sources || []);
    }
    if (pathname === '/api/admin/users') {
      const safeUsers = {};
      for (const [k, u] of Object.entries(storage.users || {})) {
        const { password_hash: _ph, ...rest } = u;
        safeUsers[k] = rest;
      }
      return sendJSON(res, 200, safeUsers);
    }
    if (pathname === '/api/admin/billing/settings') {
      if (req.method === 'POST') {
        try {
          const nextB = JSON.parse(await readBody(req));
          const net = String(nextB.usdt_network || 'TRC20').toUpperCase();
          const addr = String(nextB.usdt_address || '').trim();
          const m = Number(nextB.monthly_price_usdt || 0);
          const q = Number(nextB.quarterly_price_usdt || 0);
          const y = Number(nextB.yearly_price_usdt || 0);
          if (m < 0 || q < 0 || y < 0 || Number.isNaN(m) || Number.isNaN(q) || Number.isNaN(y)) {
            return sendJSON(res, 400, { error: 'Ücretler negatif olamaz' });
          }
          storage.billing = {
            usdt_address: addr,
            usdt_network: ['TRC20', 'BEP20', 'POLYGON', 'ERC20'].includes(net) ? net : 'TRC20',
            monthly_price_usdt: m,
            quarterly_price_usdt: q,
            yearly_price_usdt: y,
            auto_verify_enabled: nextB.auto_verify_enabled !== false
          };
          writeStorage(storage);
          return sendJSON(res, 200, { status: 'success', settings: storage.billing });
        } catch {
          return sendJSON(res, 400, { error: 'Invalid JSON payload' });
        }
      }
      return sendJSON(res, 200, storage.billing || { usdt_address: '', usdt_network: 'TRC20', monthly_price_usdt: 10, quarterly_price_usdt: 25, yearly_price_usdt: 80, auto_verify_enabled: true });
    }
    if (pathname === '/api/admin/billing/orders') {
      const orders = Object.values(storage.payment_orders || {}).map(({ password_hash: _ph, ...rest }) => rest);
      return sendJSON(res, 200, orders);
    }
    if (pathname === '/api/admin/epg/sources' || pathname === '/api/admin/epg/channels') {
      return sendJSON(res, 200, []);
    }
    if (pathname === '/api/admin/acestream/status') {
      return sendJSON(res, 200, {
        enabled: true,
        engine_path: 'C:\\Users\\admin\\AppData\\Roaming\\acestream\\engine\\ace_engine.exe',
        binary_exists: false,
        api_host: '127.0.0.1',
        stream_mode: 'ffmpeg_fmp4',
        primary_ready: false,
        active_count: 0,
        slots: []
      });
    }
    if (pathname === '/api/admin/mesh/status') {
      return sendJSON(res, 200, readTailscalePreviewStatus());
    }
    return sendJSON(res, 501, { error: 'Full streaming/write operations require the Go server (go run cmd/server/main.go)' });
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not Found');
});

// Lightweight RFC 6455 WebSocket Tracker on /ws/signaling for preview mode
const previewSwarms = new Map(); // channelId -> Map(peerId -> peerObj)
let previewPeerSeq = 1;

function sendWSFrame(socket, opcode, dataBuf) {
  if (socket.destroyed || !socket.writable) return;
  const len = dataBuf.length;
  let hdr;
  if (len < 126) {
    hdr = Buffer.allocUnsafe(2);
    hdr[0] = 0x80 | opcode;
    hdr[1] = len;
  } else if (len < 65536) {
    hdr = Buffer.allocUnsafe(4);
    hdr[0] = 0x80 | opcode;
    hdr[1] = 126;
    hdr.writeUInt16BE(len, 2);
  } else {
    hdr = Buffer.allocUnsafe(10);
    hdr[0] = 0x80 | opcode;
    hdr[1] = 127;
    hdr.writeBigUInt64BE(BigInt(len), 2);
  }
  try {
    socket.write(Buffer.concat([hdr, dataBuf]));
  } catch {}
}

function sendWSJson(socket, obj) {
  sendWSFrame(socket, 0x1, Buffer.from(JSON.stringify(obj), 'utf8'));
}

function broadcastPreviewSwarm(chId) {
  const swarm = previewSwarms.get(chId);
  if (!swarm || swarm.size === 0) return;
  const allInfos = Array.from(swarm.values()).map((p) => ({
    id: p.id,
    tier: p.tier,
    is_ipv6: p.isIPv6,
    is_tailscale: Boolean(p.isTailscale)
  }));
  for (const recipient of swarm.values()) {
    const others = allInfos.filter((i) => i.id !== recipient.id);
    sendWSJson(recipient.socket, {
      type: 'SWARM_PEERS',
      sender_id: 'server',
      target_id: recipient.id,
      self_tier: recipient.tier,
      peers: others.map((o) => o.id),
      peer_infos: others,
      total_peers: allInfos.length
    });
  }
}

function getPreviewSwarmStats() {
  const out = {};
  for (const [chId, swarm] of previewSwarms.entries()) {
    let ipv6 = 0;
    let tailscale = 0;
    let p2pIn = 0;
    let httpIn = 0;
    let outFree = 0;
    let outPaid = 0;
    for (const p of swarm.values()) {
      if (p.isIPv6) ipv6++;
      if (p.isTailscale) tailscale++;
      p2pIn += p.p2pBytesIn || 0;
      httpIn += p.httpBytesIn || 0;
      outFree += p.outFreeBytes || 0;
      outPaid += p.outPaidBytes || 0;
    }
    out[chId] = {
      total_peers: swarm.size,
      ipv6_peers: ipv6,
      ipv4_peers: swarm.size - ipv6,
      tailscale_peers: tailscale,
      p2p_kb_in: Math.floor(p2pIn / 1024),
      http_kb_in: Math.floor(httpIn / 1024),
      p2p_out_free_kb: Math.floor(outFree / 1024),
      p2p_out_paid_kb: Math.floor(outPaid / 1024)
    };
  }
  return out;
}

server.on('upgrade', (req, socket) => {
  const urlObj = new URL(req.url || '/', `http://localhost:${PORT}`);
  if (urlObj.pathname !== '/ws/signaling') {
    socket.destroy();
    return;
  }
  const wsKey = req.headers['sec-websocket-key'];
  if (!wsKey) {
    socket.destroy();
    return;
  }
  const acceptKey = crypto
    .createHash('sha1')
    .update(wsKey + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${acceptKey}\r\n\r\n`
  );

  const { jwtSecret } = readConfigDefaults();
  const channelId = (urlObj.searchParams.get('channel_id') || '').trim();
  if (!channelId) {
    sendWSJson(socket, { error: 'channel_id required' });
    socket.end();
    return;
  }

  let tokenStr = (urlObj.searchParams.get('token') || '').trim();
  if (!tokenStr) {
    const cookies = parseCookies(req);
    tokenStr = cookies.iptv_session || '';
  }

  let userTier = 'guest';
  if (tokenStr) {
    const claims = verifyToken(tokenStr, jwtSecret);
    if (!claims) {
      sendWSJson(socket, { error: 'unauthorized' });
      socket.end();
      return;
    }
    userTier = String(claims.tier || 'guest').toLowerCase();
  }

  const st = readStorage();
  const chCfg = (st.streams || {})[channelId];
  if (chCfg && chCfg.tier === 'paid' && userTier !== 'paid' && userTier !== 'admin') {
    sendWSJson(socket, { error: 'subscription required for paid channel' });
    socket.end();
    return;
  }

  const remoteAddr = String(req.socket.remoteAddress || '');
  const peerId = `peer_${previewPeerSeq++}_${crypto.randomBytes(3).toString('hex')}`;
  const peerObj = {
    id: peerId,
    channelId,
    tier: userTier,
    isIPv6: remoteAddr.includes(':'),
    isTailscale: isTailscaleAddress(remoteAddr),
    socket,
    p2pBytesIn: 0,
    httpBytesIn: 0,
    outFreeBytes: 0,
    outPaidBytes: 0
  };

  if (!previewSwarms.has(channelId)) previewSwarms.set(channelId, new Map());
  previewSwarms.get(channelId).set(peerId, peerObj);

  sendWSJson(socket, { type: 'WELCOME', sender_id: 'server', target_id: peerId, self_tier: userTier });
  broadcastPreviewSwarm(channelId);

  let recvBuf = Buffer.alloc(0);
  const cleanup = () => {
    const swarm = previewSwarms.get(channelId);
    if (swarm && swarm.has(peerId)) {
      swarm.delete(peerId);
      if (swarm.size === 0) previewSwarms.delete(channelId);
      else broadcastPreviewSwarm(channelId);
    }
  };

  socket.on('data', (chunk) => {
    recvBuf = Buffer.concat([recvBuf, chunk]);
    while (recvBuf.length >= 2) {
      const b0 = recvBuf[0];
      const b1 = recvBuf[1];
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;
      if (len === 126) {
        if (recvBuf.length < 4) return;
        len = recvBuf.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (recvBuf.length < 10) return;
        const bigLen = recvBuf.readBigUInt64BE(2);
        if (bigLen > 1048576n) {
          socket.destroy();
          return;
        }
        len = Number(bigLen);
        offset = 10;
      }
      if (len > 1048576) {
        socket.destroy();
        return;
      }
      const need = offset + (masked ? 4 : 0) + len;
      if (recvBuf.length < need) return;
      let maskKey = null;
      if (masked) {
        maskKey = recvBuf.subarray(offset, offset + 4);
        offset += 4;
      }
      const payload = Buffer.from(recvBuf.subarray(offset, offset + len));
      if (maskKey) {
        for (let i = 0; i < payload.length; i++) {
          payload[i] ^= maskKey[i % 4];
        }
      }
      recvBuf = recvBuf.subarray(offset + len);

      if (opcode === 0x8) {
        socket.end();
        return;
      }
      if (opcode === 0x9) {
        sendWSFrame(socket, 0xA, payload);
        continue;
      }
      if (opcode === 0x1 || opcode === 0x2) {
        try {
          const msg = JSON.parse(payload.toString('utf8'));
          if (msg.type === 'P2P_STATS' && msg.stats) {
            peerObj.p2pBytesIn = Math.max(0, Number(msg.stats.p2p_bytes_in || 0));
            peerObj.httpBytesIn = Math.max(0, Number(msg.stats.http_bytes_in || 0));
            peerObj.outFreeBytes = Math.max(0, Number(msg.stats.p2p_out_free_bytes || 0));
            peerObj.outPaidBytes = Math.max(0, Number(msg.stats.p2p_out_paid_bytes || 0));
          } else if (msg.target_id) {
            const swarm = previewSwarms.get(channelId);
            const target = swarm && swarm.get(msg.target_id);
            if (target) {
              msg.sender_id = peerId;
              sendWSJson(target.socket, msg);
            }
          }
        } catch {}
      }
    }
  });

  socket.on('close', cleanup);
  socket.on('error', cleanup);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Preview server listening on http://0.0.0.0:${PORT}`);
});
