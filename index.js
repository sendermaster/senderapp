// ═══════════════════════════════════════════════════════════════
// SENDER — Servidor de WhatsApp Coexistencia (multi-tenant) v2
// Baileys (no oficial, como WhatsApp Web) + Supabase.
// No usa disco local: credenciales en Supabase Storage, datos en tablas.
// Por eso cabe en el plan gratuito de Railway.
// ═══════════════════════════════════════════════════════════════
import express from 'express';
import cors from 'cors';
import { createClient } from '@supabase/supabase-js';
import { WebSocket } from 'ws';
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  initAuthCreds,
  BufferJSON,
  makeCacheableSignalKeyStore,
} from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import pino from 'pino';
import QRCode from 'qrcode';

const logger = pino({ level: 'info' });
const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SESSIONS_BUCKET = process.env.SESSIONS_BUCKET || 'wa-sessions';
const PORT = process.env.PORT || 3000;
const MAX_BULK_PER_JOB = parseInt(process.env.MAX_BULK_PER_JOB || '80', 10);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || SUPABASE_SERVICE_ROLE_KEY.startsWith('REEMPLAZAR')) {
  logger.error('Falta configurar SUPABASE_SERVICE_ROLE_KEY en las variables de entorno de Railway.');
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  realtime: { transport: WebSocket },
});

// owner_id -> { sock, status, qr }
const sessions = new Map();
// owner_id -> { running, queue: [], sent, failed, total }
const bulkJobs = new Map();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = (min, max) => Math.floor(min + Math.random() * (max - min + 1));

// ─────────────────────────────────────────────────────────────
// Credenciales de Baileys guardadas en Supabase Storage
// ─────────────────────────────────────────────────────────────
async function readJsonFromStorage(path) {
  const { data, error } = await supabase.storage.from(SESSIONS_BUCKET).download(path);
  if (error || !data) return null;
  const text = await data.text();
  try { return JSON.parse(text, BufferJSON.reviver); } catch { return null; }
}
async function writeJsonToStorage(path, obj) {
  const text = JSON.stringify(obj, BufferJSON.replacer, 2);
  await supabase.storage.from(SESSIONS_BUCKET).upload(path, new Blob([text], { type: 'application/json' }), { upsert: true });
}
async function useSupabaseAuthState(ownerId) {
  const credsPath = `${ownerId}/creds.json`;
  let creds = await readJsonFromStorage(credsPath);
  if (!creds) creds = initAuthCreds();
  const keysCache = {};
  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          for (const id of ids) {
            const cacheKey = `${type}-${id}`;
            if (keysCache[cacheKey] !== undefined) { data[id] = keysCache[cacheKey]; continue; }
            const value = await readJsonFromStorage(`${ownerId}/keys/${type}-${id}.json`);
            if (value) { keysCache[cacheKey] = value; data[id] = value; }
          }
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const category of Object.keys(data)) {
            for (const id of Object.keys(data[category])) {
              const value = data[category][id];
              keysCache[`${category}-${id}`] = value;
              tasks.push(writeJsonToStorage(`${ownerId}/keys/${category}-${id}.json`, value));
            }
          }
          await Promise.all(tasks);
        },
      },
    },
    saveCreds: async () => writeJsonToStorage(credsPath, creds),
  };
}

// ─────────────────────────────────────────────────────────────
// Estado de la sesión (lo lee el panel de SENDER)
// ─────────────────────────────────────────────────────────────
async function updateSessionRow(ownerId, patch) {
  await supabase.from('whatsapp_sessions').upsert({ owner_id: ownerId, updated_at: new Date().toISOString(), ...patch });
}
async function isModuleEnabled(ownerId) {
  const { data } = await supabase.from('whatsapp_sessions').select('enabled').eq('owner_id', ownerId).maybeSingle();
  return !!(data && data.enabled);
}

// ─────────────────────────────────────────────────────────────
// Contactos y mensajes (mismas tablas que Telegram, Web, etc.)
// ─────────────────────────────────────────────────────────────
function jidToPhone(jid) {
  if (!jid) return null;
  const num = jid.split('@')[0].split(':')[0];
  if (jid.endsWith('@lid')) return 'lid:' + num;
  return '+' + num;
}
function extractText(message) {
  if (!message) return '';
  const m = message.ephemeralMessage?.message || message.viewOnceMessage?.message || message;
  return (
    m.conversation ||
    m.extendedTextMessage?.text ||
    m.imageMessage?.caption || (m.imageMessage ? '[imagen]' : '') ||
    m.videoMessage?.caption || (m.videoMessage ? '[video]' : '') ||
    (m.audioMessage ? '[audio]' : '') ||
    (m.documentMessage ? '[documento] ' + (m.documentMessage.fileName || '') : '') ||
    (m.stickerMessage ? '[sticker]' : '') ||
    (m.contactMessage ? '[contacto]' : '') ||
    (m.locationMessage ? '[ubicación]' : '') ||
    ''
  );
}
async function upsertContactByPhone(ownerId, phone, name) {
  let { data: row } = await supabase.from('contacts').select('id,name').eq('owner_id', ownerId).eq('phone', phone).maybeSingle();
  if (!row) {
    const { data: inserted } = await supabase.from('contacts')
      .insert({ owner_id: ownerId, name: name || phone, phone, channel: 'whatsapp', labels: ['WhatsApp'] })
      .select('id').single();
    row = inserted;
  } else if (name && row.name === phone && name !== phone) {
    await supabase.from('contacts').update({ name }).eq('id', row.id);
  }
  return row?.id || null;
}
async function logMessage(ownerId, contactId, direction, text) {
  if (!contactId || !text) return;
  await supabase.from('messages').insert({ owner_id: ownerId, contact_id: contactId, direction, text });
}

// Sincroniza la agenda completa del teléfono en lotes (sin duplicar)
const pendingContacts = new Map(); // ownerId -> Map(phone -> name)
function queueContacts(ownerId, list) {
  if (!pendingContacts.has(ownerId)) pendingContacts.set(ownerId, new Map());
  const m = pendingContacts.get(ownerId);
  for (const c of list) {
    if (!c.id || !c.id.endsWith('@s.whatsapp.net')) continue;
    const phone = jidToPhone(c.id);
    const name = c.name || c.notify || c.verifiedName || phone;
    m.set(phone, name);
  }
}
async function flushContacts() {
  for (const [ownerId, m] of pendingContacts.entries()) {
    if (!m.size) continue;
    const entries = Array.from(m.entries());
    m.clear();
    for (let i = 0; i < entries.length; i += 200) {
      const chunk = entries.slice(i, i + 200);
      try {
        const phones = chunk.map(([p]) => p);
        const { data: existing } = await supabase.from('contacts').select('phone').eq('owner_id', ownerId).in('phone', phones);
        const have = new Set((existing || []).map((r) => r.phone));
        const rows = chunk.filter(([p]) => !have.has(p)).map(([phone, name]) => ({
          owner_id: ownerId, name, phone, channel: 'whatsapp', labels: ['WhatsApp'],
        }));
        if (rows.length) await supabase.from('contacts').insert(rows);
      } catch (e) { logger.error(e); }
    }
    logger.info(`[${ownerId}] contactos sincronizados (${entries.length} procesados)`);
  }
}
setInterval(() => flushContacts().catch((e) => logger.error(e)), 8000);

async function syncGroups(ownerId, sock) {
  const groups = await sock.groupFetchAllParticipating();
  const rows = Object.values(groups).map((g) => ({
    owner_id: ownerId,
    group_jid: g.id,
    name: g.subject || 'Grupo sin nombre',
    participants_count: (g.participants || []).length,
    updated_at: new Date().toISOString(),
  }));
  if (rows.length) await supabase.from('whatsapp_groups').upsert(rows, { onConflict: 'owner_id,group_jid' });
  logger.info(`[${ownerId}] ${rows.length} grupo(s) sincronizados`);
  return rows.length;
}
const groupSyncTimers = new Map();
function scheduleGroupSync(ownerId, sock) {
  clearTimeout(groupSyncTimers.get(ownerId));
  groupSyncTimers.set(ownerId, setTimeout(() => syncGroups(ownerId, sock).catch((e) => logger.error(e)), 4000));
}

// ─────────────────────────────────────────────────────────────
// Sesión de WhatsApp por usuario
// ─────────────────────────────────────────────────────────────
// Avisa al motor de flujos (Supabase) que llegó un mensaje por WhatsApp.
// El motor decide si responde: está apagado por defecto y exige palabra clave.
const ENGINE_URL = process.env.ENGINE_URL || `${SUPABASE_URL}/functions/v1/telegram-webhook`;
function triggerFlowEngine(ownerId, phone, name, text) {
  if (!phone || phone.startsWith('lid:')) return; // sin número real no se puede responder con seguridad
  fetch(`${ENGINE_URL}?owner=${ownerId}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ channel: 'whatsapp', phone, name, text }),
  }).catch((e) => logger.error('motor de flujos: ' + e.message));
}

async function startSession(ownerId) {
  if (sessions.has(ownerId) && sessions.get(ownerId).status === 'connected') return { ok: true, status: 'connected' };
  if (!(await isModuleEnabled(ownerId))) {
    return { ok: false, error: 'El módulo de Coexistencia no está habilitado para este usuario. El súper admin debe activarlo primero.' };
  }
  const { state, saveCreds } = await useSupabaseAuthState(ownerId);
  let version;
  try { ({ version } = await fetchLatestBaileysVersion()); } catch { /* usa la versión por defecto */ }

  const sock = makeWASocket({
    ...(version ? { version } : {}),
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
    logger,
    printQRInTerminal: false,
    browser: ['SENDER', 'Chrome', '1.0.0'],
    markOnlineOnConnect: false,   // no silencia las notificaciones de tu celular
    syncFullHistory: false,       // sin historial completo: cabe en planes pequeños
  });

  sessions.set(ownerId, { sock, status: 'connecting', qr: null });
  await updateSessionRow(ownerId, { status: 'connecting', qr_data: null });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;
    const entry = sessions.get(ownerId) || {};
    if (qr) {
      const qrImage = await QRCode.toDataURL(qr);
      sessions.set(ownerId, { ...entry, status: 'qr_pending', qr: qrImage });
      await updateSessionRow(ownerId, { status: 'qr_pending', qr_data: qrImage });
    }
    if (connection === 'open') {
      const phone = sock.user?.id?.split(':')[0] || null;
      sessions.set(ownerId, { ...entry, status: 'connected', qr: null });
      await updateSessionRow(ownerId, { status: 'connected', qr_data: null, phone, last_seen: new Date().toISOString() });
      logger.info(`[${ownerId}] Conectado como ${phone}`);
      syncGroups(ownerId, sock).catch((e) => logger.error(e));
    }
    if (connection === 'close') {
      const statusCode = (lastDisconnect?.error instanceof Boom) ? lastDisconnect.error.output?.statusCode : null;
      const loggedOut = statusCode === DisconnectReason.loggedOut;
      sessions.delete(ownerId);
      await updateSessionRow(ownerId, { status: loggedOut ? 'disconnected' : 'error', qr_data: null });
      logger.warn(`[${ownerId}] Conexión cerrada (loggedOut=${loggedOut})`);
      if (!loggedOut) setTimeout(() => startSession(ownerId).catch(() => {}), 4000);
    }
  });

  // Agenda de contactos del teléfono
  sock.ev.on('contacts.upsert', (list) => queueContacts(ownerId, list));
  sock.ev.on('contacts.update', (list) => queueContacts(ownerId, list));
  sock.ev.on('messaging-history.set', ({ contacts }) => { if (contacts) queueContacts(ownerId, contacts); });

  // Cambios en grupos
  sock.ev.on('groups.upsert', () => scheduleGroupSync(ownerId, sock));
  sock.ev.on('groups.update', () => scheduleGroupSync(ownerId, sock));
  sock.ev.on('group-participants.update', () => scheduleGroupSync(ownerId, sock));

  // Mensajes: entrantes y también los que escribes desde tu celular (como WhatsApp Web)
  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify' && type !== 'append') return;
    for (const msg of messages) {
      try {
        if (!msg.message) continue;
        const jid = msg.key.remoteJid;
        if (!jid || jid === 'status@broadcast' || jid.endsWith('@g.us') || jid.endsWith('@broadcast')) continue;
        const altJid = msg.key.remoteJidAlt || msg.key.senderPn;
        const realJid = jid.endsWith('@lid') && altJid && altJid.endsWith('@s.whatsapp.net') ? altJid : jid;
        const phone = jidToPhone(realJid);
        const text = extractText(msg.message);
        if (!text) continue;
        const fromMe = !!msg.key.fromMe;
        if (type === 'append' && !fromMe) continue; // historial viejo ajeno: lo ignoramos
        const contactId = await upsertContactByPhone(ownerId, phone, fromMe ? null : (msg.pushName || phone));
        await logMessage(ownerId, contactId, fromMe ? 'out' : 'in', text);
        if (!fromMe && type === 'notify') triggerFlowEngine(ownerId, phone, msg.pushName || phone, text);
      } catch (e) { logger.error(e); }
    }
    // Aquí se conectará el motor de flujos (carrito, agenda, cierre de venta) para responder por WhatsApp.
  });

  return { ok: true, status: 'connecting' };
}

async function stopSession(ownerId) {
  const entry = sessions.get(ownerId);
  if (entry?.sock) { try { await entry.sock.logout(); } catch {} }
  sessions.delete(ownerId);
  await updateSessionRow(ownerId, { status: 'disconnected', qr_data: null });
}

// ─────────────────────────────────────────────────────────────
// Envío (individual, grupo y masivo con pausas aleatorias)
// ─────────────────────────────────────────────────────────────
function normalizeJid(target) {
  if (target.includes('@')) return target;
  return target.replace(/[^\d]/g, '') + '@s.whatsapp.net';
}
function buildContent(text, mediaUrl) {
  if (!mediaUrl) return { text };
  const clean = mediaUrl.split('?')[0].toLowerCase();
  if (/\.(jpe?g|png|webp|gif)$/.test(clean)) return { image: { url: mediaUrl }, caption: text };
  if (/\.(mp4|mov|3gp)$/.test(clean)) return { video: { url: mediaUrl }, caption: text };
  if (/\.(mp3|ogg|m4a|wav|opus)$/.test(clean)) return { audio: { url: mediaUrl }, mimetype: 'audio/mpeg' };
  const fileName = decodeURIComponent(clean.split('/').pop() || 'archivo');
  return { document: { url: mediaUrl }, caption: text, fileName };
}
async function sendOne(sock, jid, text, mediaUrl) {
  try { await sock.presenceSubscribe(jid); await sock.sendPresenceUpdate('composing', jid); } catch {}
  await sleep(rand(1500, 4000));
  const result = await sock.sendMessage(jid, buildContent(text, mediaUrl));
  try { await sock.sendPresenceUpdate('paused', jid); } catch {}
  return result;
}

async function runBulk(ownerId) {
  const job = bulkJobs.get(ownerId);
  if (!job || job.running) return;
  job.running = true;
  while (job.queue.length) {
    const entry = sessions.get(ownerId);
    if (!entry || entry.status !== 'connected') { job.failed += job.queue.length; job.queue = []; break; }
    const item = job.queue.shift();
    try {
      await sendOne(entry.sock, item.jid, item.text, item.mediaUrl);
      job.sent++;
    } catch (e) {
      job.failed++;
      logger.error(`[${ownerId}] fallo al enviar a ${item.jid}: ${e.message}`);
    }
    if (job.queue.length) await sleep(rand(item.minDelay, item.maxDelay) * 1000);
  }
  job.running = false;
  logger.info(`[${ownerId}] envío masivo terminado: ${job.sent} enviados, ${job.failed} fallidos`);
}

app.get('/health', (req, res) => res.json({ ok: true, sessions: sessions.size }));

app.post('/session/start', async (req, res) => {
  const { owner_id } = req.body;
  if (!owner_id) return res.status(400).json({ error: 'Falta owner_id' });
  try { res.json(await startSession(owner_id)); }
  catch (e) { logger.error(e); res.status(500).json({ error: String(e.message || e) }); }
});
app.post('/session/stop', async (req, res) => {
  const { owner_id } = req.body;
  if (!owner_id) return res.status(400).json({ error: 'Falta owner_id' });
  await stopSession(owner_id);
  res.json({ ok: true });
});
app.get('/session/status/:owner_id', async (req, res) => {
  const entry = sessions.get(req.params.owner_id);
  const { data } = await supabase.from('whatsapp_sessions').select('*').eq('owner_id', req.params.owner_id).maybeSingle();
  res.json({ live: entry ? { status: entry.status, hasQr: !!entry.qr } : null, db: data || null });
});
app.post('/groups/sync', async (req, res) => {
  const { owner_id } = req.body;
  const entry = sessions.get(owner_id);
  if (!entry || entry.status !== 'connected') return res.status(409).json({ error: 'WhatsApp no está conectado ahora mismo' });
  try { res.json({ ok: true, count: await syncGroups(owner_id, entry.sock) }); }
  catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

// Envío individual o a un grupo (group_jid) — con "escribiendo…" y media opcional
app.post('/send', async (req, res) => {
  const { owner_id, phone, text, group_jid, media_url, no_log } = req.body;
  const target = group_jid || phone;
  if (!owner_id || !target || (!text && !media_url)) return res.status(400).json({ error: 'Faltan owner_id, destino o contenido' });
  const entry = sessions.get(owner_id);
  if (!entry || entry.status !== 'connected') return res.status(409).json({ error: 'Esta cuenta no tiene WhatsApp conectado ahora mismo' });
  try {
    const jid = normalizeJid(target);
    await sendOne(entry.sock, jid, text || '', media_url);
    if (!jid.endsWith('@g.us') && !no_log) {
      const contactId = await upsertContactByPhone(owner_id, jidToPhone(jid));
      await logMessage(owner_id, contactId, 'out', text || '[archivo]');
    }
    res.json({ ok: true });
  } catch (e) { logger.error(e); res.status(500).json({ error: String(e.message || e) }); }
});

// Envío masivo en segundo plano con pausas aleatorias entre cada destino (anti-spam)
// body: { owner_id, targets: [jid|telefono], text, media_url, min_delay_s, max_delay_s }
app.post('/send-bulk', (req, res) => {
  const { owner_id, targets, text, media_url, min_delay_s, max_delay_s } = req.body;
  if (!owner_id || !Array.isArray(targets) || !targets.length || (!text && !media_url)) {
    return res.status(400).json({ error: 'Faltan owner_id, targets o contenido' });
  }
  const entry = sessions.get(owner_id);
  if (!entry || entry.status !== 'connected') return res.status(409).json({ error: 'Esta cuenta no tiene WhatsApp conectado ahora mismo' });
  const minDelay = Math.max(5, parseInt(min_delay_s || 12, 10));
  const maxDelay = Math.max(minDelay, parseInt(max_delay_s || 40, 10));
  const limited = targets.slice(0, MAX_BULK_PER_JOB);
  if (!bulkJobs.has(owner_id)) bulkJobs.set(owner_id, { running: false, queue: [], sent: 0, failed: 0 });
  const job = bulkJobs.get(owner_id);
  if (!job.running) { job.sent = 0; job.failed = 0; }
  limited.forEach((t) => job.queue.push({ jid: normalizeJid(t), text: text || '', mediaUrl: media_url || null, minDelay, maxDelay }));
  runBulk(owner_id).catch((e) => logger.error(e));
  res.status(202).json({ ok: true, queued: limited.length, truncated: targets.length > limited.length });
});
app.get('/bulk/status/:owner_id', (req, res) => {
  const job = bulkJobs.get(req.params.owner_id);
  res.json(job ? { running: job.running, pending: job.queue.length, sent: job.sent, failed: job.failed } : { running: false, pending: 0, sent: 0, failed: 0 });
});

app.listen(PORT, () => logger.info(`SENDER Coexistencia v2 escuchando en puerto ${PORT}`));

// Retoma sesiones que estaban activas antes de un reinicio
(async () => {
  try {
    const { data } = await supabase.from('whatsapp_sessions').select('owner_id').eq('enabled', true).in('status', ['connected', 'error']);
    for (const row of data || []) {
      logger.info(`Retomando sesión de ${row.owner_id}...`);
      startSession(row.owner_id).catch((e) => logger.error(e));
    }
  } catch (e) { logger.error(e); }
})();
