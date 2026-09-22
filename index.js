// ═══════════════════════════════════════════════════════════════
// SENDER — Servidor de WhatsApp Coexistencia (multi-tenant)
// Usa Baileys (no oficial) para conectar por QR, como WhatsApp Web.
// Una sola instalación de este servidor sirve a TODOS los usuarios
// de SENDER — el súper admin habilita/deshabilita el módulo por
// usuario desde whatsapp_sessions.enabled en Supabase.
//
// ⚠️ Automatizar WhatsApp por esta vía no es la API oficial de Meta.
// Usa siempre el envío orgánico (módulo anti-spam) para reducir el
// riesgo de restricción del número conectado.
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
app.use(express.json());

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SESSIONS_BUCKET = process.env.SESSIONS_BUCKET || 'wa-sessions';
const PORT = process.env.PORT || 3000;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || SUPABASE_SERVICE_ROLE_KEY.startsWith('REEMPLAZAR')) {
  logger.error('Falta configurar SUPABASE_SERVICE_ROLE_KEY en las variables de entorno de Railway.');
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  realtime: { transport: WebSocket },
});

// Sesiones activas en memoria: owner_id -> { sock, status, qr }
const sessions = new Map();

// ─────────────────────────────────────────────────────────────
// Persistencia de credenciales de Baileys en Supabase Storage
// (para que un redeploy o reinicio de Railway NO borre la sesión
// y el usuario no tenga que volver a escanear el QR cada vez)
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
              const cacheKey = `${category}-${id}`;
              keysCache[cacheKey] = value;
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
// Estado en Supabase (para que el panel de SENDER lo lea en vivo)
// ─────────────────────────────────────────────────────────────
async function updateSessionRow(ownerId, patch) {
  await supabase.from('whatsapp_sessions').upsert({ owner_id: ownerId, updated_at: new Date().toISOString(), ...patch });
}
async function isModuleEnabled(ownerId) {
  const { data } = await supabase.from('whatsapp_sessions').select('enabled').eq('owner_id', ownerId).maybeSingle();
  return !!(data && data.enabled);
}

// ─────────────────────────────────────────────────────────────
// Contacto/mensaje real compartido con el resto del ecosistema
// (misma tabla que usan Telegram y el resto de canales)
// ─────────────────────────────────────────────────────────────
async function upsertContactByPhone(ownerId, phone, name) {
  let { data: row } = await supabase.from('contacts').select('id').eq('owner_id', ownerId).eq('phone', phone).maybeSingle();
  if (!row) {
    const { data: inserted } = await supabase.from('contacts')
      .insert({ owner_id: ownerId, name: name || phone, phone, channel: 'whatsapp', labels: ['WhatsApp'] })
      .select('id').single();
    row = inserted;
  }
  return row?.id || null;
}
async function logMessage(ownerId, contactId, direction, text) {
  if (!contactId) return;
  await supabase.from('messages').insert({ owner_id: ownerId, contact_id: contactId, direction, text });
}
async function syncGroups(ownerId, sock) {
  const groups = await sock.groupFetchAllParticipating();
  const rows = Object.values(groups).map((g) => ({
    owner_id: ownerId,
    group_jid: g.id,
    name: g.subject || 'Grupo sin nombre',
    participants_count: (g.participants || []).length,
    updated_at: new Date().toISOString(),
  }));
  if (rows.length) {
    await supabase.from('whatsapp_groups').upsert(rows, { onConflict: 'owner_id,group_jid' });
  }
  logger.info(`[${ownerId}] ${rows.length} grupo(s) sincronizados`);
  return rows.length;
}
// ─────────────────────────────────────────────────────────────
// Arranca (o retoma) la sesión de WhatsApp de un usuario
// ─────────────────────────────────────────────────────────────
async function startSession(ownerId) {
  if (sessions.has(ownerId) && sessions.get(ownerId).status === 'connected') {
    return { ok: true, status: 'connected' };
  }
  const enabled = await isModuleEnabled(ownerId);
  if (!enabled) {
    return { ok: false, error: 'El módulo de Coexistencia no está habilitado para este usuario. El súper admin debe activarlo primero.' };
  }

  const { state, saveCreds } = await useSupabaseAuthState(ownerId);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    logger,
    printQRInTerminal: false,
    browser: ['SENDER', 'Chrome', '1.0.0'],
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
      logger.info(`[${ownerId}] QR generado — esperando escaneo`);
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
      if (!loggedOut) {
        // Reintenta automáticamente si no fue un cierre de sesión explícito
        setTimeout(() => startSession(ownerId).catch(() => {}), 4000);
      }
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return;
    for (const msg of messages) {
      if (!msg.message || msg.key.fromMe) continue;
      const from = msg.key.remoteJid;
      if (!from || from.endsWith('@g.us')) continue; // por ahora solo chats 1 a 1
      const phone = '+' + from.split('@')[0];
      const text = msg.message.conversation || msg.message.extendedTextMessage?.text || '';
      if (!text) continue;
      const name = msg.pushName || phone;
      const contactId = await upsertContactByPhone(ownerId, phone, name);
      await logMessage(ownerId, contactId, 'in', text);
      logger.info(`[${ownerId}] Mensaje entrante de ${phone}: ${text.slice(0, 60)}`);
      // Nota: aquí es donde, en el siguiente paso, se conecta el mismo motor de
      // flujos server-side que ya corre para Telegram (cart-node, calendar-node,
      // close-node, etc.) para responder automáticamente por WhatsApp también.
    }
  });

  return { ok: true, status: 'connecting' };
}

async function stopSession(ownerId) {
  const entry = sessions.get(ownerId);
  if (entry?.sock) {
    try { await entry.sock.logout(); } catch {}
  }
  sessions.delete(ownerId);
  await updateSessionRow(ownerId, { status: 'disconnected', qr_data: null });
}

// ─────────────────────────────────────────────────────────────
// API HTTP — la llama el panel de SENDER
// ─────────────────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ ok: true, sessions: sessions.size }));

app.post('/session/start', async (req, res) => {
  const { owner_id } = req.body;
  if (!owner_id) return res.status(400).json({ error: 'Falta owner_id' });
  try {
    const result = await startSession(owner_id);
    res.json(result);
  } catch (e) {
    logger.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
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

app.post('/send', async (req, res) => {
  const { owner_id, phone, text } = req.body;
  if (!owner_id || !phone || !text) return res.status(400).json({ error: 'Faltan owner_id, phone o text' });
  const entry = sessions.get(owner_id);
  if (!entry || entry.status !== 'connected') return res.status(409).json({ error: 'Esta cuenta no tiene WhatsApp conectado ahora mismo' });
  try {
    const jid = phone.replace(/[^\d]/g, '') + '@s.whatsapp.net';
    await entry.sock.sendMessage(jid, { text });
    const contactId = await upsertContactByPhone(owner_id, phone);
    await logMessage(owner_id, contactId, 'out', text);
    res.json({ ok: true });
  } catch (e) {
    logger.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.listen(PORT, () => {
  logger.info(`SENDER Coexistencia escuchando en puerto ${PORT}`);
});
app.post('/groups/sync', async (req, res) => {
  const { owner_id } = req.body;
  const entry = sessions.get(owner_id);
  if (!entry || entry.status !== 'connected') return res.status(409).json({ error: 'WhatsApp no está conectado ahora mismo' });
  try {
    const count = await syncGroups(owner_id, entry.sock);
    res.json({ ok: true, count });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});
// Al arrancar, retoma automáticamente las sesiones que estaban conectadas
// antes del último reinicio/redeploy, sin que el usuario tenga que hacer nada.
(async () => {
  try {
    const { data } = await supabase.from('whatsapp_sessions').select('owner_id').eq('enabled', true).eq('status', 'connected');
    for (const row of data || []) {
      logger.info(`Retomando sesión de ${row.owner_id}...`);
      startSession(row.owner_id).catch((e) => logger.error(e));
    }
  } catch (e) {
    logger.error(e);
  }
})();
