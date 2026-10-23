/* ============================================================
 * VANTS-BUS · Puente FACEIT → bus (drop-in para vants-faceit-webhook)
 *
 * Convierte cada webhook de FACEIT en eventos del contrato VANTS,
 * con DEDUPLICACIÓN por event_id (FACEIT reintenta webhooks) y
 * REINTENTOS con backoff hacia el bus. Si el bus está caído, tu
 * webhook igual responde 2xx a FACEIT y el evento no se pierde:
 * la deduplicación libera el event_id para el siguiente intento.
 *
 * CÓMO CONECTARLO (2 pasos):
 *
 *   1) Copia a tu repo este archivo + vants-bus-client.js
 *   2) En tu handler de webhook, DESPUÉS de validar la firma de
 *      FACEIT y guardar el payload en tu Neon:
 *
 *        import { createFaceitBridge } from './vants-bus-faceit.js';
 *
 *        const bridge = createFaceitBridge({
 *          busUrl: env.VANTS_BUS_URL,          // o process.env en Node
 *          secret: env.VANTS_BUS_SECRET,
 *          sql: tuConexionNeonExistente,        // opcional: fn(query, params) → rows
 *          DATABASE_URL: env.DATABASE_URL,      // ...o deja que el módulo cree su conexión
 *        });
 *
 *        // Workers/Pages:  ctx.waitUntil(bridge.handle(faceitEvent))
 *        // Node/Express:   bridge.handle(faceitEvent).catch(console.error)
 *
 *   Recomendación: si bridge.handle() devuelve {ok:false}, responde
 *   5xx a FACEIT para que reintente (el módulo ya libera el dedup).
 * ============================================================ */

import { VantsBus, EVENTS } from './vants-bus-client.js';

/* Tabla de deduplicación (se crea sola la primera vez). */
const DEDUP_DDL = `
  CREATE TABLE IF NOT EXISTS vants_bus_dedup (
    event_id TEXT PRIMARY KEY,
    seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
  )`;

/* ---------- mapeo FACEIT → contrato VANTS ---------- */

function normalizeGame(g) {
  const s = String(g || '').toLowerCase();
  if (s === 'csgo') return 'cs2';
  return s || 'cs2';
}

export function mapFaceitToVants(faceitEvent) {
  const out = [];
  const ev = String(faceitEvent?.event || '');
  const p = faceitEvent?.payload || {};

  switch (ev) {
    case 'match_status_finished': {
      out.push({
        type: EVENTS.MATCH_FINISHED,
        data: {
          matchId: p.match_id,
          game: normalizeGame(p.game),
          a: p?.teams?.faction1?.name,
          b: p?.teams?.faction2?.name,
          sa: p?.results?.score?.faction1,
          sb: p?.results?.score?.faction2,
          finishedAt: p.finished_at || new Date().toISOString(),
        },
      });
      break;
    }
    case 'match_object_created': {
      out.push({
        type: EVENTS.MATCH_SCHEDULED,
        data: {
          matchId: p.match_id,
          game: normalizeGame(p.game),
          startsAt: p.started_at || null,
        },
      });
      break;
    }
    case 'match_status_canceled': {
      out.push({
        type: EVENTS.MATCH_CANCELED,
        data: {
          matchId: p.match_id,
          game: normalizeGame(p.game),
          canceledAt: new Date().toISOString(),
        },
      });
      break;
    }
    default:
      // Evento de FACEIT sin mapeo: se ignora silenciosamente.
      break;
  }
  return out;
}

/* ---------- fábrica del puente ---------- */

export function createFaceitBridge({
  busUrl,
  secret,
  projectId = 'faceit-webhook',
  sql = null,           // tu conexión Neon ya existente: fn(query, params) → rows
  DATABASE_URL = null,  // ...o la cadena para que este módulo cree la suya
  maxRetries = 3,
  logger = console,
}) {
  if (!busUrl)  throw new Error('createFaceitBridge: falta busUrl');
  if (!secret)  throw new Error('createFaceitBridge: falta secret');

  const bus = new VantsBus({ busUrl, projectId, secret });

  let _sql = sql;
  let _tableReady = false;

  function getSql() {
    if (_sql) return _sql;
    if (!DATABASE_URL) {
      throw new Error('createFaceitBridge: pasa `sql` (tu conexión Neon) o `DATABASE_URL`');
    }
    throw new Error('createFaceitBridge: usa initNeon() (async) o pasa tu `sql` ya creada');
  }

  /* Para quien no pase `sql`: llama una vez al arrancar. */
  async function initNeon() {
    if (_sql) return;
    const mod = await import('@neondatabase/serverless');
    const neon = mod.neon(DATABASE_URL);
    _sql = (query, params) => neon.query(query, params);
  }

  async function ensureTable() {
    if (_tableReady) return;
    await getSql()(DEDUP_DDL, []);
    _tableReady = true;
  }

  /* Claim atómico: true si somos los primeros en ver este event_id. */
  async function claim(eventId) {
    const res = await getSql()(
      `INSERT INTO vants_bus_dedup (event_id) VALUES ($1)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [eventId]
    );
    const rows = Array.isArray(res) ? res : (res && res.rows) || [];
    return rows.length > 0;
  }

  /* Si el publish falla definitivamente, devolvemos el event_id
   * para que el reintento de FACEIT (o manual) pueda reclamarlo otra vez. */
  async function releaseClaim(eventId) {
    try {
      await getSql()(`DELETE FROM vants_bus_dedup WHERE event_id = $1`, [eventId]);
    } catch (e) {
      logger.warn?.('[vants-faceit] no pude liberar el dedup:', e?.message || e);
    }
  }

  async function publishWithRetry(events) {
    let lastErr;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await bus.publishBatch(events);
      } catch (err) {
        lastErr = err;
        if (attempt < maxRetries) {
          const waitMs = 500 * 2 ** (attempt - 1);
          logger.warn?.(`[vants-faceit] publish falló (intento ${attempt}/${maxRetries}), reintento en ${waitMs}ms`);
          await new Promise(r => setTimeout(r, waitMs));
        }
      }
    }
    throw lastErr;
  }

  /* ---------- handler principal: NUNCA lanza por fallos del bus ---------- */
  async function handle(faceitEvent) {
    if (!faceitEvent || typeof faceitEvent !== 'object') {
      return { ok: false, skipped: 'payload no es objeto' };
    }

    const mapped = mapFaceitToVants(faceitEvent);
    if (mapped.length === 0) {
      return { ok: true, published: 0, ignored: faceitEvent.event || 'sin campo event' };
    }

    const eventId = faceitEvent.event_id || faceitEvent.id || '';
    if (!eventId) {
      // Sin event_id no hay dedup posible: publicamos igual (avisa en logs).
      logger.warn?.('[vants-faceit] webhook sin event_id — publicando sin dedup');
      try {
        const res = await publishWithRetry(mapped);
        return { ok: true, published: mapped.length, ids: res.ids };
      } catch (err) {
        return { ok: false, error: `bus: ${err.message}` };
      }
    }

    try {
      await ensureTable();
      const firstTime = await claim(eventId);
      if (!firstTime) {
        return { ok: true, published: 0, dedup: true, eventId };
      }
    } catch (err) {
      // Neon caído: no podemos deduplicar. Lanzamos → tu handler puede
      // responder 5xx y FACEIT reintenta con el dedup ya disponible.
      throw new Error(`dedup indisponible: ${err.message}`);
    }

    try {
      const res = await publishWithRetry(mapped);
      return { ok: true, published: mapped.length, ids: res.ids, eventId };
    } catch (err) {
      await releaseClaim(eventId);
      logger.error?.('[vants-faceit] publish falló tras reintentos — dedup liberado:', err.message);
      return { ok: false, error: `bus: ${err.message}`, eventId };
    }
  }

  return { handle, mapFaceitToVants, initNeon };
}

export default createFaceitBridge;
