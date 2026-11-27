/* ============================================================
 * VANTS-BUS · Puente FACEIT → bus (drop-in para vants-faceit-webhook)
 * EDICIÓN D1 (todo en Cloudflare): la deduplicación vive en el binding D1
 * `db` (vants-db), ya no en Postgres (Neon/Supabase).
 *
 * Convierte cada webhook de FACEIT en eventos del contrato VANTS,
 * con DEDUPLICACIÓN por event_id (FACEIT reintenta webhooks) y
 * REINTENTOS con backoff hacia el bus. Si el bus está caído, tu
 * webhook igual responde 2xx a FACEIT y el evento no se pierde:
 * la deduplicación libera el event_id para el siguiente intento.
 *
 * CÓMO CONECTARLO (2 pasos):
 *
 *   1) Copia a tu repo este archivo + vants-bus-client.js + _d1.js
 *   2) En tu handler de webhook, DESPUÉS de validar la firma de
 *      FACEIT y guardar el payload en tu D1:
 *
 *        import { createFaceitBridge } from './vants-bus-faceit.js';
 *
 *        const bridge = createFaceitBridge({
 *          busUrl: env.VANTS_BUS_URL,          // o process.env en Node
 *          secret: env.VANTS_BUS_SECRET,
 *          db: env.DB,                          // binding D1 (vants-db)
 *        });
 *
 *        // Workers/Pages:  ctx.waitUntil(bridge.handle(faceitEvent))
 *        // Node/Express:   bridge.handle(faceitEvent).catch(console.error)
 *
 *   Recomendación: si bridge.handle() devuelve {ok:false}, responde
 *   5xx a FACEIT para que reintente (el módulo ya libera el dedup).
 * ============================================================ */

import { crearSql } from './_d1.js';
import { VantsBus, EVENTS } from './vants-bus-client.js';

/* Tabla de deduplicación (ya existe en vants-db; DDL idempotente por si acaso). */
const DEDUP_DDL = `
  CREATE TABLE IF NOT EXISTS vants_bus_dedup (
    event_id TEXT PRIMARY KEY,
    seen_at  TEXT NOT NULL DEFAULT (datetime('now'))
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
  const matchId = p.match_id || p.id || null;
  const game = normalizeGame(p.game);

  switch (ev) {
    case 'match_status_finished': {
      const teamA = Array.isArray(p.teams) ? p.teams[0]?.name : p?.teams?.faction1?.name;
      const teamB = Array.isArray(p.teams) ? p.teams[1]?.name : p?.teams?.faction2?.name;
      const scores = p?.results?.score || {};
      const vals = Object.values(scores);
      out.push({
        type: EVENTS.MATCH_FINISHED,
        data: {
          matchId,
          game,
          a: teamA || null,
          b: teamB || null,
          sa: scores.faction1 ?? vals[0] ?? null,
          sb: scores.faction2 ?? vals[1] ?? null,
          finishedAt: p.finished_at || new Date().toISOString(),
        },
      });
      break;
    }
    case 'match_object_created': {
      out.push({ type: EVENTS.MATCH_SCHEDULED, data: { matchId, game, startsAt: p.started_at || null } });
      break;
    }
    case 'match_status_canceled':
    case 'match_status_cancelled':
    case 'match_status_aborted': {
      out.push({ type: EVENTS.MATCH_CANCELED, data: { matchId, game, canceledAt: new Date().toISOString() } });
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
  busService = null,
  secret,
  projectId = 'faceit-webhook',
  sql = null,           // tu conexión ya existente (interfaz crearSql)
  db = null,            // ...o el binding D1 para que este módulo cree la suya
  maxRetries = 3,
  logger = console,
  fetchImpl,
}) {
  if (!busUrl && !busService) throw new Error('createFaceitBridge: falta busUrl o busService');
  if (!secret) throw new Error('createFaceitBridge: falta secret');

  const resolvedFetchImpl = fetchImpl ?? (busService ? (u, init) => busService.fetch(new Request(u, init)) : undefined);
  const bus = new VantsBus({ busUrl: busUrl || 'https://vants-bus', projectId, secret, fetchImpl: resolvedFetchImpl });

  let _sql = sql;
  let _tableReady = false;

  function getSql() {
    if (_sql) return _sql;
    if (!db) {
      throw new Error('createFaceitBridge: pasa `sql` (tu conexión crearSql) o `db` (binding D1)');
    }
    _sql = crearSql(db);
    return _sql;
  }

  /* Compatibilidad: ya no hace falta llamarla (getSql crea la conexión si
   * hay binding D1), pero se conserva para quien la invocara antes. */
  async function initNeon() {
    getSql();
  }

  async function ensureTable() {
    if (_tableReady) return;
    await getSql().query(DEDUP_DDL, []);
    _tableReady = true;
  }

  /* Claim atómico: true si somos los primeros en ver este event_id. */
  async function claim(eventId) {
    const res = await getSql().query(
      `INSERT INTO vants_bus_dedup (event_id) VALUES ($1)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [eventId]
    );
    const rows = (res && res.rows) || [];
    return rows.length > 0;
  }

  /* Si el publish falla definitivamente, devolvemos el event_id
   * para que el reintento de FACEIT (o manual) pueda reclamarlo otra vez. */
  async function releaseClaim(eventId) {
    try {
      await getSql().query(`DELETE FROM vants_bus_dedup WHERE event_id = $1`, [eventId]);
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
