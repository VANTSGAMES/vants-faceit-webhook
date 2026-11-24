// VANTSINC - FACEIT webhook backend v2
// ------------------------------------------------------------
// Cambios v2 (por qué): el guardado de la suscripción en FACEIT
// App Studio daba "Subscription update error". Causas probables que
// esta versión elimina todas a la vez:
//   1) El worker respondía 202 a las entregas válidas → ahora 200.
//   2) GET al callback respondía 405 → ahora 200 (sondeo amistoso).
//   3) Solo aceptaba el header EXACTO X-VANTS-FACEIT-SECRET → ahora
//      acepta alias (con guiones, con guiones bajos y el estilo
//      Faceit_webhook_secret) Y también por query string, para que
//      el dueño pueda guardar en el formulario lo que FACEIT acepte.
// Contrato de datos intacto: INSERT INTO faceit_webhook_events
// (event_type, payload) — el procesador de XP (migraciones 08/10 y
// workflow faceit-xp.yml) no cambia.
// ------------------------------------------------------------
import { timingSafeEqual } from "node:crypto";
import { createFaceitBridge } from "./vants-bus-faceit.js";
import { crearSql } from "./_d1.js";

function safeEqual(a, b) {
  if (!a || !b || a.length !== b.length) return false;
  try {
    return timingSafeEqual(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"));
  } catch {
    return false;
  }
}

// Puente FACEIT → VANTS-BUS (publicación de eventos del contrato VANTS).
// Usa Service Binding VANTS_BUS_SERVICE si existe (evita error 1042) o VANTS_BUS_URL.
// Nunca altera la respuesta a FACEIT: publica en background con waitUntil.
function bridgeFor(env) {
  const binding = env.VANTS_BUS_SERVICE;
  return createFaceitBridge({
    busUrl: env.VANTS_BUS_URL || "https://bus.vants.cc",
    secret: env.VANTS_BUS_SECRET,
    db: env.DB,
    fetchImpl: binding ? (u, init) => binding.fetch(new Request(u, init)) : undefined,
  });
}
function busConfigurado(env) {
  return Boolean((env.VANTS_BUS_SERVICE || env.VANTS_BUS_URL) && env.VANTS_BUS_SECRET && env.DB);
}

// Nombres de header aceptados (headers.get ya es case-insensitive)
const HEADER_ALIASES = [
  "x-vants-faceit-secret",
  "x_vants_faceit_secret",
  "faceit_webhook_secret",
  "faceit-webhook-secret",
];

// Nombres de query string aceptados (todos en minúscula)
const QUERY_ALIASES = [
  "secret",
  "faceit_webhook_secret",
  "faceit-webhook-secret",
  "x-vants-faceit-secret",
  "x_vants_faceit_secret",
];

const json = (body, status) =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/healthz" && request.method === "GET") {
      return json(
        {
          ok: true,
          service: "vants-faceit-webhook",
          version: 3,
          bus: { configurado: busConfigurado(env) },
        },
        200,
      );
    }

    if (url.pathname !== "/api/webhooks/faceit") {
      return json({ ok: false, error: "not_found" }, 404);
    }

    // Sondeo de disponibilidad (FACEIT, uptime, navegador) → 200 sin exponer nada
    if (request.method === "GET" || request.method === "HEAD") {
      return json({ ok: true, ready: true }, 200);
    }

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", {
        status: 405,
        headers: { Allow: "GET, HEAD, POST" },
      });
    }

    // Autenticación: header (cualquier alias) o query string (cualquier alias)
    const expected = env.FACEIT_WEBHOOK_SECRET || "";
    let authed = false;
    if (expected) {
      for (const name of HEADER_ALIASES) {
        const v = request.headers.get(name);
        if (safeEqual(v, expected)) { authed = true; break; }
      }
      if (!authed) {
        for (const [k, val] of url.searchParams) {
          if (QUERY_ALIASES.includes(k.toLowerCase()) && safeEqual(val, expected)) {
            authed = true;
            break;
          }
        }
      }
    }
    if (!authed) {
      return json({ ok: false, error: "unauthorized" }, 401);
    }

    let payload;
    try {
      payload = await request.json();
    } catch {
      return json({ ok: false, error: "invalid_json" }, 400);
    }

    // D1 (todo en Cloudflare): el binding `DB` apunta a vants-db, donde vive
    // la tabla faceit_webhook_events que consume el procesador de XP.
    const sql = crearSql(env.DB);

    const eventType =
      typeof payload?.event === "string"
        ? payload.event
        : typeof payload?.type === "string"
          ? payload.type
          : null;

    try {
      await sql.query(
        `INSERT INTO faceit_webhook_events (event_type, payload)
         VALUES ($1, $2)`,
        [eventType, JSON.stringify(payload)]
      );

      // Publicar al bus DESPUÉS de guardar (contrato intacto: si el bus
      // falla, FACEIT ya recibió 200 y el pipeline de XP no cambia).
      if (busConfigurado(env) && ctx) {
        ctx.waitUntil(
          bridgeFor(env)
            .handle(payload)
            .then((r) => {
              if (!r?.ok) console.warn("vants-bus: publicación diferida", JSON.stringify(r));
            })
            .catch((e) => console.error("vants-bus bridge:", e?.message)),
        );
      }

      return json({ ok: true, stored: true }, 200);
    } catch (error) {
      console.error("Failed to store FACEIT event");
      return json({ ok: false, error: "internal" }, 500);
    }
  }
};
