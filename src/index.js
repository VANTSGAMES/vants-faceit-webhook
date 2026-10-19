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
import { neon } from "@neondatabase/serverless";

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
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/healthz" && request.method === "GET") {
      return json({ ok: true, service: "vants-faceit-webhook", version: 2 }, 200);
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
        if (v && v === expected) { authed = true; break; }
      }
      if (!authed) {
        for (const [k, val] of url.searchParams) {
          if (QUERY_ALIASES.includes(k.toLowerCase()) && val === expected) {
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

    try {
      const sql = neon(env.DATABASE_URL);

      const eventType =
        typeof payload?.event === "string"
          ? payload.event
          : typeof payload?.type === "string"
            ? payload.type
            : null;

      await sql`
        INSERT INTO faceit_webhook_events (event_type, payload)
        VALUES (
          ${eventType},
          ${JSON.stringify(payload)}::jsonb
        )
      `;

      return json({ ok: true, stored: true }, 200);
    } catch (error) {
      console.error("Failed to store FACEIT event");
      return json({ ok: false, error: "internal" }, 500);
    }
  }
};
