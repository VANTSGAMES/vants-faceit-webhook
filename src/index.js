
import { neon } from "@neondatabase/serverless";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/healthz" && request.method === "GET") {
      return new Response("ok", { status: 200 });
    }

    if (url.pathname !== "/api/webhooks/faceit") {
      return new Response("Not found", { status: 404 });
    }

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", {
        status: 405,
        headers: { Allow: "POST" }
      });
    }

    const suppliedSecret = request.headers.get(
      "X-VANTS-FACEIT-SECRET"
    );

    if (
      !env.FACEIT_WEBHOOK_SECRET ||
      suppliedSecret !== env.FACEIT_WEBHOOK_SECRET
    ) {
      return new Response("Unauthorized", { status: 401 });
    }

    let payload;

    try {
      payload = await request.json();
    } catch {
      return new Response("Invalid JSON", { status: 400 });
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

      return new Response("accepted", { status: 202 });
    } catch (error) {
      console.error("Failed to store FACEIT event");
      return new Response("Internal Server Error", { status: 500 });
    }
  }
};
