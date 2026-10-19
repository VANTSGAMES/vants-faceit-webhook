# vants-faceit-webhook
Backend de VANTSINC para recibir webhooks de FACEIT (App Studio) e integrarlos con Neon PostgreSQL.

- POST /api/webhooks/faceit — valida el secreto por header (X-VANTS-FACEIT-SECRET o alias) o query string, guarda el evento en faceit_webhook_events y responde 200.
- GET /api/webhooks/faceit y GET /healthz — sondeos de disponibilidad (200).

Secrets del Worker: FACEIT_WEBHOOK_SECRET y DATABASE_URL (no viven en el repo).
