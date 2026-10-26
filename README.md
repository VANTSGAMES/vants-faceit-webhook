# vants-faceit-webhook
Backend de VANTSINC para recibir webhooks de FACEIT (App Studio) e integrarlos con Neon PostgreSQL.

- POST /api/webhooks/faceit — valida el secreto por header (X-VANTS-FACEIT-SECRET o alias) o query string, guarda el evento en faceit_webhook_events y responde 200.
- GET /api/webhooks/faceit y GET /healthz — sondeos de disponibilidad (200).

Secrets del Worker: FACEIT_WEBHOOK_SECRET y DATABASE_URL (no viven en el repo).

## Agentes: el cuartel es flask-Agents

**Anuncio del ecosistema:** el cuartel de los agentes es
[`VANTSNET/flask-Agents`](https://github.com/VANTSNET/flask-Agents): ahí vive
el código de todos los agentes, los viejos y los nuevos, y su web es
https://vants-ai.vercel.app/. Este Worker guarda los eventos en
`faceit_webhook_events` y los publica en VANTS-BUS (ver `VANTS-BUS.md`); el
consumidor de los agentes viejos, que vive en el cuartel (`vants_bus/`), los
escucha y reacciona con `post.generated` y alertas.
