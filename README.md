# vants-faceit-webhook — Receptor de Webhooks FACEIT (`vants.cc/api/webhooks/faceit`)

Cloudflare Worker oficial de **VANTS GAMES** para recibir eventos en tiempo real desde **FACEIT App Studio**, guardarlos en **Cloudflare D1 (`vants-db`)** y publicarlos de forma deduplicada en **`vants-bus`** (`https://bus.vants.cc`).

---

## Rutas

| Ruta | Método | Descripción |
| --- | --- | --- |
| `/api/webhooks/faceit` | `POST` | Valida `FACEIT_WEBHOOK_SECRET` con comparación **timing-safe** (`node:crypto`), inserta el evento en `faceit_webhook_events` (D1) y publica al `vants-bus` en segundo plano (`ctx.waitUntil`). |
| `/api/webhooks/faceit` | `GET` / `HEAD` | Sondeo de disponibilidad (`200 OK`). |
| `/healthz` | `GET` | Estado del servicio y configuración del bus. |

---

## Bindings y configuración (`wrangler.jsonc`)

- **D1:** `DB` → `vants-db` (`11e5d82c-307b-44ba-9682-501d8df593d3`)
- **Service Binding:** `VANTS_BUS_SERVICE` → `vants-bus` *(transporte interno sin error 1042; fallback `https://bus.vants.cc`)*
- **Runtime:** `compatibility_flags = ["nodejs_compat"]`
- **Secretos (`wrangler secret put`):**
  - `FACEIT_WEBHOOK_SECRET`
  - `VANTS_BUS_SECRET`

---

## Flujo en el ecosistema VANTS

1. FACEIT envía eventos (`match_status_ready`, `match_status_finished`, `match_status_cancelled`, etc.) a `https://vants.cc/api/webhooks/faceit`.
2. El Worker persiste en `faceit_webhook_events` (leído por `vants-integraciones` en `https://int.vants.cc` para estado *"en partida"*, notificaciones y resultados).
3. En paralelo, [`src/vants-bus-faceit.js`](./src/vants-bus-faceit.js) deduplica en `vants_bus_dedup` y publica el evento normalizado al **`vants-bus`** (`https://bus.vants.cc`), consumido por **`vantsbot`** (`https://bot.vants.cc`) y los agentes de VANTS.

---

## Verificación

```bash
node --check src/index.js
node --check src/vants-bus-faceit.js
node --check monitor/monitor.js
```
