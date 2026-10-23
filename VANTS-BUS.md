# VANTS-BUS · Puente FACEIT → bus de eventos

Este Worker ya está cableado al **bus de eventos central de VANTS**
(`https://vants-bus.vants.workers.dev`). Cada webhook verificado de FACEIT
se guarda en `faceit_webhook_events` (contrato intacto, el pipeline de XP
no cambia) y **además** se publica en el bus para que la web, el launcher
y los agentes reaccionen en vivo.

## Qué mapea

| Evento FACEIT | Evento del bus | Reacción esperada |
|---|---|---|
| `match_status_finished` | `match.finished` | web refresca ranking en vivo, agentes generan resumen, launcher avisa con toast |
| `match_object_created` | `match.scheduled` | agenda de la arena se actualiza sola |
| cancelaciones | `match.canceled` | se retira de la agenda |

Eventos que no mapean a nada interesante se ignoran sin coste.

## Garantías

- **La respuesta a FACEIT nunca cambia**: primero se guarda en Neon
  (`faceit_webhook_events`) y se responde `200`; la publicación al bus va
  en background (`ctx.waitUntil`). Si el bus está caído, FACEIT ya recibió
  su 200 y tu pipeline de XP sigue igual.
- **Deduplicación por `event_id`**: FACEIT reintenta webhooks; la tabla
  `vants_bus_dedup` (se crea sola en Neon) garantiza que un mismo webhook
  se publique **una sola vez** al bus.
- **Reintentos con backoff** hacia el bus (3 intentos) dentro del ciclo
  de vida del request.

## Configuración (secrets del Worker, ya creados)

```
VANTS_BUS_URL     = https://vants-bus.vants.workers.dev
VANTS_BUS_SECRET  = (secreto HMAC de este proyecto — NO está en el repo)
```

Para cambiarlos: `npx wrangler secret put VANTS_BUS_URL` / `VANTS_BUS_SECRET`.

## Cómo consumir los eventos (otros proyectos)

```
GET https://vants-bus.vants.workers.dev/events?since=<cursor>&types=match.finished
X-VANTS-Project: <tu-proyecto>
X-VANTS-Timestamp: <unix seconds>
X-VANTS-Signature: hex(hmac_sha256(SECRETO, `${ts}.`))
```

Clientes listos: `clients/js/vants-bus-client.js` (JS/Workers/Electron) y
`clients/python/vants_bus_client.py` (Python stdlib) en el esqueleto del bus.
