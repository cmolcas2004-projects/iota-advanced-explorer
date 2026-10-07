# IOTA Advanced Explorer

Sistema de observabilidad y trazabilidad sobre una Tangle privada de IOTA (versión Stardust).
Proyecto para VelesHack (UPV), desafío O-CEI.

La Tangle es inmutable pero opaca para un operador: BlockIds en hexadecimal y payloads codificados.
Este proyecto guarda cada mensaje en una base de datos paralela, lo verifica contra Hornet y lo
muestra en información legible, con alertas cuando algo falla.

## Arquitectura

    Aplicación -> Messages API (modificada) -> Hornet + Coordinator (Tangle)
                         |
                         v
                  Traceability API (FastAPI) <-> BD SQLite
                         |
                         v
           Explorer UI / Incident Explorer (comprueba contra Hornet)

1. La Messages API envía el bloque a la Tangle y, si Hornet lo acepta, reenvía el mensaje
   (BlockId, tag, contenido y fecha de envío) a la Traceability API.
2. La Traceability API lo guarda y lo verifica contra la API de Hornet.
3. Si el mensaje aún no está confirmado, queda en estado pendiente y se reintenta cada 5 s (máximo 10 intentos).

## Qué se verifica

Un mensaje es valid solo si se cumplen las cuatro condiciones:

- Es solid (GET /api/core/v2/blocks/{id}/metadata, campo isSolid).
- Está referenciado por un milestone (referencedByMilestoneIndex).
- ledgerInclusionState no es conflicting.
- El tag y los datos del bloque (GET /api/core/v2/blocks/{id}, hex decodificado a JSON)
  son idénticos a lo guardado.

Estados: valid, pending o invalid (si el contenido no coincide o se agotan los reintentos).

## Requisitos

- Linux (en Windows, WSL2 con Ubuntu), Docker y Docker Compose.
- La Tangle privada de https://github.com/eclipse-aerios/iota-tangle en marcha (red Docker iota-net).
- La imagen iota_api construida desde https://github.com/eclipse-aerios/iota-messages-api,
  usando el send_data.py de la carpeta messages-api/ de este repositorio.

## Puesta en marcha

1. Arranca la Tangle (desde su carpeta docker/main):

       docker compose -f hornet-main.yaml up -d

   Dashboard en http://localhost:31011 y API de Hornet en http://localhost:14265.

2. Sustituye send_data.py en iota-messages-api por el de messages-api/ y reconstruye la imagen:

       docker build -t iota_api .

3. Arranca la Traceability API y la Messages API (desde este repositorio):

       docker compose up -d --build

## Enviar un mensaje de prueba

El parámetro node es el nombre del contenedor de Hornet dentro de la red iota-net.

    curl -X POST "http://localhost:5555/upload?node=iota-hornet" \
      -H "Content-Type: application/json" \
      -d '{"tag": "sensor.demo", "message": {"sensor_id": "s-1", "valor": 25}}'

## Interfaces web

- http://localhost:8000/ : Explorer. Tabla de mensajes con semáforo, filtros por tag, estado y fecha.
- http://localhost:8000/incident-explorer : Incident Explorer. Flujos agrupados por
  correlation_id, sensor_id o sensor, timeline verificado y alerta en vivo.
- http://localhost:8000/docs : documentación Swagger.

## REST API

- POST /messages : recibe un mensaje desde la Messages API.
- GET /messages : lista con filtros tag, status, date_from, date_to y limit.
- GET /messages/{block_id} : consulta por BlockId.
- POST /messages/{block_id}/verify : fuerza la verificación.
- GET /flows y GET /flows/{flow_id} : flujos y su timeline.
- GET /incidents : eventos críticos (tags alert.* y critical.*) y mensajes inválidos.

## Estructura

- main.py : Traceability API (FastAPI + SQLite).
- static/ : Explorer e Incident Explorer.
- messages-api/send_data.py : Messages API modificada para reenviar cada mensaje.
- Dockerfile y docker-compose.yml : despliegue.

## Siguientes pasos

- MQTT para notificaciones en tiempo real desde la Messages API.
- Firma de mensajes para autenticar al emisor.
- PostgreSQL en lugar de SQLite.

## Requisitos previos

La imagen `messages-api` parte de `iota_api:base`. Hay que construirla antes, desde el repo `iota-messages-api`:

    docker build -t iota_api:base .

## Seguridad

El broker Mosquitto usa `allow_anonymous true`. Es solo para desarrollo en local, no para producción.
