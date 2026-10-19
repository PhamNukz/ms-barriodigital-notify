# ms-barriodigital-notify

Microservicio de notificaciones: consume eventos de trámites desde **RabbitMQ**
(publicados por `ms-barriodigital-requests` tras cada cambio de estado) y simula
el envío de la notificación al vecino. No expone HTTP — es solo un consumidor de
cola, no necesita validar JWT.

Si el listener lanza una excepción, RabbitMQ reintenta hasta 3 veces y después
enruta el mensaje a la dead-letter queue.

## Cómo correr local

Requiere Java 17, Maven y un RabbitMQ accesible.

```bash
./mvnw spring-boot:run
```

No expone puerto HTTP propio; su trabajo es solo consumir de la cola. Para probar
el flujo completo, correr también `ms-barriodigital-requests` y crear/cambiar de
estado un trámite — el mensaje debería aparecer procesado en los logs de este
servicio.

Tests: `./mvnw verify`.

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `localhost` / `5672` | Broker RabbitMQ |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | `guest` / `guest` | Credenciales RabbitMQ |

## Docker

```bash
docker build -t ms-barriodigital-notify .
docker run --env-file .env ms-barriodigital-notify
```

Imagen publicada automáticamente en cada push a `main`:
`ghcr.io/phamnukz/ms-barriodigital-notify:latest`.

## Matriz de funcionalidades

El estado de cumplimiento de la Prueba 2 (EP3/EP4), con la matriz de funcionalidades y la evidencia de cada
componente, está en [Cumplimiento-Prueba2.md](https://github.com/PhamNukz/barriodigital-docs/blob/main/Cumplimiento-Prueba2.md)
(repo `barriodigital-docs`).
