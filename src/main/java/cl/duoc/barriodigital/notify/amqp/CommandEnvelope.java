package cl.duoc.barriodigital.notify.amqp;

import java.time.Instant;
import java.util.Map;

/**
 * Envelope comun para todos los comandos de BarrioDigital (email, cuadrilla,
 * certificado). Lo publica ms-barriodigital-requests y lo consume este
 * servicio; ambos comparten esta misma forma (duplicada, no una libreria
 * publicada, por la misma razon que cl.duoc.barriodigital.security).
 */
public record CommandEnvelope(
        String type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        Map<String, Object> data) {
}
