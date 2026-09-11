package cl.duoc.barriodigital.notify.amqp;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * No levanta RabbitMQ: llama a los metodos @RabbitListener directamente para
 * verificar la logica de negocio (idempotencia). La topologia y el ACK/NACK
 * real solo se pueden probar contra un broker de verdad.
 */
class CommandListenerTest {

    private CommandEnvelope envelope(String eventId, Map<String, Object> data) {
        return new CommandEnvelope("email.send", eventId, Instant.now(), "trace-1", "corr-1", data);
    }

    @Test
    void procesa_un_evento_nuevo_una_sola_vez() {
        ProcessedEventsGuard guard = new ProcessedEventsGuard();
        CommandListener listener = new CommandListener(guard);

        listener.email(envelope("evt-1", Map.of("tramiteId", 10L, "tipoNotificacion", "ADMITIDO")));

        assertThat(guard.esNuevo("evt-1")).isFalse(); // ya quedo marcado
    }

    @Test
    void evento_duplicado_no_se_reprocesa() {
        ProcessedEventsGuard guard = new ProcessedEventsGuard();
        CommandListener listener = new CommandListener(guard);

        var cmd = envelope("evt-dup", Map.of("tramiteId", 1L, "tipoNotificacion", "RESUELTO"));
        listener.email(cmd);
        listener.email(cmd); // segunda vez: debe detectarse como duplicado, sin lanzar error

        assertThat(guard.esNuevo("evt-dup")).isFalse();
    }
}
