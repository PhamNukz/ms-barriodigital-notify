package cl.duoc.barriodigital.notify.amqp.listener;

import cl.duoc.barriodigital.notify.amqp.CommandEnvelope;
import cl.duoc.barriodigital.notify.amqp.ProcessedEventsGuard;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * No levanta RabbitMQ: llama a los metodos @RabbitListener directamente para
 * verificar la logica de negocio (idempotencia) de cada listener por dominio. La
 * topologia y el ACK/NACK real solo se pueden probar contra un broker de verdad.
 */
class CommandListenersTest {

    private CommandEnvelope envelope(String tipo, String eventId, Map<String, Object> data) {
        return new CommandEnvelope(tipo, eventId, Instant.now(), "trace-1", "corr-1", data);
    }

    @Test
    void email_procesa_un_evento_nuevo_y_descarta_el_duplicado() {
        var guard = new ProcessedEventsGuard();
        var listener = new EmailCommandListener(guard);
        var cmd = envelope("email.send", "evt-email", Map.of("tramiteId", 10L, "tipoNotificacion", "ADMITIDO"));

        listener.email(cmd);
        assertThat(guard.esNuevo("evt-email")).isFalse(); // ya quedo marcado
        listener.email(cmd); // duplicado: no lanza error
    }

    @Test
    void crew_procesa_un_evento_nuevo_y_descarta_el_duplicado() {
        var guard = new ProcessedEventsGuard();
        var listener = new CrewCommandListener(guard);
        var cmd = envelope("crew.ticket", "evt-crew", Map.of("tramiteId", 11L, "direccion", "Calle 1"));

        listener.crew(cmd);
        assertThat(guard.esNuevo("evt-crew")).isFalse();
        listener.crew(cmd);
    }

    @Test
    void certificate_procesa_un_evento_nuevo_y_descarta_el_duplicado() {
        var guard = new ProcessedEventsGuard();
        var listener = new CertificateCommandListener(guard);
        var cmd = envelope("certificate.gen", "evt-cert", Map.of("tramiteId", 12L, "motivo", "RESUELTO"));

        listener.certificate(cmd);
        assertThat(guard.esNuevo("evt-cert")).isFalse();
        listener.certificate(cmd);
    }
}
