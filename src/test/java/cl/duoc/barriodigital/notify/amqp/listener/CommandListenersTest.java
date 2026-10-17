package cl.duoc.barriodigital.notify.amqp.listener;

import cl.duoc.barriodigital.notify.amqp.AckManual;
import cl.duoc.barriodigital.notify.amqp.CommandEnvelope;
import cl.duoc.barriodigital.notify.amqp.ProcessedEventsGuard;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Cada listener por dominio: comando valido -> ACK, payload incompleto -> NACK a la DLQ,
 * duplicado -> ACK. Sin RabbitMQ: llama a los metodos @RabbitListener con un Channel mockeado.
 * La topologia y el ACK real solo se prueban contra un broker.
 */
class CommandListenersTest {

    private final AckManual ack = new AckManual(new ProcessedEventsGuard(), 3);
    private final Channel channel = mock(Channel.class);

    private CommandEnvelope cmd(String tipo, String eventId, Map<String, Object> data) {
        return new CommandEnvelope(tipo, eventId, Instant.now(), "trace-1", "corr-1", data);
    }

    @Test
    void email_valido_hace_ack_y_duplicado_tambien() throws IOException {
        var listener = new EmailCommandListener(ack);
        var c = cmd("email.send", "evt-email", Map.of("tramiteId", 10L, "tipoNotificacion", "ADMITIDO"));

        listener.email(c, channel, 1L, null, "q.cmd.email");
        listener.email(c, channel, 2L, null, "q.cmd.email");

        verify(channel).basicAck(1L, false);
        verify(channel).basicAck(2L, false);
    }

    @Test
    void email_sin_tipoNotificacion_va_a_la_dlq() throws IOException {
        new EmailCommandListener(ack).email(
                cmd("email.send", "evt-1", Map.of("tramiteId", 10L)), channel, 3L, null, "q.cmd.email");

        verify(channel).basicNack(3L, false, false);
    }

    @Test
    void crew_valido_hace_ack_y_sin_tramiteId_va_a_la_dlq() throws IOException {
        var listener = new CrewCommandListener(ack);

        listener.crew(cmd("crew.ticket", "evt-crew", Map.of("tramiteId", 11L, "direccion", "Calle 1")),
                channel, 4L, null, "q.cmd.crew");
        listener.crew(cmd("crew.ticket", "evt-crew2", Map.of("direccion", "Calle 1")),
                channel, 5L, null, "q.cmd.crew");

        verify(channel).basicAck(4L, false);
        verify(channel).basicNack(5L, false, false);
    }

    @Test
    void certificate_valido_hace_ack_y_sin_motivo_va_a_la_dlq() throws IOException {
        var listener = new CertificateCommandListener(ack);

        listener.certificate(cmd("certificate.gen", "evt-cert", Map.of("tramiteId", 12L, "motivo", "RESUELTO")),
                channel, 6L, null, "q.cmd.certificate");
        listener.certificate(cmd("certificate.gen", "evt-cert2", Map.of("tramiteId", 12L)),
                channel, 7L, null, "q.cmd.certificate");

        verify(channel).basicAck(6L, false);
        verify(channel).basicNack(7L, false, false);
    }
}
