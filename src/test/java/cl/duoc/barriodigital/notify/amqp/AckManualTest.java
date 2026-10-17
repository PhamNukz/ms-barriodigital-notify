package cl.duoc.barriodigital.notify.amqp;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/** Una prueba por rama de decision de AckManual, con Channel mockeado (sin broker). */
class AckManualTest {

    private static final long TAG = 7L;
    private static final String COLA = "q.cmd.email";

    private final ProcessedEventsGuard guard = new ProcessedEventsGuard();
    private final AckManual ack = new AckManual(guard, 3);
    private Channel channel;

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
    }

    private CommandEnvelope cmd(String eventId) {
        return new CommandEnvelope("email.send", eventId, Instant.now(), "t", "c", Map.of("tramiteId", 1L));
    }

    @Test
    void exito_hace_ack() throws IOException {
        ack.procesar(cmd("e1"), channel, TAG, null, COLA, () -> {});

        verify(channel).basicAck(TAG, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void duplicado_hace_ack_sin_ejecutar() throws IOException {
        ack.procesar(cmd("e2"), channel, TAG, null, COLA, () -> {});
        Runnable noDebeEjecutarse = () -> { throw new AssertionError("se reproceso un duplicado"); };

        ack.procesar(cmd("e2"), channel, TAG, null, COLA, noDebeEjecutarse);

        verify(channel, times(2)).basicAck(TAG, false);
    }

    @Test
    void error_recuperable_hace_nack_con_requeue_y_el_reintento_no_es_duplicado() throws IOException {
        ack.procesar(cmd("e3"), channel, TAG, null, COLA, () -> { throw new IllegalStateException("timeout"); });

        verify(channel).basicNack(TAG, false, true);
        assertThat(guard.esNuevo("e3")).isTrue(); // liberado: el reintento se procesara
    }

    @Test
    void error_recuperable_con_reintentos_agotados_va_a_la_dlq() throws IOException {
        // x-delivery-count=2 -> esta es la 3a entrega = max-intentos
        ack.procesar(cmd("e4"), channel, TAG, 2L, COLA, () -> { throw new IllegalStateException("timeout"); });

        verify(channel).basicNack(TAG, false, false);
    }

    @Test
    void error_no_recuperable_va_directo_a_la_dlq_sin_gastar_reintentos() throws IOException {
        ack.procesar(cmd("e5"), channel, TAG, null, COLA,
                () -> { throw new ComandoNoRecuperableException("payload sin 'tramiteId'"); });

        verify(channel).basicNack(TAG, false, false);
        verify(channel, never()).basicNack(TAG, false, true);
    }

    @Test
    void comando_sin_eventId_va_a_la_dlq() throws IOException {
        ack.procesar(cmd(null), channel, TAG, null, COLA, () -> {});

        verify(channel).basicNack(TAG, false, false);
    }
}
