package cl.duoc.barriodigital.notify.amqp;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Decision de ACK de los tres listeners, en un solo lugar para que sea auditable:
 * <ul>
 *   <li>exito o evento duplicado -> basicAck (un duplicado ya esta procesado, no fallido)</li>
 *   <li>{@link ComandoNoRecuperableException} -> basicNack(requeue=false): DLQ directa</li>
 *   <li>cualquier otra excepcion (recuperable) -> basicNack(requeue=true) hasta {@code max-intentos}
 *       entregas; agotadas, basicNack(requeue=false): DLQ</li>
 * </ul>
 * Cada NACK deja un log.error con eventId, cola y motivo. Las entregas se cuentan con el
 * header x-delivery-count que RabbitMQ pone en las colas quorum.
 */
@Component
public class AckManual {

    private static final Logger log = LoggerFactory.getLogger(AckManual.class);

    private final ProcessedEventsGuard guard;
    private final int maxIntentos;

    public AckManual(ProcessedEventsGuard guard,
                     @Value("${barriodigital.rabbitmq.max-intentos:3}") int maxIntentos) {
        this.guard = guard;
        this.maxIntentos = maxIntentos;
    }

    /** Falla como no recuperable si falta algun campo obligatorio del payload. */
    public static void exigir(CommandEnvelope cmd, String... campos) {
        for (String campo : campos) {
            if (cmd.data() == null || cmd.data().get(campo) == null) {
                throw new ComandoNoRecuperableException("payload sin '" + campo + "'");
            }
        }
    }

    public void procesar(CommandEnvelope cmd, Channel channel, long tag, Number entregas,
                         String cola, Runnable accion) throws IOException {
        String eventId = cmd.eventId();
        if (eventId == null) {
            log.error("NACK->DLQ cola={} eventId=null motivo=comando sin eventId", cola);
            channel.basicNack(tag, false, false);
            return;
        }
        if (!guard.esNuevo(eventId)) {
            log.info("{} duplicado, se ignora (eventId={})", cmd.type(), eventId);
            channel.basicAck(tag, false);
            return;
        }
        try {
            accion.run();
        } catch (ComandoNoRecuperableException e) {
            log.error("NACK->DLQ cola={} eventId={} motivo={}", cola, eventId, e.getMessage());
            channel.basicNack(tag, false, false);
            return;
        } catch (RuntimeException e) {
            int intento = (entregas == null ? 0 : entregas.intValue()) + 1;
            if (intento >= maxIntentos) {
                log.error("NACK->DLQ cola={} eventId={} motivo=reintentos agotados ({}/{}): {}",
                        cola, eventId, intento, maxIntentos, e.toString());
                channel.basicNack(tag, false, false);
            } else {
                guard.liberar(eventId); // el reintento no debe tomarse por duplicado
                log.error("NACK->requeue cola={} eventId={} motivo=error recuperable, intento {}/{}: {}",
                        cola, eventId, intento, maxIntentos, e.toString());
                channel.basicNack(tag, false, true);
            }
            return;
        }
        channel.basicAck(tag, false);
    }
}
