package cl.duoc.barriodigital.notify.amqp.listener;

import cl.duoc.barriodigital.notify.amqp.AckManual;
import cl.duoc.barriodigital.notify.amqp.CommandEnvelope;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consume el comando crew.ticket. No hay proveedor real: se simula con log estructurado.
 * <p>Decision de error (ver {@link AckManual}): payload sin 'tramiteId' lanza
 * {@code ComandoNoRecuperableException} -> NACK sin requeue, directo a la DLQ; cualquier otra
 * excepcion (proveedor caido, timeout) es recuperable -> NACK con requeue hasta agotar los
 * reintentos y luego a la DLQ; evento duplicado o exito -> ACK.
 */
@Component
public class CrewCommandListener {

    private static final Logger log = LoggerFactory.getLogger(CrewCommandListener.class);

    private final AckManual ack;

    public CrewCommandListener(AckManual ack) {
        this.ack = ack;
    }

    @RabbitListener(queues = "${barriodigital.rabbitmq.colas.crew}")
    public void crew(CommandEnvelope cmd, Channel channel,
                     @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                     @Header(name = "x-delivery-count", required = false) Number entregas,
                     @Header(AmqpHeaders.CONSUMER_QUEUE) String cola) throws IOException {
        ack.procesar(cmd, channel, tag, entregas, cola, () -> {
            AckManual.exigir(cmd, "tramiteId");
            log.info("[CUADRILLA] ticket de visita creado para tramite={} direccion={}",
                    cmd.data().get("tramiteId"), cmd.data().get("direccion"));
        });
    }
}
