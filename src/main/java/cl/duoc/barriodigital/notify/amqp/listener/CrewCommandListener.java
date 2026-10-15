package cl.duoc.barriodigital.notify.amqp.listener;

import cl.duoc.barriodigital.notify.amqp.CommandEnvelope;
import cl.duoc.barriodigital.notify.amqp.ProcessedEventsGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Consume el comando crew.ticket de la seccion 8 del caso. No hay proveedor real: se simula
 * con log estructurado. Una excepcion aqui hace NACK y RabbitMQ enruta el mensaje
 * a su DLQ via x-dead-letter-exchange (declarado en RabbitTopologyConfig).
 */
@Component
public class CrewCommandListener {

    private static final Logger log = LoggerFactory.getLogger(CrewCommandListener.class);

    private final ProcessedEventsGuard guard;

    public CrewCommandListener(ProcessedEventsGuard guard) {
        this.guard = guard;
    }

    @RabbitListener(queues = "${barriodigital.rabbitmq.colas.crew}")
    public void crew(CommandEnvelope cmd) {
        if (!guard.esNuevo(cmd.eventId())) {
            log.info("crew.ticket duplicado, se ignora (eventId={})", cmd.eventId());
            return;
        }
        log.info("[CUADRILLA] ticket de visita creado para tramite={} direccion={}",
                cmd.data().get("tramiteId"), cmd.data().get("direccion"));
    }
}
