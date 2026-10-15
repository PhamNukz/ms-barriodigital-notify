package cl.duoc.barriodigital.notify.amqp.listener;

import cl.duoc.barriodigital.notify.amqp.CommandEnvelope;
import cl.duoc.barriodigital.notify.amqp.ProcessedEventsGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Consume el comando email.send de la seccion 8 del caso. No hay proveedor real: se simula
 * con log estructurado. Una excepcion aqui hace NACK y RabbitMQ enruta el mensaje
 * a su DLQ via x-dead-letter-exchange (declarado en RabbitTopologyConfig).
 */
@Component
public class EmailCommandListener {

    private static final Logger log = LoggerFactory.getLogger(EmailCommandListener.class);

    private final ProcessedEventsGuard guard;

    public EmailCommandListener(ProcessedEventsGuard guard) {
        this.guard = guard;
    }

    @RabbitListener(queues = "${barriodigital.rabbitmq.colas.email}")
    public void email(CommandEnvelope cmd) {
        if (!guard.esNuevo(cmd.eventId())) {
            log.info("email.send duplicado, se ignora (eventId={})", cmd.eventId());
            return;
        }
        log.info("[EMAIL/PUSH] tramite={} tipo={} -> vecino notificado",
                cmd.data().get("tramiteId"), cmd.data().get("tipoNotificacion"));
    }
}
