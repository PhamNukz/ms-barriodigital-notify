package cl.duoc.barriodigital.notify.amqp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Consume los 3 comandos de la seccion 8 del caso. No hay proveedor real de
 * email/push/PDF en este ejercicio: se simula con log estructurado. Una
 * excepcion aqui hace NACK y RabbitMQ enruta el mensaje a su DLQ via
 * x-dead-letter-exchange (declarado en RabbitTopologyConfig) -- no hace falta
 * reintentar a mano.
 */
@Component
public class CommandListener {

    private static final Logger log = LoggerFactory.getLogger(CommandListener.class);

    private final ProcessedEventsGuard guard;

    public CommandListener(ProcessedEventsGuard guard) {
        this.guard = guard;
    }

    @RabbitListener(queues = "q.cmd.email")
    public void email(CommandEnvelope cmd) {
        if (!guard.esNuevo(cmd.eventId())) {
            log.info("email.send duplicado, se ignora (eventId={})", cmd.eventId());
            return;
        }
        log.info("[EMAIL/PUSH] tramite={} tipo={} -> vecino notificado",
                cmd.data().get("tramiteId"), cmd.data().get("tipoNotificacion"));
    }

    @RabbitListener(queues = "q.cmd.crew")
    public void crew(CommandEnvelope cmd) {
        if (!guard.esNuevo(cmd.eventId())) {
            log.info("crew.ticket duplicado, se ignora (eventId={})", cmd.eventId());
            return;
        }
        log.info("[CUADRILLA] ticket de visita creado para tramite={} direccion={}",
                cmd.data().get("tramiteId"), cmd.data().get("direccion"));
    }

    @RabbitListener(queues = "q.cmd.certificate")
    public void certificate(CommandEnvelope cmd) {
        if (!guard.esNuevo(cmd.eventId())) {
            log.info("certificate.gen duplicado, se ignora (eventId={})", cmd.eventId());
            return;
        }
        log.info("[CERTIFICADO] PDF generado para tramite={} motivo={}",
                cmd.data().get("tramiteId"), cmd.data().get("motivo"));
    }
}
