package cl.duoc.barriodigital.notify.amqp.listener;

import cl.duoc.barriodigital.notify.amqp.AckManual;
import cl.duoc.barriodigital.notify.amqp.CommandEnvelope;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Consume el comando email.send. Siempre deja el correo en un log estructurado; si hay SMTP
 * configurado (MAIL_HOST) y el comando trae 'destinatario', ademas lo envia de verdad.
 * <p>Decision de error (ver {@link AckManual}): payload sin 'tramiteId' o 'tipoNotificacion' lanza
 * {@code ComandoNoRecuperableException} -> NACK sin requeue, directo a la DLQ; cualquier otra
 * excepcion (SMTP caido, timeout) es recuperable -> NACK con requeue hasta agotar los
 * reintentos y luego a la DLQ; evento duplicado o exito -> ACK.
 */
@Component
public class EmailCommandListener {

    private static final Logger log = LoggerFactory.getLogger(EmailCommandListener.class);

    private final AckManual ack;
    /** null sin MAIL_HOST: el correo queda solo en el log. */
    private final JavaMailSender smtp;
    private final String remitente;

    @Autowired
    public EmailCommandListener(AckManual ack, ObjectProvider<JavaMailSender> smtp,
                                @Value("${spring.mail.host:}") String host,
                                @Value("${barriodigital.mail.from:no-reply@barriodigital.cl}") String remitente) {
        this.ack = ack;
        this.smtp = host.isBlank() ? null : smtp.getIfAvailable();
        this.remitente = remitente;
    }

    /** Sin SMTP: solo log (tests y cualquier ambiente sin MAIL_HOST). */
    public EmailCommandListener(AckManual ack) {
        this.ack = ack;
        this.smtp = null;
        this.remitente = null;
    }

    @RabbitListener(queues = "${barriodigital.rabbitmq.colas.email}")
    public void email(CommandEnvelope cmd, Channel channel,
                      @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                      @Header(name = "x-delivery-count", required = false) Number entregas,
                      @Header(AmqpHeaders.CONSUMER_QUEUE) String cola) throws IOException {
        ack.procesar(cmd, channel, tag, entregas, cola, () -> {
            AckManual.exigir(cmd, "tramiteId", "tipoNotificacion");
            Map<String, Object> d = cmd.data();
            // destinatario, cc, direccion, visita y motivo son opcionales: los comandos anteriores no los traen.
            boolean enviado = enviarSiHaySmtp(d);
            log.info("[EMAIL/PUSH] tramite={} tipo={} destinatario={} cc={} direccion={} visita={} motivo={} smtp={} -> vecino notificado",
                    d.get("tramiteId"), d.get("tipoNotificacion"), d.get("destinatario"), d.get("cc"), d.get("direccion"),
                    d.get("visita"), d.get("motivo"), enviado ? "enviado" : "no");
        });
    }

    private boolean enviarSiHaySmtp(Map<String, Object> d) {
        Object para = d.get("destinatario");
        if (smtp == null || para == null || para.toString().isBlank()) return false;
        SimpleMailMessage m = new SimpleMailMessage();
        m.setFrom(remitente);
        m.setTo(para.toString());
        // Familiares que el vecino agrego al tramite (maximo 3, los valida requests).
        if (d.get("cc") instanceof List<?> cc && !cc.isEmpty()) {
            m.setCc(cc.stream().map(String::valueOf).toArray(String[]::new));
        }
        m.setSubject("BarrioDigital: tu tramite #" + d.get("tramiteId"));
        m.setText(cuerpo(d));
        smtp.send(m);
        return true;
    }

    static String cuerpo(Map<String, Object> d) {
        String tipo = String.valueOf(d.get("tipoNotificacion"));
        String novedad = switch (tipo) {
            case "ADMITIDO" -> "fue admitido.";
            case "VISITA_PROPUESTA" -> "tiene una visita propuesta. Confirmala o pide otro horario en BarrioDigital.";
            case "VISITA_AGENDADA" -> "tiene la visita en terreno agendada.";
            case "VISITA_CONFIRMADA" -> "tiene la visita confirmada.";
            case "EN_CAMINO" -> "tiene a la cuadrilla en camino.";
            case "RESUELTO" -> "fue resuelto.";
            case "RECHAZADO" -> "fue rechazado.";
            default -> "tiene una novedad (" + tipo + ").";
        };
        StringBuilder sb = new StringBuilder("Tu tramite #" + d.get("tramiteId"));
        Object direccion = d.get("direccion");
        if (direccion != null && !direccion.toString().isBlank()) sb.append(" (").append(direccion).append(")");
        sb.append(' ').append(novedad);
        if (d.get("visita") instanceof Map<?, ?> v) {
            String horas = "TARDE".equals(v.get("franja")) ? "14:00 a 17:00" : "9:00 a 12:00";
            sb.append("\nVisita: ").append(v.get("fecha")).append(", ").append(horas).append(", ").append(v.get("cuadrilla"));
            if (v.get("nota") != null) sb.append("\nNota: ").append(v.get("nota"));
            if (v.get("codigo") != null) {
                sb.append("\nCodigo de la visita: ").append(v.get("codigo"))
                  .append(". Pideselo a la cuadrilla al llegar: si no coincide, no abras.");
            }
        }
        if (d.get("motivo") != null) sb.append("\nMotivo: ").append(d.get("motivo"));
        return sb.toString();
    }
}
