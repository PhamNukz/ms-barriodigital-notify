package cl.duoc.barriodigital.notify.amqp;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * Topologia de la seccion 8 del caso: 2 exchanges de comando + 1 de dead-letter,
 * 3 colas principales con su DLQ, cada una alcanzable por binding direct y topic.
 * Este servicio es el dueño de la topologia (el productor en ms-barriodigital-requests
 * solo declara los exchanges, de forma idempotente, para poder publicar aunque
 * arranque antes que este consumidor).
 */
@Configuration
public class RabbitTopologyConfig {

    public static final String EXCHANGE_DIRECT = "cmd.direct";
    public static final String EXCHANGE_TOPIC = "cmd.topic";
    public static final String EXCHANGE_DLX = "cmd.dead.dlx";

    @Bean
    MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    DirectExchange cmdDirectExchange() {
        return new DirectExchange(EXCHANGE_DIRECT, true, false);
    }

    @Bean
    TopicExchange cmdTopicExchange() {
        return new TopicExchange(EXCHANGE_TOPIC, true, false);
    }

    @Bean
    DirectExchange cmdDeadLetterExchange() {
        return new DirectExchange(EXCHANGE_DLX, true, false);
    }

    private Queue cola(String nombre, String dlqRoutingKey) {
        return QueueBuilder.durable(nombre)
                .withArgument("x-dead-letter-exchange", EXCHANGE_DLX)
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey)
                .build();
    }

    // ---- q.cmd.email ----
    @Bean
    Queue qCmdEmail() { return cola("q.cmd.email", "email.dlq"); }
    @Bean
    Queue qCmdEmailDlq() { return QueueBuilder.durable("q.cmd.email.dlq").build(); }
    @Bean
    Binding bindEmailDirect() { return BindingBuilder.bind(qCmdEmail()).to(cmdDirectExchange()).with("email.send"); }
    @Bean
    Binding bindEmailTopic() { return BindingBuilder.bind(qCmdEmail()).to(cmdTopicExchange()).with("email.*"); }
    @Bean
    Binding bindEmailDlq() { return BindingBuilder.bind(qCmdEmailDlq()).to(cmdDeadLetterExchange()).with("email.dlq"); }

    // ---- q.cmd.crew ----
    @Bean
    Queue qCmdCrew() { return cola("q.cmd.crew", "crew.dlq"); }
    @Bean
    Queue qCmdCrewDlq() { return QueueBuilder.durable("q.cmd.crew.dlq").build(); }
    @Bean
    Binding bindCrewDirect() { return BindingBuilder.bind(qCmdCrew()).to(cmdDirectExchange()).with("crew.ticket"); }
    @Bean
    Binding bindCrewTopic() { return BindingBuilder.bind(qCmdCrew()).to(cmdTopicExchange()).with("crew.#"); }
    @Bean
    Binding bindCrewDlq() { return BindingBuilder.bind(qCmdCrewDlq()).to(cmdDeadLetterExchange()).with("crew.dlq"); }

    // ---- q.cmd.certificate ----
    @Bean
    Queue qCmdCertificate() { return cola("q.cmd.certificate", "certificate.dlq"); }
    @Bean
    Queue qCmdCertificateDlq() { return QueueBuilder.durable("q.cmd.certificate.dlq").build(); }
    @Bean
    Binding bindCertificateDirect() { return BindingBuilder.bind(qCmdCertificate()).to(cmdDirectExchange()).with("certificate.gen"); }
    @Bean
    Binding bindCertificateTopic() { return BindingBuilder.bind(qCmdCertificate()).to(cmdTopicExchange()).with("certificate.*"); }
    @Bean
    Binding bindCertificateDlq() { return BindingBuilder.bind(qCmdCertificateDlq()).to(cmdDeadLetterExchange()).with("certificate.dlq"); }
}
