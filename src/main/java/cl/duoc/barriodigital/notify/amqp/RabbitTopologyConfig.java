package cl.duoc.barriodigital.notify.amqp;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topologia de la seccion 8 del caso: 2 exchanges de comando + 1 de dead-letter,
 * 3 colas principales con su DLQ, cada una alcanzable por binding direct y topic.
 * Los nombres viven en application.yml (barriodigital.rabbitmq.*, ver RabbitProperties).
 * Este servicio es el dueño de la topologia (el productor en ms-barriodigital-requests
 * solo declara los exchanges, de forma idempotente, para poder publicar aunque
 * arranque antes que este consumidor).
 * Todas las colas son quorum: replican los mensajes entre nodos, pero con 2 nodos
 * la mayoria Raft es 2, asi que NO toleran la caida de un nodo (haria falta un tercero).
 */
@Configuration
@EnableConfigurationProperties(RabbitProperties.class)
public class RabbitTopologyConfig {

    private final RabbitProperties p;

    public RabbitTopologyConfig(RabbitProperties p) {
        this.p = p;
    }

    @Bean
    MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    DirectExchange cmdDirectExchange() {
        return new DirectExchange(p.exchanges().direct(), true, false);
    }

    @Bean
    TopicExchange cmdTopicExchange() {
        return new TopicExchange(p.exchanges().topic(), true, false);
    }

    @Bean
    DirectExchange cmdDeadLetterExchange() {
        return new DirectExchange(p.exchanges().deadLetter(), true, false);
    }

    private Queue cola(String nombre, String dlqRoutingKey) {
        return QueueBuilder.durable(nombre)
                .quorum()
                .withArgument("x-dead-letter-exchange", p.exchanges().deadLetter())
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey)
                .build();
    }

    private Queue dlq(String nombre) {
        return QueueBuilder.durable(nombre).quorum().build();
    }

    // ---- email ----
    @Bean
    Queue qCmdEmail() { return cola(p.colas().email(), p.routingKeys().emailDlq()); }
    @Bean
    Queue qCmdEmailDlq() { return dlq(p.colas().emailDlq()); }
    @Bean
    Binding bindEmailDirect() { return BindingBuilder.bind(qCmdEmail()).to(cmdDirectExchange()).with(p.routingKeys().emailSend()); }
    @Bean
    Binding bindEmailTopic() { return BindingBuilder.bind(qCmdEmail()).to(cmdTopicExchange()).with(p.routingKeys().emailTopic()); }
    @Bean
    Binding bindEmailDlq() { return BindingBuilder.bind(qCmdEmailDlq()).to(cmdDeadLetterExchange()).with(p.routingKeys().emailDlq()); }

    // ---- crew ----
    @Bean
    Queue qCmdCrew() { return cola(p.colas().crew(), p.routingKeys().crewDlq()); }
    @Bean
    Queue qCmdCrewDlq() { return dlq(p.colas().crewDlq()); }
    @Bean
    Binding bindCrewDirect() { return BindingBuilder.bind(qCmdCrew()).to(cmdDirectExchange()).with(p.routingKeys().crewTicket()); }
    @Bean
    Binding bindCrewTopic() { return BindingBuilder.bind(qCmdCrew()).to(cmdTopicExchange()).with(p.routingKeys().crewTopic()); }
    @Bean
    Binding bindCrewDlq() { return BindingBuilder.bind(qCmdCrewDlq()).to(cmdDeadLetterExchange()).with(p.routingKeys().crewDlq()); }

    // ---- certificate ----
    @Bean
    Queue qCmdCertificate() { return cola(p.colas().certificate(), p.routingKeys().certificateDlq()); }
    @Bean
    Queue qCmdCertificateDlq() { return dlq(p.colas().certificateDlq()); }
    @Bean
    Binding bindCertificateDirect() { return BindingBuilder.bind(qCmdCertificate()).to(cmdDirectExchange()).with(p.routingKeys().certificateGen()); }
    @Bean
    Binding bindCertificateTopic() { return BindingBuilder.bind(qCmdCertificate()).to(cmdTopicExchange()).with(p.routingKeys().certificateTopic()); }
    @Bean
    Binding bindCertificateDlq() { return BindingBuilder.bind(qCmdCertificateDlq()).to(cmdDeadLetterExchange()).with(p.routingKeys().certificateDlq()); }
}
