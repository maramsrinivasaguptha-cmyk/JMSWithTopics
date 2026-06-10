package com.ems.jmsservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jms.artemis.ArtemisConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import jakarta.jms.ConnectionFactory;

@Configuration
@EnableJms
public class EmbeddedJmsConfig {

    @Value("${spring.artemis.embedded.port:61616}")
    private int brokerPort;

    @Value("${spring.artemis.embedded.tcp-enabled:true}")
    private boolean tcpEnabled;

    @Bean
    public ArtemisConfigurationCustomizer artemisConfigurationCustomizer() {
        return configuration -> {
            try {
                // Configure in-vm acceptor for secure local-only message passing inside the JVM
                configuration.addAcceptorConfiguration("in-vm", "vm://0");
                
                // If TCP is enabled, configure a TCP acceptor to allow external JMS client connections
                if (tcpEnabled) {
                    configuration.addAcceptorConfiguration("tcp", "tcp://0.0.0.0:" + brokerPort);
                }
                
                // Disable JMS security for simple sandbox operations
                configuration.setSecurityEnabled(false);
                // Keep the broker storage in-memory for fast execution
                configuration.setPersistenceEnabled(false);
            } catch (Exception e) {
                throw new RuntimeException("Failed to customize Artemis configuration", e);
            }
        };
    }

    @Bean
    public DefaultJmsListenerContainerFactory topicJmsListenerContainerFactory(
            ConnectionFactory connectionFactory,
            DefaultJmsListenerContainerFactoryConfigurer configurer) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setPubSubDomain(true); // Specifically for pub-sub topics
        return factory;
    }
}
