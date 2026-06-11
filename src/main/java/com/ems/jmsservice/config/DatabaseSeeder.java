package com.ems.jmsservice.config;

import com.ems.jmsservice.model.Destination;
import com.ems.jmsservice.model.JmsBridge;
import com.ems.jmsservice.repository.DestinationRepository;
import com.ems.jmsservice.repository.JmsBridgeRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
public class DatabaseSeeder implements CommandLineRunner {

    private final DestinationRepository destinationRepository;
    private final JmsBridgeRepository jmsBridgeRepository;

    @Autowired
    public DatabaseSeeder(DestinationRepository destinationRepository, JmsBridgeRepository jmsBridgeRepository) {
        this.destinationRepository = destinationRepository;
        this.jmsBridgeRepository = jmsBridgeRepository;
    }

    @Override
    public void run(String... args) throws Exception {
        if (destinationRepository.count() == 0) {
            log.info("Seeding default JMS destinations into H2 database...");
            destinationRepository.save(new Destination("Queue.A", Destination.Type.QUEUE, Instant.now()));
            destinationRepository.save(new Destination("Queue.B", Destination.Type.QUEUE, Instant.now()));
            destinationRepository.save(new Destination("Topic.X", Destination.Type.TOPIC, Instant.now()));
        }

        if (jmsBridgeRepository.count() == 0) {
            log.info("Seeding default Topic-Queue pairing bridge into H2 database...");
            jmsBridgeRepository.save(new JmsBridge(null, "Topic.X", "Queue.A", "", true, Instant.now()));
        }
    }
}
