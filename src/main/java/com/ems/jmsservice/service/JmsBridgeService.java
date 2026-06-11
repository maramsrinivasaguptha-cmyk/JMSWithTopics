package com.ems.jmsservice.service;

import com.ems.jmsservice.model.Destination;
import com.ems.jmsservice.model.JmsBridge;
import com.ems.jmsservice.model.MessageLog;
import com.ems.jmsservice.repository.DestinationRepository;
import com.ems.jmsservice.repository.JmsBridgeRepository;
import com.ems.jmsservice.repository.MessageLogRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.jms.*;
import jakarta.jms.Queue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@Transactional
public class JmsBridgeService {

    private final ConnectionFactory connectionFactory;
    private final JmsBridgeRepository jmsBridgeRepository;
    private final DestinationRepository destinationRepository;
    private final MessageLogRepository messageLogRepository;
    private final EmsEventBroadcaster eventBroadcaster;
    private final JmsDestinationService destinationService;

    // Registry of currently active message listener containers for bridges
    private final Map<Long, DefaultMessageListenerContainer> activeContainers = new ConcurrentHashMap<>();

    @Autowired
    public JmsBridgeService(ConnectionFactory connectionFactory,
                            JmsBridgeRepository jmsBridgeRepository,
                            DestinationRepository destinationRepository,
                            MessageLogRepository messageLogRepository,
                            EmsEventBroadcaster eventBroadcaster,
                            @Lazy JmsDestinationService destinationService) {
        this.connectionFactory = connectionFactory;
        this.jmsBridgeRepository = jmsBridgeRepository;
        this.destinationRepository = destinationRepository;
        this.messageLogRepository = messageLogRepository;
        this.eventBroadcaster = eventBroadcaster;
        this.destinationService = destinationService;
    }

    @PostConstruct
    public void startAllActiveBridges() {
        log.info("Initializing active EMS style JMS bridges from H2 database...");
        for (JmsBridge bridge : jmsBridgeRepository.findAll()) {
            if (bridge.isEnabled()) {
                try {
                    startBridgeListener(bridge);
                } catch (Exception e) {
                    log.error("Failed to start bridge listener for bridge ID: " + bridge.getId(), e);
                }
            }
        }
    }

    @PreDestroy
    public void stopAllBridges() {
        log.info("Shutting down active JMS bridges...");
        for (Long id : activeContainers.keySet()) {
            stopBridgeListener(id);
        }
    }

    public synchronized void createBridge(String sourceTopic, String targetQueue, String selector) {
        // Validation: Verify source topic and target queue exist in database
        boolean sourceExists = destinationRepository.existsByNameIgnoreCaseAndType(sourceTopic, Destination.Type.TOPIC);
        boolean targetExists = destinationRepository.existsByNameIgnoreCaseAndType(targetQueue, Destination.Type.QUEUE);

        if (!sourceExists) {
            throw new IllegalArgumentException("Source topic '" + sourceTopic + "' does not exist");
        }
        if (!targetExists) {
            throw new IllegalArgumentException("Target queue '" + targetQueue + "' does not exist");
        }

        JmsBridge bridge = new JmsBridge(null, sourceTopic, targetQueue, selector, true, Instant.now());
        JmsBridge savedBridge = jmsBridgeRepository.save(bridge);

        startBridgeListener(savedBridge);
        eventBroadcaster.broadcast("bridge-created", savedBridge);
    }

    public synchronized void deleteBridge(Long id) {
        stopBridgeListener(id);
        if (jmsBridgeRepository.existsById(id)) {
            jmsBridgeRepository.deleteById(id);
            Map<String, Object> payload = new HashMap<>();
            payload.put("id", id);
            eventBroadcaster.broadcast("bridge-deleted", payload);
        } else {
            throw new IllegalArgumentException("Bridge with ID " + id + " not found");
        }
    }

    public synchronized void toggleBridge(Long id) {
        Optional<JmsBridge> bridgeOpt = jmsBridgeRepository.findById(id);
        if (bridgeOpt.isEmpty()) {
            throw new IllegalArgumentException("Bridge with ID " + id + " not found");
        }

        JmsBridge bridge = bridgeOpt.get();
        boolean nextState = !bridge.isEnabled();
        bridge.setEnabled(nextState);
        jmsBridgeRepository.save(bridge);

        if (nextState) {
            startBridgeListener(bridge);
        } else {
            stopBridgeListener(id);
        }

        eventBroadcaster.broadcast("bridge-updated", bridge);
    }

    public List<JmsBridge> getAllBridges() {
        return jmsBridgeRepository.findAll();
    }

    private void startBridgeListener(JmsBridge bridge) {
        // Clean start - stop existing if present
        stopBridgeListener(bridge.getId());

        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(bridge.getSourceTopic());
        container.setPubSubDomain(true); // Must be a topic

        // Set optional JMS SQL-92 selector filter
        if (bridge.getSelector() != null && !bridge.getSelector().trim().isEmpty()) {
            container.setMessageSelector(bridge.getSelector());
        }

        container.setMessageListener((MessageListener) message -> {
            try {
                forwardMessage(bridge, message);
            } catch (Exception e) {
                log.error("JMS Bridge forward operation failed for bridge " + bridge.getId(), e);
            }
        });

        // Initialize and start container
        container.afterPropertiesSet();
        container.start();

        activeContainers.put(bridge.getId(), container);
        log.info("Activated dynamic Topic-Queue bridge listener (id: {}, {} -> {})", 
                bridge.getId(), bridge.getSourceTopic(), bridge.getTargetQueue());
    }

    private void stopBridgeListener(Long id) {
        DefaultMessageListenerContainer container = activeContainers.remove(id);
        if (container != null) {
            try {
                container.shutdown();
                log.info("Stopped and released listener container for bridge ID: {}", id);
            } catch (Exception e) {
                log.error("Failed to cleanly stop listener for bridge ID: " + id, e);
            }
        }
    }

    private void forwardMessage(JmsBridge bridge, Message message) throws JMSException {
        String payload = "";
        if (message instanceof TextMessage) {
            payload = ((TextMessage) message).getText();
        } else {
            payload = message.toString();
        }

        // Duplicate properties/headers
        Map<String, Object> headers = new HashMap<>();
        Enumeration<?> propNames = message.getPropertyNames();
        while (propNames.hasMoreElements()) {
            String propName = (String) propNames.nextElement();
            headers.put(propName, message.getObjectProperty(propName));
        }

        // Bridge forwarding execution
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Queue queue = session.createQueue(bridge.getTargetQueue());
            try (MessageProducer producer = session.createProducer(queue)) {

                TextMessage forwardMessage = session.createTextMessage(payload);
                for (Map.Entry<String, Object> entry : headers.entrySet()) {
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    if (val instanceof Number) {
                        forwardMessage.setObjectProperty(key, val);
                    } else if (val instanceof Boolean) {
                        forwardMessage.setBooleanProperty(key, (Boolean) val);
                    } else {
                        forwardMessage.setStringProperty(key, val.toString());
                    }
                }

                producer.send(forwardMessage);

                // Log forward operation
                MessageLog logItem = new MessageLog(
                        UUID.randomUUID().toString(),
                        Instant.now(),
                        MessageLog.ActionType.BRIDGE_FORWARD,
                        bridge.getSourceTopic(),
                        bridge.getTargetQueue(),
                        payload,
                        headers
                );
                messageLogRepository.save(logItem);

                // Broadast events to the Web Console UI
                eventBroadcaster.broadcast("message-bridged", logItem);
                
                // Broadcast statistics updates for target queue message count
                Map<String, Object> queueStats = new HashMap<>();
                queueStats.put("name", bridge.getTargetQueue());
                queueStats.put("type", Destination.Type.QUEUE.name());
                queueStats.put("messageCount", destinationService.getQueueDepth(bridge.getTargetQueue()));
                eventBroadcaster.broadcast("stats-updated", queueStats);
            }
        }
    }
}
