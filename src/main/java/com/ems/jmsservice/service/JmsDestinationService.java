package com.ems.jmsservice.service;

import com.ems.jmsservice.model.Destination;
import com.ems.jmsservice.model.MessageLog;
import com.ems.jmsservice.repository.EmsConfigRepository;
import jakarta.jms.*;
import jakarta.jms.Queue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class JmsDestinationService {

    private final ConnectionFactory connectionFactory;
    private final EmsConfigRepository repository;
    private final EmsEventBroadcaster eventBroadcaster;
    
    // In-memory counter for total messages published to a topic since system start
    private final Map<String, AtomicInteger> topicPublishCounters = new ConcurrentHashMap<>();

    @Autowired
    public JmsDestinationService(ConnectionFactory connectionFactory,
                                 EmsConfigRepository repository,
                                 EmsEventBroadcaster eventBroadcaster) {
        this.connectionFactory = connectionFactory;
        this.repository = repository;
        this.eventBroadcaster = eventBroadcaster;
    }

    public synchronized void createDestination(String name, Destination.Type type) {
        // Prevent duplicate names
        boolean exists = repository.getDestinations().stream()
                .anyMatch(d -> d.getName().equalsIgnoreCase(name) && d.getType() == type);
        
        if (exists) {
            throw new IllegalArgumentException("Destination " + name + " (" + type + ") already exists");
        }

        Destination dest = new Destination(name, type, Instant.now());
        repository.addDestination(dest);
        
        // Ensure connection verification works for dynamic destinations
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            if (type == Destination.Type.QUEUE) {
                session.createQueue(name);
            } else {
                session.createTopic(name);
                topicPublishCounters.put(name, new AtomicInteger(0));
            }
        } catch (Exception e) {
            log.error("Failed to verify/pre-create destination at JMS broker layer: " + name, e);
        }

        eventBroadcaster.broadcast("destination-created", getDestinationWithStats(dest));
    }

    public synchronized void deleteDestination(String name, Destination.Type type) {
        boolean removed = repository.removeDestination(name, type);
        if (removed) {
            if (type == Destination.Type.TOPIC) {
                topicPublishCounters.remove(name);
            }
            Map<String, String> payload = new HashMap<>();
            payload.put("name", name);
            payload.put("type", type.name());
            eventBroadcaster.broadcast("destination-deleted", payload);
        } else {
            throw new IllegalArgumentException("Destination " + name + " not found");
        }
    }

    public List<Map<String, Object>> getAllDestinationsWithStats() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Destination d : repository.getDestinations()) {
            result.add(getDestinationWithStats(d));
        }
        return result;
    }

    private Map<String, Object> getDestinationWithStats(Destination destination) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", destination.getName());
        map.put("type", destination.getType().name());
        map.put("createdAt", destination.getCreatedAt().toString());
        
        if (destination.getType() == Destination.Type.QUEUE) {
            map.put("messageCount", getQueueDepth(destination.getName()));
        } else {
            AtomicInteger counter = topicPublishCounters.computeIfAbsent(destination.getName(), k -> new AtomicInteger(0));
            map.put("messageCount", counter.get());
        }
        return map;
    }

    public int getQueueDepth(String queueName) {
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Queue queue = session.createQueue(queueName);
            try (QueueBrowser browser = session.createBrowser(queue)) {
                connection.start();
                Enumeration<?> enumeration = browser.getEnumeration();
                int count = 0;
                while (enumeration.hasMoreElements()) {
                    enumeration.nextElement();
                    count++;
                }
                return count;
            }
        } catch (Exception e) {
            log.error("Failed to fetch queue depth for " + queueName, e);
            return 0;
        }
    }

    public void incrementTopicPublishCount(String topicName) {
        topicPublishCounters.computeIfAbsent(topicName, k -> new AtomicInteger(0)).incrementAndGet();
    }

    public void sendMessage(String name, Destination.Type type, String payload, Map<String, Object> headers) {
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            
            jakarta.jms.Destination destination;
            if (type == Destination.Type.QUEUE) {
                destination = session.createQueue(name);
            } else {
                destination = session.createTopic(name);
                incrementTopicPublishCount(name);
            }

            try (MessageProducer producer = session.createProducer(destination)) {
                TextMessage message = session.createTextMessage(payload);
                if (headers != null) {
                    for (Map.Entry<String, Object> entry : headers.entrySet()) {
                        String key = entry.getKey();
                        Object val = entry.getValue();
                        if (val instanceof Number) {
                            message.setObjectProperty(key, val);
                        } else if (val instanceof Boolean) {
                            message.setBooleanProperty(key, (Boolean) val);
                        } else {
                            message.setStringProperty(key, val.toString());
                        }
                    }
                }
                producer.send(message);

                MessageLog logItem = new MessageLog(
                        UUID.randomUUID().toString(),
                        Instant.now(),
                        MessageLog.ActionType.PUBLISH,
                        name,
                        null,
                        payload,
                        headers != null ? new HashMap<>(headers) : new HashMap<>()
                );
                repository.addMessageLog(logItem);
                
                // Broadcast updates to the UI
                eventBroadcaster.broadcast("message-published", logItem);
                eventBroadcaster.broadcast("stats-updated", getDestinationWithStats(new Destination(name, type, Instant.now())));
            }
        } catch (Exception e) {
            log.error("Failed to send message to " + name, e);
            throw new RuntimeException("Failed to send JMS message", e);
        }
    }

    public List<Map<String, Object>> browseQueue(String queueName) {
        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Queue queue = session.createQueue(queueName);
            try (QueueBrowser browser = session.createBrowser(queue)) {
                connection.start();
                Enumeration<?> enumeration = browser.getEnumeration();
                while (enumeration.hasMoreElements()) {
                    Message msg = (Message) enumeration.nextElement();
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", msg.getJMSMessageID());
                    map.put("timestamp", Instant.ofEpochMilli(msg.getJMSTimestamp()).toString());
                    
                    if (msg instanceof TextMessage) {
                        map.put("payload", ((TextMessage) msg).getText());
                    } else {
                        map.put("payload", msg.toString());
                    }
                    
                    Map<String, Object> headersMap = new HashMap<>();
                    Enumeration<?> propNames = msg.getPropertyNames();
                    while (propNames.hasMoreElements()) {
                        String propName = (String) propNames.nextElement();
                        headersMap.put(propName, msg.getObjectProperty(propName));
                    }
                    map.put("headers", headersMap);
                    result.add(map);
                }
            }
        } catch (Exception e) {
            log.error("Failed to browse queue " + queueName, e);
        }
        return result;
    }

    public Map<String, Object> consumeMessage(String queueName) {
        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Queue queue = session.createQueue(queueName);
            try (MessageConsumer consumer = session.createConsumer(queue)) {
                connection.start();
                Message msg = consumer.receive(1000); // 1-second timeout
                if (msg == null) {
                    return null;
                }
                
                Map<String, Object> result = new HashMap<>();
                result.put("id", msg.getJMSMessageID());
                result.put("timestamp", Instant.ofEpochMilli(msg.getJMSTimestamp()).toString());
                
                String payload;
                if (msg instanceof TextMessage) {
                    payload = ((TextMessage) msg).getText();
                } else {
                    payload = msg.toString();
                }
                result.put("payload", payload);
                
                Map<String, Object> headersMap = new HashMap<>();
                Enumeration<?> propNames = msg.getPropertyNames();
                while (propNames.hasMoreElements()) {
                    String propName = (String) propNames.nextElement();
                    headersMap.put(propName, msg.getObjectProperty(propName));
                }
                result.put("headers", headersMap);

                MessageLog logItem = new MessageLog(
                        UUID.randomUUID().toString(),
                        Instant.now(),
                        MessageLog.ActionType.CONSUME,
                        queueName,
                        null,
                        payload,
                        headersMap
                );
                repository.addMessageLog(logItem);

                eventBroadcaster.broadcast("message-consumed", logItem);
                eventBroadcaster.broadcast("stats-updated", getDestinationWithStats(new Destination(queueName, Destination.Type.QUEUE, Instant.now())));

                return result;
            }
        } catch (Exception e) {
            log.error("Failed to consume from queue " + queueName, e);
            throw new RuntimeException("Failed to consume from queue", e);
        }
    }
}
