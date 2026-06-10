package com.ems.jmsservice.controller;

import com.ems.jmsservice.model.Destination;
import com.ems.jmsservice.model.JmsBridge;
import com.ems.jmsservice.model.MessageLog;
import com.ems.jmsservice.repository.EmsConfigRepository;
import com.ems.jmsservice.service.EmsEventBroadcaster;
import com.ems.jmsservice.service.JmsBridgeService;
import com.ems.jmsservice.service.JmsDestinationService;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class JmsApiController {

    private final JmsDestinationService destinationService;
    private final JmsBridgeService bridgeService;
    private final EmsConfigRepository repository;
    private final EmsEventBroadcaster eventBroadcaster;

    @Value("${spring.artemis.embedded.port:61616}")
    private int brokerPort;

    @Value("${spring.artemis.embedded.tcp-enabled:true}")
    private boolean tcpEnabled;

    @Autowired
    public JmsApiController(JmsDestinationService destinationService,
                            JmsBridgeService bridgeService,
                            EmsConfigRepository repository,
                            EmsEventBroadcaster eventBroadcaster) {
        this.destinationService = destinationService;
        this.bridgeService = bridgeService;
        this.repository = repository;
        this.eventBroadcaster = eventBroadcaster;
    }

    // Connection Info API
    @GetMapping("/connection-info")
    public ResponseEntity<Map<String, Object>> getConnectionInfo() {
        return ResponseEntity.ok(Map.of(
            "providerUrl", tcpEnabled ? "tcp://localhost:" + brokerPort : "vm://0",
            "vmUrl", "vm://0",
            "connectionFactoryClass", "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory",
            "jndiContextFactory", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory",
            "username", "anonymous",
            "password", "None (Disabled)",
            "securityEnabled", false
        ));
    }

    // SSE Stream
    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter registerSseClient() {
        return eventBroadcaster.registerEmitter();
    }

    // Destination APIs
    @GetMapping("/destinations")
    public ResponseEntity<List<Map<String, Object>>> getDestinations() {
        return ResponseEntity.ok(destinationService.getAllDestinationsWithStats());
    }

    @PostMapping("/destinations")
    public ResponseEntity<?> createDestination(@RequestBody DestinationRequest request) {
        try {
            Destination.Type type = Destination.Type.valueOf(request.getType().toUpperCase());
            destinationService.createDestination(request.getName(), type);
            return ResponseEntity.ok().body(Map.of("message", "Destination created successfully"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/destinations/{type}/{name}")
    public ResponseEntity<?> deleteDestination(@PathVariable String type, @PathVariable String name) {
        try {
            Destination.Type destType = Destination.Type.valueOf(type.toUpperCase());
            destinationService.deleteDestination(name, destType);
            return ResponseEntity.ok().body(Map.of("message", "Destination deleted successfully"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    // Bridge APIs
    @GetMapping("/bridges")
    public ResponseEntity<List<JmsBridge>> getBridges() {
        return ResponseEntity.ok(bridgeService.getAllBridges());
    }

    @PostMapping("/bridges")
    public ResponseEntity<?> createBridge(@RequestBody BridgeRequest request) {
        try {
            bridgeService.createBridge(request.getSourceTopic(), request.getTargetQueue(), request.getSelector());
            return ResponseEntity.ok().body(Map.of("message", "Bridge created successfully"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/bridges/{id}")
    public ResponseEntity<?> deleteBridge(@PathVariable Long id) {
        try {
            bridgeService.deleteBridge(id);
            return ResponseEntity.ok().body(Map.of("message", "Bridge deleted successfully"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/bridges/{id}/toggle")
    public ResponseEntity<?> toggleBridge(@PathVariable Long id) {
        try {
            bridgeService.toggleBridge(id);
            return ResponseEntity.ok().body(Map.of("message", "Bridge toggled successfully"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    // Message APIs
    @PostMapping("/messages/send")
    public ResponseEntity<?> sendMessage(@RequestBody MessageRequest request) {
        try {
            Destination.Type type = Destination.Type.valueOf(request.getType().toUpperCase());
            destinationService.sendMessage(request.getDestination(), type, request.getPayload(), request.getHeaders());
            return ResponseEntity.ok().body(Map.of("message", "Message published successfully"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/queues/{name}/browse")
    public ResponseEntity<?> browseQueue(@PathVariable String name) {
        try {
            return ResponseEntity.ok(destinationService.browseQueue(name));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/queues/{name}/consume")
    public ResponseEntity<?> consumeFromQueue(@PathVariable String name) {
        try {
            Map<String, Object> msg = destinationService.consumeMessage(name);
            if (msg == null) {
                return ResponseEntity.ok().body(Map.of("message", "Queue is empty"));
            }
            return ResponseEntity.ok(msg);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/messages/history")
    public ResponseEntity<List<MessageLog>> getHistory() {
        return ResponseEntity.ok(repository.getMessageLogs());
    }

    // DTO Helper classes
    @Data
    public static class DestinationRequest {
        private String name;
        private String type;
    }

    @Data
    public static class BridgeRequest {
        private String sourceTopic;
        private String targetQueue;
        private String selector;
    }

    @Data
    public static class MessageRequest {
        private String destination;
        private String type;
        private String payload;
        private Map<String, Object> headers;
    }
}
