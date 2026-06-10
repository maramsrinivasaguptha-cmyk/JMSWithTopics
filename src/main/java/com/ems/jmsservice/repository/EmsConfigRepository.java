package com.ems.jmsservice.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ems.jmsservice.model.Destination;
import com.ems.jmsservice.model.JmsBridge;
import com.ems.jmsservice.model.MessageLog;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Repository
public class EmsConfigRepository {

    private static final String CONFIG_FILE_PATH = "data/ems-config.json";
    
    private final ObjectMapper objectMapper;
    private final List<Destination> destinations = new CopyOnWriteArrayList<>();
    private final List<JmsBridge> bridges = new CopyOnWriteArrayList<>();
    private final List<MessageLog> messageLogs = new CopyOnWriteArrayList<>();
    private static final int MAX_LOGS = 100;

    public EmsConfigRepository() {
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @PostConstruct
    public void init() {
        try {
            File configFile = new File(CONFIG_FILE_PATH);
            if (configFile.exists()) {
                loadConfig(configFile);
            } else {
                createDefaultConfig(configFile);
            }
        } catch (Exception e) {
            log.error("Failed to initialize EMS config", e);
        }
    }

    private synchronized void loadConfig(File file) throws IOException {
        ConfigData data = objectMapper.readValue(file, ConfigData.class);
        destinations.clear();
        bridges.clear();
        if (data.getDestinations() != null) {
            destinations.addAll(data.getDestinations());
        }
        if (data.getBridges() != null) {
            bridges.addAll(data.getBridges());
        }
        log.info("Loaded {} destinations and {} bridges from persistence store", destinations.size(), bridges.size());
    }

    private synchronized void createDefaultConfig(File file) throws IOException {
        // Ensure parent folder exists
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        // Add default destinations for a warm startup
        destinations.add(new Destination("Queue.A", Destination.Type.QUEUE, Instant.now()));
        destinations.add(new Destination("Queue.B", Destination.Type.QUEUE, Instant.now()));
        destinations.add(new Destination("Topic.X", Destination.Type.TOPIC, Instant.now()));

        // Add a default pairing bridge
        bridges.add(new JmsBridge(1L, "Topic.X", "Queue.A", "", true, Instant.now()));

        saveConfig();
        log.info("Created default EMS config file at {}", file.getAbsolutePath());
    }

    public synchronized void saveConfig() {
        try {
            File file = new File(CONFIG_FILE_PATH);
            ConfigData data = new ConfigData();
            data.setDestinations(new ArrayList<>(destinations));
            data.setBridges(new ArrayList<>(bridges));
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, data);
        } catch (Exception e) {
            log.error("Failed to persist config", e);
        }
    }

    // Destinations
    public List<Destination> getDestinations() {
        return destinations;
    }

    public void addDestination(Destination destination) {
        destinations.add(destination);
        saveConfig();
    }

    public boolean removeDestination(String name, Destination.Type type) {
        boolean removed = destinations.removeIf(d -> d.getName().equalsIgnoreCase(name) && d.getType() == type);
        if (removed) {
            saveConfig();
        }
        return removed;
    }

    // Bridges
    public List<JmsBridge> getBridges() {
        return bridges;
    }

    public void addBridge(JmsBridge bridge) {
        bridges.add(bridge);
        saveConfig();
    }

    public boolean removeBridge(Long id) {
        boolean removed = bridges.removeIf(b -> b.getId().equals(id));
        if (removed) {
            saveConfig();
        }
        return removed;
    }

    public JmsBridge getBridgeById(Long id) {
        return bridges.stream().filter(b -> b.getId().equals(id)).findFirst().orElse(null);
    }

    // Message Logs
    public List<MessageLog> getMessageLogs() {
        List<MessageLog> logs = new ArrayList<>(messageLogs);
        Collections.reverse(logs);
        return logs;
    }

    public void addMessageLog(MessageLog logItem) {
        if (messageLogs.size() >= MAX_LOGS) {
            messageLogs.remove(0);
        }
        messageLogs.add(logItem);
    }

    @Data
    public static class ConfigData {
        private List<Destination> destinations;
        private List<JmsBridge> bridges;
    }
}
