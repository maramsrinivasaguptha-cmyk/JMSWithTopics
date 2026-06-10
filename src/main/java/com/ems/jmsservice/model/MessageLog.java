package com.ems.jmsservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageLog {
    private String id;
    private Instant timestamp;
    private ActionType actionType;
    private String destination;
    private String targetDestination; // set if action is forward
    private String payload;
    private Map<String, Object> headers;

    public enum ActionType {
        PUBLISH, BRIDGE_FORWARD, CONSUME, BROWSE
    }
}
