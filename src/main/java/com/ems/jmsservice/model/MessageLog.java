package com.ems.jmsservice.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "message_logs")
public class MessageLog {
    @Id
    private String id;

    private Instant timestamp;

    @Enumerated(EnumType.STRING)
    private ActionType actionType;

    private String destination;
    private String targetDestination; // set if action is forward

    @Lob
    @Column(columnDefinition = "CLOB")
    private String payload;

    @Convert(converter = HeadersConverter.class)
    @Column(length = 2000)
    private Map<String, Object> headers;

    public enum ActionType {
        PUBLISH, BRIDGE_FORWARD, CONSUME, BROWSE
    }
}
