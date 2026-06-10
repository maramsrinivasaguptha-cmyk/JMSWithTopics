package com.ems.jmsservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Destination {
    private String name;
    private Type type;
    private Instant createdAt;

    public enum Type {
        QUEUE, TOPIC
    }
}
