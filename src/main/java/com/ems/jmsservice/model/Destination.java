package com.ems.jmsservice.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "destinations")
public class Destination {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    @Enumerated(EnumType.STRING)
    private Type type;

    private Instant createdAt;

    public Destination(String name, Type type, Instant createdAt) {
        this.name = name;
        this.type = type;
        this.createdAt = createdAt;
    }

    public enum Type {
        QUEUE, TOPIC
    }
}
