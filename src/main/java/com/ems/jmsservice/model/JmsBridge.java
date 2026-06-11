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
@Table(name = "bridges")
public class JmsBridge {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String sourceTopic;
    private String targetQueue;
    
    @Column(length = 500)
    private String selector; // JMS SQL-92 style selector (optional)
    
    private boolean enabled;
    private Instant createdAt;
}
