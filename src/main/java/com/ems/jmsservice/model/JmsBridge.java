package com.ems.jmsservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JmsBridge {
    private Long id;
    private String sourceTopic;
    private String targetQueue;
    private String selector; // JMS SQL-92 style selector (optional)
    private boolean enabled;
    private Instant createdAt;
}
