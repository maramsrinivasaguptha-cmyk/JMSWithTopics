package com.ems.jmsservice.repository;

import com.ems.jmsservice.model.JmsBridge;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JmsBridgeRepository extends JpaRepository<JmsBridge, Long> {
}
