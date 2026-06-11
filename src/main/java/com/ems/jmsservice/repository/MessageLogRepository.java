package com.ems.jmsservice.repository;

import com.ems.jmsservice.model.MessageLog;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface MessageLogRepository extends JpaRepository<MessageLog, String> {
    List<MessageLog> findAllByOrderByTimestampDesc();

    @Query("SELECT m FROM MessageLog m ORDER BY m.timestamp DESC")
    List<MessageLog> findLatestLogs(Pageable pageable);
}
