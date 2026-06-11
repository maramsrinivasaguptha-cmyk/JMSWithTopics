package com.ems.jmsservice.repository;

import com.ems.jmsservice.model.Destination;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface DestinationRepository extends JpaRepository<Destination, Long> {
    Optional<Destination> findByNameIgnoreCaseAndType(String name, Destination.Type type);
    boolean existsByNameIgnoreCaseAndType(String name, Destination.Type type);
    void deleteByNameIgnoreCaseAndType(String name, Destination.Type type);
}
