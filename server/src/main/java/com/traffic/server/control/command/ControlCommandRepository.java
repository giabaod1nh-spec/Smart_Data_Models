package com.traffic.server.control.command;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ControlCommandRepository extends JpaRepository<ControlCommandEntity, UUID> {

    Optional<ControlCommandEntity> findByOperatorIdAndIdempotencyKey(String operatorId, String idempotencyKey);

    @Query("""
            SELECT c FROM ControlCommandEntity c
            WHERE c.lifecycleStatus NOT IN :terminal
            """)
    List<ControlCommandEntity> findNonTerminal(@Param("terminal") Collection<ControlLifecycleStatus> terminal);

    List<ControlCommandEntity> findByExpiresAtUtcBeforeAndLifecycleStatusNotIn(
            Instant expiresBefore, Collection<ControlLifecycleStatus> terminal);

    List<ControlCommandEntity> findByObservationStatusAndExecutionStatus(
            ControlObservationStatus observationStatus,
            ControlExecutionStatus executionStatus);

    long countByLifecycleStatusIn(Collection<ControlLifecycleStatus> statuses);
}
