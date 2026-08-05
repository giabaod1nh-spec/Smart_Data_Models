package com.traffic.server.control.command;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "control_command",
        uniqueConstraints = @UniqueConstraint(name = "uk_control_command_idempotency", columnNames = {"operator_id", "idempotency_key"})
)
@Getter
@Setter
public class ControlCommandEntity {

    @Id
    @Column(name = "command_id", nullable = false, updatable = false)
    private UUID commandId;

    @Version
    private Long version;

    @Column(name = "contract_version", nullable = false, length = 16)
    private String contractVersion;

    @Column(name = "idempotency_key", nullable = false, length = 128)
    private String idempotencyKey;

    @Column(name = "request_fingerprint", nullable = false, length = 64)
    private String requestFingerprint;

    @Column(name = "operator_id", nullable = false, length = 128)
    private String operatorId;

    @Column(name = "source", nullable = false, length = 32)
    private String source;

    @Enumerated(EnumType.STRING)
    @Column(name = "command_type", nullable = false, length = 64)
    private ControlCommandType commandType;

    @Column(name = "target_json", nullable = false, columnDefinition = "TEXT")
    private String targetJson;

    @Column(name = "payload_json", nullable = false, columnDefinition = "TEXT")
    private String payloadJson;

    @Column(name = "expected_run_id", nullable = false, length = 128)
    private String expectedRunId;

    @Column(name = "accepted_run_id", length = 128)
    private String acceptedRunId;

    @Column(name = "resource_key", length = 256)
    private String resourceKey;

    @Enumerated(EnumType.STRING)
    @Column(name = "lifecycle_status", nullable = false, length = 32)
    private ControlLifecycleStatus lifecycleStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "dispatch_status", nullable = false, length = 32)
    private ControlDispatchStatus dispatchStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "execution_status", nullable = false, length = 32)
    private ControlExecutionStatus executionStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "observation_status", nullable = false, length = 32)
    private ControlObservationStatus observationStatus;

    @Column(name = "requested_at_utc", nullable = false)
    private Instant requestedAtUtc;

    @Column(name = "expires_at_utc", nullable = false)
    private Instant expiresAtUtc;

    @Column(name = "error_code", length = 64)
    private String errorCode;

    @Column(name = "error_safe_detail", length = 512)
    private String errorSafeDetail;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }
}
