package com.traffic.server.control.command;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "control_resource_lock")
@Getter
@Setter
public class ControlResourceLockEntity {

    @Id
    @Column(name = "resource_key", nullable = false, length = 256)
    private String resourceKey;

    @Column(name = "command_id", nullable = false)
    private UUID commandId;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;
}
