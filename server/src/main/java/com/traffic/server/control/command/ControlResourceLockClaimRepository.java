package com.traffic.server.control.command;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

/** Atomic INSERT-based resource lock claim (no merge/update path). */
@Repository
public class ControlResourceLockClaimRepository {

    private final JdbcTemplate jdbcTemplate;

    public ControlResourceLockClaimRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean tryClaim(String resourceKey, UUID commandId, Instant expiresAt) {
        Instant createdAt = Instant.now();
        try {
            int rows = jdbcTemplate.update(
                    """
                    INSERT INTO control_resource_lock (resource_key, command_id, expires_at, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    resourceKey,
                    commandId,
                    Timestamp.from(expiresAt),
                    Timestamp.from(createdAt));
            return rows == 1;
        } catch (DataAccessException e) {
            if (isDuplicateKey(e)) {
                return false;
            }
            throw e;
        }
    }

    public void release(String resourceKey) {
        jdbcTemplate.update("DELETE FROM control_resource_lock WHERE resource_key = ?", resourceKey);
    }

    private static boolean isDuplicateKey(Throwable e) {
        Throwable cur = e;
        while (cur != null) {
            String msg = cur.getMessage();
            if (msg != null && (msg.contains("duplicate key")
                    || msg.contains("Unique index")
                    || msg.contains("unique constraint")
                    || msg.contains("23505"))) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }
}
