package com.traffic.server.control.command;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * G1 atomic lock — PostgreSQL INSERT duplicate-key enforcement.
 * Sequential duplicate rejection mirrors concurrent race outcome (one row wins).
 */
@SpringBootTest
@ActiveProfiles("postgres-test")
@EnabledIf("com.traffic.server.control.command.ControlResourceLockConcurrencyTest#postgresAvailable")
class ControlResourceLockConcurrencyTest {

    @Autowired
    private ControlResourceLockClaimRepository claimRepository;

    @Autowired
    private ControlCommandService commandService;

    static boolean postgresAvailable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 5432), 500);
            ensureProbeDatabase();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void ensureProbeDatabase() throws Exception {
        try (Connection admin = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:5432/postgres", "erp_user", "123456");
                Statement st = admin.createStatement()) {
            st.executeUpdate("CREATE DATABASE traffic_rc_probe");
        } catch (Exception ignored) {
            // already exists
        }
    }

    @Test
    void insertDuplicateKeyRejected() {
        String resourceKey = "signal:seq-" + UUID.randomUUID();
        Instant exp = Instant.parse("2026-08-05T13:00:00Z");
        assertThat(claimRepository.tryClaim(resourceKey, UUID.randomUUID(), exp)).isTrue();
        assertThat(claimRepository.tryClaim(resourceKey, UUID.randomUUID(), exp)).isFalse();
    }

    @Test
    void serviceLevelResourceBusyOnSecondCommand() {
        String intersection = "probe-" + UUID.randomUUID();
        var req1 = ControlCommandPostgresProbeTestHelper.forcePhaseRequest(intersection, "key-a-" + UUID.randomUUID());
        var req2 = ControlCommandPostgresProbeTestHelper.forcePhaseRequest(intersection, "key-b-" + UUID.randomUUID());
        commandService.accept(req1, "pg-lock-op");
        assertThatThrownBy(() -> commandService.accept(req2, "pg-lock-op"))
                .isInstanceOf(ResourceBusyException.class);
    }
}
