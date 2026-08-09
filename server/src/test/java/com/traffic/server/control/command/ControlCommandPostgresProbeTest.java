package com.traffic.server.control.command;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

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
 * G1 / RC1-T2A — PostgreSQL persistence probe (requires local PostgreSQL on 5432).
 */
@SpringBootTest
@ActiveProfiles("postgres-test")
@EnabledIf("com.traffic.server.control.command.ControlCommandPostgresProbeTest#postgresAvailable")
class ControlCommandPostgresProbeTest {

    @Autowired
    private ControlCommandRepository commandRepository;

    @Autowired
    private ControlResourceLockRepository lockRepository;

    @Autowired
    private ControlCommandService commandService;

    private final ObjectMapper mapper = new ObjectMapper();

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
            // database may already exist
        }
    }

    @BeforeAll
    static void probeDb() {
        if (!postgresAvailable()) {
            throw new IllegalStateException("PostgreSQL probe skipped");
        }
    }

    @Test
    void uuidInstantAndEnumRoundTrip() {
        UUID id = UUID.randomUUID();
        Instant requested = Instant.parse("2026-08-05T12:00:00Z");
        ControlCommandEntity e = baseEntity(id, "pg-op", "pg-key-" + id, "fp-" + id);
        e.setRequestedAtUtc(requested);
        commandRepository.saveAndFlush(e);

        ControlCommandEntity loaded = commandRepository.findById(id).orElseThrow();
        assertThat(loaded.getCommandId()).isEqualTo(id);
        assertThat(loaded.getRequestedAtUtc()).isEqualTo(requested);
        assertThat(loaded.getLifecycleStatus()).isEqualTo(ControlLifecycleStatus.RECEIVED);
        assertThat(loaded.getCommandType()).isEqualTo(ControlCommandType.FORCE_PHASE);
    }

    @Test
    void optimisticLockingOnVersion() {
        UUID id = UUID.randomUUID();
        ControlCommandEntity e = baseEntity(id, "pg-op2", "key-" + id, "fp");
        commandRepository.saveAndFlush(e);

        ControlCommandEntity first = commandRepository.findById(id).orElseThrow();
        ControlCommandEntity second = commandRepository.findById(id).orElseThrow();
        first.setLifecycleStatus(ControlLifecycleStatus.QUEUED);
        second.setLifecycleStatus(ControlLifecycleStatus.FAILED);
        commandRepository.saveAndFlush(first);
        assertThatThrownBy(() -> commandRepository.saveAndFlush(second))
                .isInstanceOf(ObjectOptimisticLockingFailureException.class);
    }

    @Test
    void resourceLockClaimViaService() {
        String intersection = "probe-" + UUID.randomUUID();
        CreateControlCommandRequest first = forcePhaseRequest(intersection, "key-a-" + UUID.randomUUID());
        CreateControlCommandRequest second = forcePhaseRequest(intersection, "key-b-" + UUID.randomUUID());

        commandService.accept(first, "pg-lock-op");
        assertThatThrownBy(() -> commandService.accept(second, "pg-lock-op"))
                .isInstanceOf(ResourceBusyException.class);
    }

    private CreateControlCommandRequest forcePhaseRequest(String intersectionId, String idempotencyKey) {
        ObjectNode target = mapper.createObjectNode().put("intersectionId", intersectionId);
        ObjectNode payload = mapper.createObjectNode().put("phase", "GREEN_NS");
        return new CreateControlCommandRequest(
                "1.0",
                UUID.randomUUID(),
                ControlCommandType.FORCE_PHASE,
                target,
                payload,
                "run-pg-lock",
                idempotencyKey,
                Instant.parse("2026-08-05T12:00:00Z"),
                Instant.parse("2026-08-05T12:05:00Z"),
                "DASHBOARD");
    }

    @Test
    void idempotencyUniqueOperatorKey() {
        String operator = "race-op";
        String key = "race-key-" + UUID.randomUUID();
        commandRepository.saveAndFlush(baseEntity(UUID.randomUUID(), operator, key, "fp1"));
        assertThatThrownBy(() ->
                        commandRepository.saveAndFlush(baseEntity(UUID.randomUUID(), operator, key, "fp2")))
                .isInstanceOf(Exception.class);
        assertThat(commandRepository.findByOperatorIdAndIdempotencyKey(operator, key)).isPresent();
    }

    @Test
    void fingerprintUsesCanonicalJson() {
        ObjectNode target = mapper.createObjectNode().put("intersectionId", "A");
        ObjectNode payload = mapper.createObjectNode().put("phase", "GREEN_NS");
        String fp = RequestFingerprint.compute("1.0", "FORCE_PHASE", target, payload, "run-pg", "DASHBOARD");
        assertThat(fp).hasSize(64);
    }

    private static ControlCommandEntity baseEntity(UUID id, String operator, String key, String fp) {
        ControlCommandEntity e = new ControlCommandEntity();
        e.setCommandId(id);
        e.setContractVersion("1.0");
        e.setIdempotencyKey(key);
        e.setRequestFingerprint(fp);
        e.setOperatorId(operator);
        e.setSource("DASHBOARD");
        e.setCommandType(ControlCommandType.FORCE_PHASE);
        e.setTargetJson("{\"intersectionId\":\"A\"}");
        e.setPayloadJson("{\"phase\":\"GREEN_NS\"}");
        e.setExpectedRunId("run-1");
        e.setLifecycleStatus(ControlLifecycleStatus.RECEIVED);
        e.setDispatchStatus(ControlDispatchStatus.PENDING);
        e.setExecutionStatus(ControlExecutionStatus.NOT_STARTED);
        e.setObservationStatus(ControlObservationStatus.NOT_REQUESTED);
        e.setRequestedAtUtc(Instant.parse("2026-08-05T12:00:00Z"));
        e.setExpiresAtUtc(Instant.parse("2026-08-05T12:05:00Z"));
        return e;
    }
}
