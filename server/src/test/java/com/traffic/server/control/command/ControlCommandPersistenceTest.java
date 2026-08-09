package com.traffic.server.control.command;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
class ControlCommandPersistenceTest {

    @Autowired
    private ControlCommandRepository repository;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void uuidRoundTripAndUniqueIdempotency() {
        UUID id = UUID.randomUUID();
        ControlCommandEntity e = baseEntity(id, "op1", "key1", "fp1");
        repository.saveAndFlush(e);

        ControlCommandEntity loaded = repository.findById(id).orElseThrow();
        assertThat(loaded.getCommandId()).isEqualTo(id);
        assertThat(loaded.getLifecycleStatus()).isEqualTo(ControlLifecycleStatus.RECEIVED);

        ControlCommandEntity dup = baseEntity(UUID.randomUUID(), "op1", "key1", "fp2");
        assertThatThrownBy(() -> repository.saveAndFlush(dup)).isInstanceOf(Exception.class);
    }

    @Test
    void fingerprintDeterministic() {
        ObjectNode target = mapper.createObjectNode().put("intersectionId", "A");
        ObjectNode payload = mapper.createObjectNode().put("phase", "GREEN_NS");
        String a = RequestFingerprint.compute("1.0", "FORCE_PHASE", target, payload, "run-1", "DASHBOARD");
        String b = RequestFingerprint.compute("1.0", "FORCE_PHASE", target, payload, "run-1", "DASHBOARD");
        assertThat(a).isEqualTo(b);
        assertThat(a).hasSize(64);
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
        e.setTargetJson("{}");
        e.setPayloadJson("{}");
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
