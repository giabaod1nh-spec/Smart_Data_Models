package com.traffic.server.control.command;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.UUID;

final class ControlCommandPostgresProbeTestHelper {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ControlCommandPostgresProbeTestHelper() {}

    static CreateControlCommandRequest forcePhaseRequest(String intersectionId, String idempotencyKey) {
        ObjectNode target = MAPPER.createObjectNode().put("intersectionId", intersectionId);
        ObjectNode payload = MAPPER.createObjectNode().put("phase", "NS_GREEN");
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
}
