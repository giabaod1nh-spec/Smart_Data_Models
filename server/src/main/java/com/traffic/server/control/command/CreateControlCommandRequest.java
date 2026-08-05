package com.traffic.server.control.command;

import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public record CreateControlCommandRequest(
        String contractVersion,
        UUID commandId,
        ControlCommandType commandType,
        JsonNode target,
        JsonNode payload,
        String expectedRunId,
        String idempotencyKey,
        Instant requestedAt,
        Instant expiresAt,
        String source
) {}
