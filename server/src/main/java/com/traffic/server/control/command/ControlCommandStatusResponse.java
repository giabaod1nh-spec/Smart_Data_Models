package com.traffic.server.control.command;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record ControlCommandStatusResponse(
        UUID commandId,
        ControlLifecycleStatus lifecycleStatus,
        ControlDispatchStatus dispatchStatus,
        ControlExecutionStatus executionStatus,
        ControlObservationStatus observationStatus,
        String expectedRunId,
        String acceptedRunId,
        Instant createdAt,
        Instant updatedAt,
        Map<String, String> error,
        Map<String, String> links
) {
    public static ControlCommandStatusResponse from(ControlCommandEntity e) {
        Map<String, String> err = null;
        if (e.getErrorCode() != null) {
            err = Map.of(
                    "code", e.getErrorCode(),
                    "message", e.getErrorSafeDetail() != null ? e.getErrorSafeDetail() : e.getErrorCode());
        }
        return new ControlCommandStatusResponse(
                e.getCommandId(),
                e.getLifecycleStatus(),
                e.getDispatchStatus(),
                e.getExecutionStatus(),
                e.getObservationStatus(),
                e.getExpectedRunId(),
                e.getAcceptedRunId(),
                e.getCreatedAt(),
                e.getUpdatedAt(),
                err,
                Map.of("self", "/api/control/commands/" + e.getCommandId()));
    }
}
