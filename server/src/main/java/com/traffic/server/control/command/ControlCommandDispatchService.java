package com.traffic.server.control.command;

import com.traffic.server.config.AppProperties;
import com.traffic.server.control.ControlApiClient;
import com.traffic.server.exception.ControlApiTimeoutException;
import com.traffic.server.exception.ControlApiUnavailableException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.UUID;

/** HTTP dispatch to Python Control API outside DB TX1 (RC4-T1). */
@Service
public class ControlCommandDispatchService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ControlCommandService commandService;
    private final ControlApiClient controlApiClient;
    private final ControlCommandMetrics metrics;

    public ControlCommandDispatchService(
            ControlCommandService commandService,
            ControlApiClient controlApiClient,
            ControlCommandMetrics metrics,
            AppProperties appProperties) {
        this.commandService = commandService;
        this.controlApiClient = controlApiClient;
        this.metrics = metrics;
    }

    public ControlCommandStatusResponse dispatchIfNeeded(UUID commandId, String requestId) {
        return commandService.findEntity(commandId)
                .filter(entity -> entity.getDispatchStatus() == ControlDispatchStatus.PENDING)
                .map(entity -> dispatch(entity, requestId))
                .orElseGet(() -> commandService.get(commandId).orElseThrow());
    }

    private ControlCommandStatusResponse dispatch(ControlCommandEntity entity, String requestId) {
        String body = buildEnvelope(entity);
        try {
            ResponseEntity<String> response =
                    controlApiClient.submitCommand(body, requestId);
            if (response.getStatusCode().isSameCodeAs(HttpStatus.ACCEPTED)
                    || response.getStatusCode().is2xxSuccessful()) {
                String acceptedRun = extractAcceptedRunId(response.getBody());
                commandService.recordDispatchAccepted(entity.getCommandId(), acceptedRun);
            } else if (response.getStatusCode().value() == 503) {
                commandService.recordDispatchFailed(entity.getCommandId(), "QUEUE_FULL", "queue full");
                metrics.dispatchFailure();
            } else {
                commandService.recordDispatchFailed(
                        entity.getCommandId(),
                        "UPSTREAM_UNAVAILABLE",
                        "dispatch rejected");
                metrics.dispatchFailure();
            }
        } catch (ControlApiTimeoutException e) {
            commandService.recordDispatchUnknown(entity.getCommandId());
        } catch (ControlApiUnavailableException e) {
            commandService.recordDispatchFailed(
                    entity.getCommandId(), "UPSTREAM_UNAVAILABLE", e.getMessage());
            metrics.dispatchFailure();
        }
        return commandService.get(entity.getCommandId()).orElseThrow();
    }

    private static String buildEnvelope(ControlCommandEntity entity) {
        try {
            ObjectNode root = MAPPER.createObjectNode();
            root.put("contractVersion", entity.getContractVersion());
            root.put("commandId", entity.getCommandId().toString());
            root.put("commandType", entity.getCommandType().name());
            root.set("target", MAPPER.readTree(entity.getTargetJson()));
            root.set("payload", MAPPER.readTree(entity.getPayloadJson()));
            root.put("expectedRunId", entity.getExpectedRunId());
            root.put("idempotencyKey", entity.getIdempotencyKey());
            root.put("requestedAt", entity.getRequestedAtUtc().toString());
            root.put("expiresAt", entity.getExpiresAtUtc().toString());
            root.put("source", entity.getSource());
            return MAPPER.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalStateException("failed to build command envelope", e);
        }
    }

    private static String extractAcceptedRunId(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            JsonNode node = MAPPER.readTree(body);
            JsonNode run = node.get("acceptedRunId");
            return run != null && !run.isNull() ? run.asString() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
