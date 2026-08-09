package com.traffic.server.control.command;

import com.traffic.server.config.AppProperties;
import com.traffic.server.payload.ApiResponse;
import com.traffic.server.payload.ProjectorCurrentRunResponse;
import com.traffic.server.service.ProjectorClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;

/** RC-6 — legacy routes adapted into canonical command domain. */
@RestController
@RequestMapping("/api/control")
@ConditionalOnProperty(name = "app.control.command-domain-enabled", havingValue = "true")
@ConditionalOnProperty(name = "app.control.compatibility-adapters-enabled", havingValue = "true")
public class LegacyControlAdapterController {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ControlCommandService commandService;
    private final ControlCommandDispatchService dispatchService;
    private final ProjectorClient projectorClient;
    private final boolean adaptersEnabled;

    public LegacyControlAdapterController(
            ControlCommandService commandService,
            ControlCommandDispatchService dispatchService,
            ProjectorClient projectorClient,
            AppProperties appProperties) {
        this.commandService = commandService;
        this.dispatchService = dispatchService;
        this.projectorClient = projectorClient;
        this.adaptersEnabled = appProperties.control().compatibilityAdaptersEnabled();
    }

    @PostMapping("/phase")
    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> phase(
            @RequestBody Map<String, String> body,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            Authentication auth) {
        return adapt(
                ControlCommandType.FORCE_PHASE,
                target(body.get("intersection_id")),
                payload("phase", body.get("phase")),
                requestId,
                auth,
                "legacy-phase");
    }

    @PostMapping("/green-duration")
    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> greenDuration(
            @RequestBody Map<String, Object> body,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            Authentication auth) {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("seconds", ((Number) body.get("seconds")).intValue());
        return adapt(
                ControlCommandType.SET_GREEN_DURATION,
                target(String.valueOf(body.get("intersection_id"))),
                payload,
                requestId,
                auth,
                "legacy-green");
    }

    @PostMapping("/scenario")
    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> scenario(
            @RequestBody Map<String, String> body,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            Authentication auth) {
        ObjectNode target = MAPPER.createObjectNode();
        if (body.get("target_intersection") != null) {
            target.put("intersectionId", body.get("target_intersection"));
        }
        if (body.get("target_direction") != null) {
            target.put("direction", body.get("target_direction"));
        }
        return adapt(
                ControlCommandType.SET_SCENARIO,
                target,
                payload("scenario", body.get("scenario")),
                requestId,
                auth,
                "legacy-scenario");
    }

    @PostMapping("/demand-profile")
    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> demandProfile(
            @RequestBody Map<String, String> body,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            Authentication auth) {
        return adapt(
                ControlCommandType.SET_DEMAND_PROFILE,
                MAPPER.createObjectNode(),
                payload("profile", body.get("profile")),
                requestId,
                auth,
                "legacy-demand");
    }

    @PostMapping("/overlays")
    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> addOverlay(
            @RequestBody Map<String, Object> body,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            Authentication auth) {
        ObjectNode target = target(String.valueOf(body.get("intersection_id")));
        ObjectNode payload = MAPPER.createObjectNode();
        putIfPresent(payload, "overlayType", body.get("overlay_type"));
        putIfPresent(payload, "direction", body.get("direction"));
        putIfPresent(payload, "segmentRole", body.get("segment_role"));
        putIfPresent(payload, "targetEdge", body.get("target_edge"));
        putIfPresent(payload, "durationS", body.get("duration_s"));
        putIfPresent(payload, "overlayId", body.get("overlay_id"));
        return adapt(
                ControlCommandType.ADD_OVERLAY,
                target,
                payload,
                requestId,
                auth,
                "legacy-overlay-add");
    }

    @DeleteMapping("/overlays/{overlayId}")
    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> removeOverlay(
            @PathVariable String overlayId,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            Authentication auth) {
        ObjectNode target = MAPPER.createObjectNode();
        target.put("overlayId", overlayId);
        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("overlayId", overlayId);
        return adapt(
                ControlCommandType.REMOVE_OVERLAY,
                target,
                payload,
                requestId,
                auth,
                "legacy-overlay-remove");
    }

    @PostMapping("/control-mode")
    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> controlMode(
            @RequestBody Map<String, String> body,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            Authentication auth) {
        return adapt(
                ControlCommandType.SET_CONTROL_MODE,
                MAPPER.createObjectNode(),
                payload("mode", body.get("mode")),
                requestId,
                auth,
                "legacy-control-mode");
    }

    private ResponseEntity<ApiResponse<ControlCommandStatusResponse>> adapt(
            ControlCommandType type,
            ObjectNode target,
            ObjectNode payload,
            String requestId,
            Authentication auth,
            String idempotencyPrefix) {
        if (!adaptersEnabled || !commandService.isEnabled()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "compatibility adapter disabled");
        }
        String runId = resolveRunId();
        Instant now = Instant.now();
        CreateControlCommandRequest req = new CreateControlCommandRequest(
                "1.0",
                UUID.randomUUID(),
                type,
                target,
                payload,
                runId,
                idempotencyPrefix + "-" + UUID.randomUUID(),
                now,
                now.plus(5, ChronoUnit.MINUTES),
                "DASHBOARD");
        String operator = auth != null ? auth.getName() : "anonymous";
        String corr = requestId != null ? requestId : UUID.randomUUID().toString();
        ControlCommandService.AcceptResult accepted = commandService.accept(req, operator);
        ControlCommandStatusResponse body = accepted.status();
        if (accepted.newlyCreated()) {
            body = dispatchService.dispatchIfNeeded(body.commandId(), corr);
        }
        return ResponseEntity.accepted()
                .body(ApiResponse.success(body));
    }

    private String resolveRunId() {
        ProjectorClient.CurrentRunResult result = projectorClient.fetchCurrentRun();
        if (result instanceof ProjectorClient.CurrentRunResult.Ok ok) {
            ProjectorCurrentRunResponse run = ok.body();
            if (run.simulationRunId() != null && !run.simulationRunId().isBlank()) {
                return run.simulationRunId();
            }
        }
        throw new ResponseStatusException(HttpStatus.CONFLICT, "STALE_RUN");
    }

    private static ObjectNode target(String intersectionId) {
        ObjectNode t = MAPPER.createObjectNode();
        if (intersectionId != null && !"null".equals(intersectionId)) {
            t.put("intersectionId", intersectionId);
        }
        return t;
    }

    private static ObjectNode payload(String key, String value) {
        ObjectNode p = MAPPER.createObjectNode();
        if (value != null) {
            p.put(key, value);
        }
        return p;
    }

    private static void putIfPresent(ObjectNode node, String field, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof Number n) {
            node.put(field, n.doubleValue());
        } else {
            node.put(field, String.valueOf(value));
        }
    }
}
