package com.traffic.server.control.command;

import com.traffic.server.exception.RealtimeIdleException;
import com.traffic.server.exception.RealtimeRunConflictException;
import com.traffic.server.exception.RealtimeUnavailableException;
import com.traffic.server.payload.RealtimeIntersectionResponse;
import com.traffic.server.payload.TrafficLightResponse;
import com.traffic.server.service.RealtimeAggregateService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;

/** RC-5 — Orion read-path observation matcher (execution-independent). */
@Component
public class ObservationConfirmationMatcher {

    public enum Outcome {
        CONFIRMED,
        MISMATCH,
        TIMED_OUT,
        UNAVAILABLE,
        PENDING
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RealtimeAggregateService aggregateService;
    private final long observationTimeoutSec;

    public ObservationConfirmationMatcher(
            RealtimeAggregateService aggregateService,
            @Value("${app.control.observation-timeout-sec:45}") long observationTimeoutSec) {
        this.aggregateService = aggregateService;
        this.observationTimeoutSec = observationTimeoutSec;
    }

    public Outcome match(ControlCommandEntity entity, JsonNode pythonResult) {
        if (entity.getUpdatedAt() != null
                && Duration.between(entity.getUpdatedAt(), Instant.now()).getSeconds() > observationTimeoutSec) {
            return Outcome.TIMED_OUT;
        }
        try {
            JsonNode target = MAPPER.readTree(entity.getTargetJson());
            JsonNode payload = MAPPER.readTree(entity.getPayloadJson());
            String intersectionId = text(target, "intersectionId");
            if (intersectionId == null || intersectionId.isBlank()) {
                intersectionId = "A";
            }
            RealtimeIntersectionResponse aggregate =
                    aggregateService.getIntersectionAggregate(intersectionId, entity.getAcceptedRunId());
            TrafficLightResponse tl = firstTrafficLight(aggregate);
            if (tl == null) {
                return Outcome.UNAVAILABLE;
            }
            if (entity.getAcceptedRunId() != null
                    && tl.getSimulationRunId() != null
                    && !entity.getAcceptedRunId().equals(tl.getSimulationRunId())) {
                return Outcome.MISMATCH;
            }
            double appliedSim = appliedSimulationTime(pythonResult);
            if (tl.getSimulationTime() != null && appliedSim > 0
                    && tl.getSimulationTime() + 0.001 < appliedSim) {
                return Outcome.PENDING;
            }
            return switch (entity.getCommandType()) {
                case FORCE_PHASE -> matchPhase(payload, tl);
                case SET_GREEN_DURATION -> matchGreenDuration(payload, tl);
                case SET_SCENARIO -> matchScenario(payload, tl);
                default -> Outcome.PENDING;
            };
        } catch (RealtimeUnavailableException | RealtimeIdleException | RealtimeRunConflictException e) {
            return Outcome.UNAVAILABLE;
        } catch (Exception e) {
            return Outcome.UNAVAILABLE;
        }
    }

    private static Outcome matchPhase(JsonNode payload, TrafficLightResponse tl) {
        String expected = text(payload, "phase");
        if (expected == null || tl.getCurrentPhase() == null) {
            return Outcome.UNAVAILABLE;
        }
        return expected.equals(tl.getCurrentPhase()) ? Outcome.CONFIRMED : Outcome.MISMATCH;
    }

    private static Outcome matchGreenDuration(JsonNode payload, TrafficLightResponse tl) {
        JsonNode secondsNode = payload.get("seconds");
        if (secondsNode == null || !secondsNode.isNumber() || tl.getGreenDurationCurrent() == null) {
            return Outcome.UNAVAILABLE;
        }
        int expected = secondsNode.intValue();
        return expected == tl.getGreenDurationCurrent() ? Outcome.CONFIRMED : Outcome.MISMATCH;
    }

    private static Outcome matchScenario(JsonNode payload, TrafficLightResponse tl) {
        String expected = text(payload, "scenario");
        if (expected == null || tl.getScenarioId() == null) {
            return Outcome.UNAVAILABLE;
        }
        return expected.equals(tl.getScenarioId()) ? Outcome.CONFIRMED : Outcome.MISMATCH;
    }

    private static TrafficLightResponse firstTrafficLight(RealtimeIntersectionResponse aggregate) {
        if (aggregate.getTrafficLights() == null || aggregate.getTrafficLights().isEmpty()) {
            return null;
        }
        return aggregate.getTrafficLights().getFirst();
    }

    private static double appliedSimulationTime(JsonNode pythonResult) {
        if (pythonResult == null) {
            return 0;
        }
        JsonNode result = pythonResult.get("result");
        if (result == null || result.isNull()) {
            return 0;
        }
        JsonNode applied = result.get("appliedSimulationTime");
        if (applied != null && applied.isNumber()) {
            return applied.doubleValue();
        }
        return 0;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v != null && !v.isNull() ? v.asString() : null;
    }
}
