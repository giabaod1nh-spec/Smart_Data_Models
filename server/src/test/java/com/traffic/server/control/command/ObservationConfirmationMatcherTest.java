package com.traffic.server.control.command;

import com.traffic.server.payload.*;
import com.traffic.server.service.RealtimeAggregateService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ObservationConfirmationMatcherTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    RealtimeAggregateService aggregateService;

    ObservationConfirmationMatcher matcher;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        matcher = new ObservationConfirmationMatcher(aggregateService, 45);
    }

    @Test
    void confirmsForcePhaseWhenOrionMatches() throws Exception {
        ControlCommandEntity entity = entity(
                ControlCommandType.FORCE_PHASE,
                target("A"),
                payloadPhase("EW_GREEN"),
                "run-1");
        when(aggregateService.getIntersectionAggregate(eq("A"), eq("run-1")))
                .thenReturn(aggregate("run-1", 100.0, "EW_GREEN", null, null));

        ObjectNode python = MAPPER.createObjectNode();
        python.set("result", MAPPER.createObjectNode().put("appliedSimulationTime", 99.0));

        assertThat(matcher.match(entity, python)).isEqualTo(ObservationConfirmationMatcher.Outcome.CONFIRMED);
    }

    @Test
    void mismatchWhenPhaseDiffers() throws Exception {
        ControlCommandEntity entity = entity(
                ControlCommandType.FORCE_PHASE,
                target("A"),
                payloadPhase("EW_GREEN"),
                "run-1");
        when(aggregateService.getIntersectionAggregate(eq("A"), eq("run-1")))
                .thenReturn(aggregate("run-1", 100.0, "NS_GREEN", null, null));

        ObjectNode python = MAPPER.createObjectNode();
        python.set("result", MAPPER.createObjectNode().put("appliedSimulationTime", 99.0));

        assertThat(matcher.match(entity, python)).isEqualTo(ObservationConfirmationMatcher.Outcome.MISMATCH);
    }

    @Test
    void confirmsGreenDuration() throws Exception {
        ControlCommandEntity entity = entity(
                ControlCommandType.SET_GREEN_DURATION,
                target("A"),
                payloadSeconds(45),
                "run-1");
        when(aggregateService.getIntersectionAggregate(eq("A"), eq("run-1")))
                .thenReturn(aggregate("run-1", 50.0, "NS_GREEN", 45, "normal"));

        ObjectNode python = MAPPER.createObjectNode();
        python.set("result", MAPPER.createObjectNode().put("appliedSimulationTime", 49.0));

        assertThat(matcher.match(entity, python)).isEqualTo(ObservationConfirmationMatcher.Outcome.CONFIRMED);
    }

    private static ControlCommandEntity entity(
            ControlCommandType type, ObjectNode target, ObjectNode payload, String runId) {
        ControlCommandEntity e = new ControlCommandEntity();
        e.setCommandId(UUID.randomUUID());
        e.setCommandType(type);
        e.setTargetJson(target.toString());
        e.setPayloadJson(payload.toString());
        e.setAcceptedRunId(runId);
        e.setUpdatedAt(Instant.now());
        return e;
    }

    private static ObjectNode target(String intersectionId) {
        ObjectNode t = MAPPER.createObjectNode();
        t.put("intersectionId", intersectionId);
        return t;
    }

    private static ObjectNode payloadPhase(String phase) {
        ObjectNode p = MAPPER.createObjectNode();
        p.put("phase", phase);
        return p;
    }

    private static ObjectNode payloadSeconds(int seconds) {
        ObjectNode p = MAPPER.createObjectNode();
        p.put("seconds", seconds);
        return p;
    }

    private static RealtimeIntersectionResponse aggregate(
            String runId, double simTime, String phase, Integer green, String scenario) {
        TrafficLightResponse tl = TrafficLightResponse.builder()
                .simulationRunId(runId)
                .simulationTime(simTime)
                .currentPhase(phase)
                .greenDurationCurrent(green)
                .scenarioId(scenario)
                .build();
        RealtimeMetadata meta = RealtimeMetadata.builder().consistent(true).build();
        return RealtimeIntersectionResponse.builder()
                .trafficLights(List.of(tl))
                .metadata(meta)
                .build();
    }
}
