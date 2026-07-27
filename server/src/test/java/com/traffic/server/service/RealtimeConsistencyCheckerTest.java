package com.traffic.server.service;

import com.traffic.server.config.AppProperties;
import com.traffic.server.payload.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealtimeConsistencyCheckerTest {

    private final RealtimeConsistencyChecker checker = new RealtimeConsistencyChecker(
            new AppProperties(
                    new AppProperties.ControlApi("http://localhost:9090", 5000, 65536),
                    new AppProperties.ContextProvider("http://localhost:3004/x"),
                    new AppProperties.Realtime(150L, 0.0001),
                    new AppProperties.Security(new AppProperties.Security.Admin("admin", "admin123"))
            ));

    @Test
    void consistentWhenMetadataMatches() {
        IntersectionResponse intersection = baseIntersection();
        TrafficLightResponse tl = trafficLightMatching(intersection);
        RealtimeMetadata meta = checker.buildMetadata(
                intersection, List.of(tl), List.of(), List.of());
        assertTrue(meta.getConsistent());
    }

    @Test
    void inconsistentWhenSimulationTimeDiffers() {
        IntersectionResponse intersection = baseIntersection();
        TrafficLightResponse tl = trafficLightMatching(intersection);
        tl.setSimulationTime(999.0);
        RealtimeMetadata meta = checker.buildMetadata(
                intersection, List.of(tl), List.of(), List.of());
        assertFalse(meta.getConsistent());
        assertTrue(meta.getConsistencyIssues().stream()
                .anyMatch(s -> s.contains("simulationTime")));
    }

    private static IntersectionResponse baseIntersection() {
        return IntersectionResponse.builder()
                .id("urn:ngsi-ld:Intersection:A")
                .simulationRunId("run-1")
                .simulationTime(120.5)
                .scenarioId("normal")
                .build();
    }

    private static TrafficLightResponse trafficLightMatching(IntersectionResponse intersection) {
        return TrafficLightResponse.builder()
                .id("urn:ngsi-ld:TrafficLight:A-North")
                .simulationRunId(intersection.getSimulationRunId())
                .simulationTime(intersection.getSimulationTime())
                .scenarioId(intersection.getScenarioId())
                .build();
    }
}
