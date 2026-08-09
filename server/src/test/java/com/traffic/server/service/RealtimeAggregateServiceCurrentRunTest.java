package com.traffic.server.service;

import com.traffic.server.payload.IntersectionResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RealtimeAggregateServiceCurrentRunTest {

    @Test
    void currentRunFilterExcludesShadowProbeAndOlderRunEntities() {
        IntersectionResponse currentA = intersection("urn:ngsi-ld:Intersection:A", "run-current");
        IntersectionResponse currentB = intersection("urn:ngsi-ld:Intersection:B", "run-current");
        IntersectionResponse old = intersection("urn:ngsi-ld:Intersection:C", "run-old");
        IntersectionResponse probe = intersection("urn:ngsi-ld:Intersection:BatchSpike:A", "probe-run");

        List<IntersectionResponse> result = RealtimeAggregateService.filterCurrentRun(
                List.of(old, currentA, probe, currentB), "run-current");

        assertThat(result).extracting(IntersectionResponse::getId)
                .containsExactly("urn:ngsi-ld:Intersection:A", "urn:ngsi-ld:Intersection:B");
    }

    @Test
    void missingActiveRunFailsClosedWithEmptyInventory() {
        assertThat(RealtimeAggregateService.filterCurrentRun(
                List.of(intersection("urn:ngsi-ld:Intersection:A", "run-a")), null))
                .isEmpty();
    }

    private static IntersectionResponse intersection(String id, String runId) {
        return IntersectionResponse.builder().id(id).simulationRunId(runId).build();
    }
}
