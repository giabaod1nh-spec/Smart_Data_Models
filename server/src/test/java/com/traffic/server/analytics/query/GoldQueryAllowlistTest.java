package com.traffic.server.analytics.query;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GoldQueryAllowlistTest {

    @Test
    void comparisonMetricsMatchGoldTransformationContract() {
        assertEquals(Set.of(
                "AVG_SPEED_KMH",
                "AVG_QUEUE_LENGTH_M",
                "MAX_QUEUE_LENGTH_M",
                "AVG_OCCUPANCY_PCT",
                "AVG_VEHICLE_COUNT",
                "AVG_ARRIVAL_RATE_PCU_PER_SEC"), GoldQueryAllowlist.COMPARISON_METRICS);
    }
}
