package com.traffic.server.analytics.mapper;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class GoldAnalyticsMapperTest {

    @Test
    void mapsIntersectionWindowWithoutSourceSetHash() {
        Instant computed = Instant.parse("2026-08-05T10:00:00Z");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("simulation_run_id", "run-1");
        row.put("scenario_id", "normal");
        row.put("namespace", "live");
        row.put("intersection_id", "int-1");
        row.put("window_id", "w1");
        row.put("window_size_sec", 60);
        row.put("window_start_sim_sec", 0.0);
        row.put("window_end_sim_sec", 60.0);
        row.put("avg_total_vehicle_count", 12.5);
        row.put("max_total_vehicle_count", 20);
        row.put("latest_total_vehicle_count", null);
        row.put("latest_overall_traffic_status", "MODERATE");
        row.put("latest_derived_traffic_state", "FLOWING");
        row.put("latest_phase", "G1");
        row.put("incident_occurrence", 0);
        row.put("spillback_occurrence", 1);
        row.put("box_blocked_occurrence", 0);
        row.put("quality_status", "VALID");
        row.put("quality_flags", "");
        row.put("analytical_freshness_status", "FRESH");
        row.put("revision_seq", 3L);
        row.put("computed_at", Timestamp.from(computed));
        row.put("source_set_hash", "abc");

        var dto = GoldAnalyticsMapper.toIntersectionWindow(row);
        assertEquals("run-1", dto.simulationRunId());
        assertEquals(60, dto.windowSizeSec());
        assertNull(dto.latestTotalVehicleCount());
        assertEquals(computed, dto.computedAt());
    }
}
