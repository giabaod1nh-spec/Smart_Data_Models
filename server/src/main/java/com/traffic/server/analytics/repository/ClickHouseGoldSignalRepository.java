package com.traffic.server.analytics.repository;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.dto.signal.SignalOperationWindowDto;
import com.traffic.server.analytics.mapper.GoldAnalyticsMapper;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;
import com.traffic.server.analytics.query.GoldQueryAllowlist;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseGoldSignalRepository implements GoldSignalRepository {

    private static final String SIGNAL_PROJECTION = """
            simulation_run_id, scenario_id, namespace, intersection_id, direction, window_id,
            window_size_sec, window_start_sim_sec, window_end_sim_sec,
            observation_count, green_observation_count, red_observation_count,
            yellow_observation_count, other_status_count,
            green_share_pct, red_share_pct, yellow_share_pct,
            dominant_signal_status, dominant_phase, latest_timing_mode,
            avg_configured_green_duration_sec,
            quality_status, quality_flags, analytical_freshness_status,
            revision_seq, computed_at
            """;

    private final ClickHouseQueryExecutor executor;

    public ClickHouseGoldSignalRepository(ClickHouseQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public List<SignalOperationWindowDto> findSignalWindows(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition) {
        StringBuilder sql = new StringBuilder(
                "SELECT " + SIGNAL_PROJECTION + " FROM gold_mart_signal_operation_window WHERE namespace = ?");
        List<Object> params = new ArrayList<>();
        params.add(GoldQueryAllowlist.NAMESPACE_LIVE);
        append(sql, params, "simulation_run_id = ?", criteria.simulationRunId());
        append(sql, params, "scenario_id = ?", criteria.scenarioId());
        append(sql, params, "window_size_sec = ?", criteria.windowSizeSec());
        append(sql, params, "definition_major = ?", definition.major());
        append(sql, params, "definition_minor = ?", definition.minor());
        criteria.intersectionId().ifPresent(id -> append(sql, params, "intersection_id = ?", id));
        criteria.direction().ifPresent(d -> append(sql, params, "direction = ?", d));
        criteria.fromSimulationSec().ifPresent(v -> append(sql, params, "window_start_sim_sec >= ?", v));
        criteria.toSimulationSec().ifPresent(v -> append(sql, params, "window_end_sim_sec < ?", v));
        sql.append(" ORDER BY intersection_id ASC, direction ASC, window_id ASC, revision_seq DESC");
        sql.append(" LIMIT ?");
        params.add(criteria.limitPlusOne());
        if (criteria.offset() > 0) {
            sql.append(" OFFSET ?");
            params.add(criteria.offset());
        }
        return executor.queryMapped(
                sql.toString(), GoldAnalyticsMapper::toSignalOperationWindow, params.toArray());
    }

    private static void append(StringBuilder sql, List<Object> params, String clause, Object value) {
        sql.append(" AND ").append(clause);
        params.add(value);
    }
}
