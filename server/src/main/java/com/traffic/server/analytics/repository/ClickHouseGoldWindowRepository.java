package com.traffic.server.analytics.repository;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.dto.comparison.TrafficComparisonDto;
import com.traffic.server.analytics.dto.direction.DirectionWindowDto;
import com.traffic.server.analytics.dto.intersection.IntersectionWindowDto;
import com.traffic.server.analytics.dto.network.NetworkWindowDto;
import com.traffic.server.analytics.mapper.GoldAnalyticsMapper;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;
import com.traffic.server.analytics.query.GoldQueryAllowlist;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseGoldWindowRepository implements GoldWindowRepository {

    private static final String INTERSECTION_PROJECTION = """
            simulation_run_id, scenario_id, namespace, intersection_id, window_id,
            window_size_sec, window_start_sim_sec, window_end_sim_sec,
            avg_total_vehicle_count, max_total_vehicle_count, latest_total_vehicle_count,
            latest_overall_traffic_status, latest_derived_traffic_state, latest_phase,
            incident_occurrence, spillback_occurrence, box_blocked_occurrence,
            quality_status, quality_flags, analytical_freshness_status,
            revision_seq, computed_at
            """;

    private static final String DIRECTION_PROJECTION = """
            simulation_run_id, scenario_id, namespace, intersection_id, direction, source_direction,
            window_id, window_size_sec, window_start_sim_sec, window_end_sim_sec,
            avg_vehicle_count, max_vehicle_count, latest_vehicle_count,
            avg_pcu_equivalent, avg_speed_kmh, avg_queue_length_m, max_queue_length_m,
            latest_queue_length_m, avg_occupancy_pct, spillback_ratio_pct, latest_traffic_status,
            quality_status, quality_flags, analytical_freshness_status,
            revision_seq, computed_at
            """;

    private static final String COMPARISON_PROJECTION = """
            simulation_run_id, scenario_id, namespace, intersection_id, direction, metric_code,
            current_window_id, current_window_size_sec, current_window_start_sim_sec, current_window_end_sim_sec,
            previous_window_id, previous_window_start_sim_sec, previous_window_end_sim_sec,
            current_value, previous_value, absolute_change, percent_change,
            change_direction, comparison_status,
            quality_status, quality_flags, analytical_freshness_status,
            revision_seq, computed_at
            """;

    private static final String NETWORK_PROJECTION = """
            simulation_run_id, scenario_id, namespace, window_id, window_size_sec,
            window_start_sim_sec, window_end_sim_sec, avg_total_vehicle_count,
            latest_overall_traffic_status, quality_status, analytical_freshness_status,
            revision_seq, computed_at
            """;

    private final ClickHouseQueryExecutor executor;

    public ClickHouseGoldWindowRepository(ClickHouseQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public List<IntersectionWindowDto> findIntersectionWindows(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition) {
        SqlQuery q = baseWindowQuery(
                "gold_mart_intersection_window_summary",
                INTERSECTION_PROJECTION,
                criteria,
                definition,
                "intersection_id ASC, window_id ASC, revision_seq DESC");
        return executor.queryMapped(q.sql(), GoldAnalyticsMapper::toIntersectionWindow, q.params());
    }

    @Override
    public List<DirectionWindowDto> findDirectionWindows(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition) {
        SqlQuery q = baseWindowQuery(
                "gold_mart_direction_window_summary",
                DIRECTION_PROJECTION,
                criteria,
                definition,
                "intersection_id ASC, direction ASC, window_id ASC, revision_seq DESC");
        if (criteria.direction().isPresent()) {
            q = q.and("direction = ?", criteria.direction().get());
        }
        return executor.queryMapped(q.sql(), GoldAnalyticsMapper::toDirectionWindow, q.params());
    }

    @Override
    public List<TrafficComparisonDto> findComparisons(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition) {
        SqlQuery q = new SqlQuery(
                "SELECT " + COMPARISON_PROJECTION + " FROM gold_fact_traffic_comparison WHERE namespace = ?",
                GoldQueryAllowlist.NAMESPACE_LIVE);
        q = q.and("simulation_run_id = ?", criteria.simulationRunId())
                .and("scenario_id = ?", criteria.scenarioId())
                .and("definition_major = ?", definition.major())
                .and("definition_minor = ?", definition.minor());
        if (criteria.metricCode().isPresent()) {
            q = q.and("metric_code = ?", criteria.metricCode().get());
        }
        if (criteria.intersectionId().isPresent()) {
            q = q.and("intersection_id = ?", criteria.intersectionId().get());
        }
        if (criteria.direction().isPresent()) {
            q = q.and("direction = ?", criteria.direction().get());
        }
        if (criteria.windowSizeSec() > 0) {
            q = q.and("current_window_size_sec = ?", criteria.windowSizeSec());
        }
        if (criteria.fromSimulationSec().isPresent()) {
            q = q.and("current_window_start_sim_sec >= ?", criteria.fromSimulationSec().get());
        }
        if (criteria.toSimulationSec().isPresent()) {
            q = q.and("current_window_end_sim_sec < ?", criteria.toSimulationSec().get());
        }
        q = q.orderBy("intersection_id ASC, direction ASC, metric_code ASC, current_window_start_sim_sec ASC, revision_seq DESC")
                .limit(criteria.limitPlusOne())
                .offset(criteria.offset());
        return executor.queryMapped(q.sql(), GoldAnalyticsMapper::toTrafficComparison, q.params());
    }

    @Override
    public List<NetworkWindowDto> findNetworkWindows(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition) {
        SqlQuery q = baseWindowQuery(
                "gold_mart_network_window_overview",
                NETWORK_PROJECTION,
                criteria,
                definition,
                "window_id ASC, revision_seq DESC");
        return executor.queryMapped(q.sql(), GoldAnalyticsMapper::toNetworkWindow, q.params());
    }

    private static SqlQuery baseWindowQuery(
            String table,
            String projection,
            AnalyticsQueryCriteria criteria,
            AnalyticsProperties.Definition definition,
            String orderBy) {
        SqlQuery q = new SqlQuery(
                "SELECT " + projection + " FROM " + table + " WHERE namespace = ?",
                GoldQueryAllowlist.NAMESPACE_LIVE);
        q = q.and("simulation_run_id = ?", criteria.simulationRunId())
                .and("scenario_id = ?", criteria.scenarioId())
                .and("window_size_sec = ?", criteria.windowSizeSec())
                .and("definition_major = ?", definition.major())
                .and("definition_minor = ?", definition.minor());
        if (criteria.intersectionId().isPresent()) {
            q = q.and("intersection_id = ?", criteria.intersectionId().get());
        }
        if (criteria.fromSimulationSec().isPresent()) {
            q = q.and("window_start_sim_sec >= ?", criteria.fromSimulationSec().get());
        }
        if (criteria.toSimulationSec().isPresent()) {
            q = q.and("window_end_sim_sec < ?", criteria.toSimulationSec().get());
        }
        return q.orderBy(orderBy).limit(criteria.limitPlusOne()).offset(criteria.offset());
    }

    static final class SqlQuery {
        private final StringBuilder sql;
        private final List<Object> params = new ArrayList<>();
        private String orderBy;
        private Integer limit;
        private Integer offset;

        SqlQuery(String base, Object... initialParams) {
            this.sql = new StringBuilder(base);
            for (Object p : initialParams) {
                params.add(p);
            }
        }

        SqlQuery and(String clause, Object value) {
            sql.append(" AND ").append(clause);
            params.add(value);
            return this;
        }

        SqlQuery orderBy(String order) {
            this.orderBy = order;
            return this;
        }

        SqlQuery limit(int n) {
            this.limit = n;
            return this;
        }

        SqlQuery offset(int n) {
            this.offset = n;
            return this;
        }

        String sql() {
            StringBuilder out = new StringBuilder(sql);
            if (orderBy != null) {
                out.append(" ORDER BY ").append(orderBy);
            }
            if (limit != null) {
                out.append(" LIMIT ?");
                params.add(limit);
            }
            if (offset != null && offset > 0) {
                out.append(" OFFSET ?");
                params.add(offset);
            }
            return out.toString();
        }

        Object[] params() {
            return params.toArray();
        }
    }
}
