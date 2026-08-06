package com.traffic.server.analytics.repository;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.dto.congestion.CongestionWindowDto;
import com.traffic.server.analytics.dto.priority.PriorityRankingDto;
import com.traffic.server.analytics.mapper.GoldAnalyticsMapper;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;
import com.traffic.server.analytics.query.GoldQueryAllowlist;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseGoldKpiRepository implements GoldKpiRepository {

    private static final String CONGESTION_PROJECTION = """
            simulation_run_id, scenario_id, namespace, intersection_id, direction, window_id,
            window_size_sec, window_start_sim_sec, window_end_sim_sec,
            metric_code, metric_version, numeric_value, unit_code, status, explanation_json,
            quality_status, quality_flags, analytical_freshness_status,
            revision_seq, computed_at
            """;

    private final ClickHouseQueryExecutor executor;

    public ClickHouseGoldKpiRepository(ClickHouseQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public List<CongestionWindowDto> findCongestion(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition) {
        SqlBuilder q = windowBase("gold_mart_congestion_window", CONGESTION_PROJECTION, criteria, definition);
        q.and("metric_code = ?", GoldQueryAllowlist.METRIC_CONGESTION);
        q.orderBy("intersection_id ASC, direction ASC, window_id ASC, revision_seq DESC");
        q.page(criteria);
        return executor.queryMapped(q.sql(), GoldAnalyticsMapper::toCongestionWindow, q.params());
    }

    @Override
    public List<PriorityRankingDto> findPriority(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition) {
        String sql = """
                SELECT
                    simulation_run_id,
                    scenario_id,
                    namespace,
                    intersection_id,
                    any(direction) AS direction,
                    window_id,
                    window_size_sec,
                    window_start_sim_sec,
                    window_end_sim_sec,
                    maxIf(numeric_value, metric_code = 'INTERSECTION_PRIORITY_WINDOW') AS priority_score,
                    maxIf(numeric_value, metric_code = 'PRIORITY_RANK') AS priority_rank,
                    maxIf(status, metric_code = 'INTERSECTION_PRIORITY_WINDOW') AS score_status,
                    maxIf(status, metric_code = 'PRIORITY_RANK') AS rank_status,
                    maxIf(explanation_json, metric_code = 'INTERSECTION_PRIORITY_WINDOW') AS explanation_json,
                    any(quality_status) AS quality_status,
                    any(quality_flags) AS quality_flags,
                    any(analytical_freshness_status) AS analytical_freshness_status,
                    max(revision_seq) AS revision_seq,
                    max(computed_at) AS computed_at
                FROM gold_mart_priority_window_ranking
                WHERE namespace = ?
                  AND simulation_run_id = ?
                  AND scenario_id = ?
                  AND window_size_sec = ?
                  AND definition_major = ?
                  AND definition_minor = ?
                  AND metric_code IN ('INTERSECTION_PRIORITY_WINDOW', 'PRIORITY_RANK')
                """;
        SqlBuilder q = new SqlBuilder(sql,
                GoldQueryAllowlist.NAMESPACE_LIVE,
                criteria.simulationRunId(),
                criteria.scenarioId(),
                criteria.windowSizeSec(),
                definition.major(),
                definition.minor());
        if (criteria.intersectionId().isPresent()) {
            q.and("intersection_id = ?", criteria.intersectionId().get());
        }
        if (criteria.fromSimulationSec().isPresent()) {
            q.and("window_start_sim_sec >= ?", criteria.fromSimulationSec().get());
        }
        if (criteria.toSimulationSec().isPresent()) {
            q.and("window_end_sim_sec < ?", criteria.toSimulationSec().get());
        }
        q.groupBy("""
                simulation_run_id, scenario_id, namespace, intersection_id, window_id,
                window_size_sec, window_start_sim_sec, window_end_sim_sec
                """);
        q.orderBy("intersection_id ASC, window_id ASC, revision_seq DESC");
        q.page(criteria);
        return executor.queryMapped(q.sql(), GoldAnalyticsMapper::toPriorityRanking, q.params());
    }

    private static SqlBuilder windowBase(
            String table,
            String projection,
            AnalyticsQueryCriteria criteria,
            AnalyticsProperties.Definition definition) {
        SqlBuilder q = new SqlBuilder(
                "SELECT " + projection + " FROM " + table + " WHERE namespace = ?",
                GoldQueryAllowlist.NAMESPACE_LIVE);
        q.and("simulation_run_id = ?", criteria.simulationRunId())
                .and("scenario_id = ?", criteria.scenarioId())
                .and("window_size_sec = ?", criteria.windowSizeSec())
                .and("definition_major = ?", definition.major())
                .and("definition_minor = ?", definition.minor());
        if (criteria.intersectionId().isPresent()) {
            q.and("intersection_id = ?", criteria.intersectionId().get());
        }
        if (criteria.direction().isPresent()) {
            q.and("direction = ?", criteria.direction().get());
        }
        if (criteria.fromSimulationSec().isPresent()) {
            q.and("window_start_sim_sec >= ?", criteria.fromSimulationSec().get());
        }
        if (criteria.toSimulationSec().isPresent()) {
            q.and("window_end_sim_sec < ?", criteria.toSimulationSec().get());
        }
        return q;
    }

    static final class SqlBuilder {
        private final StringBuilder sql;
        private final List<Object> params = new ArrayList<>();
        private String groupBy;
        private String orderBy;
        private Integer limit;
        private Integer offset;

        SqlBuilder(String base, Object... initial) {
            sql = new StringBuilder(base);
            for (Object p : initial) {
                params.add(p);
            }
        }

        SqlBuilder and(String clause, Object value) {
            sql.append(" AND ").append(clause);
            params.add(value);
            return this;
        }

        SqlBuilder groupBy(String clause) {
            this.groupBy = clause;
            return this;
        }

        SqlBuilder orderBy(String clause) {
            this.orderBy = clause;
            return this;
        }

        void page(AnalyticsQueryCriteria criteria) {
            this.limit = criteria.limitPlusOne();
            this.offset = criteria.offset();
        }

        String sql() {
            StringBuilder out = new StringBuilder(sql);
            if (groupBy != null) {
                out.append(" GROUP BY ").append(groupBy);
            }
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
