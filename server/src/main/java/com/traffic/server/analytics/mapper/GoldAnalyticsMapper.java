package com.traffic.server.analytics.mapper;

import com.traffic.server.analytics.dto.comparison.TrafficComparisonDto;
import com.traffic.server.analytics.dto.congestion.CongestionWindowDto;
import com.traffic.server.analytics.dto.direction.DirectionWindowDto;
import com.traffic.server.analytics.dto.intersection.IntersectionWindowDto;
import com.traffic.server.analytics.dto.network.NetworkWindowDto;
import com.traffic.server.analytics.dto.priority.PriorityRankingDto;
import com.traffic.server.analytics.dto.signal.SignalOperationWindowDto;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

/** Pass-through row mapper; no Gold calculation (BS-4). */
public final class GoldAnalyticsMapper {

    private GoldAnalyticsMapper() {}

    public static IntersectionWindowDto toIntersectionWindow(Map<String, Object> row) {
        return new IntersectionWindowDto(
                str(row, "simulation_run_id"),
                str(row, "scenario_id"),
                str(row, "namespace"),
                str(row, "intersection_id"),
                str(row, "window_id"),
                intVal(row, "window_size_sec"),
                dbl(row, "window_start_sim_sec"),
                dbl(row, "window_end_sim_sec"),
                dbl(row, "avg_total_vehicle_count"),
                intObj(row, "max_total_vehicle_count"),
                intObj(row, "latest_total_vehicle_count"),
                str(row, "latest_overall_traffic_status"),
                str(row, "latest_derived_traffic_state"),
                str(row, "latest_phase"),
                intVal(row, "incident_occurrence"),
                intVal(row, "spillback_occurrence"),
                intVal(row, "box_blocked_occurrence"),
                str(row, "quality_status"),
                str(row, "quality_flags"),
                str(row, "analytical_freshness_status"),
                longVal(row, "revision_seq"),
                instant(row, "computed_at"));
    }

    public static DirectionWindowDto toDirectionWindow(Map<String, Object> row) {
        return new DirectionWindowDto(
                str(row, "simulation_run_id"),
                str(row, "scenario_id"),
                str(row, "namespace"),
                str(row, "intersection_id"),
                str(row, "direction"),
                str(row, "source_direction"),
                str(row, "window_id"),
                intVal(row, "window_size_sec"),
                dbl(row, "window_start_sim_sec"),
                dbl(row, "window_end_sim_sec"),
                dblObj(row, "avg_vehicle_count"),
                intObj(row, "max_vehicle_count"),
                intObj(row, "latest_vehicle_count"),
                dblObj(row, "avg_pcu_equivalent"),
                dblObj(row, "avg_speed_kmh"),
                dblObj(row, "avg_queue_length_m"),
                dblObj(row, "max_queue_length_m"),
                dblObj(row, "latest_queue_length_m"),
                dblObj(row, "avg_occupancy_pct"),
                dblObj(row, "spillback_ratio_pct"),
                strNullable(row, "latest_traffic_status"),
                str(row, "quality_status"),
                str(row, "quality_flags"),
                str(row, "analytical_freshness_status"),
                longVal(row, "revision_seq"),
                instant(row, "computed_at"));
    }

    public static TrafficComparisonDto toTrafficComparison(Map<String, Object> row) {
        return new TrafficComparisonDto(
                str(row, "simulation_run_id"),
                str(row, "scenario_id"),
                str(row, "namespace"),
                str(row, "intersection_id"),
                str(row, "direction"),
                str(row, "metric_code"),
                str(row, "current_window_id"),
                intVal(row, "current_window_size_sec"),
                dbl(row, "current_window_start_sim_sec"),
                dbl(row, "current_window_end_sim_sec"),
                str(row, "previous_window_id"),
                dbl(row, "previous_window_start_sim_sec"),
                dbl(row, "previous_window_end_sim_sec"),
                dblObj(row, "current_value"),
                dblObj(row, "previous_value"),
                dblObj(row, "absolute_change"),
                dblObj(row, "percent_change"),
                str(row, "change_direction"),
                str(row, "comparison_status"),
                str(row, "quality_status"),
                str(row, "quality_flags"),
                str(row, "analytical_freshness_status"),
                longVal(row, "revision_seq"),
                instant(row, "computed_at"));
    }

    public static CongestionWindowDto toCongestionWindow(Map<String, Object> row) {
        return new CongestionWindowDto(
                str(row, "simulation_run_id"),
                str(row, "scenario_id"),
                str(row, "namespace"),
                str(row, "intersection_id"),
                str(row, "direction"),
                str(row, "window_id"),
                intVal(row, "window_size_sec"),
                dbl(row, "window_start_sim_sec"),
                dbl(row, "window_end_sim_sec"),
                str(row, "metric_code"),
                str(row, "metric_version"),
                dblObj(row, "numeric_value"),
                str(row, "unit_code"),
                str(row, "status"),
                str(row, "explanation_json"),
                str(row, "quality_status"),
                str(row, "quality_flags"),
                str(row, "analytical_freshness_status"),
                longVal(row, "revision_seq"),
                instant(row, "computed_at"));
    }

    public static PriorityRankingDto toPriorityRanking(Map<String, Object> row) {
        return new PriorityRankingDto(
                str(row, "simulation_run_id"),
                str(row, "scenario_id"),
                str(row, "namespace"),
                str(row, "intersection_id"),
                strNullable(row, "direction"),
                str(row, "window_id"),
                intVal(row, "window_size_sec"),
                dbl(row, "window_start_sim_sec"),
                dbl(row, "window_end_sim_sec"),
                dblObj(row, "priority_score"),
                dblObj(row, "priority_rank"),
                strNullable(row, "score_status"),
                strNullable(row, "rank_status"),
                strNullable(row, "explanation_json"),
                str(row, "quality_status"),
                str(row, "quality_flags"),
                str(row, "analytical_freshness_status"),
                longVal(row, "revision_seq"),
                instant(row, "computed_at"));
    }

    public static SignalOperationWindowDto toSignalOperationWindow(Map<String, Object> row) {
        return new SignalOperationWindowDto(
                str(row, "simulation_run_id"),
                str(row, "scenario_id"),
                str(row, "namespace"),
                str(row, "intersection_id"),
                str(row, "direction"),
                str(row, "window_id"),
                intVal(row, "window_size_sec"),
                dbl(row, "window_start_sim_sec"),
                dbl(row, "window_end_sim_sec"),
                longVal(row, "observation_count"),
                longVal(row, "green_observation_count"),
                longVal(row, "red_observation_count"),
                longVal(row, "yellow_observation_count"),
                longVal(row, "other_status_count"),
                dblObj(row, "green_share_pct"),
                dblObj(row, "red_share_pct"),
                dblObj(row, "yellow_share_pct"),
                str(row, "dominant_signal_status"),
                str(row, "dominant_phase"),
                strNullable(row, "latest_timing_mode"),
                dblObj(row, "avg_configured_green_duration_sec"),
                str(row, "quality_status"),
                str(row, "quality_flags"),
                str(row, "analytical_freshness_status"),
                longVal(row, "revision_seq"),
                instant(row, "computed_at"));
    }

    public static NetworkWindowDto toNetworkWindow(Map<String, Object> row) {
        return new NetworkWindowDto(
                str(row, "simulation_run_id"),
                str(row, "scenario_id"),
                str(row, "namespace"),
                str(row, "window_id"),
                intVal(row, "window_size_sec"),
                dbl(row, "window_start_sim_sec"),
                dbl(row, "window_end_sim_sec"),
                dblObj(row, "avg_total_vehicle_count"),
                strNullable(row, "latest_overall_traffic_status"),
                str(row, "quality_status"),
                str(row, "analytical_freshness_status"),
                longVal(row, "revision_seq"),
                instant(row, "computed_at"));
    }

    private static String str(Map<String, Object> row, String col) {
        Object v = row.get(col);
        return v == null ? "" : String.valueOf(v).trim();
    }

    private static String strNullable(Map<String, Object> row, String col) {
        Object v = row.get(col);
        return v == null ? null : String.valueOf(v).trim();
    }

    private static int intVal(Map<String, Object> row, String col) {
        Object v = row.get(col);
        if (v instanceof Number n) {
            return n.intValue();
        }
        return Integer.parseInt(String.valueOf(v));
    }

    private static Integer intObj(Map<String, Object> row, String col) {
        Object v = row.get(col);
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return n.intValue();
        }
        return Integer.parseInt(String.valueOf(v));
    }

    private static long longVal(Map<String, Object> row, String col) {
        Object v = row.get(col);
        if (v instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(String.valueOf(v));
    }

    private static double dbl(Map<String, Object> row, String col) {
        Object v = row.get(col);
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        return Double.parseDouble(String.valueOf(v));
    }

    private static Double dblObj(Map<String, Object> row, String col) {
        Object v = row.get(col);
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        return Double.parseDouble(String.valueOf(v));
    }

    private static Instant instant(Map<String, Object> row, String col) {
        Object v = row.get(col);
        if (v instanceof Instant i) {
            return i;
        }
        if (v instanceof Timestamp ts) {
            return ts.toInstant();
        }
        if (v instanceof java.time.LocalDateTime ldt) {
            return ldt.atZone(java.time.ZoneOffset.UTC).toInstant();
        }
        return Instant.parse(String.valueOf(v));
    }
}
