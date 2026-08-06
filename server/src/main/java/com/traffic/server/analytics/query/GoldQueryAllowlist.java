package com.traffic.server.analytics.query;

import java.util.Set;

/** Approved metric codes, directions and comparison inventory (BS-0 freeze). */
public final class GoldQueryAllowlist {

    public static final String NAMESPACE_LIVE = "live";
    public static final String METRIC_CONGESTION = "CONGESTION_SCORE_WINDOW";
    public static final String METRIC_PRIORITY_SCORE = "INTERSECTION_PRIORITY_WINDOW";
    public static final String METRIC_PRIORITY_RANK = "PRIORITY_RANK";

    public static final Set<Integer> WINDOW_SIZES = Set.of(60, 300);

    public static final Set<String> DIRECTIONS = Set.of(
            "NORTH", "SOUTH", "EAST", "WEST", "UNKNOWN");

    public static final Set<String> COMPARISON_METRICS = Set.of(
            "VEHICLE_COUNT",
            "QUEUE_LENGTH_M",
            "SPEED_KMH",
            "OCCUPANCY_PCT",
            "PCU_EQUIVALENT");

    private GoldQueryAllowlist() {}
}
