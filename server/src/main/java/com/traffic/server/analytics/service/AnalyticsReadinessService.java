package com.traffic.server.analytics.service;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.exception.AnalyticsNotReadyException;
import com.traffic.server.analytics.query.GoldQueryAllowlist;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Per-mart schema/live-row and terminal-ledger probes (BS-0-T3 / G3-P0-011). */
@Service
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class AnalyticsReadinessService {

    public enum ReadinessState {
        SCHEMA_READY,
        LIVE_ROW_READY,
        BLOCKED
    }

    private static final Map<String, String> MART_TABLE = Map.of(
            "intersection_window", "gold_mart_intersection_window_summary",
            "direction_window", "gold_mart_direction_window_summary",
            "traffic_comparison", "gold_fact_traffic_comparison",
            "congestion", "gold_mart_congestion_window",
            "priority", "gold_mart_priority_window_ranking",
            "signal_operation", "gold_mart_signal_operation_window",
            "network_window", "gold_mart_network_window_overview");

    private final ClickHouseQueryExecutor executor;
    private final AnalyticsProperties properties;
    private final Map<String, ReadinessState> cache = new ConcurrentHashMap<>();
    private volatile Boolean terminalLedgerReady;

    public AnalyticsReadinessService(ClickHouseQueryExecutor executor, AnalyticsProperties properties) {
        this.executor = executor;
        this.properties = properties;
    }

    public void requireMartReady(String martKey) {
        if ("network_window".equals(martKey)) {
            throw new AnalyticsNotReadyException("Network overview mart is blocked (Gold WHERE 0 scaffold)");
        }
        if (!isTerminalLedgerReady()) {
            throw new AnalyticsNotReadyException(
                    "Terminal ledger visibility not proven for live namespace (G3-P0-011)");
        }
        ReadinessState state = probeMart(martKey);
        if (state == ReadinessState.BLOCKED) {
            throw new AnalyticsNotReadyException("Mart not ready: " + martKey);
        }
    }

    public Map<String, ReadinessState> probeAll() {
        Map<String, ReadinessState> out = new LinkedHashMap<>();
        for (String key : MART_TABLE.keySet()) {
            if ("network_window".equals(key)) {
                out.put(key, ReadinessState.BLOCKED);
            } else {
                out.put(key, probeMart(key));
            }
        }
        return out;
    }

    public boolean isTerminalLedgerReady() {
        if (terminalLedgerReady != null) {
            return terminalLedgerReady;
        }
        synchronized (this) {
            if (terminalLedgerReady != null) {
                return terminalLedgerReady;
            }
            if (!executor.ping()) {
                terminalLedgerReady = false;
                return false;
            }
            long count = executor.scalar("""
                    SELECT count()
                    FROM gold_processing_ledger
                    WHERE namespace = ?
                      AND definition_major = ?
                      AND definition_minor = ?
                      AND disposition IN ('CHECKPOINTED', 'REPLAYED', 'QUARANTINED')
                    """,
                    GoldQueryAllowlist.NAMESPACE_LIVE,
                    properties.definition().major(),
                    properties.definition().minor());
            terminalLedgerReady = count > 0;
            return terminalLedgerReady;
        }
    }

    ReadinessState probeMart(String martKey) {
        return cache.computeIfAbsent(martKey, this::computeMartState);
    }

    private ReadinessState computeMartState(String martKey) {
        if ("network_window".equals(martKey)) {
            return ReadinessState.BLOCKED;
        }
        if (!executor.ping()) {
            return ReadinessState.BLOCKED;
        }
        String table = MART_TABLE.get(martKey);
        if (table == null) {
            return ReadinessState.BLOCKED;
        }
        try {
            executor.scalar("SELECT count() FROM " + table + " WHERE namespace = ? LIMIT 1",
                    GoldQueryAllowlist.NAMESPACE_LIVE);
        } catch (RuntimeException e) {
            return ReadinessState.BLOCKED;
        }
        long rows = executor.scalar(
                "SELECT count() FROM " + table + " WHERE namespace = ?",
                GoldQueryAllowlist.NAMESPACE_LIVE);
        return rows > 0 ? ReadinessState.LIVE_ROW_READY : ReadinessState.SCHEMA_READY;
    }

    public void invalidateCache() {
        cache.clear();
        terminalLedgerReady = null;
    }
}
