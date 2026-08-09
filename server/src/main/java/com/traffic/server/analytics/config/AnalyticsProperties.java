package com.traffic.server.analytics.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.analytics")
public record AnalyticsProperties(
        boolean enabled,
        boolean networkOverviewEnabled,
        long maxSimulationRangeSec,
        ClickHouse clickhouse,
        Definition definition
) {
    public AnalyticsProperties {
        if (maxSimulationRangeSec <= 0) {
            maxSimulationRangeSec = 86400L;
        }
        if (clickhouse == null) {
            clickhouse = new ClickHouse(null, 0, null, null, null, 0, 0, true);
        }
        if (definition == null) {
            definition = new Definition(null, 1, 0);
        }
    }

    public record ClickHouse(
            String host,
            int port,
            String database,
            String user,
            String password,
            int connectTimeoutMs,
            int queryTimeoutMs,
            boolean readOnly
    ) {
        public ClickHouse {
            if (host == null || host.isBlank()) {
                host = "localhost";
            }
            if (port <= 0) {
                port = 8123;
            }
            if (database == null || database.isBlank()) {
                database = "smart_traffic";
            }
            if (user == null) {
                user = "default";
            }
            if (password == null) {
                password = "";
            }
            if (connectTimeoutMs <= 0) {
                connectTimeoutMs = 3000;
            }
            if (queryTimeoutMs <= 0) {
                queryTimeoutMs = 10000;
            }
        }
    }

    public record Definition(String version, int major, int minor) {
        public Definition {
            if (version == null || version.isBlank()) {
                version = "v1.0";
            }
        }
    }
}
