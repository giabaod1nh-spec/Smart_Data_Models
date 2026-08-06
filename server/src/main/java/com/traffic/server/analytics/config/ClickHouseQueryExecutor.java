package com.traffic.server.analytics.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.traffic.server.analytics.exception.AnalyticsQueryTimeoutException;
import com.traffic.server.analytics.exception.AnalyticsUnavailableException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parameterized read-only ClickHouse query executor (BS-2). */
public class ClickHouseQueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseQueryExecutor.class);

    private final AnalyticsProperties properties;
    private volatile ClickHouseDataSource dataSource;

    public ClickHouseQueryExecutor(AnalyticsProperties properties) {
        this.properties = properties;
    }

    public boolean ping() {
        try {
            scalar("SELECT 1");
            return true;
        } catch (RuntimeException e) {
            log.warn("ClickHouse ping failed: {}", e.getMessage());
            return false;
        }
    }

    public long scalar(String sql, Object... params) {
        List<Map<String, Object>> rows = query(sql, params);
        if (rows.isEmpty() || rows.getFirst().isEmpty()) {
            return 0L;
        }
        Object v = rows.getFirst().values().iterator().next();
        if (v instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(String.valueOf(v));
    }

    public List<Map<String, Object>> query(String sql, Object... params) {
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(Math.max(1, properties.clickhouse().queryTimeoutMs() / 1000));
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return mapRows(rs);
            }
        } catch (SQLTimeoutException e) {
            throw new AnalyticsQueryTimeoutException("ClickHouse query timed out");
        } catch (SQLException e) {
            throw new AnalyticsUnavailableException("ClickHouse query failed: " + e.getMessage());
        }
    }

    public <T> List<T> queryMapped(String sql, Function<Map<String, Object>, T> mapper, Object... params) {
        return query(sql, params).stream().map(mapper).toList();
    }

    private ClickHouseDataSource dataSource() {
        if (dataSource == null) {
            synchronized (this) {
                if (dataSource == null) {
                    dataSource = createDataSource();
                }
            }
        }
        return dataSource;
    }

    private ClickHouseDataSource createDataSource() {
        AnalyticsProperties.ClickHouse ch = properties.clickhouse();
        String url = String.format(
                "jdbc:clickhouse://%s:%d/%s?ssl=false",
                ch.host(), ch.port(), ch.database());
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", ch.user());
        props.setProperty("password", ch.password());
        props.setProperty("socket_timeout", String.valueOf(ch.queryTimeoutMs()));
        props.setProperty("connection_timeout", String.valueOf(ch.connectTimeoutMs()));
        try {
            return new ClickHouseDataSource(url, props);
        } catch (SQLException e) {
            throw new AnalyticsUnavailableException("ClickHouse datasource init failed: " + e.getMessage());
        }
    }

    private static void bind(PreparedStatement ps, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }

    private static List<Map<String, Object>> mapRows(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        List<Map<String, Object>> out = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int c = 1; c <= cols; c++) {
                row.put(meta.getColumnLabel(c), rs.getObject(c));
            }
            out.add(row);
        }
        return out;
    }
}
