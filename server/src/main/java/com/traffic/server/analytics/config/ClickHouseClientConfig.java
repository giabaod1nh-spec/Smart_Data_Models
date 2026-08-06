package com.traffic.server.analytics.config;

import com.traffic.server.analytics.repository.ClickHouseGoldKpiRepository;
import com.traffic.server.analytics.repository.ClickHouseGoldSignalRepository;
import com.traffic.server.analytics.repository.ClickHouseGoldWindowRepository;
import com.traffic.server.analytics.repository.GoldKpiRepository;
import com.traffic.server.analytics.repository.GoldSignalRepository;
import com.traffic.server.analytics.repository.GoldWindowRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/** BS-1/BS-2 — conditional lazy ClickHouse read-only client. */
@Configuration
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class ClickHouseClientConfig {

    @Bean
    @Lazy
    public ClickHouseQueryExecutor clickHouseQueryExecutor(AnalyticsProperties properties) {
        return new ClickHouseQueryExecutor(properties);
    }

    @Bean
    @Lazy
    public GoldWindowRepository goldWindowRepository(ClickHouseQueryExecutor executor) {
        return new ClickHouseGoldWindowRepository(executor);
    }

    @Bean
    @Lazy
    public GoldKpiRepository goldKpiRepository(ClickHouseQueryExecutor executor) {
        return new ClickHouseGoldKpiRepository(executor);
    }

    @Bean
    @Lazy
    public GoldSignalRepository goldSignalRepository(ClickHouseQueryExecutor executor) {
        return new ClickHouseGoldSignalRepository(executor);
    }
}
