package com.traffic.server.analytics.repository;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.dto.congestion.CongestionWindowDto;
import com.traffic.server.analytics.dto.priority.PriorityRankingDto;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;

import java.util.List;

public interface GoldKpiRepository {

    List<CongestionWindowDto> findCongestion(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition);

    List<PriorityRankingDto> findPriority(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition);
}
