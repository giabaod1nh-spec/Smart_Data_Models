package com.traffic.server.analytics.repository;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.dto.comparison.TrafficComparisonDto;
import com.traffic.server.analytics.dto.direction.DirectionWindowDto;
import com.traffic.server.analytics.dto.intersection.IntersectionWindowDto;
import com.traffic.server.analytics.dto.network.NetworkWindowDto;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;

import java.util.List;

public interface GoldWindowRepository {

    List<IntersectionWindowDto> findIntersectionWindows(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition);

    List<DirectionWindowDto> findDirectionWindows(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition);

    List<TrafficComparisonDto> findComparisons(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition);

    List<NetworkWindowDto> findNetworkWindows(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition);
}
