package com.traffic.server.analytics.repository;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.dto.signal.SignalOperationWindowDto;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;

import java.util.List;

public interface GoldSignalRepository {

    List<SignalOperationWindowDto> findSignalWindows(
            AnalyticsQueryCriteria criteria, AnalyticsProperties.Definition definition);
}
