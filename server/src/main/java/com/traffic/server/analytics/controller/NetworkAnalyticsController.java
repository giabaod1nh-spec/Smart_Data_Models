package com.traffic.server.analytics.controller;

import com.traffic.server.analytics.dto.common.AnalyticsPage;
import com.traffic.server.analytics.dto.network.NetworkWindowDto;
import com.traffic.server.analytics.service.KpiAnalyticsService;
import com.traffic.server.payload.ApiResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/analytics/network")
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class NetworkAnalyticsController {

    private final KpiAnalyticsService service;

    public NetworkAnalyticsController(KpiAnalyticsService service) {
        this.service = service;
    }

    @GetMapping("/windows")
    public ApiResponse<AnalyticsPage<NetworkWindowDto>> windows(
            @RequestParam String simulationRunId,
            @RequestParam String scenarioId,
            @RequestParam Integer windowSizeSec,
            @RequestParam(required = false) Double fromSimulationSec,
            @RequestParam(required = false) Double toSimulationSec,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam(required = false, defaultValue = "50") Integer pageSize,
            @RequestParam(required = false) String sort) {
        return ApiResponse.success(service.networkWindows(
                simulationRunId, scenarioId, windowSizeSec,
                fromSimulationSec, toSimulationSec, page, pageSize, sort));
    }
}
