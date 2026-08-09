package com.traffic.server.analytics.controller;

import com.traffic.server.analytics.dto.common.AnalyticsPage;
import com.traffic.server.analytics.dto.congestion.CongestionWindowDto;
import com.traffic.server.analytics.dto.network.NetworkWindowDto;
import com.traffic.server.analytics.dto.priority.PriorityRankingDto;
import com.traffic.server.analytics.service.KpiAnalyticsService;
import com.traffic.server.payload.ApiResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/analytics")
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class KpiAnalyticsController {

    private final KpiAnalyticsService service;

    public KpiAnalyticsController(KpiAnalyticsService service) {
        this.service = service;
    }

    @GetMapping("/congestion")
    public ApiResponse<AnalyticsPage<CongestionWindowDto>> congestion(
            @RequestParam String simulationRunId,
            @RequestParam String scenarioId,
            @RequestParam Integer windowSizeSec,
            @RequestParam(required = false) Double fromSimulationSec,
            @RequestParam(required = false) Double toSimulationSec,
            @RequestParam(required = false) String intersectionId,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam(required = false, defaultValue = "50") Integer pageSize,
            @RequestParam(required = false) String sort) {
        return ApiResponse.success(service.congestion(
                simulationRunId, scenarioId, windowSizeSec,
                fromSimulationSec, toSimulationSec, intersectionId, page, pageSize, sort));
    }

    @GetMapping("/priority")
    public ApiResponse<AnalyticsPage<PriorityRankingDto>> priority(
            @RequestParam String simulationRunId,
            @RequestParam String scenarioId,
            @RequestParam Integer windowSizeSec,
            @RequestParam(required = false) Double fromSimulationSec,
            @RequestParam(required = false) Double toSimulationSec,
            @RequestParam(required = false) String intersectionId,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam(required = false, defaultValue = "50") Integer pageSize,
            @RequestParam(required = false) String sort) {
        return ApiResponse.success(service.priority(
                simulationRunId, scenarioId, windowSizeSec,
                fromSimulationSec, toSimulationSec, intersectionId, page, pageSize, sort));
    }
}
