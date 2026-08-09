package com.traffic.server.analytics.controller;

import com.traffic.server.analytics.dto.common.AnalyticsPage;
import com.traffic.server.analytics.dto.comparison.TrafficComparisonDto;
import com.traffic.server.analytics.dto.direction.DirectionWindowDto;
import com.traffic.server.analytics.dto.intersection.IntersectionWindowDto;
import com.traffic.server.analytics.service.IntersectionAnalyticsService;
import com.traffic.server.payload.ApiResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/analytics/intersections")
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class IntersectionAnalyticsController {

    private final IntersectionAnalyticsService service;

    public IntersectionAnalyticsController(IntersectionAnalyticsService service) {
        this.service = service;
    }

    @GetMapping("/{intersectionId}/windows")
    public ApiResponse<AnalyticsPage<IntersectionWindowDto>> windows(
            @PathVariable String intersectionId,
            @RequestParam String simulationRunId,
            @RequestParam String scenarioId,
            @RequestParam Integer windowSizeSec,
            @RequestParam(required = false) Double fromSimulationSec,
            @RequestParam(required = false) Double toSimulationSec,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam(required = false, defaultValue = "50") Integer pageSize,
            @RequestParam(required = false) String sort) {
        return ApiResponse.success(service.intersectionWindows(
                intersectionId, simulationRunId, scenarioId, windowSizeSec,
                fromSimulationSec, toSimulationSec, page, pageSize, sort));
    }

    @GetMapping("/{intersectionId}/directions/windows")
    public ApiResponse<AnalyticsPage<DirectionWindowDto>> directionWindows(
            @PathVariable String intersectionId,
            @RequestParam String simulationRunId,
            @RequestParam String scenarioId,
            @RequestParam Integer windowSizeSec,
            @RequestParam(required = false) Double fromSimulationSec,
            @RequestParam(required = false) Double toSimulationSec,
            @RequestParam(required = false) String direction,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam(required = false, defaultValue = "50") Integer pageSize,
            @RequestParam(required = false) String sort) {
        return ApiResponse.success(service.directionWindows(
                intersectionId, simulationRunId, scenarioId, windowSizeSec,
                fromSimulationSec, toSimulationSec, direction, page, pageSize, sort));
    }

    @GetMapping("/{intersectionId}/comparisons")
    public ApiResponse<AnalyticsPage<TrafficComparisonDto>> comparisons(
            @PathVariable String intersectionId,
            @RequestParam String simulationRunId,
            @RequestParam String scenarioId,
            @RequestParam Integer windowSizeSec,
            @RequestParam String metricCode,
            @RequestParam(required = false) Double fromSimulationSec,
            @RequestParam(required = false) Double toSimulationSec,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam(required = false, defaultValue = "50") Integer pageSize,
            @RequestParam(required = false) String sort) {
        return ApiResponse.success(service.comparisons(
                intersectionId, simulationRunId, scenarioId, windowSizeSec, metricCode,
                fromSimulationSec, toSimulationSec, page, pageSize, sort));
    }

    @GetMapping("/{intersectionId}/trends")
    public ApiResponse<AnalyticsPage<TrafficComparisonDto>> trends(
            @PathVariable String intersectionId,
            @RequestParam String simulationRunId,
            @RequestParam String scenarioId,
            @RequestParam Integer windowSizeSec,
            @RequestParam String metricCode,
            @RequestParam(required = false) Double fromSimulationSec,
            @RequestParam(required = false) Double toSimulationSec,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam(required = false, defaultValue = "50") Integer pageSize,
            @RequestParam(required = false) String sort) {
        return ApiResponse.success(service.trends(
                intersectionId, simulationRunId, scenarioId, windowSizeSec, metricCode,
                fromSimulationSec, toSimulationSec, page, pageSize, sort));
    }
}
