package com.traffic.server.analytics.controller;

import com.traffic.server.analytics.dto.common.AnalyticsPage;
import com.traffic.server.analytics.dto.signal.SignalOperationWindowDto;
import com.traffic.server.analytics.service.SignalAnalyticsService;
import com.traffic.server.payload.ApiResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/analytics/signals")
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class SignalAnalyticsController {

    private final SignalAnalyticsService service;

    public SignalAnalyticsController(SignalAnalyticsService service) {
        this.service = service;
    }

    @GetMapping("/operation-windows")
    public ApiResponse<AnalyticsPage<SignalOperationWindowDto>> operationWindows(
            @RequestParam String simulationRunId,
            @RequestParam String scenarioId,
            @RequestParam Integer windowSizeSec,
            @RequestParam(required = false) Double fromSimulationSec,
            @RequestParam(required = false) Double toSimulationSec,
            @RequestParam(required = false) String intersectionId,
            @RequestParam(required = false) String direction,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam(required = false, defaultValue = "50") Integer pageSize,
            @RequestParam(required = false) String sort) {
        return ApiResponse.success(service.operationWindows(
                simulationRunId, scenarioId, windowSizeSec,
                fromSimulationSec, toSimulationSec, intersectionId, direction,
                page, pageSize, sort));
    }
}
