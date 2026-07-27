package com.traffic.server.controller;

import com.traffic.server.payload.ApiResponse;
import com.traffic.server.payload.RealtimeIntersectionResponse;
import com.traffic.server.payload.SystemHealthDetailsResponse;
import com.traffic.server.payload.SystemHealthResponse;
import com.traffic.server.service.RealtimeAggregateService;
import com.traffic.server.service.SystemHealthService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class RealtimeController {

    private final RealtimeAggregateService aggregateService;
    private final SystemHealthService systemHealthService;

    public RealtimeController(RealtimeAggregateService aggregateService,
                              SystemHealthService systemHealthService) {
        this.aggregateService = aggregateService;
        this.systemHealthService = systemHealthService;
    }

    @GetMapping("/realtime/intersections/{intersectionId}")
    public ApiResponse<RealtimeIntersectionResponse> getIntersectionAggregate(
            @PathVariable String intersectionId) {
        return ApiResponse.success(aggregateService.getIntersectionAggregate(intersectionId));
    }

    @GetMapping("/system/health")
    public SystemHealthResponse systemHealth() {
        return systemHealthService.publicHealth();
    }

    @GetMapping("/system/health/details")
    public ApiResponse<SystemHealthDetailsResponse> systemHealthDetails() {
        return ApiResponse.success(systemHealthService.adminDetails());
    }
}
