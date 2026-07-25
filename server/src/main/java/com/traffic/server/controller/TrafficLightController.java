package com.traffic.server.controller;

import com.traffic.server.payload.ApiResponse;
import com.traffic.server.payload.TrafficLightResponse;
import com.traffic.server.service.OrionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/traffic-lights")
public class TrafficLightController {

    private final OrionService orionService;

    public TrafficLightController(OrionService orionService) {
        this.orionService = orionService;
    }

    @GetMapping
    public ApiResponse<List<TrafficLightResponse>> getTrafficLights() {
        return ApiResponse.success(orionService.getTrafficLights());
    }

    @GetMapping("/{trafficLightId}")
    public ApiResponse<TrafficLightResponse> getTrafficLight(@PathVariable String trafficLightId) {
        return ApiResponse.success(orionService.getTrafficLight(trafficLightId));
    }
}
