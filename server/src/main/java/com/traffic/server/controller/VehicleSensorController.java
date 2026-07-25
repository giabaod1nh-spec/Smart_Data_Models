package com.traffic.server.controller;

import com.traffic.server.payload.ApiResponse;
import com.traffic.server.payload.VehicleSensorResponse;
import com.traffic.server.service.OrionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/vehicle-sensors")
public class VehicleSensorController {

    private final OrionService orionService;

    public VehicleSensorController(OrionService orionService) {
        this.orionService = orionService;
    }

    @GetMapping
    public ApiResponse<List<VehicleSensorResponse>> getVehicleSensors() {
        return ApiResponse.success(orionService.getVehicleSensors());
    }

    @GetMapping("/{sensorId}")
    public ApiResponse<VehicleSensorResponse> getVehicleSensor(@PathVariable String sensorId) {
        return ApiResponse.success(orionService.getVehicleSensor(sensorId));
    }
}
