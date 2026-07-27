package com.traffic.server.controller;

import com.traffic.server.payload.ApiResponse;
import com.traffic.server.payload.CameraResponse;
import com.traffic.server.service.OrionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/cameras")
public class CameraController {

    private final OrionService orionService;

    public CameraController(OrionService orionService) {
        this.orionService = orionService;
    }

    @GetMapping
    public ApiResponse<List<CameraResponse>> getCameras() {
        return ApiResponse.success(orionService.getCameras());
    }

    @GetMapping("/{cameraId}")
    public ApiResponse<CameraResponse> getCamera(@PathVariable String cameraId) {
        return ApiResponse.success(orionService.getCamera(cameraId));
    }
}
