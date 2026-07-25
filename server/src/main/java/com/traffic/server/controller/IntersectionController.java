package com.traffic.server.controller;

import com.traffic.server.payload.ApiResponse;
import com.traffic.server.payload.IntersectionResponse;
import com.traffic.server.service.OrionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/intersections")
public class IntersectionController {

    private final OrionService orionService;

    public IntersectionController(OrionService orionService) {
        this.orionService = orionService;
    }

    @GetMapping
    public ApiResponse<List<IntersectionResponse>> getIntersections() {
        return ApiResponse.success(orionService.getIntersections());
    }

    @GetMapping("/{intersectionId}")
    public ApiResponse<IntersectionResponse> getIntersection(@PathVariable String intersectionId) {
        return ApiResponse.success(orionService.getIntersection(intersectionId));
    }
}
