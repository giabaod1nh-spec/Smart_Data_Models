package com.traffic.server.controller;

import com.traffic.server.payload.ApiResponse;
import com.traffic.server.payload.IntersectionResponse;
import com.traffic.server.service.OrionService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/test")
public class TestController {

    private final OrionService orionService;

    public TestController(OrionService orionService) {
        this.orionService = orionService;
    }

    /** Tra ve payload NGSI-LD tho tu Orion, phuc vu debug. */
    @GetMapping("/raw/{entityId}")
    public ApiResponse<String> getRawEntity(@PathVariable String entityId) {
        return ApiResponse.success(orionService.getRawEntity(entityId));
    }

    @GetMapping("/intersections/{intersectionId}")
    public ApiResponse<IntersectionResponse> getIntersection(@PathVariable String intersectionId) {
        return ApiResponse.success(orionService.getIntersection(intersectionId));
    }
}
