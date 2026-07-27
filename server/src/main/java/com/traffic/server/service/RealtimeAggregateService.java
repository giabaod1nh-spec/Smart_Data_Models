package com.traffic.server.service;

import com.traffic.server.config.AppProperties;
import com.traffic.server.payload.*;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class RealtimeAggregateService {

    private final OrionService orionService;
    private final RealtimeConsistencyChecker consistencyChecker;
    private final long retryDelayMs;

    public RealtimeAggregateService(OrionService orionService,
                                    RealtimeConsistencyChecker consistencyChecker,
                                    AppProperties appProperties) {
        this.orionService = orionService;
        this.consistencyChecker = consistencyChecker;
        this.retryDelayMs = appProperties.realtime().consistencyRetryMs();
    }

    public RealtimeIntersectionResponse getIntersectionAggregate(String intersectionId) {
        RealtimeIntersectionResponse first = loadAggregate(intersectionId);
        if (Boolean.TRUE.equals(first.getMetadata().getConsistent())) {
            return first;
        }
        sleepQuietly(retryDelayMs);
        return loadAggregate(intersectionId);
    }

    private RealtimeIntersectionResponse loadAggregate(String intersectionId) {
        IntersectionResponse intersection = orionService.getIntersection(intersectionId);

        List<TrafficLightResponse> trafficLights = new ArrayList<>();
        if (intersection.getRefTrafficLights() != null) {
            for (String urn : intersection.getRefTrafficLights()) {
                trafficLights.add(orionService.getTrafficLight(entityIdFromUrn(urn)));
            }
        }

        List<VehicleSensorResponse> vehicleSensors = new ArrayList<>();
        if (intersection.getRefVehicleSensors() != null) {
            for (String urn : intersection.getRefVehicleSensors()) {
                vehicleSensors.add(orionService.getVehicleSensor(entityIdFromUrn(urn)));
            }
        }

        List<CameraResponse> cameras = new ArrayList<>();
        if (intersection.getRefCameras() != null) {
            for (String urn : intersection.getRefCameras()) {
                cameras.add(orionService.getCamera(entityIdFromUrn(urn)));
            }
        }

        RealtimeMetadata metadata = consistencyChecker.buildMetadata(
                intersection, trafficLights, vehicleSensors, cameras);

        return RealtimeIntersectionResponse.builder()
                .intersection(intersection)
                .trafficLights(trafficLights)
                .vehicleSensors(vehicleSensors)
                .cameras(cameras)
                .metadata(metadata)
                .build();
    }

    static String entityIdFromUrn(String urn) {
        if (urn == null) {
            return null;
        }
        if (urn.startsWith("urn:ngsi-ld:")) {
            String rest = urn.substring("urn:ngsi-ld:".length());
            int typeEnd = rest.indexOf(':');
            if (typeEnd >= 0) {
                return rest.substring(typeEnd + 1);
            }
        }
        return urn;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
