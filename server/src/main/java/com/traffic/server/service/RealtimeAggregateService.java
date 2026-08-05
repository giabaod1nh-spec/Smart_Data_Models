package com.traffic.server.service;

import com.traffic.server.config.AppProperties;
import com.traffic.server.exception.RealtimeIdleException;
import com.traffic.server.exception.RealtimeUnavailableException;
import com.traffic.server.exception.RealtimeRunConflictException;
import com.traffic.server.payload.*;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class RealtimeAggregateService {

    private final OrionService orionService;
    private final ProjectorClient projectorClient;
    private final RealtimeConsistencyChecker consistencyChecker;
    private final long retryDelayMs;
    private final double freshnessThresholdSec;
    private final AtomicLong mismatchCounter = new AtomicLong();

    public RealtimeAggregateService(OrionService orionService,
                                    ProjectorClient projectorClient,
                                    RealtimeConsistencyChecker consistencyChecker,
                                    AppProperties appProperties) {
        this.orionService = orionService;
        this.projectorClient = projectorClient;
        this.consistencyChecker = consistencyChecker;
        this.retryDelayMs = appProperties.realtime().consistencyRetryMs();
        this.freshnessThresholdSec = appProperties.realtime().freshnessThresholdSec();
    }

    public RealtimeIntersectionResponse getIntersectionAggregate(String intersectionId) {
        return getIntersectionAggregate(intersectionId, null);
    }

    public RealtimeIntersectionResponse getIntersectionAggregate(String intersectionId,
                                                                 String requestedRunId) {
        ProjectorClient.CurrentRunResult currentRun = projectorClient.fetchCurrentRun();
        if (currentRun instanceof ProjectorClient.CurrentRunResult.Unavailable) {
            throw new RealtimeUnavailableException("projector current-run unavailable");
        }
        if (currentRun instanceof ProjectorClient.CurrentRunResult.Idle) {
            throw new RealtimeIdleException();
        }
        ProjectorCurrentRunResponse run = ((ProjectorClient.CurrentRunResult.Ok) currentRun).body();
        if (requestedRunId != null && !requestedRunId.equals(run.simulationRunId())) {
            throw new RealtimeRunConflictException(
                    "requested run " + requestedRunId + " != active " + run.simulationRunId());
        }
        if (run.freshnessSeconds() != null && run.freshnessSeconds() > freshnessThresholdSec) {
            throw new RealtimeUnavailableException("snapshot stale");
        }
        RealtimeIntersectionResponse first = loadAggregate(intersectionId, run);
        if (Boolean.TRUE.equals(first.getMetadata().getConsistent())) {
            return first;
        }
        mismatchCounter.incrementAndGet();
        sleepQuietly(retryDelayMs);
        RealtimeIntersectionResponse second = loadAggregate(intersectionId, run);
        if (!Boolean.TRUE.equals(second.getMetadata().getConsistent())) {
            throw new RealtimeUnavailableException("mixed run after retry");
        }
        return second;
    }

    public long getMismatchCount() {
        return mismatchCounter.get();
    }

    private RealtimeIntersectionResponse loadAggregate(String intersectionId,
                                                       ProjectorCurrentRunResponse currentRun) {
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
                currentRun, intersection, trafficLights, vehicleSensors, cameras);

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
