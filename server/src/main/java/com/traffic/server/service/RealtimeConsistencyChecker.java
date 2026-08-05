package com.traffic.server.service;

import com.traffic.server.config.AppProperties;
import com.traffic.server.payload.*;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class RealtimeConsistencyChecker {

    private final double tolerance;

    public RealtimeConsistencyChecker(AppProperties appProperties) {
        this.tolerance = appProperties.realtime().simulationTimeTolerance();
    }

    public RealtimeMetadata buildMetadata(ProjectorCurrentRunResponse currentRun,
                                          IntersectionResponse intersection,
                                          List<TrafficLightResponse> trafficLights,
                                          List<VehicleSensorResponse> vehicleSensors,
                                          List<CameraResponse> cameras) {
        List<String> issues = new ArrayList<>();
        String runId = currentRun.simulationRunId();
        Double simTime = currentRun.simulationTime();
        String scenarioId = currentRun.scenarioId();

        checkEntity("Intersection", intersection.getId(), runId, simTime, scenarioId,
                intersection.getSimulationRunId(), intersection.getSimulationTime(), intersection.getScenarioId(),
                issues);

        for (TrafficLightResponse tl : trafficLights) {
            checkEntity("TrafficLight", tl.getId(), runId, simTime, scenarioId,
                    tl.getSimulationRunId(), tl.getSimulationTime(), tl.getScenarioId(), issues);
        }
        for (VehicleSensorResponse vs : vehicleSensors) {
            checkEntity("VehicleSensor", vs.getId(), runId, simTime, scenarioId,
                    vs.getSimulationRunId(), vs.getSimulationTime(), vs.getScenarioId(), issues);
        }
        for (CameraResponse cam : cameras) {
            checkEntity("Camera", cam.getId(), runId, simTime, scenarioId,
                    cam.getSimulationRunId(), cam.getSimulationTime(), cam.getScenarioId(), issues);
        }

        boolean consistent = issues.isEmpty();
        return RealtimeMetadata.builder()
                .simulationRunId(runId)
                .simulationTime(simTime)
                .scenarioId(scenarioId)
                .consistent(consistent)
                .consistencyIssues(consistent ? null : issues)
                .projectorStatus(currentRun.status())
                .freshnessSeconds(currentRun.freshnessSeconds())
                .build();
    }

    private void checkEntity(String type,
                             String id,
                             String expectedRunId,
                             Double expectedSimTime,
                             String expectedScenario,
                             String actualRunId,
                             Double actualSimTime,
                             String actualScenario,
                             List<String> issues) {
        String label = type + " " + (id != null ? id : "?");
        if (actualRunId == null || actualSimTime == null || actualScenario == null) {
            issues.add(label + " missing simulation metadata");
            return;
        }
        if (expectedRunId != null && !expectedRunId.equals(actualRunId)) {
            issues.add(label + " simulationRunId " + actualRunId + " != current-run " + expectedRunId);
        }
        if (expectedScenario != null && !expectedScenario.equals(actualScenario)) {
            issues.add(label + " scenarioId " + actualScenario + " != current-run " + expectedScenario);
        }
        if (expectedSimTime != null && !timesEqual(expectedSimTime, actualSimTime)) {
            issues.add(label + " simulationTime " + actualSimTime + " != current-run " + expectedSimTime);
        }
    }

    private boolean timesEqual(double a, double b) {
        return Math.abs(a - b) <= tolerance;
    }
}
