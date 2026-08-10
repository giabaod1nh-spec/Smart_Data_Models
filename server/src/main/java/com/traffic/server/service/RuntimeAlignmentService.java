package com.traffic.server.service;

import com.traffic.server.config.AppProperties;
import com.traffic.server.payload.ControlApiHealthResponse;
import com.traffic.server.payload.IntersectionResponse;
import com.traffic.server.payload.ProjectorCurrentRunResponse;
import com.traffic.server.payload.RuntimeAlignmentResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;

@Service
public class RuntimeAlignmentService {

    private final ProjectorClient projectorClient;
    private final WebClient controlApiWebClient;
    private final OrionService orionService;

    public RuntimeAlignmentService(ProjectorClient projectorClient,
                                   WebClient controlApiWebClient,
                                   OrionService orionService) {
        this.projectorClient = projectorClient;
        this.controlApiWebClient = controlApiWebClient;
        this.orionService = orionService;
    }

    public RuntimeAlignmentResponse evaluate() {
        String projectorRunId = null;
        String reason = null;

        ProjectorClient.CurrentRunResult projectorResult = projectorClient.fetchCurrentRun();
        if (projectorResult instanceof ProjectorClient.CurrentRunResult.Idle) {
            reason = "projector_idle";
        } else if (projectorResult instanceof ProjectorClient.CurrentRunResult.Unavailable) {
            reason = "projector_unavailable";
        } else if (projectorResult instanceof ProjectorClient.CurrentRunResult.Ok ok) {
            ProjectorCurrentRunResponse body = ok.body();
            if (body != null) {
                projectorRunId = body.simulationRunId();
            }
        }

        String producerRunId = fetchProducerRunId();
        String orionSampleRunId = fetchOrionSampleRunId();

        boolean aligned = projectorRunId != null
                && producerRunId != null
                && projectorRunId.equals(producerRunId);

        if (reason == null && !aligned) {
            if (projectorRunId == null) {
                reason = "projector_idle";
            } else if (producerRunId == null) {
                reason = "producer_unavailable";
            } else {
                reason = "run_mismatch";
            }
        } else if (reason == null && aligned) {
            reason = "aligned";
        }

        return RuntimeAlignmentResponse.builder()
                .projectorRunId(projectorRunId)
                .producerRunId(producerRunId)
                .orionSampleRunId(orionSampleRunId)
                .aligned(aligned)
                .reason(reason)
                .build();
    }

    private String fetchProducerRunId() {
        try {
            ControlApiHealthResponse health = controlApiWebClient.get()
                    .uri("/health")
                    .retrieve()
                    .bodyToMono(ControlApiHealthResponse.class)
                    .block();
            if (health == null || health.simulationRunId() == null || health.simulationRunId().isBlank()) {
                return null;
            }
            return health.simulationRunId();
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private String fetchOrionSampleRunId() {
        try {
            IntersectionResponse intersection = orionService.getIntersection("A");
            return intersection.getSimulationRunId();
        } catch (ResponseStatusException ex) {
            return null;
        } catch (RuntimeException ex) {
            return null;
        }
    }
}
