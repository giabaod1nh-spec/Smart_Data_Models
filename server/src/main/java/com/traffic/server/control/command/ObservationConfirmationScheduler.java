package com.traffic.server.control.command;

import com.traffic.server.config.AppProperties;
import com.traffic.server.control.ControlApiClient;
import com.traffic.server.payload.ProjectorCurrentRunResponse;
import com.traffic.server.service.ProjectorClient;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

/** RC-5 — observation confirmation (execution-independent). */
@Component
public class ObservationConfirmationScheduler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final EnumSet<ControlCommandType> NOT_OBSERVABLE = EnumSet.of(
            ControlCommandType.SET_DEMAND_PROFILE,
            ControlCommandType.ADD_OVERLAY,
            ControlCommandType.REMOVE_OVERLAY,
            ControlCommandType.SET_CONTROL_MODE,
            ControlCommandType.EMERGENCY_PREEMPTION);

    private final ControlCommandRepository commandRepository;
    private final ControlApiClient controlApiClient;
    private final ObservationConfirmationMatcher matcher;
    private final ControlCommandMetrics metrics;
    private final boolean observationEnabled;

    public ObservationConfirmationScheduler(
            ControlCommandRepository commandRepository,
            ControlApiClient controlApiClient,
            ObservationConfirmationMatcher matcher,
            ControlCommandMetrics metrics,
            AppProperties appProperties) {
        this.commandRepository = commandRepository;
        this.controlApiClient = controlApiClient;
        this.matcher = matcher;
        this.metrics = metrics;
        this.observationEnabled = appProperties.control().observationConfirmationEnabled();
    }

    @Scheduled(fixedDelayString = "${app.control.observation-interval-ms:10000}")
    @Transactional
    public void confirmObservations() {
        if (!observationEnabled) {
            return;
        }
        List<ControlCommandEntity> applied = commandRepository.findByObservationStatusAndExecutionStatus(
                ControlObservationStatus.NOT_REQUESTED,
                ControlExecutionStatus.APPLIED_AT_SUMO);
        for (ControlCommandEntity entity : applied) {
            if (NOT_OBSERVABLE.contains(entity.getCommandType())) {
                entity.setObservationStatus(ControlObservationStatus.NOT_OBSERVABLE);
                continue;
            }
            entity.setObservationStatus(ControlObservationStatus.PENDING);
        }

        List<ControlCommandEntity> pending = commandRepository.findByObservationStatusAndExecutionStatus(
                ControlObservationStatus.PENDING,
                ControlExecutionStatus.APPLIED_AT_SUMO);
        for (ControlCommandEntity entity : pending) {
            JsonNode pythonBody = fetchPythonStatus(entity.getCommandId());
            ObservationConfirmationMatcher.Outcome outcome = matcher.match(entity, pythonBody);
            switch (outcome) {
                case CONFIRMED -> {
                    entity.setObservationStatus(ControlObservationStatus.CONFIRMED);
                    if (entity.getUpdatedAt() != null) {
                        long ms = Duration.between(entity.getUpdatedAt(), Instant.now()).toMillis();
                        metrics.recordObservationLatencyMs(Math.max(0, ms));
                    }
                }
                case MISMATCH -> entity.setObservationStatus(ControlObservationStatus.MISMATCH);
                case TIMED_OUT -> entity.setObservationStatus(ControlObservationStatus.TIMED_OUT);
                case UNAVAILABLE -> entity.setObservationStatus(ControlObservationStatus.UNAVAILABLE);
                case PENDING -> {
                    // keep PENDING
                }
            }
        }
    }

    private JsonNode fetchPythonStatus(UUID commandId) {
        try {
            ResponseEntity<String> response =
                    controlApiClient.getCommandStatus(commandId, commandId.toString());
            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
                return null;
            }
            return MAPPER.readTree(response.getBody());
        } catch (Exception e) {
            return null;
        }
    }
}
