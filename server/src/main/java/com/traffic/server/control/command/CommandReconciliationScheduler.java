package com.traffic.server.control.command;

import com.traffic.server.control.ControlApiClient;
import com.traffic.server.config.AppProperties;
import com.traffic.server.payload.ProjectorCurrentRunResponse;
import com.traffic.server.service.ProjectorClient;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

/** Bounded reconciliation for non-terminal commands (RC4-T2). */
@Component
public class CommandReconciliationScheduler {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static final EnumSet<ControlLifecycleStatus> TERMINAL = EnumSet.of(
            ControlLifecycleStatus.COMPLETED,
            ControlLifecycleStatus.FAILED,
            ControlLifecycleStatus.EXPIRED);

    static final EnumSet<ControlLifecycleStatus> ACTIVE_LIFECYCLE = EnumSet.of(
            ControlLifecycleStatus.RECEIVED,
            ControlLifecycleStatus.VALIDATED,
            ControlLifecycleStatus.QUEUED,
            ControlLifecycleStatus.APPLYING,
            ControlLifecycleStatus.UNKNOWN_OUTCOME);

    private final ControlCommandRepository commandRepository;
    private final ControlCommandService commandService;
    private final ControlCommandDispatchService dispatchService;
    private final ControlApiClient controlApiClient;
    private final ControlCommandMetrics metrics;
    private final ProjectorClient projectorClient;
    private final boolean commandDomainEnabled;

    public CommandReconciliationScheduler(
            ControlCommandRepository commandRepository,
            ControlCommandService commandService,
            ControlCommandDispatchService dispatchService,
            ControlApiClient controlApiClient,
            ControlCommandMetrics metrics,
            ProjectorClient projectorClient,
            AppProperties appProperties) {
        this.commandRepository = commandRepository;
        this.commandService = commandService;
        this.dispatchService = dispatchService;
        this.controlApiClient = controlApiClient;
        this.metrics = metrics;
        this.projectorClient = projectorClient;
        this.commandDomainEnabled = appProperties.control().commandDomainEnabled();
    }

    @Scheduled(fixedDelayString = "${app.control.reconciliation-interval-ms:5000}")
    public void reconcileTick() {
        if (!commandDomainEnabled) {
            return;
        }
        metrics.reconciliationAttempt();
        expireDueCommands();
        redispatchPending();
        pollPythonStatus();
    }

    @Transactional
    public void expireDueCommands() {
        Instant now = Instant.now();
        List<ControlCommandEntity> expired =
                commandRepository.findByExpiresAtUtcBeforeAndLifecycleStatusNotIn(now, TERMINAL);
        for (ControlCommandEntity entity : expired) {
            commandService.markExpired(entity);
        }
    }

    public void redispatchPending() {
        List<ControlCommandEntity> nonTerminal = commandRepository.findNonTerminal(TERMINAL);
        for (ControlCommandEntity entity : nonTerminal) {
            if (entity.getDispatchStatus() == ControlDispatchStatus.PENDING) {
                dispatchService.dispatchIfNeeded(entity.getCommandId(), entity.getCommandId().toString());
            }
        }
    }

    public void pollPythonStatus() {
        List<ControlCommandEntity> nonTerminal = commandRepository.findNonTerminal(TERMINAL);
        String currentRunId = fetchCurrentRunId();
        for (ControlCommandEntity entity : nonTerminal) {
            if (entity.getDispatchStatus() == ControlDispatchStatus.PENDING) {
                continue;
            }
            if (currentRunId != null
                    && entity.getExpectedRunId() != null
                    && !currentRunId.equals(entity.getExpectedRunId())
                    && entity.getExecutionStatus() != ControlExecutionStatus.APPLIED_AT_SUMO) {
                commandService.markStaleRun(entity.getCommandId());
                continue;
            }
            ResponseEntity<String> response =
                    controlApiClient.getCommandStatus(entity.getCommandId(), entity.getCommandId().toString());
            if (response.getStatusCode().value() == 404) {
                if (entity.getExecutionStatus() == ControlExecutionStatus.QUEUED
                        || entity.getExecutionStatus() == ControlExecutionStatus.EXECUTING
                        || entity.getExecutionStatus() == ControlExecutionStatus.TRANSITIONING) {
                    commandService.markRuntimeRestarted(entity.getCommandId());
                }
                continue;
            }
            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
                continue;
            }
            commandService.syncFromPythonStatus(entity.getCommandId(), response.getBody());
        }
    }

    private String fetchCurrentRunId() {
        ProjectorClient.CurrentRunResult result = projectorClient.fetchCurrentRun();
        if (result instanceof ProjectorClient.CurrentRunResult.Ok ok) {
            ProjectorCurrentRunResponse run = ok.body();
            return run.simulationRunId();
        }
        return null;
    }
}
