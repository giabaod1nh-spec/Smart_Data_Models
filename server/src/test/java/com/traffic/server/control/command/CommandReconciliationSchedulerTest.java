package com.traffic.server.control.command;

import com.traffic.server.control.ControlApiClient;
import com.traffic.server.payload.ProjectorCurrentRunResponse;
import com.traffic.server.service.ProjectorClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CommandReconciliationSchedulerTest {

    @Mock
    ControlCommandRepository commandRepository;
    @Mock
    ControlCommandService commandService;
    @Mock
    ControlCommandDispatchService dispatchService;
    @Mock
    ControlApiClient controlApiClient;
    @Mock
    ControlCommandMetrics metrics;
    @Mock
    ProjectorClient projectorClient;

    CommandReconciliationScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new CommandReconciliationScheduler(
                commandRepository,
                commandService,
                dispatchService,
                controlApiClient,
                metrics,
                projectorClient,
                new com.traffic.server.config.AppProperties(
                        new com.traffic.server.config.AppProperties.ControlApi(
                                "http://localhost:9090", 2000, 65536, ""),
                        new com.traffic.server.config.AppProperties.Control(true, false, false),
                        new com.traffic.server.config.AppProperties.ContextProvider("http://x"),
                        new com.traffic.server.config.AppProperties.Realtime(150, 0.0001, 2.0),
                        new com.traffic.server.config.AppProperties.Security(
                                new com.traffic.server.config.AppProperties.Security.Admin("a", "b"))));
    }

    @Test
    void pollPythonStatus_marksRuntimeRestartedOn404ForQueuedCommand() {
        UUID id = UUID.randomUUID();
        ControlCommandEntity entity = queuedEntity(id);
        when(projectorClient.fetchCurrentRun()).thenReturn(
                new ProjectorClient.CurrentRunResult.Ok(
                        new ProjectorCurrentRunResponse("run-1", "normal", 0.0, "running", 1, 0.1, null)));
        when(commandRepository.findNonTerminal(any())).thenReturn(List.of(entity));
        when(controlApiClient.getCommandStatus(eq(id), any())).thenReturn(ResponseEntity.notFound().build());

        scheduler.pollPythonStatus();

        verify(commandService).markRuntimeRestarted(id);
    }

    @Test
    void pollPythonStatus_marksStaleRunWhenProjectorRunDiffers() {
        UUID id = UUID.randomUUID();
        ControlCommandEntity entity = queuedEntity(id);
        entity.setExpectedRunId("run-old");
        when(projectorClient.fetchCurrentRun()).thenReturn(
                new ProjectorClient.CurrentRunResult.Ok(
                        new ProjectorCurrentRunResponse("run-new", "normal", 0.0, "running", 1, 0.1, null)));
        when(commandRepository.findNonTerminal(any())).thenReturn(List.of(entity));

        scheduler.pollPythonStatus();

        verify(commandService).markStaleRun(id);
        verify(controlApiClient, never()).getCommandStatus(any(), any());
    }

    @Test
    void pollPythonStatus_syncsFromPythonOn200() {
        UUID id = UUID.randomUUID();
        ControlCommandEntity entity = queuedEntity(id);
        entity.setDispatchStatus(ControlDispatchStatus.ACCEPTED);
        when(projectorClient.fetchCurrentRun()).thenReturn(
                new ProjectorClient.CurrentRunResult.Ok(
                        new ProjectorCurrentRunResponse("run-1", "normal", 0.0, "running", 1, 0.1, null)));
        when(commandRepository.findNonTerminal(any())).thenReturn(List.of(entity));
        when(controlApiClient.getCommandStatus(eq(id), any()))
                .thenReturn(ResponseEntity.ok("{\"lifecycleStatus\":\"COMPLETED\"}"));

        scheduler.pollPythonStatus();

        verify(commandService).syncFromPythonStatus(eq(id), any());
        verify(commandService, never()).markRuntimeRestarted(id);
    }

    private static ControlCommandEntity queuedEntity(UUID id) {
        ControlCommandEntity entity = new ControlCommandEntity();
        entity.setCommandId(id);
        entity.setDispatchStatus(ControlDispatchStatus.ACCEPTED);
        entity.setExecutionStatus(ControlExecutionStatus.QUEUED);
        entity.setLifecycleStatus(ControlLifecycleStatus.QUEUED);
        entity.setExpectedRunId("run-1");
        entity.setRequestedAtUtc(Instant.now());
        entity.setExpiresAtUtc(Instant.now().plusSeconds(300));
        return entity;
    }
}
