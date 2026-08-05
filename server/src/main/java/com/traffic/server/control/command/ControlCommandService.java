package com.traffic.server.control.command;

import com.traffic.server.config.AppProperties;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class ControlCommandService {

    private final ControlCommandRepository commandRepository;
    private final ControlResourceLockService lockService;
    private final ControlCommandEventRepository eventRepository;
    private final boolean commandDomainEnabled;
    private final ControlCommandMetrics metrics;

    public ControlCommandService(
            ControlCommandRepository commandRepository,
            ControlResourceLockService lockService,
            ControlCommandEventRepository eventRepository,
            AppProperties appProperties,
            ControlCommandMetrics metrics) {
        this.commandRepository = commandRepository;
        this.lockService = lockService;
        this.eventRepository = eventRepository;
        this.commandDomainEnabled = appProperties.control().commandDomainEnabled();
        this.metrics = metrics;
    }

    public boolean isEnabled() {
        return commandDomainEnabled;
    }

    @Transactional
    public AcceptResult accept(CreateControlCommandRequest req, String operatorId) {
        if (!commandDomainEnabled) {
            throw new IllegalStateException("command domain disabled");
        }
        metrics.commandReceived();
        String fingerprint = RequestFingerprint.compute(
                req.contractVersion(),
                req.commandType().name(),
                req.target(),
                req.payload(),
                req.expectedRunId(),
                req.source());

        Optional<ControlCommandEntity> existing =
                commandRepository.findByOperatorIdAndIdempotencyKey(operatorId, req.idempotencyKey());
        if (existing.isPresent()) {
            ControlCommandEntity row = existing.get();
            if (!fingerprint.equals(row.getRequestFingerprint())) {
                throw new IdempotencyConflictException(
                        "IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_REQUEST");
            }
            return new AcceptResult(ControlCommandStatusResponse.from(row), false);
        }

        UUID commandId = req.commandId() != null ? req.commandId() : UUID.randomUUID();
        Optional<String> resourceKey = ControlResourceKeyResolver.resolve(req.commandType(), req.target());

        ControlCommandEntity entity = new ControlCommandEntity();
        entity.setCommandId(commandId);
        entity.setContractVersion(req.contractVersion());
        entity.setIdempotencyKey(req.idempotencyKey());
        entity.setRequestFingerprint(fingerprint);
        entity.setOperatorId(operatorId);
        entity.setSource(req.source());
        entity.setCommandType(req.commandType());
        entity.setTargetJson(req.target().toString());
        entity.setPayloadJson(req.payload().toString());
        entity.setExpectedRunId(req.expectedRunId());
        resourceKey.ifPresent(entity::setResourceKey);
        entity.setLifecycleStatus(ControlLifecycleStatus.RECEIVED);
        entity.setDispatchStatus(ControlDispatchStatus.PENDING);
        entity.setExecutionStatus(ControlExecutionStatus.NOT_STARTED);
        entity.setObservationStatus(ControlObservationStatus.NOT_REQUESTED);
        entity.setRequestedAtUtc(req.requestedAt());
        entity.setExpiresAtUtc(req.expiresAt());

        if (resourceKey.isPresent()) {
            boolean claimed = lockService.tryClaim(resourceKey.get(), commandId, req.expiresAt());
            if (!claimed) {
                metrics.resourceConflict();
                throw new ResourceBusyException("RESOURCE_BUSY");
            }
        }

        try {
            commandRepository.save(entity);
            appendEvent(commandId, "RECEIVED", null);
            metrics.commandAccepted();
        } catch (RuntimeException e) {
            resourceKey.ifPresent(lockService::release);
            throw e;
        }
        return new AcceptResult(ControlCommandStatusResponse.from(entity), true);
    }

    @Transactional
    public void recordDispatchAccepted(UUID commandId, String acceptedRunId) {
        commandRepository.findById(commandId).ifPresent(entity -> {
            entity.setDispatchStatus(ControlDispatchStatus.ACCEPTED);
            entity.setExecutionStatus(ControlExecutionStatus.QUEUED);
            entity.setLifecycleStatus(ControlLifecycleStatus.QUEUED);
            entity.setAcceptedRunId(acceptedRunId);
            appendEvent(commandId, "DISPATCH_ACCEPTED", acceptedRunId);
        });
    }

    @Transactional
    public void recordDispatchFailed(UUID commandId, String errorCode, String detail) {
        commandRepository.findById(commandId).ifPresent(entity -> {
            entity.setDispatchStatus(ControlDispatchStatus.FAILED);
            entity.setLifecycleStatus(ControlLifecycleStatus.FAILED);
            entity.setErrorCode(errorCode);
            entity.setErrorSafeDetail(detail);
            releaseLock(entity.getResourceKey());
            appendEvent(commandId, "DISPATCH_FAILED", errorCode);
        });
    }

    @Transactional
    public void recordDispatchUnknown(UUID commandId) {
        commandRepository.findById(commandId).ifPresent(entity -> {
            entity.setDispatchStatus(ControlDispatchStatus.UNKNOWN);
            entity.setLifecycleStatus(ControlLifecycleStatus.UNKNOWN_OUTCOME);
            appendEvent(commandId, "DISPATCH_UNKNOWN", null);
        });
    }

    @Transactional(readOnly = true)
    public Optional<ControlCommandStatusResponse> get(UUID commandId) {
        return commandRepository.findById(commandId).map(ControlCommandStatusResponse::from);
    }

    @Transactional(readOnly = true)
    public Optional<ControlCommandEntity> findEntity(UUID commandId) {
        return commandRepository.findById(commandId);
    }

    public void releaseResourceLock(String resourceKey) {
        lockService.release(resourceKey);
    }

    @Transactional
    public void markExpired(ControlCommandEntity entity) {
        entity.setLifecycleStatus(ControlLifecycleStatus.EXPIRED);
        entity.setErrorCode("COMMAND_EXPIRED");
        entity.setErrorSafeDetail("command expired");
        releaseResourceLock(entity.getResourceKey());
        metrics.commandExpired();
    }

    @Transactional
    public void markRuntimeRestarted(UUID commandId) {
        commandRepository.findById(commandId).ifPresent(entity -> {
            if (entity.getLifecycleStatus() == ControlLifecycleStatus.COMPLETED
                    || entity.getLifecycleStatus() == ControlLifecycleStatus.FAILED
                    || entity.getLifecycleStatus() == ControlLifecycleStatus.EXPIRED) {
                return;
            }
            entity.setLifecycleStatus(ControlLifecycleStatus.FAILED);
            entity.setExecutionStatus(ControlExecutionStatus.FAILED_AT_RUNTIME);
            entity.setErrorCode("RUNTIME_RESTARTED");
            entity.setErrorSafeDetail("command lost after runtime restart");
            releaseResourceLock(entity.getResourceKey());
            metrics.commandFailed();
            appendEvent(commandId, "RUNTIME_RESTARTED", null);
        });
    }

    @Transactional
    public void markStaleRun(UUID commandId) {
        commandRepository.findById(commandId).ifPresent(entity -> {
            if (entity.getLifecycleStatus() == ControlLifecycleStatus.COMPLETED
                    || entity.getLifecycleStatus() == ControlLifecycleStatus.FAILED
                    || entity.getLifecycleStatus() == ControlLifecycleStatus.EXPIRED) {
                return;
            }
            entity.setLifecycleStatus(ControlLifecycleStatus.FAILED);
            entity.setExecutionStatus(ControlExecutionStatus.FAILED_AT_RUNTIME);
            entity.setErrorCode("STALE_RUN");
            entity.setErrorSafeDetail("simulation run changed");
            releaseResourceLock(entity.getResourceKey());
            metrics.commandFailed();
            appendEvent(commandId, "STALE_RUN", null);
        });
    }

    @Transactional
    public void syncFromPythonStatus(UUID commandId, String body) {
        commandRepository.findById(commandId).ifPresent(entity -> {
            ControlLifecycleStatus priorLifecycle = entity.getLifecycleStatus();
            Instant requestedAt = entity.getRequestedAtUtc();
            try {
                JsonNode node = MAPPER.readTree(body);
                String lifecycle = text(node, "lifecycleStatus");
                String execution = text(node, "executionStatus");
                String dispatch = text(node, "dispatchStatus");
                if (lifecycle != null) {
                    entity.setLifecycleStatus(ControlLifecycleStatus.valueOf(lifecycle));
                }
                if (execution != null) {
                    entity.setExecutionStatus(ControlExecutionStatus.valueOf(execution));
                }
                if (dispatch != null) {
                    entity.setDispatchStatus(ControlDispatchStatus.valueOf(dispatch));
                }
                JsonNode err = node.get("error");
                if (err != null && !err.isNull()) {
                    entity.setErrorCode(text(err, "code"));
                    entity.setErrorSafeDetail(text(err, "message"));
                }
                if (entity.getLifecycleStatus() == ControlLifecycleStatus.COMPLETED
                        && entity.getExecutionStatus() == ControlExecutionStatus.APPLIED_AT_SUMO) {
                    releaseResourceLock(entity.getResourceKey());
                    if (priorLifecycle != ControlLifecycleStatus.COMPLETED) {
                        metrics.commandApplied();
                        if (requestedAt != null) {
                            metrics.recordExecutionLatencyMs(
                                    java.time.Duration.between(requestedAt, Instant.now()).toMillis());
                        }
                    }
                }
                if (entity.getLifecycleStatus() == ControlLifecycleStatus.FAILED
                        && priorLifecycle != ControlLifecycleStatus.FAILED) {
                    releaseResourceLock(entity.getResourceKey());
                    metrics.commandFailed();
                }
                if (entity.getLifecycleStatus() == ControlLifecycleStatus.UNKNOWN_OUTCOME
                        && priorLifecycle != ControlLifecycleStatus.UNKNOWN_OUTCOME) {
                    metrics.commandUnknownOutcome();
                }
            } catch (Exception ignored) {
                // leave entity unchanged on parse errors
            }
        });
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asString();
    }

    private static final tools.jackson.databind.ObjectMapper MAPPER =
            new tools.jackson.databind.ObjectMapper();

    private void releaseLock(String resourceKey) {
        releaseResourceLock(resourceKey);
    }

    private void appendEvent(UUID commandId, String eventType, String payload) {
        ControlCommandEventEntity event = new ControlCommandEventEntity();
        event.setCommandId(commandId);
        event.setEventType(eventType);
        event.setPayloadJson(payload);
        eventRepository.save(event);
    }

    public record AcceptResult(ControlCommandStatusResponse status, boolean newlyCreated) {}
}
