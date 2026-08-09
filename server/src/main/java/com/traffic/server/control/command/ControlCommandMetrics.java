package com.traffic.server.control.command;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Micrometer command metrics without high-cardinality labels (RC8). */
@Component
public class ControlCommandMetrics {

    private final Counter commandsReceived;
    private final Counter commandsAccepted;
    private final Counter commandsApplied;
    private final Counter commandsFailed;
    private final Counter commandsUnknownOutcome;
    private final Counter commandsExpired;
    private final Counter resourceConflict;
    private final Counter dispatchFailures;
    private final Counter reconciliationAttempts;
    private final Timer commandExecutionLatency;
    private final Timer commandObservationLatency;

    public ControlCommandMetrics(MeterRegistry registry, ControlCommandRepository commandRepository) {
        this.commandsReceived = counter(registry, "commands_received_total");
        this.commandsAccepted = counter(registry, "commands_accepted_total");
        this.commandsApplied = counter(registry, "commands_applied_total");
        this.commandsFailed = counter(registry, "commands_failed_total");
        this.commandsUnknownOutcome = counter(registry, "commands_unknown_outcome_total");
        this.commandsExpired = counter(registry, "commands_expired_total");
        this.resourceConflict = counter(registry, "resource_conflict_total");
        this.dispatchFailures = counter(registry, "command_dispatch_failures_total");
        this.reconciliationAttempts = counter(registry, "command_reconciliation_attempts_total");
        this.commandExecutionLatency = Timer.builder("command_execution_latency_ms")
                .publishPercentiles(0.5, 0.95)
                .register(registry);
        this.commandObservationLatency = Timer.builder("command_observation_latency_ms")
                .publishPercentiles(0.5, 0.95)
                .register(registry);
        Gauge.builder("command_queue_depth", queueDepthSupplier(commandRepository))
                .register(registry);
    }

    private static Supplier<Number> queueDepthSupplier(ControlCommandRepository commandRepository) {
        return () -> commandRepository.countByLifecycleStatusIn(
                CommandReconciliationScheduler.ACTIVE_LIFECYCLE);
    }

    private static Counter counter(MeterRegistry registry, String name) {
        return Counter.builder(name).register(registry);
    }

    public void commandReceived() {
        commandsReceived.increment();
    }

    public void commandAccepted() {
        commandsAccepted.increment();
    }

    public void commandApplied() {
        commandsApplied.increment();
    }

    public void commandFailed() {
        commandsFailed.increment();
    }

    public void commandUnknownOutcome() {
        commandsUnknownOutcome.increment();
    }

    public void commandExpired() {
        commandsExpired.increment();
    }

    public void resourceConflict() {
        resourceConflict.increment();
    }

    public void dispatchFailure() {
        dispatchFailures.increment();
    }

    public void reconciliationAttempt() {
        reconciliationAttempts.increment();
    }

    public void recordExecutionLatencyMs(long millis) {
        commandExecutionLatency.record(millis, TimeUnit.MILLISECONDS);
    }

    public void recordObservationLatencyMs(long millis) {
        commandObservationLatency.record(millis, TimeUnit.MILLISECONDS);
    }
}
