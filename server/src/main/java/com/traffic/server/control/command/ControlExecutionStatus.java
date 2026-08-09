package com.traffic.server.control.command;

public enum ControlExecutionStatus {
    NOT_STARTED,
    QUEUED,
    EXECUTING,
    TRANSITIONING,
    APPLIED_AT_SUMO,
    FAILED_AT_RUNTIME
}
