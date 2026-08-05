package com.traffic.server.control.command;

public enum ControlLifecycleStatus {
    RECEIVED,
    VALIDATED,
    QUEUED,
    APPLYING,
    COMPLETED,
    FAILED,
    EXPIRED,
    UNKNOWN_OUTCOME
}
