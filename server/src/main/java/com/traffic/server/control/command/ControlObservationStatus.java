package com.traffic.server.control.command;

public enum ControlObservationStatus {
    NOT_REQUESTED,
    PENDING,
    CONFIRMED,
    MISMATCH,
    TIMED_OUT,
    UNAVAILABLE,
    NOT_OBSERVABLE
}
