package com.traffic.server.exception;

public class RealtimeIdleException extends RuntimeException {
    public RealtimeIdleException() {
        super("no active simulation run");
    }
}
