package com.traffic.server.exception;

public class ControlApiTimeoutException extends RuntimeException {

    public ControlApiTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
