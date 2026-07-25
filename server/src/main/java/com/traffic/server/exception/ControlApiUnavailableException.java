package com.traffic.server.exception;

public class ControlApiUnavailableException extends RuntimeException {

    public ControlApiUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
