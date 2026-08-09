package com.traffic.server.control.command;

public class ResourceBusyException extends RuntimeException {

    public ResourceBusyException(String message) {
        super(message);
    }
}
