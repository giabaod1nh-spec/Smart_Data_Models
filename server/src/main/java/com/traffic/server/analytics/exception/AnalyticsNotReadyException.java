package com.traffic.server.analytics.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
public class AnalyticsNotReadyException extends RuntimeException {

    public AnalyticsNotReadyException(String message) {
        super(message);
    }

    public String errorCode() {
        return "ANALYTICS_NOT_READY";
    }
}
