package com.traffic.server.analytics.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
public class AnalyticsQueryTimeoutException extends RuntimeException {

    public AnalyticsQueryTimeoutException(String message) {
        super(message);
    }

    public String errorCode() {
        return "QUERY_TIMEOUT";
    }
}
