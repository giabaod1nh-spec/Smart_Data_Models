package com.traffic.server.exception;

import com.traffic.server.payload.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BadCredentialsException.class)
    public ResponseEntity<ApiResponse<Object>> handleBadCredentials(BadCredentialsException e) {
        int status = HttpStatus.UNAUTHORIZED.value();
        return ResponseEntity.status(status)
                .body(ApiResponse.error(status, "Invalid username or password"));
    }

    @ExceptionHandler(AuthenticationException.class)
    public ResponseEntity<ApiResponse<Object>> handleAuthentication(AuthenticationException e) {
        int status = HttpStatus.UNAUTHORIZED.value();
        return ResponseEntity.status(status)
                .body(ApiResponse.error(status, "Authentication failed: " + e.getMessage()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiResponse<Object>> handleResponseStatus(ResponseStatusException e) {
        int status = e.getStatusCode().value();
        String message = e.getReason() != null ? e.getReason() : e.getMessage();
        return ResponseEntity.status(status).body(ApiResponse.error(status, message));
    }

    @ExceptionHandler(ControlApiUnavailableException.class)
    public ResponseEntity<ErrorResponse> handleControlUnavailable(ControlApiUnavailableException e,
                                                                  HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ErrorResponse.of(503, "SERVICE_UNAVAILABLE", e.getMessage(), request.getRequestURI()));
    }

    @ExceptionHandler(ControlApiTimeoutException.class)
    public ResponseEntity<ErrorResponse> handleControlTimeout(ControlApiTimeoutException e,
                                                              HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(ErrorResponse.of(504, "GATEWAY_TIMEOUT", e.getMessage(), request.getRequestURI()));
    }

    @ExceptionHandler(WebClientResponseException.class)
    public ResponseEntity<ApiResponse<Object>> handleOrionError(WebClientResponseException e) {
        int status = e.getStatusCode().value();
        return ResponseEntity.status(status)
                .body(ApiResponse.error(status, "Orion error: " + e.getMessage()));
    }

    @ExceptionHandler(WebClientRequestException.class)
    public ResponseEntity<ApiResponse<Object>> handleOrionUnreachable(WebClientRequestException e) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(ApiResponse.error(HttpStatus.BAD_GATEWAY.value(),
                        "Cannot reach Orion Context Broker: " + e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Object>> handleUnexpected(Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(HttpStatus.INTERNAL_SERVER_ERROR.value(),
                        "Internal server error: " + e.getMessage()));
    }
}
