package com.traffic.server.control;

import com.traffic.server.config.AppProperties;
import com.traffic.server.exception.ControlApiTimeoutException;
import com.traffic.server.exception.ControlApiUnavailableException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.netty.http.client.PrematureCloseException;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Service
public class ControlApiClient {

    private final WebClient controlApiWebClient;
    private final String internalToken;

    public ControlApiClient(WebClient controlApiWebClient, AppProperties appProperties) {
        this.controlApiWebClient = controlApiWebClient;
        this.internalToken = appProperties.controlApi().internalToken();
    }

    public ResponseEntity<String> forward(HttpMethod method,
                                          String upstreamPath,
                                          Map<String, List<String>> queryParams,
                                          String requestBody,
                                          HttpHeaders incomingHeaders,
                                          String requestId) {
        return exchange(method, upstreamPath, queryParams, requestBody, incomingHeaders, requestId);
    }

    public ResponseEntity<String> submitCommand(String requestBody, String requestId) {
        return exchange(
                HttpMethod.POST,
                "/commands",
                null,
                requestBody,
                null,
                requestId);
    }

    public ResponseEntity<String> getCommandStatus(UUID commandId, String requestId) {
        return exchange(
                HttpMethod.GET,
                "/commands/" + commandId,
                null,
                null,
                null,
                requestId);
    }

    private ResponseEntity<String> exchange(HttpMethod method,
                                            String upstreamPath,
                                            Map<String, List<String>> queryParams,
                                            String requestBody,
                                            HttpHeaders incomingHeaders,
                                            String requestId) {
        try {
            WebClient.RequestBodySpec spec = controlApiWebClient
                    .method(method)
                    .uri(uriBuilder -> {
                        var b = uriBuilder.path(upstreamPath);
                        if (queryParams != null) {
                            queryParams.forEach((key, values) -> {
                                if (values != null) {
                                    values.forEach(v -> b.queryParam(key, v));
                                }
                            });
                        }
                        return b.build();
                    })
                    .headers(h -> applyHeaders(incomingHeaders, h, requestId));

            WebClient.ResponseSpec responseSpec;
            if (method == HttpMethod.GET || method == HttpMethod.DELETE) {
                responseSpec = spec.retrieve();
            } else {
                String body = requestBody != null ? requestBody : "{}";
                responseSpec = spec
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(body)
                        .retrieve();
            }

            return responseSpec.toEntity(String.class).block();
        } catch (WebClientResponseException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .headers(e.getHeaders())
                    .body(e.getResponseBodyAsString());
        } catch (WebClientRequestException e) {
            if (isTimeout(e)) {
                throw new ControlApiTimeoutException("Control API request timed out", e);
            }
            throw new ControlApiUnavailableException("Control API unavailable", e);
        }
    }

    private static boolean isTimeout(Throwable e) {
        Throwable cur = e;
        while (cur != null) {
            if (cur instanceof TimeoutException || cur instanceof PrematureCloseException) {
                return true;
            }
            String name = cur.getClass().getName();
            if (name.contains("TimeoutException") || name.contains("ReadTimeoutException")) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    private void applyHeaders(HttpHeaders incoming, HttpHeaders outgoing, String requestId) {
        if (incoming != null) {
            copyHeader(incoming, outgoing, HttpHeaders.ACCEPT);
            copyHeader(incoming, outgoing, HttpHeaders.CONTENT_TYPE);
            copyHeader(incoming, outgoing, "X-Request-Id");
            copyHeader(incoming, outgoing, "X-Correlation-Id");
        }
        if (outgoing.getFirst("X-Request-Id") == null && requestId != null) {
            outgoing.set("X-Request-Id", requestId);
        }
        if (internalToken != null && !internalToken.isBlank()) {
            outgoing.setBearerAuth(internalToken);
        }
        if (outgoing.getAccept().isEmpty()) {
            outgoing.setAccept(List.of(MediaType.APPLICATION_JSON));
        }
    }

    private static void copyHeader(HttpHeaders from, HttpHeaders to, String name) {
        List<String> values = from.get(name);
        if (values != null && !values.isEmpty()) {
            to.put(name, values);
        }
    }
}
