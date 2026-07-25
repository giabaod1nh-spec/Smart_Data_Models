package com.traffic.server.controller;

import com.traffic.server.config.AppProperties;
import com.traffic.server.control.ControlApiClient;
import com.traffic.server.control.ControlProxyAllowlist;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/api/control")
public class ControlProxyController {

    private static final Set<HttpMethod> ALLOWED_METHODS = Set.of(
            HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE);

    private final ControlApiClient controlApiClient;
    private final ControlProxyAllowlist allowlist;
    private final AppProperties appProperties;

    public ControlProxyController(ControlApiClient controlApiClient,
                                    ControlProxyAllowlist allowlist,
                                    AppProperties appProperties) {
        this.controlApiClient = controlApiClient;
        this.allowlist = allowlist;
        this.appProperties = appProperties;
    }

    @RequestMapping(value = "/**")
    public ResponseEntity<String> proxy(HttpServletRequest request,
                                        @RequestBody(required = false) byte[] body) {
        HttpMethod method = HttpMethod.valueOf(request.getMethod());
        if (!ALLOWED_METHODS.contains(method)) {
            return ResponseEntity.status(405).body("Method not allowed");
        }

        String upstreamPath = ControlProxyAllowlist.extractUpstreamPath(request.getRequestURI());
        if (upstreamPath == null || !allowlist.isAllowed(upstreamPath)) {
            return ResponseEntity.notFound().build();
        }

        if ((method == HttpMethod.POST || method == HttpMethod.DELETE) && body != null) {
            if (body.length > appProperties.controlApi().maxBodyBytes()) {
                return ResponseEntity.status(413).body("Request body too large");
            }
            String contentType = request.getContentType();
            if (contentType == null || !contentType.contains(MediaType.APPLICATION_JSON_VALUE)) {
                return ResponseEntity.status(415).body("Content-Type must be application/json");
            }
        }

        String requestBody = body != null ? new String(body, StandardCharsets.UTF_8) : null;
        String requestId = request.getHeader("X-Request-Id");
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }

        HttpHeaders headers = new HttpHeaders();
        Collections.list(request.getHeaderNames()).forEach(name ->
                headers.addAll(name, Collections.list(request.getHeaders(name))));

        LinkedMultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        request.getParameterMap().forEach((key, values) -> {
            if (values != null) {
                for (String value : values) {
                    queryParams.add(key, value);
                }
            }
        });

        ResponseEntity<String> upstream = controlApiClient.forward(
                method,
                upstreamPath,
                queryParams,
                requestBody,
                headers,
                requestId);

        return ResponseEntity.status(upstream.getStatusCode())
                .headers(filterResponseHeaders(upstream.getHeaders()))
                .body(upstream.getBody());
    }

    private static HttpHeaders filterResponseHeaders(HttpHeaders upstream) {
        HttpHeaders out = new HttpHeaders();
        if (upstream == null) {
            return out;
        }
        upstream.forEach((name, values) -> {
            if (HttpHeaders.TRANSFER_ENCODING.equalsIgnoreCase(name)
                    || HttpHeaders.CONNECTION.equalsIgnoreCase(name)) {
                return;
            }
            out.addAll(name, values);
        });
        return out;
    }
}
