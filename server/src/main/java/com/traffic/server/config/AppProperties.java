package com.traffic.server.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        ControlApi controlApi,
        ContextProvider contextProvider,
        Realtime realtime,
        Security security
) {
    public record ControlApi(String baseUrl, int timeoutMs, int maxBodyBytes) {}
    public record ContextProvider(String healthUrl) {}
    public record Realtime(long consistencyRetryMs, double simulationTimeTolerance) {}
    public record Security(Admin admin) {
        public record Admin(String username, String password) {}
    }
}
