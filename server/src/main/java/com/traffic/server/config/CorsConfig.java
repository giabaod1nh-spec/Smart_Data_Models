package com.traffic.server.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.List;

@Configuration
@EnableConfigurationProperties(CorsProperties.class)
public class CorsConfig {

    private static final List<String> API_PATHS = List.of("/api/**");

    @Bean
    public CorsConfigurationSource corsConfigurationSource(CorsProperties cors) {
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        if (!cors.enabled()) {
            return source;
        }

        if (cors.allowedOrigins().isEmpty()) {
            throw new IllegalStateException(
                    "app.cors.enabled=true requires at least one origin in app.cors.allowed-origins");
        }
        if (cors.allowCredentials() && cors.allowedOrigins().stream().anyMatch("*"::equals)) {
            throw new IllegalStateException(
                    "app.cors.allow-credentials=true cannot be combined with wildcard allowed-origins");
        }

        CorsConfiguration config = new CorsConfiguration();
        config.setAllowedOrigins(cors.allowedOrigins());
        config.setAllowedMethods(cors.allowedMethods());
        config.setAllowedHeaders(cors.allowedHeaders());
        config.setExposedHeaders(cors.exposedHeaders());
        config.setAllowCredentials(cors.allowCredentials());
        config.setMaxAge(cors.maxAgeSec());

        for (String path : API_PATHS) {
            source.registerCorsConfiguration(path, config);
        }
        return source;
    }
}
