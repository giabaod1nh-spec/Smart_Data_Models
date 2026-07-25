package com.traffic.server.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

@Configuration
@EnableConfigurationProperties({AppProperties.class, OrionProperties.class})
public class WebClientConfig {

    @Bean
    public WebClient orionWebClient(OrionProperties orion) {
        String linkHeader = String.format(
                "<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"",
                orion.contextUrl());

        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofMillis(orion.timeoutMs()));

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .baseUrl(orion.apiBaseUrl())
                .defaultHeader(HttpHeaders.LINK, linkHeader)
                .defaultHeader(HttpHeaders.ACCEPT, "application/ld+json")
                .build();
    }

    @Bean
    public WebClient controlApiWebClient(AppProperties app) {
        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofMillis(app.controlApi().timeoutMs()));

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .baseUrl(app.controlApi().baseUrl())
                .build();
    }

    @Bean
    public WebClient healthCheckWebClient() {
        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(3));
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
