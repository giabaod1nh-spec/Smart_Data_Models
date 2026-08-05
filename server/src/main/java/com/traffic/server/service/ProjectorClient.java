package com.traffic.server.service;

import com.traffic.server.payload.ProjectorCurrentRunResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Service
public class ProjectorClient {

    private final RestClient restClient;
    private final String baseUrl;

    public ProjectorClient(
            RestClient.Builder builder,
            @Value("${projector.base-url:http://localhost:8093}") String baseUrl) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.restClient = builder.build();
    }

    public sealed interface CurrentRunResult {
        record Ok(ProjectorCurrentRunResponse body) implements CurrentRunResult {}
        record Idle() implements CurrentRunResult {}
        record Unavailable() implements CurrentRunResult {}
    }

    public CurrentRunResult fetchCurrentRun() {
        try {
            return restClient.get()
                    .uri(baseUrl + "/current-run")
                    .exchange((request, response) -> {
                        if (response.getStatusCode() == HttpStatus.NO_CONTENT) {
                            return new CurrentRunResult.Idle();
                        }
                        if (response.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE) {
                            return new CurrentRunResult.Unavailable();
                        }
                        if (response.getStatusCode().is2xxSuccessful()) {
                            ProjectorCurrentRunResponse body =
                                    response.bodyTo(ProjectorCurrentRunResponse.class);
                            if (body != null) {
                                return new CurrentRunResult.Ok(body);
                            }
                        }
                        return new CurrentRunResult.Unavailable();
                    });
        } catch (RestClientException ex) {
            return new CurrentRunResult.Unavailable();
        }
    }
}
