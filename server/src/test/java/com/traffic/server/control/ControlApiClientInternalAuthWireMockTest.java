package com.traffic.server.control;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.traffic.server.config.AppProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

/** G2 — Spring ControlApiClient bearer + X-Request-Id E2E to Python /commands. */
@SpringBootTest
@ActiveProfiles("test")
class ControlApiClientInternalAuthWireMockTest {

    private static WireMockServer controlMock;

    @Autowired
    private ControlApiClient controlApiClient;

    @Autowired
    private AppProperties appProperties;

    @BeforeAll
    static void startWireMock() {
        controlMock = new WireMockServer(59997);
        controlMock.start();
        WireMock.configureFor("localhost", 59997);
    }

    @AfterAll
    static void stopWireMock() {
        if (controlMock != null) {
            controlMock.stop();
        }
    }

    @DynamicPropertySource
    static void controlApiProps(DynamicPropertyRegistry registry) {
        registry.add("app.control-api.base-url", () -> "http://localhost:59997");
        registry.add("app.control-api.internal-token", () -> "test-internal-token");
    }

    @Test
    void submitCommandSendsBearerAndRequestId() {
        stubFor(post(urlEqualTo("/commands"))
                .withHeader("Authorization", equalTo("Bearer test-internal-token"))
                .withHeader("X-Request-Id", equalTo("req-123"))
                .willReturn(aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {"commandId":"550e8400-e29b-41d4-a716-446655440000",\
                                "lifecycleStatus":"QUEUED","dispatchStatus":"ACCEPTED",\
                                "executionStatus":"QUEUED","acceptedRunId":"run-1"}
                                """)));

        ResponseEntity<String> response = controlApiClient.submitCommand("{}", "req-123");
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        verify(postRequestedFor(urlEqualTo("/commands"))
                .withHeader("Authorization", equalTo("Bearer test-internal-token")));
    }

    @Test
    void getCommandStatusUsesBearer() {
        UUID id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        stubFor(get(urlEqualTo("/commands/" + id))
                .withHeader("Authorization", matching("Bearer .+"))
                .willReturn(aResponse().withStatus(200).withBody("{\"lifecycleStatus\":\"COMPLETED\"}")));

        ResponseEntity<String> response = controlApiClient.getCommandStatus(id, "req-456");
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
    }
}
