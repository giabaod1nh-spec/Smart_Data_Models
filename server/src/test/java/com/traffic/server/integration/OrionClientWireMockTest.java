package com.traffic.server.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OrionClientWireMockTest {

    private static WireMockServer orionMock;

    @Autowired
    private MockMvc mockMvc;

    @BeforeAll
    static void startOrionMock() throws Exception {
        orionMock = new WireMockServer(59999);
        orionMock.start();
        WireMock.configureFor("localhost", 59999);

        String golden;
        try (InputStream in = OrionClientWireMockTest.class.getResourceAsStream(
                "/contracts/payloads/Intersection.example.jsonld")) {
            golden = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        WireMock.stubFor(WireMock.get(WireMock.urlPathMatching("/ngsi-ld/v1/entities/.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/ld+json")
                        .withBody(golden)));
    }

    @AfterAll
    static void stopOrionMock() {
        if (orionMock != null) {
            orionMock.stop();
        }
    }

    @Test
    void mapsOrionEntityToRestDto() throws Exception {
        MockHttpSession session = login();

        mockMvc.perform(get("/api/intersections/A").session(session))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.id").value("urn:ngsi-ld:Intersection:A"))
                .andExpect(jsonPath("$.data.scenarioId").value("normal"))
                .andExpect(jsonPath("$.data.simulationTime").exists());
    }

    private MockHttpSession login() throws Exception {
        MockHttpSession session = new MockHttpSession();
        mockMvc.perform(post("/api/auth/login")
                        .session(session)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"admin\",\"password\":\"admin123\"}"))
                .andExpect(status().isOk());
        return session;
    }
}
