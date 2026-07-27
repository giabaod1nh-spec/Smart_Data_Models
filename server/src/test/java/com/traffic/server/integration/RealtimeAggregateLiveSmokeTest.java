package com.traffic.server.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Tag("live")
@EnabledIf("com.traffic.server.integration.LiveStackConditions#orionAvailable")
class RealtimeAggregateLiveSmokeTest {

    @DynamicPropertySource
    static void orionLiveUrls(DynamicPropertyRegistry registry) {
        registry.add("orion.api-base-url", () -> "http://localhost:1026/ngsi-ld/v1");
        registry.add("orion.health-url", () -> "http://localhost:1026/version");
        registry.add("orion.context-url", () -> "http://localhost:3004/datamodels.context-ngsi.jsonld");
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    void aggregateIntersectionWhenPresentInOrion() throws Exception {
        MockHttpSession session = login();

        mockMvc.perform(get("/api/realtime/intersections/A").session(session))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.intersection").exists())
                .andExpect(jsonPath("$.data.metadata.simulationRunId").exists())
                .andExpect(jsonPath("$.data.cameras").isArray());
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
