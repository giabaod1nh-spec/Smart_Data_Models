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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Tag("live")
@EnabledIf("com.traffic.server.integration.LiveStackConditions#controlApiAvailable")
class ControlProxyLiveSmokeTest {

    @DynamicPropertySource
    static void liveControlApi(DynamicPropertyRegistry registry) {
        registry.add("app.control-api.base-url", () -> "http://localhost:9090");
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    void proxiesControlHealthWhenAuthenticated() throws Exception {
        MockHttpSession session = login();
        mockMvc.perform(get("/api/control/health").session(session))
                .andExpect(status().isOk());
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
