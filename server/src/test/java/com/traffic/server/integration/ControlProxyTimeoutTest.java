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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class ControlProxyTimeoutTest {

    private static WireMockServer controlMock;

    @Autowired
    private MockMvc mockMvc;

    @BeforeAll
    static void startWireMock() {
        controlMock = new WireMockServer(59998);
        controlMock.start();
        WireMock.configureFor("localhost", 59998);
    }

    @AfterAll
    static void stopWireMock() {
        if (controlMock != null) {
            controlMock.stop();
        }
    }

    @Test
    void controlApiTimeoutReturns504() throws Exception {
        WireMock.stubFor(WireMock.get(urlEqualTo("/health"))
                .willReturn(aResponse().withFixedDelay(5000).withStatus(200).withBody("{\"status\":\"ok\"}")));

        MockHttpSession session = login();
        mockMvc.perform(get("/api/control/health").session(session))
                .andExpect(status().isGatewayTimeout());
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
