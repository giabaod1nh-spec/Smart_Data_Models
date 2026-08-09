package com.traffic.server.analytics.controller;

import com.traffic.server.analytics.exception.AnalyticsNotReadyException;
import com.traffic.server.analytics.service.KpiAnalyticsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(NetworkAnalyticsController.class)
@ActiveProfiles("test")
@Import(com.traffic.server.exception.GlobalExceptionHandler.class)
@org.springframework.test.context.TestPropertySource(properties = "app.analytics.enabled=true")
class NetworkAnalyticsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private KpiAnalyticsService kpiAnalyticsService;

    @Test
    @WithMockUser(roles = "ADMIN")
    void networkWindowsReturns503NotReady() throws Exception {
        when(kpiAnalyticsService.networkWindows(anyString(), anyString(), anyInt(), any(), any(), anyInt(), anyInt(), any()))
                .thenThrow(new AnalyticsNotReadyException("Network overview mart is not populated"));

        mockMvc.perform(get("/api/analytics/network/windows")
                        .param("simulationRunId", "run-1")
                        .param("scenarioId", "normal")
                        .param("windowSizeSec", "60"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("ANALYTICS_NOT_READY")));
    }
}
