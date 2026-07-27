package com.traffic.server.contract;

import com.traffic.server.payload.IntersectionResponse;
import com.traffic.server.service.NgsiEntityMapper;
import com.traffic.server.service.OrionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.InputStream;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class ContractMatrixRestTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrionService orionService;

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Test
    void intersectionRestJsonIncludesRequiredFieldsFromGolden() throws Exception {
        JsonNode golden = loadGolden("Intersection");
        IntersectionResponse mapped = new NgsiEntityMapper().toIntersection(golden);
        when(orionService.getIntersection("A")).thenReturn(mapped);

        MockHttpSession session = login();

        mockMvc.perform(get("/api/intersections/A").session(session))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.scenarioId").value("normal"))
                .andExpect(jsonPath("$.data.simulationRunId").exists())
                .andExpect(jsonPath("$.data.simulationTime").exists());
    }

    private JsonNode loadGolden(String type) throws Exception {
        String path = "/contracts/payloads/" + type + ".example.jsonld";
        try (InputStream in = getClass().getResourceAsStream(path)) {
            return jsonMapper.readTree(in);
        }
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
