package com.traffic.server.contract;

import com.traffic.server.payload.IntersectionResponse;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContractMatrixSerializationTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Test
    void omitsNullFieldsFromRestJson() throws Exception {
        IntersectionResponse dto = IntersectionResponse.builder()
                .id("urn:ngsi-ld:Intersection:A")
                .type("Intersection")
                .name("Test")
                .scenarioId("normal")
                .build();

        JsonNode json = jsonMapper.readTree(jsonMapper.writeValueAsString(dto));

        assertTrue(json.has("scenarioId"));
        assertFalse(json.has("description"));
        assertFalse(json.has("probableCauseType"));
    }
}
