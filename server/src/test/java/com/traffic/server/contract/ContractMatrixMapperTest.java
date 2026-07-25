package com.traffic.server.contract;

import com.traffic.server.payload.CameraResponse;
import com.traffic.server.payload.IntersectionResponse;
import com.traffic.server.payload.TrafficLightResponse;
import com.traffic.server.payload.VehicleSensorResponse;
import com.traffic.server.service.NgsiEntityMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ContractMatrixMapperTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private NgsiEntityMapper mapper;
    private JsonNode matrix;

    @BeforeEach
    void setUp() throws Exception {
        mapper = new NgsiEntityMapper();
        try (InputStream in = getClass().getResourceAsStream("/contract-matrix-v1.json")) {
            matrix = jsonMapper.readTree(in);
        }
    }

    @Test
    void intersectionGoldenMapsRequiredFields() throws Exception {
        JsonNode entity = loadGolden("Intersection");
        IntersectionResponse dto = mapper.toIntersection(entity);
        JsonNode json = jsonMapper.readTree(jsonMapper.writeValueAsString(dto));
        assertRequiredPresent("Intersection", json, entity);
    }

    @Test
    void trafficLightGoldenMapsRequiredFields() throws Exception {
        JsonNode entity = loadGolden("TrafficLight");
        TrafficLightResponse dto = mapper.toTrafficLight(entity);
        JsonNode json = jsonMapper.readTree(jsonMapper.writeValueAsString(dto));
        assertRequiredPresent("TrafficLight", json, entity);
    }

    @Test
    void vehicleSensorGoldenMapsRequiredFields() throws Exception {
        JsonNode entity = loadGolden("VehicleSensor");
        VehicleSensorResponse dto = mapper.toVehicleSensor(entity);
        JsonNode json = jsonMapper.readTree(jsonMapper.writeValueAsString(dto));
        assertRequiredPresent("VehicleSensor", json, entity);
    }

    @Test
    void cameraGoldenMapsRequiredFields() throws Exception {
        JsonNode entity = loadGolden("Camera");
        CameraResponse dto = mapper.toCamera(entity);
        JsonNode json = jsonMapper.readTree(jsonMapper.writeValueAsString(dto));
        assertRequiredPresent("Camera", json, entity);
    }

    private JsonNode loadGolden(String type) throws Exception {
        String path = "/contracts/payloads/" + type + ".example.jsonld";
        try (InputStream in = getClass().getResourceAsStream(path)) {
            assertNotNull(in, "Missing classpath golden: " + path);
            return jsonMapper.readTree(in);
        }
    }

    private void assertRequiredPresent(String entityType, JsonNode dtoJson, JsonNode golden) {
        JsonNode required = matrix.path(entityType).path("required");
        if (required.isArray()) {
            for (JsonNode fieldNode : required) {
                String field = fieldNode.asText();
                if (!goldenHasValue(golden, field)) {
                    continue;
                }
                JsonNode value = dtoJson.get(field);
                assertNotNull(value, entityType + "." + field + " missing in DTO JSON");
                assertFalse(value.isNull(), entityType + "." + field + " is null");
            }
        }
    }

    private boolean goldenHasValue(JsonNode golden, String field) {
        JsonNode node = golden.get(field);
        if (node == null || node.isMissingNode()) {
            return false;
        }
        JsonNode value = node.path("value");
        return !value.isMissingNode() && !value.isNull();
    }
}
