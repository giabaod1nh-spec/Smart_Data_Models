package com.traffic.server.service.impl;

import com.traffic.server.payload.CameraResponse;
import com.traffic.server.payload.IntersectionResponse;
import com.traffic.server.payload.TrafficLightResponse;
import com.traffic.server.payload.VehicleSensorResponse;
import com.traffic.server.service.NgsiEntityMapper;
import com.traffic.server.service.OrionService;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Service
public class OrionServiceImpl implements OrionService {

    private static final int QUERY_LIMIT = 1000;

    private final WebClient webClient;
    private final NgsiEntityMapper mapper;

    public OrionServiceImpl(WebClient orionWebClient, NgsiEntityMapper mapper) {
        this.webClient = orionWebClient;
        this.mapper = mapper;
    }

    @Override
    public String getRawEntity(String entityId) {
        return webClient.get()
                .uri("/entities/{id}", entityId)
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    // ---------------- Intersection ----------------

    @Override
    public IntersectionResponse getIntersection(String intersectionId) {
        return mapper.toIntersection(fetchEntity("Intersection", intersectionId));
    }

    @Override
    public List<IntersectionResponse> getIntersections() {
        return fetchEntitiesByType("Intersection", mapper::toIntersection);
    }

    // ---------------- Camera ----------------

    @Override
    public CameraResponse getCamera(String cameraId) {
        return mapper.toCamera(fetchEntity("Camera", cameraId));
    }

    @Override
    public List<CameraResponse> getCameras() {
        return fetchEntitiesByType("Camera", mapper::toCamera);
    }

    // ---------------- TrafficLight ----------------

    @Override
    public TrafficLightResponse getTrafficLight(String trafficLightId) {
        return mapper.toTrafficLight(fetchEntity("TrafficLight", trafficLightId));
    }

    @Override
    public List<TrafficLightResponse> getTrafficLights() {
        return fetchEntitiesByType("TrafficLight", mapper::toTrafficLight);
    }

    // ---------------- VehicleSensor ----------------

    @Override
    public VehicleSensorResponse getVehicleSensor(String sensorId) {
        return mapper.toVehicleSensor(fetchEntity("VehicleSensor", sensorId));
    }

    @Override
    public List<VehicleSensorResponse> getVehicleSensors() {
        return fetchEntitiesByType("VehicleSensor", mapper::toVehicleSensor);
    }

    // ---------------- Orion access helpers ----------------

    /** Cho phep truyen id ngan (vd "Intersection001") hoac URN day du. */
    private String toUrn(String entityType, String id) {
        return id.startsWith("urn:") ? id : "urn:ngsi-ld:" + entityType + ":" + id;
    }

    private JsonNode fetchEntity(String entityType, String id) {
        String entityId = toUrn(entityType, id);
        try {
            return webClient.get()
                    .uri("/entities/{id}", entityId)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();
        } catch (WebClientResponseException.NotFound e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    entityType + " " + entityId + " not found in Orion");
        }
    }

    private <T> List<T> fetchEntitiesByType(String entityType, Function<JsonNode, T> entityMapper) {
        JsonNode entities = webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/entities")
                        .queryParam("type", entityType)
                        .queryParam("limit", QUERY_LIMIT)
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();

        List<T> result = new ArrayList<>();
        if (entities != null && entities.isArray()) {
            entities.forEach(entity -> result.add(entityMapper.apply(entity)));
        }
        return result;
    }
}
