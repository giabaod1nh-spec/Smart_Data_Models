package com.traffic.server.service;

import com.traffic.server.payload.CameraResponse;
import com.traffic.server.payload.GeoPoint;
import com.traffic.server.payload.IntersectionResponse;
import com.traffic.server.payload.TrafficLightResponse;
import com.traffic.server.payload.VehicleSensorResponse;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Chuyen entity NGSI-LD (normalized: {"attr": {"type":"Property","value":...}})
 * tu Orion sang cac DTO phang cua backend.
 */
@Component
public class NgsiEntityMapper {

    public IntersectionResponse toIntersection(JsonNode entity) {
        return IntersectionResponse.builder()
                .id(textOrNull(entity.path("id")))
                .type(textOrNull(entity.path("type")))
                .name(textOrNull(propertyValue(entity, "name")))
                .description(textOrNull(propertyValue(entity, "description")))
                .location(geoPoint(propertyValue(entity, "location")))
                .numberOfApproaches(intOrNull(propertyValue(entity, "numberOfApproaches")))
                .intersectionStatus(textOrNull(propertyValue(entity, "intersectionStatus")))
                .frequentCongestion(boolOrNull(propertyValue(entity, "frequentCongestion")))
                .refTrafficLights(relationshipObjects(entity, "refTrafficLights"))
                .refCameras(relationshipObjects(entity, "refCameras"))
                .refVehicleSensors(relationshipObjects(entity, "refVehicleSensors"))
                .dateObserved(dateTimeOrNull(propertyValue(entity, "dateObserved")))
                .overallTrafficStatus(textOrNull(propertyValue(entity, "overallTrafficStatus")))
                .totalVehicleCount(intOrNull(propertyValue(entity, "totalVehicleCount")))
                .hasActiveIncident(boolOrNull(propertyValue(entity, "hasActiveIncident")))
                .simulationTime(doubleOrNull(propertyValue(entity, "simulationTime")))
                .simulationRunId(textOrNull(propertyValue(entity, "simulationRunId")))
                .scenarioId(textOrNull(propertyValue(entity, "scenarioId")))
                .currentPhase(textOrNull(propertyValue(entity, "currentPhase")))
                .derivedTrafficState(textOrNull(propertyValue(entity, "derivedTrafficState")))
                .hasSpillback(boolOrNull(propertyValue(entity, "hasSpillback")))
                .isBoxBlocked(boolOrNull(propertyValue(entity, "isBoxBlocked")))
                .probableCauseType(textOrNull(propertyValue(entity, "probableCauseType")))
                .affectedBy(relationshipObject(entity, "affectedBy"))
                .causeDetectedAt(doubleOrNull(propertyValue(entity, "causeDetectedAt")))
                .build();
    }

    public CameraResponse toCamera(JsonNode entity) {
        return CameraResponse.builder()
                .id(textOrNull(entity.path("id")))
                .type(textOrNull(entity.path("type")))
                .name(textOrNull(propertyValue(entity, "name")))
                .description(textOrNull(propertyValue(entity, "description")))
                .location(geoPoint(propertyValue(entity, "location")))
                .cameraNum(intOrNull(propertyValue(entity, "cameraNum")))
                .cameraType(textOrNull(propertyValue(entity, "cameraType")))
                .cameraUsage(textOrNull(propertyValue(entity, "cameraUsage")))
                .orientationAngle(doubleOrNull(propertyValue(entity, "orientationAngle")))
                .streamURL(textOrNull(propertyValue(entity, "streamURL")))
                .cameraStatus(textOrNull(propertyValue(entity, "cameraStatus")))
                .refIntersection(relationshipObject(entity, "refIntersection"))
                .refTrafficLight(relationshipObject(entity, "refTrafficLight"))
                .refVehicleSensor(relationshipObjects(entity, "refVehicleSensor"))
                .dateObserved(dateTimeOrNull(propertyValue(entity, "dateObserved")))
                .trafficDirection(textOrNull(propertyValue(entity, "trafficDirection")))
                .monitoredLane(stringList(propertyValue(entity, "monitoredLane")))
                .vehicleCount(intOrNull(propertyValue(entity, "vehicleCount")))
                .averageSpeed(doubleOrNull(propertyValue(entity, "averageSpeed")))
                .occupancyRate(doubleOrNull(propertyValue(entity, "occupancyRate")))
                .trafficStatus(textOrNull(propertyValue(entity, "trafficStatus")))
                .confidence(doubleOrNull(propertyValue(entity, "confidence")))
                .incidentDetected(boolOrNull(propertyValue(entity, "incidentDetected")))
                .simulationTime(doubleOrNull(propertyValue(entity, "simulationTime")))
                .simulationRunId(textOrNull(propertyValue(entity, "simulationRunId")))
                .scenarioId(textOrNull(propertyValue(entity, "scenarioId")))
                .recommendedSignalAction(textOrNull(propertyValue(entity, "recommendedSignalAction")))
                .incidentType(textOrNull(propertyValue(entity, "incidentType")))
                .incidentSeverity(textOrNull(propertyValue(entity, "incidentSeverity")))
                .build();
    }

    public TrafficLightResponse toTrafficLight(JsonNode entity) {
        return TrafficLightResponse.builder()
                .id(textOrNull(entity.path("id")))
                .type(textOrNull(entity.path("type")))
                .name(textOrNull(propertyValue(entity, "name")))
                .description(textOrNull(propertyValue(entity, "description")))
                .location(geoPoint(propertyValue(entity, "location")))
                .currentStatus(textOrNull(propertyValue(entity, "currentStatus")))
                .phaseStartedAt(dateTimeOrNull(propertyValue(entity, "phaseStartedAt")))
                .timingMode(textOrNull(propertyValue(entity, "timingMode")))
                .workingState(textOrNull(propertyValue(entity, "workingState")))
                .trafficDirection(textOrNull(propertyValue(entity, "trafficDirection")))
                .greenDurationCurrent(intOrNull(propertyValue(entity, "greenDurationCurrent")))
                .redDurationCurrent(intOrNull(propertyValue(entity, "redDurationCurrent")))
                .yellowDuration(intOrNull(propertyValue(entity, "yellowDuration")))
                .refIntersection(relationshipObject(entity, "refIntersection"))
                .refCamera(relationshipObject(entity, "refCamera"))
                .simulationTime(doubleOrNull(propertyValue(entity, "simulationTime")))
                .simulationRunId(textOrNull(propertyValue(entity, "simulationRunId")))
                .scenarioId(textOrNull(propertyValue(entity, "scenarioId")))
                .currentPhase(textOrNull(propertyValue(entity, "currentPhase")))
                .build();
    }

    public VehicleSensorResponse toVehicleSensor(JsonNode entity) {
        return VehicleSensorResponse.builder()
                .id(textOrNull(entity.path("id")))
                .type(textOrNull(entity.path("type")))
                .name(textOrNull(propertyValue(entity, "name")))
                .description(textOrNull(propertyValue(entity, "description")))
                .location(geoPoint(propertyValue(entity, "location")))
                .sensorType(textOrNull(propertyValue(entity, "sensorType")))
                .sensorStatus(textOrNull(propertyValue(entity, "sensorStatus")))
                .trafficDirection(textOrNull(propertyValue(entity, "trafficDirection")))
                .refIntersection(relationshipObject(entity, "refIntersection"))
                .refCamera(relationshipObject(entity, "refCamera"))
                .refTrafficLight(relationshipObject(entity, "refTrafficLight"))
                .dateObserved(dateTimeOrNull(propertyValue(entity, "dateObserved")))
                .vehicleCount(intOrNull(propertyValue(entity, "vehicleCount")))
                .pcuEquivalent(doubleOrNull(propertyValue(entity, "pcuEquivalent")))
                .vehicleClassComposition(doubleMap(propertyValue(entity, "vehicleClassComposition")))
                .leftTurnCount(intOrNull(propertyValue(entity, "leftTurnCount")))
                .straightCount(intOrNull(propertyValue(entity, "straightCount")))
                .rightTurnCount(intOrNull(propertyValue(entity, "rightTurnCount")))
                .averageSpeed(doubleOrNull(propertyValue(entity, "averageSpeed")))
                .waitingVehicleCount(intOrNull(propertyValue(entity, "waitingVehicleCount")))
                .queueLength(doubleOrNull(propertyValue(entity, "queueLength")))
                .queueStraight(doubleOrNull(propertyValue(entity, "queueStraight")))
                .queueLeft(doubleOrNull(propertyValue(entity, "queueLeft")))
                .queueRight(doubleOrNull(propertyValue(entity, "queueRight")))
                .occupancyRate(doubleOrNull(propertyValue(entity, "occupancyRate")))
                .trafficStatus(textOrNull(propertyValue(entity, "trafficStatus")))
                .arrivalRatePcuPerSec(doubleOrNull(propertyValue(entity, "arrivalRatePcuPerSec")))
                .waitingReasonCounts(intMap(propertyValue(entity, "waitingReasonCounts")))
                .dominantWaitingReason(textOrNull(propertyValue(entity, "dominantWaitingReason")))
                .theoreticalSpeed(doubleOrNull(propertyValue(entity, "theoreticalSpeed")))
                .simulationTime(doubleOrNull(propertyValue(entity, "simulationTime")))
                .simulationRunId(textOrNull(propertyValue(entity, "simulationRunId")))
                .scenarioId(textOrNull(propertyValue(entity, "scenarioId")))
                .derivedTrafficState(textOrNull(propertyValue(entity, "derivedTrafficState")))
                .spillbackRisk(boolOrNull(propertyValue(entity, "spillbackRisk")))
                .operationalState(objectMap(propertyValue(entity, "operationalState")))
                .build();
    }

    private JsonNode propertyValue(JsonNode entity, String attribute) {
        return entity.path(attribute).path("value");
    }

    private String relationshipObject(JsonNode entity, String attribute) {
        return textOrNull(entity.path(attribute).path("object"));
    }

    private List<String> relationshipObjects(JsonNode entity, String attribute) {
        return stringList(entity.path(attribute).path("object"));
    }

    private GeoPoint geoPoint(JsonNode value) {
        if (!value.isObject()) {
            return null;
        }
        List<Double> coordinates = new ArrayList<>();
        value.path("coordinates").forEach(c -> coordinates.add(c.asDouble()));
        return new GeoPoint(textOrNull(value.path("type")), coordinates);
    }

    private List<String> stringList(JsonNode node) {
        if (node.isValueNode()) {
            return List.of(node.asText());
        }
        if (!node.isArray()) {
            return null;
        }
        List<String> values = new ArrayList<>();
        node.forEach(item -> values.add(item.asText()));
        return values;
    }

    private Map<String, Double> doubleMap(JsonNode node) {
        if (!node.isObject()) {
            return null;
        }
        Map<String, Double> map = new LinkedHashMap<>();
        node.properties().forEach(e -> map.put(e.getKey(), e.getValue().asDouble()));
        return map;
    }

    private Map<String, Integer> intMap(JsonNode node) {
        if (!node.isObject()) {
            return null;
        }
        Map<String, Integer> map = new LinkedHashMap<>();
        node.properties().forEach(e -> map.put(e.getKey(), e.getValue().asInt()));
        return map;
    }

    private Map<String, Object> objectMap(JsonNode node) {
        if (!node.isObject()) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        node.properties().forEach(e -> {
            JsonNode v = e.getValue();
            if (v.isBoolean()) {
                map.put(e.getKey(), v.asBoolean());
            } else if (v.isNumber()) {
                map.put(e.getKey(), v.asDouble());
            } else if (v.isValueNode()) {
                map.put(e.getKey(), v.asText());
            }
        });
        return map;
    }

    private String dateTimeOrNull(JsonNode value) {
        if (value.isObject()) {
            return textOrNull(value.path("@value"));
        }
        return textOrNull(value);
    }

    private String textOrNull(JsonNode node) {
        return node.isValueNode() ? node.asText() : null;
    }

    private Integer intOrNull(JsonNode node) {
        return node.isNumber() ? node.asInt() : null;
    }

    private Double doubleOrNull(JsonNode node) {
        return node.isNumber() ? node.asDouble() : null;
    }

    private Boolean boolOrNull(JsonNode node) {
        return node.isBoolean() ? node.asBoolean() : null;
    }
}
