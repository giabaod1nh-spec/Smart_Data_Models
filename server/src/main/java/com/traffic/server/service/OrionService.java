package com.traffic.server.service;

import com.traffic.server.payload.CameraResponse;
import com.traffic.server.payload.IntersectionResponse;
import com.traffic.server.payload.TrafficLightResponse;
import com.traffic.server.payload.VehicleSensorResponse;

import java.util.List;

/** Doc cac entity NGSI-LD tu Orion Context Broker va tra ve DTO phang. */
public interface OrionService {

    /** Tra ve payload NGSI-LD tho tu Orion theo URN day du, phuc vu debug. */
    String getRawEntity(String entityId);

    IntersectionResponse getIntersection(String intersectionId);

    List<IntersectionResponse> getIntersections();

    CameraResponse getCamera(String cameraId);

    List<CameraResponse> getCameras();

    TrafficLightResponse getTrafficLight(String trafficLightId);

    List<TrafficLightResponse> getTrafficLights();

    VehicleSensorResponse getVehicleSensor(String sensorId);

    List<VehicleSensorResponse> getVehicleSensors();
}
