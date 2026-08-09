// realtime.ts — Types matching verified Spring realtime DTOs exactly

import type { GeoPoint } from './common'

/** RealtimeIntersectionResponse — top-level aggregate response */
export interface RealtimeIntersectionResponse {
  intersection: IntersectionResponse | null
  trafficLights: TrafficLightResponse[]
  vehicleSensors: VehicleSensorResponse[]
  cameras: CameraResponse[]
  metadata: RealtimeMetadata | null
}

/** IntersectionResponse — matches IntersectionResponse.java */
export interface IntersectionResponse {
  id: string | null
  type: string | null
  name: string | null
  description: string | null
  location: GeoPoint | null
  numberOfApproaches: number | null
  intersectionStatus: string | null
  frequentCongestion: boolean | null
  refTrafficLights: string[] | null
  refCameras: string[] | null
  refVehicleSensors: string[] | null
  dateObserved: string | null
  overallTrafficStatus: string | null
  totalVehicleCount: number | null
  hasActiveIncident: boolean | null
  simulationTime: number | null
  simulationRunId: string | null
  scenarioId: string | null
  currentPhase: string | null
  derivedTrafficState: string | null
  hasSpillback: boolean | null
  isBoxBlocked: boolean | null
  probableCauseType: string | null
  affectedBy: string | null
  causeDetectedAt: number | null
}

/** VehicleSensorResponse — matches VehicleSensorResponse.java */
export interface VehicleSensorResponse {
  id: string | null
  type: string | null
  name: string | null
  description: string | null
  location: GeoPoint | null
  sensorType: string | null
  sensorStatus: string | null
  trafficDirection: string | null
  refIntersection: string | null
  refCamera: string | null
  refTrafficLight: string | null
  dateObserved: string | null
  vehicleCount: number | null
  pcuEquivalent: number | null
  vehicleClassComposition: Record<string, number> | null
  leftTurnCount: number | null
  straightCount: number | null
  rightTurnCount: number | null
  averageSpeed: number | null
  waitingVehicleCount: number | null
  queueLength: number | null
  queueStraight: number | null
  queueLeft: number | null
  queueRight: number | null
  occupancyRate: number | null
  trafficStatus: string | null
  arrivalRatePcuPerSec: number | null
  waitingReasonCounts: Record<string, number> | null
  dominantWaitingReason: string | null
  theoreticalSpeed: number | null
  simulationTime: number | null
  simulationRunId: string | null
  scenarioId: string | null
  derivedTrafficState: string | null
  spillbackRisk: boolean | null
  operationalState: Record<string, unknown> | null
}

/** TrafficLightResponse — matches TrafficLightResponse.java */
export interface TrafficLightResponse {
  id: string | null
  type: string | null
  name: string | null
  description: string | null
  location: GeoPoint | null
  currentStatus: string | null
  phaseStartedAt: string | null
  timingMode: string | null
  workingState: string | null
  trafficDirection: string | null
  greenDurationCurrent: number | null
  redDurationCurrent: number | null
  yellowDuration: number | null
  refIntersection: string | null
  refCamera: string | null
  simulationTime: number | null
  simulationRunId: string | null
  scenarioId: string | null
  currentPhase: string | null
}

/** CameraResponse — matches CameraResponse.java */
export interface CameraResponse {
  id: string | null
  type: string | null
  name: string | null
  description: string | null
  location: GeoPoint | null
  cameraNum: string | null
  cameraType: string | null
  cameraUsage: string | null
  orientationAngle: number | null
  streamURL: string | null
  trafficDirection: string | null
  monitoredLane: string | null
  vehicleCount: number | null
  averageSpeed: number | null
  occupancyRate: number | null
  trafficStatus: string | null
  confidence: number | null
  simulationTime: number | null
  simulationRunId: string | null
  scenarioId: string | null
  dateObserved: string | null
}

/** RealtimeMetadata — matches RealtimeMetadata.java */
export interface RealtimeMetadata {
  simulationRunId: string | null
  simulationTime: number | null
  scenarioId: string | null
  consistent: boolean | null
  consistencyIssues: string[] | null
  projectorStatus: string | null
  freshnessSeconds: number | null
}

/** SystemHealthResponse */
export interface SystemHealthResponse {
  status: string
  server?: string
  [key: string]: unknown
}
