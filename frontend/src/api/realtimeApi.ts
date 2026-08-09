// realtimeApi.ts — Realtime API calls via Spring Server
// Routes verified from FRONTEND_SOURCE_AUDIT.md + IntersectionController, RealtimeController

import type { ApiResponse } from '@/types/common'
import type {
  RealtimeIntersectionResponse,
  IntersectionResponse,
  TrafficLightResponse,
  VehicleSensorResponse,
  SystemHealthResponse,
} from '@/types/realtime'
import { httpClient } from './httpClient'

/**
 * GET /api/system/health — public health check
 */
export async function getSystemHealth(): Promise<SystemHealthResponse> {
  const res = await httpClient.get<SystemHealthResponse>('/api/system/health')
  return res.data
}

/**
 * GET /api/realtime/intersections/{intersectionId}
 * Requires ADMIN session. Returns aggregate realtime snapshot.
 * May return 503 when Projector data is stale/inconsistent.
 */
export async function getRealtimeIntersection(
  intersectionId: string,
): Promise<ApiResponse<RealtimeIntersectionResponse>> {
  const res = await httpClient.get<ApiResponse<RealtimeIntersectionResponse>>(
    `/api/realtime/intersections/${encodeURIComponent(intersectionId)}`,
  )
  return res.data
}

/**
 * GET /api/realtime/intersections — current-run-safe intersection list.
 * Requires ADMIN session. Shadow/probe entities from older runs are excluded
 * by the Spring realtime boundary.
 */
export async function getIntersections(): Promise<ApiResponse<IntersectionResponse[]>> {
  const res = await httpClient.get<ApiResponse<IntersectionResponse[]>>('/api/realtime/intersections')
  return res.data
}

/**
 * GET /api/intersections/{intersectionId} — one intersection from Orion
 * Requires ADMIN session.
 */
export async function getIntersection(
  intersectionId: string,
): Promise<ApiResponse<IntersectionResponse>> {
  const res = await httpClient.get<ApiResponse<IntersectionResponse>>(
    `/api/intersections/${encodeURIComponent(intersectionId)}`,
  )
  return res.data
}

/**
 * GET /api/traffic-lights — list all traffic lights from Orion
 */
export async function getTrafficLights(): Promise<ApiResponse<TrafficLightResponse[]>> {
  const res = await httpClient.get<ApiResponse<TrafficLightResponse[]>>('/api/traffic-lights')
  return res.data
}

/**
 * GET /api/vehicle-sensors — list all vehicle sensors from Orion
 */
export async function getVehicleSensors(): Promise<ApiResponse<VehicleSensorResponse[]>> {
  const res = await httpClient.get<ApiResponse<VehicleSensorResponse[]>>('/api/vehicle-sensors')
  return res.data
}
