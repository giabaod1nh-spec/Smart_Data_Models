// realtimeTransforms.ts — Presentation-only transforms from realtime DTOs to view models
// Rules:
//   - sum vehicle count across sensors: OK (presentation aggregation)
//   - simple average speed across sensors: allowed only for display
//   - NO congestion score / priority calculation
//   - NO re-deriving traffic states not present in DTO
//   - AVG SPEED MUST NEVER show trafficStatus or derivedTrafficState strings

import type { VehicleSensorResponse, TrafficLightResponse, IntersectionResponse, RealtimeMetadata } from '@/types/realtime'
import { getIntersectionDisplayName } from '@/utils/intersectionDisplayName'

export interface DirectionSensorView {
  direction: string
  vehicleCount: number | null
  leftTurnCount: number | null
  straightCount: number | null
  rightTurnCount: number | null
  waitingVehicleCount: number | null
  pcuEquivalent: number | null
  averageSpeed: number | null
  queueLength: number | null
  occupancyRate: number | null
  trafficStatus: string | null
  derivedTrafficState: string | null
  spillbackRisk: boolean | null
  sensorId: string | null
  /** Instantaneous arrival rate on this approach (PCU/s). */
  arrivalRatePcuPerSec: number | null
}

/** Normalize the NGSI direction vocabulary to the four labels used by the UI. */
export function normalizeCardinalDirection(direction: string | null | undefined): string {
  if (!direction) return 'Unknown'
  const value = direction.trim().toUpperCase()
  const aliases: Record<string, string> = {
    NORTH: 'North',
    NORTHBOUND: 'North',
    SOUTH: 'South',
    SOUTHBOUND: 'South',
    EAST: 'East',
    EASTBOUND: 'East',
    WEST: 'West',
    WESTBOUND: 'West',
  }
  return aliases[value] ?? direction
}

/** Map VehicleSensorResponse[] to per-direction view models */
export function mapSensorsToDirections(
  sensors: VehicleSensorResponse[],
): DirectionSensorView[] {
  return sensors.map((s) => ({
    direction: normalizeCardinalDirection(s.trafficDirection),
    vehicleCount: s.vehicleCount,
    leftTurnCount: s.leftTurnCount,
    straightCount: s.straightCount,
    rightTurnCount: s.rightTurnCount,
    waitingVehicleCount: s.waitingVehicleCount,
    pcuEquivalent: s.pcuEquivalent,
    averageSpeed: s.averageSpeed,
    queueLength: s.queueLength,
    occupancyRate: s.occupancyRate,
    trafficStatus: s.trafficStatus,
    derivedTrafficState: s.derivedTrafficState,
    spillbackRisk: s.spillbackRisk,
    sensorId: s.id,
    arrivalRatePcuPerSec: s.arrivalRatePcuPerSec,
  }))
}

/** Sum totalVehicleCount across sensors (presentation aggregate) */
export function sumVehicleCount(sensors: VehicleSensorResponse[]): number {
  return sensors.reduce((acc, s) => acc + (s.vehicleCount ?? 0), 0)
}

/**
 * Format a single speed value for display (km/h).
 * CRITICAL: NEVER use trafficStatus or derivedTrafficState as speed.
 * @returns "XX.XX km/h" or "—"
 */
export function formatSpeedKmh(
  value: number | null | undefined,
  decimals = 2,
): string {
  if (value === null || value === undefined) return '—'
  return `${value.toFixed(decimals)} km/h`
}

/**
 * Sum instantaneous arrival rates (PCU/s) across approach sensors.
 * Source: VehicleSensor.arrivalRatePcuPerSec from SUMO TraCI new-on-approach counting.
 */
export function sumArrivalRatePcuPerSec(sensors: VehicleSensorResponse[]): number | null {
  const rates = sensors
    .map((s) => s.arrivalRatePcuPerSec)
    .filter((r): r is number => r !== null && r !== undefined && Number.isFinite(r))
  if (rates.length === 0) return null
  return rates.reduce((acc, r) => acc + r, 0)
}

/** Convert approach arrival rate (PCU/s) to hourly flow (PCU/h). */
export function arrivalFlowPcuPerHour(sensors: VehicleSensorResponse[]): number | null {
  const rate = sumArrivalRatePcuPerSec(sensors)
  if (rate === null) return null
  return rate * 3600
}

/**
 * Format intersection arrival flow for KPI display.
 * @returns "1,240 PCU/h" or "—"
 */
export function formatArrivalFlow(sensors: VehicleSensorResponse[]): string {
  const flow = arrivalFlowPcuPerHour(sensors)
  if (flow === null) return '—'
  return `${Math.round(flow).toLocaleString('en-US')} PCU/h`
}

/** Format a single approach arrival rate as PCU/h (or "—"). */
export function formatDirectionArrivalFlow(arrivalRatePcuPerSec: number | null | undefined): string {
  if (arrivalRatePcuPerSec === null || arrivalRatePcuPerSec === undefined) return '—'
  if (!Number.isFinite(arrivalRatePcuPerSec)) return '—'
  return `${Math.round(arrivalRatePcuPerSec * 3600).toLocaleString('en-US')} PCU/h`
}

/**
 * Simple average speed across sensors — for display only.
 * INVARIANT: MUST return number | null ONLY — never a traffic status string.
 * Sources: VehicleSensor.averageSpeed only.
 * If no valid sensors: returns null (display as "—").
 */
export function avgSpeed(sensors: VehicleSensorResponse[]): number | null {
  const valid = sensors.filter((s) => s.averageSpeed !== null && s.averageSpeed !== undefined)
  if (valid.length === 0) return null
  const sum = valid.reduce((acc, s) => acc + (s.averageSpeed ?? 0), 0)
  return sum / valid.length
}

/**
 * Format average speed for display.
 * CRITICAL: NEVER use trafficStatus or derivedTrafficState as speed.
 * @returns "XX.XX km/h" or "—"
 */
export function formatAvgSpeed(sensors: VehicleSensorResponse[]): string {
  return formatSpeedKmh(avgSpeed(sensors))
}

export interface TrafficLightView {
  id: string | null
  direction: string | null
  currentStatus: string | null
  currentPhase: string | null
  timingMode: string | null
  workingState: string | null
  greenDurationCurrent: number | null
  redDurationCurrent: number | null
  yellowDuration: number | null
  phaseStartedAt: string | null
  simulationTime: number | null
  simulationRunId: string | null
}

/** Map TrafficLightResponse to display view */
export function mapTrafficLights(lights: TrafficLightResponse[]): TrafficLightView[] {
  return lights.map((l) => ({
    id: l.id,
    direction: normalizeCardinalDirection(l.trafficDirection),
    currentStatus: l.currentStatus,
    currentPhase: l.currentPhase,
    timingMode: l.timingMode,
    workingState: l.workingState,
    greenDurationCurrent: l.greenDurationCurrent,
    redDurationCurrent: l.redDurationCurrent,
    yellowDuration: l.yellowDuration,
    phaseStartedAt: l.phaseStartedAt,
    simulationTime: l.simulationTime,
    simulationRunId: l.simulationRunId,
  }))
}

/**
 * Determine freshness badge state from RealtimeMetadata.
 */
export type FreshnessState = 'live' | 'stale' | 'error' | 'idle'

export function getFreshnessState(
  metadata: RealtimeMetadata | null | undefined,
  isError: boolean,
): FreshnessState {
  if (isError) return 'error'
  if (!metadata) return 'idle'
  if (metadata.consistent === false) return 'stale'
  if (metadata.freshnessSeconds === null || metadata.freshnessSeconds === undefined) return 'idle'
  if (metadata.freshnessSeconds > 5) return 'stale'
  return 'live'
}

/**
 * Derive a single primary realtime status for the page header.
 * Sources:
 *   - freshnessState from metadata
 *   - simulationTimeSeries to detect DELAYED (simulationTime not advancing)
 *
 * DELAYED (not PAUSED): telemetry still arrives but sim ticks are sparse.
 * Decorative canvas keeps animating; only metrics/countdown treat this as frozen.
 */
export type RealtimePageStatus =
  | 'LIVE'
  | 'DELAYED'
  | 'PAUSED' // legacy alias kept for older callers; prefer DELAYED
  | 'STALE'
  | 'WAITING'
  | 'OFFLINE'
  | 'ERROR'
  | 'UNKNOWN'

export function deriveRealtimePageStatus(
  freshnessState: FreshnessState,
  simulationTimePaused: boolean,
): RealtimePageStatus {
  if (freshnessState === 'error') return 'OFFLINE'
  if (freshnessState === 'stale') return 'STALE'
  if (freshnessState === 'idle') return 'WAITING'
  // freshnessState === 'live'
  if (simulationTimePaused) return 'DELAYED'
  return 'LIVE'
}

/** Wall-clock age of the last successful realtime poll, for Summary "Last Seen". */
export function formatLastSeen(
  dataUpdatedAtMs: number | null | undefined,
  freshnessSeconds?: number | null,
): string {
  if (dataUpdatedAtMs != null && dataUpdatedAtMs > 0) {
    const ageSec = Math.max(0, (Date.now() - dataUpdatedAtMs) / 1000)
    if (ageSec < 60) return `${ageSec.toFixed(1)}s ago`
    if (ageSec < 3600) return `${Math.floor(ageSec / 60)}m ${Math.floor(ageSec % 60)}s ago`
    return `${Math.floor(ageSec / 3600)}h ago`
  }
  if (freshnessSeconds != null && Number.isFinite(freshnessSeconds)) {
    return `${freshnessSeconds.toFixed(1)}s ago`
  }
  return '—'
}

/**
 * Compute countdown remaining seconds from a TrafficLightView.
 *
 * Contract semantics (verified from trafficlight_model.yaml):
 *   - phaseStartedAt: wall-clock ISO datetime when currentStatus last changed
 *   - greenDurationCurrent / redDurationCurrent / yellowDuration: configured durations (seconds)
 *   - Formula: remaining = currentDuration - elapsedWallClockSec
 *
 * IMPORTANT:
 *   - Uses wall-clock domain only (Date.now() / phaseStartedAt)
 *   - Does NOT mix with simulationTime domain
 *   - Caller must freeze countdown when simulation is PAUSED or data is STALE/OFFLINE
 *
 * Returns null if:
 *   - phaseStartedAt is null/unparseable
 *   - currentStatus is null/unknown
 *   - currentDuration is null/unknown
 */
export function computeCountdownSec(
  light: TrafficLightView | null | undefined,
  nowMs: number = Date.now(),
): number | null {
  if (!light) return null
  if (!light.phaseStartedAt) return null
  if (!light.currentStatus) return null

  const phaseStartMs = Date.parse(light.phaseStartedAt)
  if (isNaN(phaseStartMs)) return null

  const status = light.currentStatus.toUpperCase()
  let currentDuration: number | null = null

  if (status.includes('GREEN')) {
    currentDuration = light.greenDurationCurrent
  } else if (status.includes('RED')) {
    currentDuration = light.redDurationCurrent
  } else if (status.includes('YELLOW')) {
    currentDuration = light.yellowDuration
  }

  if (currentDuration === null || currentDuration <= 0) return null

  const elapsedSec = (nowMs - phaseStartMs) / 1000
  const remaining = currentDuration - elapsedSec

  return Math.max(0, remaining)
}

/** Format simulation time (seconds) to human-readable */
export function formatSimSec(sec: number | null | undefined): string {
  if (sec === null || sec === undefined) return '—'
  const h = Math.floor(sec / 3600)
  const m = Math.floor((sec % 3600) / 60)
  const s = Math.floor(sec % 60)
  if (h > 0) return `${h}h ${m}m ${s}s`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

/** Get traffic status color key */
export function trafficStatusColor(status: string | null | undefined): 'green' | 'yellow' | 'orange' | 'red' | 'muted' {
  if (!status) return 'muted'
  const s = status.toUpperCase()
  if (s.includes('LOW') || s.includes('FREE') || s.includes('NORMAL')) return 'green'
  if (s.includes('MEDIUM') || s.includes('SLOW')) return 'yellow'
  if (s.includes('HIGH') || s.includes('CONGESTED')) return 'orange'
  if (s.includes('SEVERE') || s.includes('JAM') || s.includes('BLOCKED') || s.includes('INCIDENT')) return 'red'
  return 'muted'
}

/**
 * Format occupancy rate correctly without multiplying already-scaled percentages.
 *
 * Backend scale semantics (from VehicleSensor.example.jsonld & entity_mapper.py):
 *   - Emits percentage scale 0–100 (e.g., 22.0 = 22.0%, 47.0 = 47.0%).
 *   - If an edge-case 0–1 ratio is provided (0 < val <= 1.0), maps to (val * 100).toFixed(1)%.
 *   - Null/undefined returns '—'.
 *   - Zero returns '0.0%'.
 *   - NEVER clamps to 100 to hide errors.
 */
export function formatOccupancyRate(rate: number | null | undefined): string {
  if (rate === null || rate === undefined) return '—'
  if (rate === 0) return '0.0%'
  if (rate > 0 && rate <= 1.0) {
    return `${(rate * 100).toFixed(1)}%`
  }
  return `${rate.toFixed(1)}%`
}

/** Phase display labels — human friendly format for UI headers and cards */
export const PHASE_DISPLAY_NAMES: Record<string, string> = {
  NS_GREEN: 'North – South Green',
  NS_YELLOW: 'North – South Yellow',
  EW_GREEN: 'East – West Green',
  EW_YELLOW: 'East – West Yellow',
}

export function formatPhaseLabel(phase: string | null | undefined): string {
  if (!phase) return '—'
  const key = phase.toUpperCase()
  return PHASE_DISPLAY_NAMES[key] ?? phase
}

/** Map scenario IDs to human-readable names */
export const SCENARIO_DISPLAY_NAMES: Record<string, string> = {
  normal: 'Normal',
  morning_peak: 'Morning Peak',
  evening_peak: 'Evening Peak',
  heavy_traffic: 'Heavy Traffic',
  oversaturated: 'Oversaturated',
  rain: 'Rain',
  heavy_rain: 'Heavy Rain',
  accident: 'Accident',
  emergency: 'Emergency',
  blocked_intersection: 'Blocked Intersection',
  spillback: 'Spillback',
}

export function formatScenarioLabel(scenario: string | null | undefined): string {
  if (!scenario) return '—'
  return SCENARIO_DISPLAY_NAMES[scenario] ?? scenario
}

/** Format RF anomaly score (0–1) for KPI display. */
export function formatAnomalyScore(score: number | null | undefined): string {
  if (score === null || score === undefined || !Number.isFinite(score)) return '—'
  return score.toFixed(2)
}

/** Status-dot color for anomaly score severity. */
export function anomalyScoreDot(
  score: number | null | undefined,
): 'green' | 'yellow' | 'orange' | 'red' | 'muted' {
  if (score === null || score === undefined || !Number.isFinite(score)) return 'muted'
  if (score < 0.25) return 'green'
  if (score < 0.5) return 'yellow'
  if (score < 0.75) return 'orange'
  return 'red'
}

/** Map IntersectionResponse to schematic node for map display */
export interface IntersectionMapNode {
  id: string
  name: string
  displayName: string
  overallTrafficStatus: string | null
  statusColor: 'green' | 'yellow' | 'orange' | 'red' | 'muted'
  hasActiveIncident: boolean
  totalVehicleCount: number | null
  scenarioId: string | null
  derivedTrafficState: string | null
}

export function mapIntersectionToNode(i: IntersectionResponse): IntersectionMapNode {
  return {
    id: i.id ?? '',
    name: i.name ?? i.id ?? 'Unknown',
    displayName: getIntersectionDisplayName(i.id, i.name),
    overallTrafficStatus: i.overallTrafficStatus,
    statusColor: trafficStatusColor(i.overallTrafficStatus),
    hasActiveIncident: i.hasActiveIncident ?? false,
    totalVehicleCount: i.totalVehicleCount,
    scenarioId: i.scenarioId,
    derivedTrafficState: i.derivedTrafficState,
  }
}
