import { describe, it, expect } from 'vitest'
import {
  mapSensorsToDirections,
  mapTrafficLights,
  normalizeCardinalDirection,
  sumVehicleCount,
  avgSpeed,
  formatAvgSpeed,
  formatSpeedKmh,
  formatOccupancyRate,
  formatPhaseLabel,
  formatScenarioLabel,
  getFreshnessState,
  formatSimSec,
  trafficStatusColor,
} from '@/transforms/realtimeTransforms'
import type { VehicleSensorResponse, RealtimeMetadata } from '@/types/realtime'

describe('realtimeTransforms', () => {
  const sampleSensors: VehicleSensorResponse[] = [
    {
      id: 'sensor-1',
      type: 'VehicleSensor',
      name: 'North Sensor',
      description: null,
      location: null,
      sensorType: 'loop',
      sensorStatus: 'active',
      trafficDirection: 'North',
      refIntersection: 'int-1',
      refCamera: null,
      refTrafficLight: null,
      dateObserved: '2026-08-06T12:00:00Z',
      vehicleCount: 100,
      pcuEquivalent: 120,
      vehicleClassComposition: null,
      leftTurnCount: 20,
      straightCount: 60,
      rightTurnCount: 20,
      averageSpeed: 30,
      waitingVehicleCount: 5,
      queueLength: 15,
      queueStraight: 10,
      queueLeft: 3,
      queueRight: 2,
      occupancyRate: 0.45,
      trafficStatus: 'NORMAL',
      arrivalRatePcuPerSec: 1.2,
      waitingReasonCounts: null,
      dominantWaitingReason: null,
      theoreticalSpeed: 50,
      simulationTime: 100,
      simulationRunId: 'run-1',
      scenarioId: 'normal',
      derivedTrafficState: 'FREE_FLOW',
      spillbackRisk: false,
      operationalState: null,
    },
    {
      id: 'sensor-2',
      type: 'VehicleSensor',
      name: 'South Sensor',
      description: null,
      location: null,
      sensorType: 'loop',
      sensorStatus: 'active',
      trafficDirection: 'South',
      refIntersection: 'int-1',
      refCamera: null,
      refTrafficLight: null,
      dateObserved: '2026-08-06T12:00:00Z',
      vehicleCount: 200,
      pcuEquivalent: 240,
      vehicleClassComposition: null,
      leftTurnCount: 40,
      straightCount: 120,
      rightTurnCount: 40,
      averageSpeed: 20,
      waitingVehicleCount: 15,
      queueLength: 35,
      queueStraight: 25,
      queueLeft: 5,
      queueRight: 5,
      occupancyRate: 0.75,
      trafficStatus: 'HIGH_CONGESTION',
      arrivalRatePcuPerSec: 2.5,
      waitingReasonCounts: null,
      dominantWaitingReason: null,
      theoreticalSpeed: 50,
      simulationTime: 100,
      simulationRunId: 'run-1',
      scenarioId: 'normal',
      derivedTrafficState: 'CONGESTED',
      spillbackRisk: true,
      operationalState: null,
    },
  ]

  it('maps sensors to direction view models correctly', () => {
    const views = mapSensorsToDirections(sampleSensors)
    expect(views).toHaveLength(2)
    expect(views[0].direction).toBe('North')
    expect(views[0].vehicleCount).toBe(100)
    expect(views[1].direction).toBe('South')
    expect(views[1].spillbackRisk).toBe(true)
  })

  it('normalizes NGSI cardinal directions used by realtime entities', () => {
    expect(normalizeCardinalDirection('NORTHBOUND')).toBe('North')
    expect(normalizeCardinalDirection('southbound')).toBe('South')
    expect(normalizeCardinalDirection('East')).toBe('East')
    expect(normalizeCardinalDirection(null)).toBe('Unknown')

    const lights = mapTrafficLights([
      {
        id: 'light-1', type: 'TrafficLight', name: null, description: null,
        location: null, currentStatus: 'GREEN', phaseStartedAt: null,
        timingMode: 'FIXED_TIME', workingState: 'OK', trafficDirection: 'WESTBOUND',
        greenDurationCurrent: 30, redDurationCurrent: 30, yellowDuration: 3,
        refIntersection: 'int-1', refCamera: null, simulationTime: 10,
        simulationRunId: 'run-1', scenarioId: 'normal', currentPhase: 'EW_GREEN',
      },
    ])
    expect(lights[0].direction).toBe('West')
  })

  it('sums total vehicle count correctly', () => {
    expect(sumVehicleCount(sampleSensors)).toBe(300)
  })

  it('calculates average speed across valid sensors', () => {
    expect(avgSpeed(sampleSensors)).toBe(25)
  })

  it('determines freshness state accurately', () => {
    const freshMeta: RealtimeMetadata = {
      simulationRunId: 'run-1',
      simulationTime: 100,
      scenarioId: 'normal',
      consistent: true,
      consistencyIssues: null,
      projectorStatus: 'OK',
      freshnessSeconds: 1.5,
    }
    expect(getFreshnessState(freshMeta, false)).toBe('live')

    const staleMeta: RealtimeMetadata = {
      ...freshMeta,
      freshnessSeconds: 10,
    }
    expect(getFreshnessState(staleMeta, false)).toBe('stale')

    expect(getFreshnessState({ ...freshMeta, freshnessSeconds: null }, false)).toBe('idle')

    expect(getFreshnessState(null, true)).toBe('error')
  })

  it('formats simulation seconds correctly', () => {
    expect(formatSimSec(0)).toBe('0s')
    expect(formatSimSec(45)).toBe('45s')
    expect(formatSimSec(125)).toBe('2m 5s')
    expect(formatSimSec(3665)).toBe('1h 1m 5s')
    expect(formatSimSec(null)).toBe('—')
  })

  it('maps traffic status string to correct color bucket', () => {
    expect(trafficStatusColor('LOW_TRAFFIC')).toBe('green')
    expect(trafficStatusColor('MEDIUM_CONGESTION')).toBe('yellow')
    expect(trafficStatusColor('HIGH_CONGESTION')).toBe('orange')
    expect(trafficStatusColor('SEVERE_INCIDENT')).toBe('red')
    expect(trafficStatusColor(null)).toBe('muted')
  })
})

// ── AVG SPEED REGRESSION TESTS ────────────────────────────────────────────────
// CRITICAL BUG FIX: avgSpeed and formatAvgSpeed must NEVER return
// trafficStatus or derivedTrafficState strings (e.g. "BUSY", "CONGESTED")
describe('AVG SPEED — regression against displaying traffic status as speed', () => {
  const speedlessSensor: VehicleSensorResponse = {
    id: 'sensor-no-speed',
    type: 'VehicleSensor',
    name: 'No Speed Sensor',
    description: null, location: null,
    sensorType: 'loop', sensorStatus: 'active',
    trafficDirection: 'North', refIntersection: 'int-1',
    refCamera: null, refTrafficLight: null,
    dateObserved: '2026-08-06T12:00:00Z',
    vehicleCount: 50, pcuEquivalent: 60,
    vehicleClassComposition: null,
    leftTurnCount: 10, straightCount: 30, rightTurnCount: 10,
    averageSpeed: null,          // intentionally null to test fallback
    waitingVehicleCount: 10, queueLength: 20,
    queueStraight: 15, queueLeft: 3, queueRight: 2,
    occupancyRate: 0.6,
    trafficStatus: 'HIGH_CONGESTION',   // Must NEVER appear as avgSpeed value
    arrivalRatePcuPerSec: 1.5,
    waitingReasonCounts: null, dominantWaitingReason: null,
    theoreticalSpeed: 50, simulationTime: 100,
    simulationRunId: 'run-1', scenarioId: 'normal',
    derivedTrafficState: 'CONGESTED',   // Must NEVER appear as avgSpeed value
    spillbackRisk: false, operationalState: null,
  }

  it('avgSpeed returns null (not traffic status string) when all speeds are null', () => {
    const result = avgSpeed([speedlessSensor])
    expect(result).toBeNull()
    // CRITICAL: result must NOT be a string like "BUSY" or "CONGESTED"
    expect(typeof result).not.toBe('string')
  })

  it('formatAvgSpeed returns "—" (not traffic status string) when all speeds are null', () => {
    const result = formatAvgSpeed([speedlessSensor])
    expect(result).toBe('—')
    expect(result).not.toContain('CONGESTED')
    expect(result).not.toContain('HIGH_CONGESTION')
    expect(result).not.toContain('MEDIUM')
  })

  it('formatAvgSpeed returns km/h formatted number when speed is available', () => {
    const result = formatAvgSpeed([{ ...speedlessSensor, averageSpeed: 35.5 }])
    expect(result).toBe('35.50 km/h')
    expect(result).not.toBe('CONGESTED')
  })

  it('formatSpeedKmh uses two decimal places by default', () => {
    expect(formatSpeedKmh(8.234)).toBe('8.23 km/h')
    expect(formatSpeedKmh(null)).toBe('—')
    expect(formatSpeedKmh(undefined)).toBe('—')
  })

  it('formatAvgSpeed averages sensors with two decimal display', () => {
    const sensors: VehicleSensorResponse[] = [
      { ...speedlessSensor, averageSpeed: 8.17 },
      { ...speedlessSensor, id: 'sensor-2', averageSpeed: 8.23 },
    ]
    expect(formatAvgSpeed(sensors)).toBe('8.20 km/h')
  })

  it('avgSpeed uses only averageSpeed field, not trafficStatus or derivedTrafficState', () => {
    const result = avgSpeed([{ ...speedlessSensor, averageSpeed: 42 }])
    expect(result).toBe(42)
    expect(typeof result).toBe('number')
  })
})

// ── OCCUPANCY FORMATTING TESTS ────────────────────────────────────────────────
// Bug fix: backend sends 0-100 scale (e.g. 47.0 or 22.0).
// Must format as 47.0% (NOT 4700% or 470%).
describe('formatOccupancyRate — correct scale formatting', () => {
  it('formats standard 0-100 scale occupancy percentages accurately', () => {
    expect(formatOccupancyRate(47.0)).toBe('47.0%')
    expect(formatOccupancyRate(22.0)).toBe('22.0%')
    expect(formatOccupancyRate(82.5)).toBe('82.5%')
    expect(formatOccupancyRate(100.0)).toBe('100.0%')
  })

  it('formats ratio scale 0-1 if provided as edge case', () => {
    expect(formatOccupancyRate(0.47)).toBe('47.0%')
    expect(formatOccupancyRate(0.22)).toBe('22.0%')
  })

  it('formats 0 as 0.0%', () => {
    expect(formatOccupancyRate(0)).toBe('0.0%')
  })

  it('returns — for null or undefined', () => {
    expect(formatOccupancyRate(null)).toBe('—')
    expect(formatOccupancyRate(undefined)).toBe('—')
  })

  it('NEVER clamps to 100 or produces 4700% / 3100%', () => {
    const formatted = formatOccupancyRate(47.0)
    expect(formatted).not.toBe('4700%')
    expect(formatted).not.toBe('470%')
    expect(formatted).toBe('47.0%')
  })
})

// ── PHASE & SCENARIO DISPLAY LABELS ───────────────────────────────────────────
describe('formatPhaseLabel and formatScenarioLabel', () => {
  it('formats phase IDs to friendly human readable labels', () => {
    expect(formatPhaseLabel('NS_GREEN')).toBe('North – South Green')
    expect(formatPhaseLabel('NS_YELLOW')).toBe('North – South Yellow')
    expect(formatPhaseLabel('EW_GREEN')).toBe('East – West Green')
    expect(formatPhaseLabel('EW_YELLOW')).toBe('East – West Yellow')
    expect(formatPhaseLabel(null)).toBe('—')
  })

  it('formats scenario IDs to friendly human readable labels', () => {
    expect(formatScenarioLabel('normal')).toBe('Normal')
    expect(formatScenarioLabel('heavy_traffic')).toBe('Heavy Traffic')
    expect(formatScenarioLabel('rain')).toBe('Rain')
    expect(formatScenarioLabel('heavy_rain')).toBe('Heavy Rain')
    expect(formatScenarioLabel('accident')).toBe('Accident')
    expect(formatScenarioLabel('emergency')).toBe('Emergency')
    expect(formatScenarioLabel('blocked_intersection')).toBe('Blocked Intersection')
    expect(formatScenarioLabel('spillback')).toBe('Spillback')
    expect(formatScenarioLabel(null)).toBe('—')
  })
})

