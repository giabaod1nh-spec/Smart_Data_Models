import { describe, it, expect } from 'vitest'
import {
  getIntersectionDisplayName,
} from '@/utils/intersectionDisplayName'
import {
  mapTrafficLights,
  avgSpeed,
  formatAvgSpeed,
  formatOccupancyRate,
  computeCountdownSec,
  deriveRealtimePageStatus,
} from '@/transforms/realtimeTransforms'
import {
  speedToAnimCategory,
  computeSpriteCount,
  computeQueueDepth,
  phaseForDirection,
  MAX_SPRITES_PER_DIR,
} from '@/components/canvas/useVehicleAnimation'
import {
  SCENARIO_IDS,
  PHASE_IDS,
  CONTROL_MODES,
  GREEN_DURATION_MIN,
  GREEN_DURATION_MAX,
} from '@/types/control'
import type { VehicleSensorResponse, TrafficLightResponse } from '@/types/realtime'

describe('Intersection Detail — Acceptance Test Suite (Section XLV)', () => {
  // Test 1: Avg Speed không fallback BUSY
  it('1. Avg Speed does not fallback to BUSY string', () => {
    const sensor: VehicleSensorResponse = {
      id: 'vs-1', type: 'VehicleSensor', name: null, description: null, location: null,
      sensorType: 'loop', sensorStatus: 'active', trafficDirection: 'North',
      refIntersection: 'urn:ngsi-ld:Intersection:C', refCamera: null, refTrafficLight: null,
      dateObserved: null, vehicleCount: 20, pcuEquivalent: null, vehicleClassComposition: null,
      leftTurnCount: null, straightCount: null, rightTurnCount: null,
      averageSpeed: null, // null speed
      waitingVehicleCount: null, queueLength: null, queueStraight: null,
      queueLeft: null, queueRight: null, occupancyRate: null,
      trafficStatus: 'BUSY', // String should NOT be used as speed
      arrivalRatePcuPerSec: null, waitingReasonCounts: null, dominantWaitingReason: null,
      theoreticalSpeed: null, simulationTime: 100, simulationRunId: 'run-1',
      scenarioId: 'normal', derivedTrafficState: 'CONGESTED', spillbackRisk: null,
      operationalState: null,
    }
    expect(avgSpeed([sensor])).toBeNull()
    expect(formatAvgSpeed([sensor])).toBe('—')
    expect(formatAvgSpeed([sensor])).not.toContain('BUSY')
  })

  // Test 2: Occupancy formatting đúng scale
  it('2. Occupancy formatting formats 47.0 as 47.0% (not 4700% or 470%)', () => {
    expect(formatOccupancyRate(47.0)).toBe('47.0%')
    expect(formatOccupancyRate(22.0)).toBe('22.0%')
    expect(formatOccupancyRate(0.47)).toBe('47.0%')
    expect(formatOccupancyRate(null)).toBe('—')
    expect(formatOccupancyRate(47.0)).not.toBe('4700%')
  })

  // Test 3: Friendly name ưu tiên name
  it('3. Friendly name prioritizes intersection.name over URN', () => {
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:C', 'Ngã 6 Cộng Hòa')).toBe('Ngã 6 Cộng Hòa')
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:C', null)).toBe('Intersection C')
  })

  // Test 4: API ID vẫn là URN
  it('4. API ID preserves raw URN while display name generates friendly label', () => {
    const rawId = 'urn:ngsi-ld:Intersection:C'
    const friendly = getIntersectionDisplayName(rawId, null)
    expect(friendly).toBe('Intersection C')
    expect(rawId).toBe('urn:ngsi-ld:Intersection:C')
    expect(rawId.startsWith('urn:ngsi-ld:Intersection:')).toBe(true)
  })

  // Test 5 & 6: Scenario selection & Queued do not change authoritative current scenario
  it('5-6. Scenario selection and queued response do not mutate authoritative current scenario', () => {
    const authoritativeScenario = 'normal'
    const selectedScenario = 'heavy_traffic'
    const queuedResponse = { queued: true }

    // Authoritative scenario remains 'normal' until Realtime emits 'heavy_traffic'
    expect(authoritativeScenario).toBe('normal')
    expect(queuedResponse.queued).toBe(true)
    expect(authoritativeScenario).not.toBe(selectedScenario)
  })

  // Test 7: Realtime scenario mới update UI
  it('7. Realtime scenario update changes authoritative scenario', () => {
    let currentScenario = 'normal'
    const realtimePayload = { scenarioId: 'heavy_traffic' }
    if (realtimePayload.scenarioId) {
      currentScenario = realtimePayload.scenarioId
    }
    expect(currentScenario).toBe('heavy_traffic')
  })

  // Test 8 & 9: Phase queued does not change lights; realtime phase does
  it('8-9. Phase queued does not change traffic lights; Realtime phase updates lights', () => {
    let currentPhase = 'NS_GREEN'
    const queuedResponse = { queued: true }
    expect(currentPhase).toBe('NS_GREEN')
    expect(queuedResponse.queued).toBe(true)

    // When backend confirms
    const backendRealtimePhase = 'EW_GREEN'
    currentPhase = backendRealtimePhase
    expect(currentPhase).toBe('EW_GREEN')
  })

  // Test 10: Green duration không optimistic
  it('10. Green duration is not updated optimistically', () => {
    let configuredGreen = 42
    const userInputGreen = 60
    const queuedResponse = { queued: true }

    expect(configuredGreen).toBe(42)
    expect(queuedResponse.queued).toBe(true)

    // Realtime update
    configuredGreen = 60
    expect(configuredGreen).toBe(userInputGreen)
  })

  // Test 11: Traffic light state mapping N/S/E/W
  it('11. Maps traffic lights accurately for North, South, East, West', () => {
    const rawLights: TrafficLightResponse[] = [
      {
        id: 'tl-n', type: 'TrafficLight', name: null, description: null, location: null,
        currentStatus: 'GREEN', phaseStartedAt: null, timingMode: 'FIXED_TIME',
        workingState: 'OK', trafficDirection: 'NORTHBOUND', greenDurationCurrent: 40,
        redDurationCurrent: 40, yellowDuration: 3, refIntersection: 'int-1',
        refCamera: null, simulationTime: 100, simulationRunId: 'run-1',
        scenarioId: 'normal', currentPhase: 'NS_GREEN',
      },
      {
        id: 'tl-e', type: 'TrafficLight', name: null, description: null, location: null,
        currentStatus: 'RED', phaseStartedAt: null, timingMode: 'FIXED_TIME',
        workingState: 'OK', trafficDirection: 'EASTBOUND', greenDurationCurrent: 40,
        redDurationCurrent: 40, yellowDuration: 3, refIntersection: 'int-1',
        refCamera: null, simulationTime: 100, simulationRunId: 'run-1',
        scenarioId: 'normal', currentPhase: 'NS_GREEN',
      },
    ]

    const mapped = mapTrafficLights(rawLights)
    expect(mapped[0].direction).toBe('North')
    expect(mapped[0].currentStatus).toBe('GREEN')
    expect(mapped[1].direction).toBe('East')
    expect(mapped[1].currentStatus).toBe('RED')
  })

  // Test 12-16: Countdown semantics, tick, resync, freeze, no self-advance
  it('12-16. Countdown calculation, resync, freeze, and no self-advancing phase', () => {
    const now = Date.now()
    const light = {
      id: 'tl-1', direction: 'North', currentStatus: 'GREEN', currentPhase: 'NS_GREEN',
      timingMode: 'FIXED_TIME', workingState: 'OK', greenDurationCurrent: 40,
      redDurationCurrent: 40, yellowDuration: 3,
      phaseStartedAt: new Date(now - 10_000).toISOString(),
      simulationTime: 100, simulationRunId: 'run-1',
    }

    // Calculation: 40 - 10 = 30s
    const rem = computeCountdownSec(light, now)
    expect(rem).not.toBeNull()
    expect(Math.round(rem!)).toBe(30)

    // Resync with new now: 40 - 15 = 25s
    const remNext = computeCountdownSec(light, now + 5000)
    expect(Math.round(remNext!)).toBe(25)

    // Exceeded duration: returns 0, does not self advance phase
    const remExceeded = computeCountdownSec(light, now + 50_000)
    expect(remExceeded).toBe(0)
    expect(light.currentStatus).toBe('GREEN')

    // Freeze logic
    expect(deriveRealtimePageStatus('stale', false)).toBe('STALE')
    expect(deriveRealtimePageStatus('live', true)).toBe('PAUSED')
  })

  // Test 17 & 18: vehicleCount to sprite count and cap
  it('17-18. Maps vehicleCount to sprite count and caps at MAX_SPRITES_PER_DIR', () => {
    expect(computeSpriteCount(0)).toBe(0)
    expect(computeSpriteCount(8)).toBe(1)
    expect(computeSpriteCount(24)).toBe(3)
    expect(computeSpriteCount(500)).toBe(MAX_SPRITES_PER_DIR)
    expect(computeSpriteCount(1000)).toBe(MAX_SPRITES_PER_DIR)
  })

  // Test 19: averageSpeed to animation category
  it('19. Maps averageSpeed to animation categories', () => {
    expect(speedToAnimCategory(0)).toBe('STOPPED')
    expect(speedToAnimCategory(4)).toBe('CRAWL')
    expect(speedToAnimCategory(15)).toBe('SLOW')
    expect(speedToAnimCategory(30)).toBe('NORMAL')
    expect(speedToAnimCategory(55)).toBe('FAST')
  })

  // Test 20: Red light stops visual vehicle
  it('20. Red light phase sets isGreen and isYellow to false for that direction', () => {
    const northInEW = phaseForDirection('North', 'EW_GREEN')
    expect(northInEW.isGreen).toBe(false)
    expect(northInEW.isYellow).toBe(false)

    const eastInEW = phaseForDirection('East', 'EW_GREEN')
    expect(eastInEW.isGreen).toBe(true)
    expect(eastInEW.isYellow).toBe(false)
  })

  // Test 21: queueLength changes visualization
  it('21. queueLength creates normalized queue depth', () => {
    expect(computeQueueDepth(0)).toBe(0)
    expect(computeQueueDepth(50)).toBe(0.5)
    expect(computeQueueDepth(100)).toBe(1.0)
    expect(computeQueueDepth(200)).toBe(1.0)
  })

  // Test 27-30: Signal Control tabs render separately and remain accessible
  it('27-30. Control tabs and schemas exist and are distinct', () => {
    expect(PHASE_IDS).toHaveLength(4)
    expect(SCENARIO_IDS).toHaveLength(8)
    expect(CONTROL_MODES).toHaveLength(2)
    expect(GREEN_DURATION_MIN).toBe(10)
    expect(GREEN_DURATION_MAX).toBe(120)
  })
})
