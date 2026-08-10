// eventFeed.test.ts — Unit tests for useRealtimeEventFeed snapshot diffing.
import { describe, it, expect } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useRealtimeEventFeed } from '@/hooks/useRealtimeEventFeed'
import type { RealtimeIntersectionResponse } from '@/types/realtime'

function makeRealtime(overrides: Partial<RealtimeIntersectionResponse> = {}): RealtimeIntersectionResponse {
  return {
    intersection: {
      id: 'urn:ngsi-ld:Intersection:C',
      type: 'Intersection',
      name: 'Intersection C',
      description: null,
      location: null,
      numberOfApproaches: 4,
      intersectionStatus: 'OK',
      frequentCongestion: false,
      refTrafficLights: [],
      refCameras: [],
      refVehicleSensors: [],
      dateObserved: '2026-08-08T10:00:00Z',
      overallTrafficStatus: 'NORMAL',
      totalVehicleCount: 120,
      hasActiveIncident: false,
      simulationTime: 100,
      simulationRunId: 'run-1',
      scenarioId: 'normal',
      currentPhase: 'NS_GREEN',
      derivedTrafficState: 'FREE_FLOW',
      hasSpillback: false,
      isBoxBlocked: false,
      probableCauseType: null,
      affectedBy: null,
      causeDetectedAt: null,
    },
    trafficLights: [
      {
        id: 'tl-1',
        type: 'TrafficLight',
        name: null,
        description: null,
        location: null,
        currentStatus: 'GREEN',
        phaseStartedAt: new Date().toISOString(),
        timingMode: 'FIXED_TIME',
        workingState: 'OK',
        trafficDirection: 'North',
        greenDurationCurrent: 40,
        redDurationCurrent: 40,
        yellowDuration: 3,
        refIntersection: 'urn:ngsi-ld:Intersection:C',
        refCamera: null,
        simulationTime: 100,
        simulationRunId: 'run-1',
        scenarioId: 'normal',
        currentPhase: 'NS_GREEN',
      },
    ],
    vehicleSensors: [
      {
        id: 'vs-1',
        type: 'VehicleSensor',
        name: 'West Sensor',
        description: null,
        location: null,
        sensorType: 'loop',
        sensorStatus: 'active',
        trafficDirection: 'West',
        refIntersection: 'urn:ngsi-ld:Intersection:C',
        refCamera: null,
        refTrafficLight: null,
        dateObserved: '2026-08-08T10:00:00Z',
        vehicleCount: 50,
        pcuEquivalent: 60,
        vehicleClassComposition: null,
        leftTurnCount: 10,
        straightCount: 30,
        rightTurnCount: 10,
        averageSpeed: 25,
        waitingVehicleCount: 5,
        queueLength: 10,
        queueStraight: 10,
        queueLeft: 0,
        queueRight: 0,
        occupancyRate: 22.0,
        trafficStatus: 'LIGHT',
        arrivalRatePcuPerSec: 1.0,
        waitingReasonCounts: null,
        dominantWaitingReason: null,
        theoreticalSpeed: 40,
        simulationTime: 100,
        simulationRunId: 'run-1',
        scenarioId: 'normal',
        derivedTrafficState: 'FREE_FLOW',
        spillbackRisk: false,
        operationalState: null,
      },
    ],
    cameras: [],
    metadata: {
      simulationRunId: 'run-1',
      simulationTime: 100,
      scenarioId: 'normal',
      consistent: true,
      consistencyIssues: null,
      projectorStatus: 'OK',
      freshnessSeconds: 1.0,
    },
    ...overrides,
  }
}

describe('useRealtimeEventFeed', () => {
  it('detects traffic light phase changes across snapshots', () => {
    let currentData = makeRealtime()
    const { result, rerender } = renderHook(() => useRealtimeEventFeed(currentData))

    // Initial load produces initial phase discovery event
    expect(result.current.events.some((e) => e.category === 'phase')).toBe(true)

    // Second snapshot with phase changed to EW_GREEN
    currentData = makeRealtime({
      intersection: {
        ...currentData.intersection!,
        currentPhase: 'EW_GREEN',
      },
      trafficLights: [
        {
          ...currentData.trafficLights[0],
          currentPhase: 'EW_GREEN',
          currentStatus: 'RED',
        },
      ],
    })
    rerender()

    // Event feed should record the phase transition
    const phaseEvent = result.current.events.find((e) =>
      e.title.includes('Traffic light changed') && e.title.includes('EW_GREEN'),
    )
    expect(phaseEvent).toBeDefined()
    expect(phaseEvent?.severity).toBe('blue')
  })

  it('detects scenario changes across snapshots', () => {
    let currentData = makeRealtime()
    const { result, rerender } = renderHook(() => useRealtimeEventFeed(currentData))

    // Change scenario to heavy_traffic
    currentData = makeRealtime({
      intersection: {
        ...currentData.intersection!,
        scenarioId: 'heavy_traffic',
      },
    })
    rerender()

    const scenarioEvent = result.current.events.find((e) =>
      e.title.includes('Scenario changed') && e.title.includes('Heavy Traffic'),
    )
    expect(scenarioEvent).toBeDefined()
    expect(scenarioEvent?.severity).toBe('blue')
  })

  it('detects traffic status changes on directional sensors', () => {
    let currentData = makeRealtime()
    const { result, rerender } = renderHook(() => useRealtimeEventFeed(currentData))

    // West approach becomes MODERATE
    currentData = makeRealtime({
      vehicleSensors: [
        {
          ...currentData.vehicleSensors[0],
          trafficStatus: 'MODERATE',
        },
      ],
    })
    rerender()

    const statusEvent = result.current.events.find((e) =>
      e.title.includes('West approach status changed to MODERATE'),
    )
    expect(statusEvent).toBeDefined()
    expect(statusEvent?.severity).toBe('yellow')
  })

  it('detects spillback and incident alerts with red severity', () => {
    let currentData = makeRealtime()
    const { result, rerender } = renderHook(() => useRealtimeEventFeed(currentData))

    // Spillback and incident emerge
    currentData = makeRealtime({
      intersection: {
        ...currentData.intersection!,
        hasSpillback: true,
        hasActiveIncident: true,
      },
    })
    rerender()

    const spillbackEvent = result.current.events.find((e) => e.category === 'spillback')
    const incidentEvent = result.current.events.find((e) => e.category === 'incident')

    expect(spillbackEvent).toBeDefined()
    expect(spillbackEvent?.severity).toBe('red')
    expect(incidentEvent).toBeDefined()
    expect(incidentEvent?.severity).toBe('red')
  })

  it('clears session events when simulationRunId changes', () => {
    let currentData = makeRealtime({
      metadata: {
        simulationRunId: 'run-1',
        simulationTime: 100,
        scenarioId: 'normal',
        consistent: true,
        consistencyIssues: null,
        projectorStatus: 'OK',
        freshnessSeconds: 1.0,
      },
    })
    const { result, rerender } = renderHook(() => useRealtimeEventFeed(currentData))

    expect(result.current.events.length).toBeGreaterThan(0)

    // New simulation run started
    currentData = makeRealtime({
      metadata: {
        simulationRunId: 'run-2',
        simulationTime: 0,
        scenarioId: 'normal',
        consistent: true,
        consistencyIssues: null,
        projectorStatus: 'OK',
        freshnessSeconds: 1.0,
      },
    })
    rerender()

    // Old events from run-1 are cleared
    expect(result.current.events.some((e) => e.title.includes('run-1'))).toBe(false)
  })

  it('does not produce duplicate events when identical snapshot repeats', () => {
    const currentData = makeRealtime()
    const { result, rerender } = renderHook(() => useRealtimeEventFeed(currentData))

    const countAfterFirst = result.current.events.length

    // Same snapshot re-polled
    rerender()
    expect(result.current.events.length).toBe(countAfterFirst)

    rerender()
    expect(result.current.events.length).toBe(countAfterFirst)
  })

  it('supports adding manual command queued/applied events', () => {
    const currentData = makeRealtime()
    const { result } = renderHook(() => useRealtimeEventFeed(currentData))

    act(() => {
      result.current.addCommandEvent('Force Phase EW_GREEN', 'Queued via Spring Server', 'green')
    })

    const cmdEvent = result.current.events.find((e) => e.title === 'Force Phase EW_GREEN')
    expect(cmdEvent).toBeDefined()
    expect(cmdEvent?.severity).toBe('green')
    expect(cmdEvent?.category).toBe('command')
  })
})
