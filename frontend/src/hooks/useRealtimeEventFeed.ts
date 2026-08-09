// useRealtimeEventFeed.ts — Derived session event feed for Realtime operations center
// Compares previousRealtimeSnapshot vs currentRealtimeSnapshot and emits events.
//
// RULES:
//   - Only emits events from REAL state changes (phase, scenario, status, spillback, box, incident)
//   - Never invents fake events
//   - Color coded: green (control/normal), blue (phase/scenario), yellow (warning/status), red (incident/spillback)
//   - Keeps recent 20-50 events in session state
//   - Clears event log when simulationRunId changes
//   - Session-only: resets on full page reload

import { useState, useRef, useEffect, useCallback } from 'react'
import type { RealtimeIntersectionResponse } from '@/types/realtime'
import { formatPhaseLabel, formatScenarioLabel, normalizeCardinalDirection } from '@/transforms/realtimeTransforms'

export type EventSeverity = 'green' | 'blue' | 'yellow' | 'orange' | 'red'
export type EventCategory = 'phase' | 'scenario' | 'status' | 'incident' | 'spillback' | 'box' | 'command'

export interface RealtimeFeedEvent {
  id: string
  timestamp: string // HH:mm:ss
  timeMs: number
  title: string
  detail?: string
  severity: EventSeverity
  category: EventCategory
}

const MAX_EVENTS = 50

function formatTime(date: Date = new Date()): string {
  const h = String(date.getHours()).padStart(2, '0')
  const m = String(date.getMinutes()).padStart(2, '0')
  const s = String(date.getSeconds()).padStart(2, '0')
  return `${h}:${m}:${s}`
}

export function useRealtimeEventFeed(
  realtimeData: RealtimeIntersectionResponse | null | undefined,
) {
  const [events, setEvents] = useState<RealtimeFeedEvent[]>([])
  const prevDataRef = useRef<RealtimeIntersectionResponse | null>(null)
  const lastRunIdRef = useRef<string | null | undefined>(undefined)
  const eventCounterRef = useRef(1)

  const currentRunId = realtimeData?.metadata?.simulationRunId ?? realtimeData?.intersection?.simulationRunId

  // Clear feed if simulationRunId changes
  useEffect(() => {
    if (lastRunIdRef.current !== undefined && lastRunIdRef.current !== currentRunId) {
      setEvents([])
      prevDataRef.current = null
    }
    lastRunIdRef.current = currentRunId
  }, [currentRunId])

  // Helper to append events
  const addEvents = useCallback((newItems: Omit<RealtimeFeedEvent, 'id' | 'timestamp' | 'timeMs'>[]) => {
    if (newItems.length === 0) return
    const now = new Date()
    const ts = formatTime(now)
    const timeMs = now.getTime()

    const created: RealtimeFeedEvent[] = newItems.map((item) => ({
      ...item,
      id: `evt-${timeMs}-${eventCounterRef.current++}`,
      timestamp: ts,
      timeMs,
    }))

    setEvents((prev) => [...created, ...prev].slice(0, MAX_EVENTS))
  }, [])

  // Manual event insertion (for control commands)
  const addCommandEvent = useCallback((title: string, detail?: string, severity: EventSeverity = 'green') => {
    addEvents([{ title, detail, severity, category: 'command' }])
  }, [addEvents])

  // Snapshot diffing effect
  useEffect(() => {
    if (!realtimeData) return

    const prev = prevDataRef.current
    if (!prev) {
      // First snapshot — record base and populate initial discovery event if incident/spillback active
      const initEvents: Omit<RealtimeFeedEvent, 'id' | 'timestamp' | 'timeMs'>[] = []
      const int = realtimeData.intersection
      if (int?.currentPhase) {
        initEvents.push({
          title: `Initial traffic light phase: ${formatPhaseLabel(int.currentPhase)}`,
          severity: 'blue',
          category: 'phase',
        })
      }
      if (int?.scenarioId) {
        initEvents.push({
          title: `Active scenario: ${formatScenarioLabel(int.scenarioId)}`,
          severity: 'blue',
          category: 'scenario',
        })
      }
      if (int?.hasActiveIncident) {
        initEvents.push({
          title: 'Active incident detected on intersection',
          severity: 'red',
          category: 'incident',
        })
      }
      if (int?.hasSpillback) {
        initEvents.push({
          title: 'Spillback detected on intersection approach',
          severity: 'red',
          category: 'spillback',
        })
      }
      if (int?.isBoxBlocked) {
        initEvents.push({
          title: 'Intersection box blocked',
          severity: 'red',
          category: 'box',
        })
      }
      if (initEvents.length > 0) {
        addEvents(initEvents)
      }
      prevDataRef.current = realtimeData
      return
    }

    const newDiffEvents: Omit<RealtimeFeedEvent, 'id' | 'timestamp' | 'timeMs'>[] = []

    const prevInt = prev.intersection
    const currInt = realtimeData.intersection

    // 1. Current Phase Change
    const prevPhase = prevInt?.currentPhase ?? prev.trafficLights[0]?.currentPhase
    const currPhase = currInt?.currentPhase ?? realtimeData.trafficLights[0]?.currentPhase
    if (prevPhase && currPhase && prevPhase !== currPhase) {
      newDiffEvents.push({
        title: `Traffic light changed: ${prevPhase} → ${currPhase}`,
        detail: `${formatPhaseLabel(prevPhase)} to ${formatPhaseLabel(currPhase)}`,
        severity: 'blue',
        category: 'phase',
      })
    }

    // 2. Scenario Change
    const prevScenario = prevInt?.scenarioId ?? prev.metadata?.scenarioId
    const currScenario = currInt?.scenarioId ?? realtimeData.metadata?.scenarioId
    if (prevScenario && currScenario && prevScenario !== currScenario) {
      newDiffEvents.push({
        title: `Scenario changed: ${formatScenarioLabel(prevScenario)} → ${formatScenarioLabel(currScenario)}`,
        detail: `New scenario active: ${currScenario}`,
        severity: 'blue',
        category: 'scenario',
      })
    }

    // 3. Overall Traffic Status Change
    if (prevInt?.overallTrafficStatus && currInt?.overallTrafficStatus && prevInt.overallTrafficStatus !== currInt.overallTrafficStatus) {
      const isSevere = currInt.overallTrafficStatus.includes('HIGH') || currInt.overallTrafficStatus.includes('CONGEST') || currInt.overallTrafficStatus.includes('JAM')
      newDiffEvents.push({
        title: `Intersection traffic status: ${currInt.overallTrafficStatus}`,
        detail: `Changed from ${prevInt.overallTrafficStatus}`,
        severity: isSevere ? 'orange' : 'yellow',
        category: 'status',
      })
    }

    // 4. Derived Traffic State Change
    if (prevInt?.derivedTrafficState && currInt?.derivedTrafficState && prevInt.derivedTrafficState !== currInt.derivedTrafficState) {
      newDiffEvents.push({
        title: `Traffic state changed to ${currInt.derivedTrafficState}`,
        severity: 'orange',
        category: 'status',
      })
    }

    // 5. Spillback Detection
    if (!prevInt?.hasSpillback && currInt?.hasSpillback) {
      newDiffEvents.push({
        title: 'Spillback detected on intersection approach',
        severity: 'red',
        category: 'spillback',
      })
    }

    // 6. Box Blocked Detection
    if (!prevInt?.isBoxBlocked && currInt?.isBoxBlocked) {
      newDiffEvents.push({
        title: 'Intersection became blocked (box blocked)',
        severity: 'red',
        category: 'box',
      })
    }

    // 7. Active Incident Detection
    if (!prevInt?.hasActiveIncident && currInt?.hasActiveIncident) {
      newDiffEvents.push({
        title: 'Incident detected on intersection',
        severity: 'red',
        category: 'incident',
      })
    }

    // 8. Directional Sensor Status Changes
    const prevSensors = prev.vehicleSensors ?? []
    const currSensors = realtimeData.vehicleSensors ?? []
    for (const currS of currSensors) {
      const dir = normalizeCardinalDirection(currS.trafficDirection)
      const prevS = prevSensors.find((s) => normalizeCardinalDirection(s.trafficDirection) === dir)
      if (prevS && currS.trafficStatus && prevS.trafficStatus !== currS.trafficStatus) {
        const ts = currS.trafficStatus.toUpperCase()
        const isSevere = ts.includes('HIGH') || ts.includes('CONGEST') || ts.includes('BLOCKED')
        newDiffEvents.push({
          title: `${dir} approach status changed to ${currS.trafficStatus}`,
          severity: isSevere ? 'orange' : 'yellow',
          category: 'status',
        })
      }
    }

    if (newDiffEvents.length > 0) {
      addEvents(newDiffEvents)
    }

    prevDataRef.current = realtimeData
  }, [realtimeData, addEvents])

  const clearFeed = useCallback(() => setEvents([]), [])

  return { events, addCommandEvent, clearFeed }
}
