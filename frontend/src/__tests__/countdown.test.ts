// countdown.test.ts — Tests for countdown and phase semantics.
//
// Test Contract (from trafficlight_model.yaml):
//   phaseStartedAt = wall-clock ISO datetime when currentStatus last changed
//   greenDurationCurrent / redDurationCurrent / yellowDuration = configured durations (seconds)
//   remaining = currentDuration - (wallClock.now - phaseStartedAt)

import { describe, it, expect } from 'vitest'
import { computeCountdownSec, deriveRealtimePageStatus } from '@/transforms/realtimeTransforms'
import type { TrafficLightView } from '@/transforms/realtimeTransforms'

function makeLight(overrides: Partial<TrafficLightView> = {}): TrafficLightView {
  return {
    id: 'tl-1',
    direction: 'North',
    currentStatus: 'GREEN',
    currentPhase: 'NS_GREEN',
    timingMode: 'FIXED_TIME',
    workingState: 'OK',
    greenDurationCurrent: 40,
    redDurationCurrent: 35,
    yellowDuration: 5,
    phaseStartedAt: new Date(Date.now() - 10_000).toISOString(), // 10 seconds ago
    simulationTime: 100,
    simulationRunId: 'run-1',
    ...overrides,
  }
}

describe('computeCountdownSec', () => {
  it('computes remaining seconds for GREEN phase', () => {
    const light = makeLight({
      currentStatus: 'GREEN',
      greenDurationCurrent: 40,
      phaseStartedAt: new Date(Date.now() - 10_000).toISOString(), // 10s elapsed
    })
    const remaining = computeCountdownSec(light)
    // 40 - 10 = 30s remaining (±1s tolerance)
    expect(remaining).not.toBeNull()
    expect(remaining!).toBeGreaterThanOrEqual(28)
    expect(remaining!).toBeLessThanOrEqual(32)
  })

  it('computes remaining seconds for RED phase', () => {
    const light = makeLight({
      currentStatus: 'RED',
      redDurationCurrent: 35,
      phaseStartedAt: new Date(Date.now() - 5_000).toISOString(), // 5s elapsed
    })
    const remaining = computeCountdownSec(light)
    // 35 - 5 = 30s remaining
    expect(remaining).not.toBeNull()
    expect(remaining!).toBeGreaterThanOrEqual(28)
    expect(remaining!).toBeLessThanOrEqual(32)
  })

  it('computes remaining seconds for YELLOW phase', () => {
    const light = makeLight({
      currentStatus: 'YELLOW',
      yellowDuration: 5,
      phaseStartedAt: new Date(Date.now() - 2_000).toISOString(), // 2s elapsed
    })
    const remaining = computeCountdownSec(light)
    // 5 - 2 = 3s remaining
    expect(remaining).not.toBeNull()
    expect(remaining!).toBeGreaterThanOrEqual(2)
    expect(remaining!).toBeLessThanOrEqual(4)
  })

  it('returns 0 (not negative) when phase has exceeded duration', () => {
    const light = makeLight({
      currentStatus: 'GREEN',
      greenDurationCurrent: 30,
      phaseStartedAt: new Date(Date.now() - 60_000).toISOString(), // 60s elapsed — way past duration
    })
    const remaining = computeCountdownSec(light)
    expect(remaining).not.toBeNull()
    expect(remaining!).toBe(0)
  })

  it('returns null when phaseStartedAt is null', () => {
    const light = makeLight({ phaseStartedAt: null })
    expect(computeCountdownSec(light)).toBeNull()
  })

  it('returns null when currentStatus is null', () => {
    const light = makeLight({ currentStatus: null })
    expect(computeCountdownSec(light)).toBeNull()
  })

  it('returns null when currentDuration is null', () => {
    const light = makeLight({
      currentStatus: 'GREEN',
      greenDurationCurrent: null,
    })
    expect(computeCountdownSec(light)).toBeNull()
  })

  it('returns null when light is null', () => {
    expect(computeCountdownSec(null)).toBeNull()
    expect(computeCountdownSec(undefined)).toBeNull()
  })

  it('returns null when phaseStartedAt is invalid ISO string', () => {
    const light = makeLight({ phaseStartedAt: 'not-a-date' })
    expect(computeCountdownSec(light)).toBeNull()
  })

  it('uses wall-clock time only — does NOT use simulationTime', () => {
    // Countdown should be derived from phaseStartedAt wall-clock, not simulationTime
    const now = Date.now()
    const light1 = makeLight({
      phaseStartedAt: new Date(now - 10_000).toISOString(),
      simulationTime: 100, // low sim time
    })
    const light2 = makeLight({
      phaseStartedAt: new Date(now - 10_000).toISOString(),
      simulationTime: 9999, // high sim time
    })
    const r1 = computeCountdownSec(light1, now)
    const r2 = computeCountdownSec(light2, now)
    // Same phaseStartedAt → same countdown regardless of simulationTime
    expect(r1).toEqual(r2)
  })

  it('PHASE RULE: countdown reaching 0 should NOT automatically advance phase', () => {
    // When remaining = 0, frontend must NOT self-advance.
    // SUMO/backend is authoritative. UI shows 0s/Syncing.
    const light = makeLight({
      currentStatus: 'GREEN',
      greenDurationCurrent: 5,
      phaseStartedAt: new Date(Date.now() - 10_000).toISOString(), // exceeded
    })
    const remaining = computeCountdownSec(light)
    // Returns 0, not negative, not throwing
    expect(remaining).toBe(0)
    // Status still shows GREEN from backend — unchanged by countdown
    expect(light.currentStatus).toBe('GREEN')
  })
})

describe('deriveRealtimePageStatus', () => {
  it('returns LIVE when fresh and not paused', () => {
    expect(deriveRealtimePageStatus('live', false)).toBe('LIVE')
  })

  it('returns DELAYED when fresh but simulation time not advancing', () => {
    expect(deriveRealtimePageStatus('live', true)).toBe('DELAYED')
  })

  it('returns STALE when freshness is stale', () => {
    expect(deriveRealtimePageStatus('stale', false)).toBe('STALE')
  })

  it('returns OFFLINE when freshness is error', () => {
    expect(deriveRealtimePageStatus('error', false)).toBe('OFFLINE')
  })

  it('returns WAITING when freshness is idle', () => {
    expect(deriveRealtimePageStatus('idle', false)).toBe('WAITING')
  })
})
