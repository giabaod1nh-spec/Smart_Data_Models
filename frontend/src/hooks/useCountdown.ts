// useCountdown.ts — Live countdown for traffic light phase duration.
//
// CONTRACT SEMANTICS (verified from trafficlight_model.yaml):
//   phaseStartedAt = wall-clock ISO datetime when currentStatus last changed
//   greenDurationCurrent / redDurationCurrent / yellowDuration = configured durations (seconds)
//   remaining = currentDuration - (wallClock.now - phaseStartedAt)
//
// TIME DOMAIN: wall-clock only. Does NOT use simulationTime.
//
// FREEZE conditions:
//   - freshnessState === 'stale' | 'error'
//   - isPaused / metricsDelayed (simulationTime not advancing)
//   - document.hidden
//   - timingMode === 'MANUAL' (traffic officer holds phase — no countdown)
// Decorative canvas animation is independent and must NOT use this freeze flag.
//
// RESYNC: Pure, immediate derivation from computeCountdownSec(light, nowMs).
//
// PHASE CHANGE RULE: When countdown reaches 0, UI shows "0s / Syncing".
//   Frontend NEVER self-advances the phase. SUMO/backend is authoritative.

import { useState, useEffect, useMemo } from 'react'
import { computeCountdownSec, type TrafficLightView } from '@/transforms/realtimeTransforms'
import type { FreshnessState } from '@/transforms/realtimeTransforms'

export interface CountdownState {
  /** Remaining seconds (null = not computable from current contract) */
  remainingSec: number | null
  /** The phase color string from backend (GREEN/RED/YELLOW) */
  currentStatus: string | null
  /** Configured duration for current phase */
  configuredDuration: number | null
  /** True when countdown has reached 0 and waiting for backend phase change */
  isSyncing: boolean
  /** True when countdown is frozen (stale/offline/paused) */
  isFrozen: boolean
}

export function useCountdown(
  light: TrafficLightView | null | undefined,
  freshnessState: FreshnessState,
  isPaused: boolean,
  options?: { forceFrozen?: boolean },
): CountdownState {
  const [nowMs, setNowMs] = useState(() => Date.now())
  const manualHold = light?.timingMode === 'MANUAL' || Boolean(options?.forceFrozen)
  const isFrozen =
    freshnessState === 'stale'
    || freshnessState === 'error'
    || isPaused
    || manualHold

  // 1-second wall-clock timer
  useEffect(() => {
    if (isFrozen) return
    const interval = setInterval(() => {
      if (!document.hidden) {
        setNowMs(Date.now())
      }
    }, 1000)
    return () => clearInterval(interval)
  }, [isFrozen])

  // Pure derivation of remaining seconds from authoritative formula
  const remainingSec = useMemo(() => {
    if (!light) return null
    if (manualHold) return null
    return computeCountdownSec(light, nowMs)
  }, [light, nowMs, manualHold])

  const isSyncing = !manualHold && remainingSec !== null && remainingSec === 0

  // Configured duration for display
  const configuredDuration: number | null = useMemo(() => {
    if (!light?.currentStatus) return null
    const s = light.currentStatus.toUpperCase()
    if (s.includes('GREEN')) return light.greenDurationCurrent
    if (s.includes('RED')) return light.redDurationCurrent
    if (s.includes('YELLOW')) return light.yellowDuration
    return null
  }, [light])

  return {
    remainingSec,
    currentStatus: light?.currentStatus ?? null,
    configuredDuration,
    isSyncing,
    isFrozen,
  }
}
