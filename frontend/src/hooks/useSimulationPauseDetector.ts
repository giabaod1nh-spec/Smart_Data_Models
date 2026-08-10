// useSimulationPauseDetector.ts — Detect when SUMO simulation time is delayed.
//
// Strategy: track simulationTime from realtime metadata. If it has not advanced
// for PAUSE_WINDOW_MS of wall-clock time, telemetry is DELAYED (sparse ticks).
//
// IMPORTANT: This only freezes metrics countdown / status badges.
// Decorative Live Traffic View animation MUST keep running — vehicles are
// representative sprites, not exact SUMO positions. Source of truth for KPI
// numbers is always the Realtime API response.

import { useRef, useState, useEffect } from 'react'

const PAUSE_WINDOW_MS = 4000     // If simTime unchanged for 4s → metrics DELAYED
const SAMPLE_INTERVAL_MS = 1000  // Check every 1s

/** @returns true when simulationTime has not advanced recently (metrics delayed). */
export function useSimulationPauseDetector(simulationTime: number | null | undefined): boolean {
  const [metricsDelayed, setMetricsDelayed] = useState(false)
  const lastSimTimeRef = useRef<number | null>(null)
  const lastChangeMs = useRef<number>(0)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Initialize lastChangeMs after mount (avoids calling impure Date.now in render body)
  useEffect(() => {
    lastChangeMs.current = Date.now()
  }, [])

  useEffect(() => {
    // Update last seen simTime and when it last changed
    if (simulationTime !== null && simulationTime !== undefined) {
      if (lastSimTimeRef.current !== simulationTime) {
        lastSimTimeRef.current = simulationTime
        lastChangeMs.current = Date.now()
        setMetricsDelayed(false)
      }
    }
  }, [simulationTime])

  useEffect(() => {
    timerRef.current = setInterval(() => {
      if (lastSimTimeRef.current === null) return
      const elapsed = Date.now() - lastChangeMs.current
      setMetricsDelayed(elapsed > PAUSE_WINDOW_MS)
    }, SAMPLE_INTERVAL_MS)

    return () => {
      if (timerRef.current) clearInterval(timerRef.current)
    }
  }, [])

  return metricsDelayed
}
