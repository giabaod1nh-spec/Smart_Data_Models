// useSimulationPauseDetector.ts — Detect when SUMO simulation is paused.
//
// Strategy: track the last N values of simulationTime. If simulationTime has
// not advanced for PAUSE_WINDOW_MS, the simulation is likely paused.
//
// IMPORTANT: This only affects UI presentation (freeze countdown/animation).
// Source of truth is always the Realtime API response.

import { useRef, useState, useEffect } from 'react'

const PAUSE_WINDOW_MS = 4000     // If simTime unchanged for 4s → PAUSED
const SAMPLE_INTERVAL_MS = 1000  // Check every 1s

export function useSimulationPauseDetector(simulationTime: number | null | undefined): boolean {
  const [isPaused, setIsPaused] = useState(false)
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
        setIsPaused(false)
      }
    }
  }, [simulationTime])

  useEffect(() => {
    timerRef.current = setInterval(() => {
      if (lastSimTimeRef.current === null) return
      const elapsed = Date.now() - lastChangeMs.current
      setIsPaused(elapsed > PAUSE_WINDOW_MS)
    }, SAMPLE_INTERVAL_MS)

    return () => {
      if (timerRef.current) clearInterval(timerRef.current)
    }
  }, [])

  return isPaused
}
