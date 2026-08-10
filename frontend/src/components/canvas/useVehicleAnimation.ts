// useVehicleAnimation.ts — Vehicle animation state manager for IntersectionScene.
//
// Manages "representative" vehicle sprites that aggregate real sensor data.
// Sprites represent traffic conditions, NOT exact SUMO vehicle coordinates.
//
// DATA SOURCES (from VehicleSensorResponse aggregates):
//   vehicleCount → sprite density (capped at MAX_SPRITES_PER_DIR)
//   averageSpeed → animation travel rate (scaled from km/h)
//   queueLength  → queue depth near stop line
//   currentPhase → which directions may cross (red = hard stop before stop line)
//   waitingVehicleCount → queued sprites near stop line
//
// ANIMATION:
//   Uses requestAnimationFrame for smooth 60fps canvas updates.
//   On RED: sprites never cross the stop line (progress stays >= STOP_LINE_PROGRESS).
//   Travel speed scales with reported averageSpeed (km/h).

import { useEffect, useRef, useState } from 'react'

export type AnimationCategory = 'STOPPED' | 'CRAWL' | 'SLOW' | 'NORMAL' | 'FAST'
export type Direction = 'North' | 'South' | 'East' | 'West'
export type SignalColor = 'RED' | 'YELLOW' | 'GREEN'

export interface VehicleSprite {
  id: string
  dir: Direction
  /** Position along the approach arm: 0 = stop line, 1 = far end of arm */
  progress: number
  /** Lane offset on inbound half only: -1 = left inbound, 1 = right inbound */
  lane: number
  /** Whether this sprite is currently queued near stop line */
  queued: boolean
  /** Effective motion speed category for rendering (body color / headlights) */
  animCategory: AnimationCategory
}

/** progress must stay at/above this when signal is red (just before white stop line). */
export const STOP_LINE_PROGRESS = 0.06
/** Soft braking zone approaching the stop line on red. */
export const BRAKE_ZONE_PROGRESS = 0.35
/** Max progress into the box while clearing on green before respawn. */
export const CLEARANCE_PROGRESS = -0.22

export const MAX_SPRITES_PER_DIR = 10
export const SPRITE_SCALE = 8   // vehicleCount / SPRITE_SCALE = sprite count (before cap)

/** Map averageSpeed (km/h) to animation category (for UI coloring). */
export function speedToAnimCategory(speedKmh: number | null | undefined): AnimationCategory {
  if (speedKmh === null || speedKmh === undefined) return 'SLOW'
  if (speedKmh <= 0.5) return 'STOPPED'
  if (speedKmh <= 5) return 'CRAWL'
  if (speedKmh <= 20) return 'SLOW'
  if (speedKmh <= 40) return 'NORMAL'
  return 'FAST'
}

/**
 * Convert averageSpeed (km/h) → normalized progress delta per frame (~60fps).
 * ~50 km/h ≈ 0.012 so a typical approach arm clears in ~1–2s at free flow.
 */
export function speedKmhToProgressDelta(speedKmh: number | null | undefined): number {
  if (speedKmh === null || speedKmh === undefined) return 0.004
  const capped = Math.max(0, Math.min(80, speedKmh))
  return (capped / 50) * 0.012
}

/** Get current phase green/yellow for a direction */
export function phaseForDirection(
  dir: Direction,
  currentPhase: string | null | undefined,
): { isGreen: boolean; isYellow: boolean } {
  if (!currentPhase) return { isGreen: false, isYellow: false }
  const p = currentPhase.toUpperCase()
  const isNS = dir === 'North' || dir === 'South'
  const isEW = dir === 'East' || dir === 'West'
  if (p === 'NS_GREEN' && isNS) return { isGreen: true, isYellow: false }
  if (p === 'NS_YELLOW' && isNS) return { isGreen: false, isYellow: true }
  if (p === 'EW_GREEN' && isEW) return { isGreen: true, isYellow: false }
  if (p === 'EW_YELLOW' && isEW) return { isGreen: false, isYellow: true }
  return { isGreen: false, isYellow: false }
}

/** Resolve bulb color for a direction from intersection phase (source of truth). */
export function signalColorForDirection(
  dir: Direction,
  currentPhase: string | null | undefined,
): SignalColor {
  const { isGreen, isYellow } = phaseForDirection(dir, currentPhase)
  if (isGreen) return 'GREEN'
  if (isYellow) return 'YELLOW'
  return 'RED'
}

/**
 * Advance one sprite one frame.
 * Red: hard clamp at/before stop line. Green: may clear through and respawn.
 * Yellow: decelerate; still may clear if already past decision point.
 */
export function advanceSpriteProgress(
  progress: number,
  dir: Direction,
  phase: string | null | undefined,
  averageSpeedKmh: number | null | undefined,
): { progress: number; queued: boolean; animCategory: AnimationCategory } {
  const { isGreen, isYellow } = phaseForDirection(dir, phase)
  const baseDelta = speedKmhToProgressDelta(averageSpeedKmh)

  if (!isGreen && !isYellow) {
    // RED — must stop before the white stop line
    if (progress <= STOP_LINE_PROGRESS) {
      return {
        progress: STOP_LINE_PROGRESS,
        queued: true,
        animCategory: 'STOPPED',
      }
    }
    // Brake harder as we approach the stop line
    const brakeT = Math.min(
      1,
      Math.max(0, (progress - STOP_LINE_PROGRESS) / (BRAKE_ZONE_PROGRESS - STOP_LINE_PROGRESS)),
    )
    const approachDelta = Math.max(0.0012, baseDelta * (0.15 + 0.85 * brakeT))
    const next = Math.max(STOP_LINE_PROGRESS, progress - approachDelta)
    const queued = next <= STOP_LINE_PROGRESS + 0.01
    return {
      progress: next,
      queued,
      animCategory: queued ? 'STOPPED' : speedToAnimCategory(Math.max(1, (averageSpeedKmh ?? 20) * brakeT)),
    }
  }

  // GREEN / YELLOW
  let delta = baseDelta
  if (isYellow) {
    delta *= 0.4
    // Far from stop → ease toward stop; near stop → clear through
    if (progress > BRAKE_ZONE_PROGRESS * 0.5) {
      const next = Math.max(STOP_LINE_PROGRESS, progress - Math.max(0.001, delta))
      const queued = next <= STOP_LINE_PROGRESS + 0.01
      return {
        progress: next,
        queued,
        animCategory: queued ? 'STOPPED' : 'SLOW',
      }
    }
  }

  // Match reported average: no creeping when sensor says ~0 km/h
  if (delta <= 0) {
    return {
      progress: Math.max(progress, STOP_LINE_PROGRESS),
      queued: progress <= STOP_LINE_PROGRESS + 0.02,
      animCategory: 'STOPPED',
    }
  }

  let nextProgress = progress - delta
  if (nextProgress < CLEARANCE_PROGRESS) {
    // Finished crossing — respawn at far end of approach
    nextProgress = 0.88 + Math.random() * 0.1
  }

  return {
    progress: nextProgress,
    queued: false,
    animCategory: isYellow ? 'SLOW' : speedToAnimCategory(averageSpeedKmh),
  }
}

export interface SensorInput {
  dir: Direction
  vehicleCount: number | null
  averageSpeed: number | null
  queueLength: number | null
  waitingVehicleCount: number | null
  trafficStatus: string | null
}

/** Compute sprite count from vehicleCount */
export function computeSpriteCount(vehicleCount: number | null): number {
  if (!vehicleCount || vehicleCount <= 0) return 0
  return Math.min(MAX_SPRITES_PER_DIR, Math.max(1, Math.ceil(vehicleCount / SPRITE_SCALE)))
}

/** Compute normalized queue depth (0..1) from queueLength */
export function computeQueueDepth(queueLength: number | null): number {
  if (!queueLength || queueLength <= 0) return 0
  return Math.min(1, queueLength / 100)
}

/**
 * Generate initial sprite positions for a direction.
 * Queued sprites sit behind the stop line with vehicle-length gaps.
 * Moving sprites stay on the approach (never past the stop line).
 */
function generateSprites(
  dir: Direction,
  count: number,
  queueDepth: number,
  prevSprites: VehicleSprite[],
  phase: string | null | undefined,
  averageSpeed: number | null,
): VehicleSprite[] {
  if (count <= 0) return []

  const { isGreen, isYellow } = phaseForDirection(dir, phase)
  const isRed = !isGreen && !isYellow
  // On red, bias toward a visible queue matching queueLength
  const queueCount = isRed
    ? Math.min(count, Math.max(1, Math.ceil(count * Math.max(0.35, queueDepth))))
    : Math.min(count, Math.floor(count * queueDepth))
  const movingCount = count - queueCount

  const sprites: VehicleSprite[] = []
  const gap = 0.055

  for (let i = 0; i < queueCount; i++) {
    const prev = prevSprites.find((s) => s.id === `${dir}-q${i}`)
    let progress = prev?.progress ?? (STOP_LINE_PROGRESS + i * gap)
    // Never place queued cars past the stop line
    progress = Math.max(STOP_LINE_PROGRESS + i * gap, progress)
    if (isRed) progress = Math.max(STOP_LINE_PROGRESS, progress)
    sprites.push({
      id: `${dir}-q${i}`,
      dir,
      progress,
      lane: i % 2 === 0 ? -0.7 : 0.7,
      queued: isRed || progress <= STOP_LINE_PROGRESS + 0.02,
      animCategory: isRed ? 'STOPPED' : speedToAnimCategory(averageSpeed),
    })
  }

  for (let i = 0; i < movingCount; i++) {
    const prev = prevSprites.find((s) => s.id === `${dir}-m${i}`)
    let progress =
      prev?.progress ??
      (0.28 + (i / Math.max(movingCount, 1)) * 0.65)
    // Keep regenerations on the approach side of the stop line
    if (progress < STOP_LINE_PROGRESS) progress = STOP_LINE_PROGRESS + 0.08 + i * 0.05
    if (isRed) progress = Math.max(STOP_LINE_PROGRESS + (queueCount + i) * gap, progress)
    sprites.push({
      id: `${dir}-m${i}`,
      dir,
      progress,
      lane: i % 2 === 0 ? -0.7 : 0.7,
      queued: false,
      animCategory: speedToAnimCategory(averageSpeed),
    })
  }

  return sprites
}

/** Main animation hook */
export function useVehicleAnimation(
  sensors: SensorInput[],
  currentPhase: string | null | undefined,
  simulationRunId: string | null | undefined,
  isFrozen: boolean,
  canvasW: number,
  canvasH: number,
) {
  const spritesRef = useRef<VehicleSprite[]>([])
  const [sprites, setSprites] = useState<VehicleSprite[]>([])
  const lastRunIdRef = useRef<string | null | undefined>(undefined)
  const phaseRef = useRef(currentPhase)
  const sensorsRef = useRef(sensors)
  const frozenRef = useRef(isFrozen)

  useEffect(() => {
    phaseRef.current = currentPhase
    sensorsRef.current = sensors
    frozenRef.current = isFrozen
  }, [currentPhase, sensors, isFrozen])

  useEffect(() => {
    if (lastRunIdRef.current !== undefined && lastRunIdRef.current !== simulationRunId) {
      spritesRef.current = []
    }
    lastRunIdRef.current = simulationRunId
  }, [simulationRunId])

  // Recompute sprite sets when sensor data or phase updates
  useEffect(() => {
    const dirs: Direction[] = ['North', 'South', 'East', 'West']
    const newSprites: VehicleSprite[] = []
    const phase = phaseRef.current

    for (const dir of dirs) {
      const sensor = sensorsRef.current.find((s) => s.dir === dir)
      const count = computeSpriteCount(sensor?.vehicleCount ?? null)
      const queueDepth = computeQueueDepth(sensor?.queueLength ?? null)
      const prev = spritesRef.current.filter((s) => s.dir === dir)
      const generated = generateSprites(
        dir,
        count,
        queueDepth,
        prev,
        phase,
        sensor?.averageSpeed ?? null,
      )
      newSprites.push(...generated)
    }

    spritesRef.current = newSprites
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    sensors.map((s) => `${s.dir}:${s.vehicleCount}:${s.queueLength}:${s.averageSpeed}`).join('|'),
    currentPhase,
  ])

  useEffect(() => {
    let rafId: number | null = null

    const tick = () => {
      if (frozenRef.current || document.hidden) {
        rafId = requestAnimationFrame(tick)
        return
      }

      const prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches
      if (prefersReduced) {
        rafId = requestAnimationFrame(tick)
        return
      }

      const currentSensors = sensorsRef.current
      const phase = phaseRef.current

      const updated = spritesRef.current.map((sprite) => {
        const sensor = currentSensors.find((s) => s.dir === sprite.dir)
        const stepped = advanceSpriteProgress(
          sprite.progress,
          sprite.dir,
          phase,
          sensor?.averageSpeed ?? null,
        )
        return {
          ...sprite,
          progress: stepped.progress,
          queued: stepped.queued,
          animCategory: stepped.animCategory,
        }
      })

      spritesRef.current = updated
      setSprites([...updated])
      rafId = requestAnimationFrame(tick)
    }

    rafId = requestAnimationFrame(tick)

    return () => {
      if (rafId !== null) {
        cancelAnimationFrame(rafId)
        rafId = null
      }
    }
  }, [canvasW, canvasH])

  return { sprites }
}
