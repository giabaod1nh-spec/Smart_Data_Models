// useVehicleAnimation.ts — Vehicle animation state manager for IntersectionScene.
//
// Manages "representative" vehicle sprites that aggregate real sensor data.
// Sprites represent traffic conditions, NOT exact SUMO vehicle coordinates.
//
// DATA SOURCES (from VehicleSensorResponse aggregates):
//   vehicleCount → sprite density (capped at MAX_SPRITES_PER_DIR)
//   averageSpeed → animation speed category (STOPPED, CRAWL, SLOW, NORMAL, FAST)
//   queueLength  → queue depth near stop line
//   currentPhase → which directions are allowed to move (Red lights enforce stopping)
//   waitingVehicleCount → queued sprites near stop line
//
// ANIMATION:
//   Uses requestAnimationFrame for smooth 60fps canvas updates.
//   API polling remains at 1-2s (not increased for animation).
//   Sprite positions smoothly advance and stop at stop lines on red signals.

import { useEffect, useRef, useState } from 'react'

export type AnimationCategory = 'STOPPED' | 'CRAWL' | 'SLOW' | 'NORMAL' | 'FAST'
export type Direction = 'North' | 'South' | 'East' | 'West'

export interface VehicleSprite {
  id: string
  dir: Direction
  /** Position along the approach arm: 0 = stop line, 1 = far end of arm */
  progress: number
  /** Lane offset: -1 = left lane, 0 = center, 1 = right lane */
  lane: number
  /** Whether this sprite is currently queued near stop line */
  queued: boolean
}

export const MAX_SPRITES_PER_DIR = 10
export const SPRITE_SCALE = 8   // vehicleCount / SPRITE_SCALE = sprite count (before cap)

/** Map averageSpeed (km/h) to animation category */
export function speedToAnimCategory(speedKmh: number | null | undefined): AnimationCategory {
  if (speedKmh === null || speedKmh === undefined) return 'SLOW'
  if (speedKmh <= 0) return 'STOPPED'
  if (speedKmh <= 5) return 'CRAWL'
  if (speedKmh <= 20) return 'SLOW'
  if (speedKmh <= 40) return 'NORMAL'
  return 'FAST'
}

/** Map animCategory to progress-advance speed per frame (normalized 0..1 scale) */
function animCategoryToSpeed(cat: AnimationCategory): number {
  switch (cat) {
    case 'STOPPED': return 0
    case 'CRAWL':   return 0.0015
    case 'SLOW':    return 0.0035
    case 'NORMAL':  return 0.007
    case 'FAST':    return 0.011
    default:        return 0.0035
  }
}

/** Get current phase green/yellow for a direction */
export function phaseForDirection(dir: Direction, currentPhase: string | null | undefined): { isGreen: boolean; isYellow: boolean } {
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
 * Queued sprites cluster near the stop line (progress near 0.02..0.15).
 * Moving sprites spread along the approach arm (progress 0.2..0.95).
 */
function generateSprites(
  dir: Direction,
  count: number,
  queueDepth: number,
  prevSprites: VehicleSprite[],
): VehicleSprite[] {
  if (count <= 0) return []

  const queueCount = Math.min(count, Math.floor(count * queueDepth))
  const movingCount = count - queueCount

  const sprites: VehicleSprite[] = []

  // Queued sprites — clustered near stop line (progress 0.02..0.2)
  for (let i = 0; i < queueCount; i++) {
    const prev = prevSprites.find((s) => s.id === `${dir}-q${i}`)
    sprites.push({
      id: `${dir}-q${i}`,
      dir,
      progress: prev?.progress ?? (0.03 + i * 0.04),
      lane: (i % 3) - 1, // -1, 0, 1
      queued: true,
    })
  }

  // Moving sprites — spread along the approach arm
  for (let i = 0; i < movingCount; i++) {
    const prev = prevSprites.find((s) => s.id === `${dir}-m${i}`)
    sprites.push({
      id: `${dir}-m${i}`,
      dir,
      progress: prev?.progress ?? (0.25 + (i / Math.max(movingCount, 1)) * 0.7),
      lane: (i % 3) - 1,
      queued: false,
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

  // Keep refs in sync
  useEffect(() => {
    phaseRef.current = currentPhase
    sensorsRef.current = sensors
    frozenRef.current = isFrozen
  }, [currentPhase, sensors, isFrozen])

  // Reset sprites on simulationRunId change
  useEffect(() => {
    if (lastRunIdRef.current !== undefined && lastRunIdRef.current !== simulationRunId) {
      spritesRef.current = []
    }
    lastRunIdRef.current = simulationRunId
  }, [simulationRunId])

  // Recompute sprite sets when sensor data updates from API
  useEffect(() => {
    const dirs: Direction[] = ['North', 'South', 'East', 'West']
    const newSprites: VehicleSprite[] = []

    for (const dir of dirs) {
      const sensor = sensorsRef.current.find((s) => s.dir === dir)
      const count = computeSpriteCount(sensor?.vehicleCount ?? null)
      const queueDepth = computeQueueDepth(sensor?.queueLength ?? null)
      const prev = spritesRef.current.filter((s) => s.dir === dir)
      const generated = generateSprites(dir, count, queueDepth, prev)
      newSprites.push(...generated)
    }

    spritesRef.current = newSprites
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sensors.map(s => `${s.dir}:${s.vehicleCount}:${s.queueLength}`).join('|')])

  // 60fps RAF loop
  useEffect(() => {
    let rafId: number | null = null

    const tick = () => {
      if (frozenRef.current || document.hidden) {
        rafId = requestAnimationFrame(tick)
        return
      }

      // Check prefers-reduced-motion
      const prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches
      if (prefersReduced) {
        rafId = requestAnimationFrame(tick)
        return
      }

      const currentSensors = sensorsRef.current
      const phase = phaseRef.current

      const updated = spritesRef.current.map((sprite) => {
        const sensor = currentSensors.find((s) => s.dir === sprite.dir)
        const animCat = speedToAnimCategory(sensor?.averageSpeed ?? null)
        const { isGreen, isYellow } = phaseForDirection(sprite.dir, phase)

        // Movement rules:
        // - RED light: Stop before the stop line (progress <= 0.05).
        // - YELLOW light: Slow down significantly (0.3x).
        // - GREEN light: Full movement across the intersection, then loop back.
        let speedMult = 1.0

        if (!isGreen && !isYellow) {
          // RED LIGHT: Vehicles cannot cross stop line
          if (sprite.progress <= 0.06) {
            // Already at stop line: full stop
            speedMult = 0
          } else {
            // Moving toward stop line on red: decelerate to stop
            speedMult = Math.max(0.1, sprite.progress * 0.8)
          }
        } else if (isYellow) {
          // YELLOW: cautious speed
          speedMult = 0.35
        }

        const baseDelta = animCategoryToSpeed(animCat)
        const delta = baseDelta * speedMult

        let nextProgress = sprite.progress - delta

        // If crossed stop line on green: continue through intersection and loop
        if (nextProgress < -0.15) {
          if (isGreen) {
            // Reset to far end of approach arm
            nextProgress = 0.95 + Math.random() * 0.05
          } else {
            // Red: clamp to stop line
            nextProgress = 0.02
          }
        }

        return { ...sprite, progress: nextProgress }
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
