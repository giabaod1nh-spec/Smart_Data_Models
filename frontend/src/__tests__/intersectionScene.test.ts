// intersectionScene.test.ts — Tests for IntersectionScene digital twin logic.
// Tests animation helpers, vehicle sprite logic, phase-awareness, and sensor mapping.

import { describe, it, expect } from 'vitest'
import {
  speedToAnimCategory,
  speedKmhToProgressDelta,
  computeSpriteCount,
  computeQueueDepth,
  phaseForDirection,
  signalColorForDirection,
  advanceSpriteProgress,
  STOP_LINE_PROGRESS,
  MAX_SPRITES_PER_DIR,
} from '@/components/canvas/useVehicleAnimation'
import { measuredSceneSize } from '@/components/canvas/IntersectionScene'

describe('measuredSceneSize', () => {
  it('ignores transient zero-width and invalid layout measurements', () => {
    expect(measuredSceneSize(0)).toBeNull()
    expect(measuredSceneSize(Number.NaN)).toBeNull()
  })

  it('always returns a non-zero Konva canvas size', () => {
    expect(measuredSceneSize(620)).toEqual({ w: 620, h: 496 })
    expect(measuredSceneSize(0.6)).toBeNull()
  })
})

describe('speedToAnimCategory', () => {
  it('returns STOPPED for speed 0', () => {
    expect(speedToAnimCategory(0)).toBe('STOPPED')
  })

  it('returns CRAWL for low speed (1-5 km/h)', () => {
    expect(speedToAnimCategory(3)).toBe('CRAWL')
    expect(speedToAnimCategory(5)).toBe('CRAWL')
  })

  it('returns SLOW for medium-low speed (6-20 km/h)', () => {
    expect(speedToAnimCategory(15)).toBe('SLOW')
    expect(speedToAnimCategory(20)).toBe('SLOW')
  })

  it('returns NORMAL for typical speed (21-40 km/h)', () => {
    expect(speedToAnimCategory(30)).toBe('NORMAL')
    expect(speedToAnimCategory(40)).toBe('NORMAL')
  })

  it('returns FAST for high speed (>40 km/h)', () => {
    expect(speedToAnimCategory(50)).toBe('FAST')
    expect(speedToAnimCategory(100)).toBe('FAST')
  })

  it('returns SLOW (default) for null/undefined speed', () => {
    expect(speedToAnimCategory(null)).toBe('SLOW')
    expect(speedToAnimCategory(undefined)).toBe('SLOW')
  })
})

describe('speedKmhToProgressDelta', () => {
  it('scales with km/h so faster averages move faster', () => {
    const slow = speedKmhToProgressDelta(10)
    const mid = speedKmhToProgressDelta(35)
    const fast = speedKmhToProgressDelta(50)
    expect(mid).toBeGreaterThan(slow)
    expect(fast).toBeGreaterThan(mid)
    expect(speedKmhToProgressDelta(50)).toBeCloseTo(0.012, 5)
  })

  it('returns 0 when average speed is 0', () => {
    expect(speedKmhToProgressDelta(0)).toBe(0)
  })
})

describe('computeSpriteCount', () => {
  it('returns 0 for null vehicleCount', () => {
    expect(computeSpriteCount(null)).toBe(0)
  })

  it('returns 0 for vehicleCount 0', () => {
    expect(computeSpriteCount(0)).toBe(0)
  })

  it('caps at MAX_SPRITES_PER_DIR for very large vehicleCount', () => {
    expect(computeSpriteCount(99999)).toBe(MAX_SPRITES_PER_DIR)
    expect(computeSpriteCount(1000)).toBe(MAX_SPRITES_PER_DIR)
  })

  it('returns correct scaled count for small vehicleCount', () => {
    const count = computeSpriteCount(8)
    expect(count).toBe(1)
    expect(count).toBeGreaterThan(0)
    expect(count).toBeLessThanOrEqual(MAX_SPRITES_PER_DIR)
  })

  it('sprite count is monotonically increasing with vehicleCount (before cap)', () => {
    const c1 = computeSpriteCount(8)
    const c2 = computeSpriteCount(16)
    const c3 = computeSpriteCount(24)
    expect(c2).toBeGreaterThanOrEqual(c1)
    expect(c3).toBeGreaterThanOrEqual(c2)
  })
})

describe('computeQueueDepth', () => {
  it('returns 0 for null queueLength', () => {
    expect(computeQueueDepth(null)).toBe(0)
  })

  it('returns 0 for zero queueLength', () => {
    expect(computeQueueDepth(0)).toBe(0)
  })

  it('returns normalized value 0..1 for typical values', () => {
    const d = computeQueueDepth(50)
    expect(d).toBeGreaterThan(0)
    expect(d).toBeLessThanOrEqual(1)
  })

  it('caps at 1 for very large queueLength', () => {
    expect(computeQueueDepth(9999)).toBe(1)
    expect(computeQueueDepth(100)).toBe(1)
  })
})

describe('phaseForDirection', () => {
  it('NS_GREEN phase: North/South are green, East/West are not', () => {
    expect(phaseForDirection('North', 'NS_GREEN')).toEqual({ isGreen: true, isYellow: false })
    expect(phaseForDirection('South', 'NS_GREEN')).toEqual({ isGreen: true, isYellow: false })
    expect(phaseForDirection('East', 'NS_GREEN')).toEqual({ isGreen: false, isYellow: false })
    expect(phaseForDirection('West', 'NS_GREEN')).toEqual({ isGreen: false, isYellow: false })
  })

  it('EW_GREEN phase: East/West are green, North/South are not', () => {
    expect(phaseForDirection('East', 'EW_GREEN')).toEqual({ isGreen: true, isYellow: false })
    expect(phaseForDirection('West', 'EW_GREEN')).toEqual({ isGreen: true, isYellow: false })
    expect(phaseForDirection('North', 'EW_GREEN')).toEqual({ isGreen: false, isYellow: false })
    expect(phaseForDirection('South', 'EW_GREEN')).toEqual({ isGreen: false, isYellow: false })
  })

  it('NS_YELLOW phase: North/South are yellow', () => {
    expect(phaseForDirection('North', 'NS_YELLOW')).toEqual({ isGreen: false, isYellow: true })
    expect(phaseForDirection('South', 'NS_YELLOW')).toEqual({ isGreen: false, isYellow: true })
    expect(phaseForDirection('East', 'NS_YELLOW')).toEqual({ isGreen: false, isYellow: false })
  })

  it('EW_YELLOW phase: East/West are yellow', () => {
    expect(phaseForDirection('East', 'EW_YELLOW')).toEqual({ isGreen: false, isYellow: true })
    expect(phaseForDirection('West', 'EW_YELLOW')).toEqual({ isGreen: false, isYellow: true })
    expect(phaseForDirection('North', 'EW_YELLOW')).toEqual({ isGreen: false, isYellow: false })
  })

  it('null/undefined phase: all directions red', () => {
    expect(phaseForDirection('North', null)).toEqual({ isGreen: false, isYellow: false })
    expect(phaseForDirection('East', undefined)).toEqual({ isGreen: false, isYellow: false })
  })

  it('phase-aware: vehicles in red directions should appear queued', () => {
    const { isGreen, isYellow } = phaseForDirection('North', 'EW_GREEN')
    expect(isGreen).toBe(false)
    expect(isYellow).toBe(false)
  })
})

describe('signalColorForDirection', () => {
  it('matches phase badge: EW_GREEN lights East/West green and North/South red', () => {
    expect(signalColorForDirection('East', 'EW_GREEN')).toBe('GREEN')
    expect(signalColorForDirection('West', 'EW_GREEN')).toBe('GREEN')
    expect(signalColorForDirection('North', 'EW_GREEN')).toBe('RED')
    expect(signalColorForDirection('South', 'EW_GREEN')).toBe('RED')
  })

  it('matches phase badge: NS_GREEN lights North/South green and East/West red', () => {
    expect(signalColorForDirection('North', 'NS_GREEN')).toBe('GREEN')
    expect(signalColorForDirection('East', 'NS_GREEN')).toBe('RED')
  })
})

describe('advanceSpriteProgress — stop-line & speed logic', () => {
  it('on red, never advances past the stop line', () => {
    const approaching = advanceSpriteProgress(0.5, 'East', 'NS_GREEN', 48)
    expect(approaching.progress).toBeGreaterThanOrEqual(STOP_LINE_PROGRESS)

    // Already at/past stop line → hard stop exactly at stop line
    const atLine = advanceSpriteProgress(0.02, 'East', 'NS_GREEN', 48)
    expect(atLine.progress).toBe(STOP_LINE_PROGRESS)
    expect(atLine.queued).toBe(true)
    expect(atLine.animCategory).toBe('STOPPED')

    // Inside intersection (negative) on red → snap behind stop line
    const inside = advanceSpriteProgress(-0.1, 'East', 'NS_GREEN', 40)
    expect(inside.progress).toBe(STOP_LINE_PROGRESS)
    expect(inside.queued).toBe(true)
  })

  it('on green, may cross below stop line toward clearance', () => {
    const stepped = advanceSpriteProgress(0.05, 'East', 'EW_GREEN', 48)
    expect(stepped.progress).toBeLessThan(0.05)
    expect(stepped.queued).toBe(false)
  })

  it('higher averageSpeed advances farther per frame on green', () => {
    const slow = advanceSpriteProgress(0.8, 'North', 'NS_GREEN', 10)
    const fast = advanceSpriteProgress(0.8, 'North', 'NS_GREEN', 50)
    expect(0.8 - fast.progress).toBeGreaterThan(0.8 - slow.progress)
  })
})
