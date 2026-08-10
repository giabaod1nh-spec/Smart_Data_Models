/** World-space drawing sizes in meters — shared scale for roads, vehicles, signals. */

export const WORLD = {
  laneWidth: 3.2,
  /** Lane divider dash (~12 cm paint width in SUMO). */
  markDashWidth: 0.12,
  /** Center solid divider between directions. */
  markSolidWidth: 0.15,
  /** Stop bar at junction. */
  markStopWidth: 0.38,
  markDashLength: 2.8,
  markDashGap: 2.8,
  /** Top-down signal head footprint. */
  signalWidth: 0.55,
  signalHeight: 1.7,
  signalLampRadius: 0.11,
  signalLabelFont: 0.38,
  junctionLabelRadius: 1.0,
} as const

/** Meters → canvas pixels; optional floor for sub-pixel hairlines. */
export function mpx(meters: number, scale: number, minPx = 0.8): number {
  return Math.max(minPx, meters * scale)
}

/** Intersection detail crop radius (m) — tighter = larger on-screen scale. */
export const INTERSECTION_APPROACH_M = 165
