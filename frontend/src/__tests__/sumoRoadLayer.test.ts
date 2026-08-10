import { describe, expect, it } from 'vitest'
import { lanePolygon, trimShapeAtJunction } from '@/components/canvas/sumoRoadLayer'

describe('sumoRoadLayer geometry', () => {
  it('lanePolygon produces 4 corners for straight 2-point lane', () => {
    const poly = lanePolygon(
      [
        [0, 142],
        [136.4, 142],
      ],
      3.2,
    )
    expect(poly).toHaveLength(4)
    const ys = poly.map((p) => p[1])
    expect(Math.min(...ys)).toBeCloseTo(140.4, 1)
    expect(Math.max(...ys)).toBeCloseTo(143.6, 1)
  })

  it('trimShapeAtJunction shortens inbound lane at junction box', () => {
    const box = { minX: 136.4, minY: 136.4, maxX: 163.6, maxY: 163.6 }
    const trimmed = trimShapeAtJunction(
      [
        [0, 142],
        [150, 142],
      ],
      box,
      150,
      150,
    )
    expect(trimmed[0]).toEqual([0, 142])
    expect(trimmed[1][0]).toBeCloseTo(136.4, 1)
    expect(trimmed[1][1]).toBeCloseTo(142, 1)
  })
})
