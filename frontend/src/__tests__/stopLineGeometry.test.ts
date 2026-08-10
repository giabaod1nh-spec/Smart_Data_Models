import { describe, expect, it } from 'vitest'
import {
  clampVehicleBeforeStopLine,
  computeStopLines,
  stopLineSegment,
  type CorridorPair,
} from '@/utils/stopLineGeometry'
import { expandEdgeLanes } from '@/utils/liveNetworkGeometry'

describe('stopLineGeometry', () => {
  const w1j1 = {
    id: 'W1J1',
    from: 'W1',
    to: 'J1',
    numLanes: 3,
    shape: [
      [0, 142],
      [136.4, 142],
    ] as [number, number][],
    lanes: expandEdgeLanes({
      id: 'W1J1',
      from: 'W1',
      to: 'J1',
      numLanes: 3,
      shape: [
        [0, 142],
        [136.4, 142],
      ],
    }),
  }

  it('stop line sits at lane end x=136.4 for west inbound', () => {
    const seg = stopLineSegment(w1j1, 150, 150)
    expect(seg).not.toBeNull()
    expect(seg![0][0]).toBeCloseTo(136.4, 1)
    expect(seg![1][0]).toBeCloseTo(136.4, 1)
  })

  it('clamp pulls vehicle front back before stop line on red', () => {
    const pairs: CorridorPair[] = [
      { inbound: w1j1, outbound: w1j1, approach: 'West' },
    ]
    const stops = computeStopLines(pairs, 150, 150)
    const stop = stops[0]
    // Car center past stop line (inside junction)
    const clamped = clampVehicleBeforeStopLine(140, 145, 4.5, stop, true)
    const frontX = clamped.x + stop.travelUnit[0] * 2.25
    expect(frontX).toBeLessThanOrEqual(stop.stopPoint[0] + 0.1)
  })
})
