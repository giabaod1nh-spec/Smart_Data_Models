import { describe, expect, it } from 'vitest'
import { expandEdgeLanes, enrichNetworkGeometry } from '@/utils/liveNetworkGeometry'

describe('liveNetworkGeometry', () => {
  it('expandEdgeLanes creates 3 lanes from legacy edge', () => {
    const lanes = expandEdgeLanes({
      id: 'W1J1',
      from: 'W1',
      to: 'J1',
      numLanes: 3,
      shape: [
        [0, 142],
        [136.4, 142],
      ],
    })
    expect(lanes).toHaveLength(3)
    expect(lanes[0].shape[0][1]).toBeCloseTo(142, 1)
    expect(lanes[1].shape[0][1]).toBeCloseTo(145.2, 1)
    expect(lanes[2].shape[0][1]).toBeCloseTo(148.4, 1)
  })

  it('enrichNetworkGeometry fills lanes from bundled net', () => {
    const enriched = enrichNetworkGeometry({
      bounds: { minX: 0, minY: 0, maxX: 800, maxY: 800, projParameter: '!', geoReferenced: false },
      junctions: [],
      edges: [{ id: 'W1J1', from: 'W1', to: 'J1', numLanes: 3, shape: [[0, 142], [136, 142]] }],
    })
    const w = enriched.edges.find((e) => e.id === 'W1J1')
    expect(w?.lanes).toHaveLength(3)
  })
})
