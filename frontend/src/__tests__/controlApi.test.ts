import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getIntersectionState,
  setGreenDuration,
  setPhase,
  setScenario,
} from '@/api/controlApi'
import { httpClient } from '@/api/httpClient'

vi.mock('@/api/httpClient', () => ({
  httpClient: {
    get: vi.fn(),
    post: vi.fn(),
  },
}))

describe('controlApi intersection id normalization', () => {
  beforeEach(() => {
    vi.mocked(httpClient.get).mockResolvedValue({ data: {} })
    vi.mocked(httpClient.post).mockResolvedValue({ data: { queued: true } })
  })

  it('maps Orion URN to SUMO id for setScenario', async () => {
    await setScenario('morning_peak', 'urn:ngsi-ld:Intersection:A')

    expect(httpClient.post).toHaveBeenCalledWith('/api/control/scenario', {
      scenario: 'morning_peak',
      target_intersection: 'A',
    })
  })

  it('maps Orion URN to SUMO id for setPhase', async () => {
    await setPhase('urn:ngsi-ld:Intersection:B', 'NS_GREEN')

    expect(httpClient.post).toHaveBeenCalledWith('/api/control/phase', {
      intersection_id: 'B',
      phase: 'NS_GREEN',
    })
  })

  it('maps Orion URN to SUMO id for setGreenDuration', async () => {
    await setGreenDuration('urn:ngsi-ld:Intersection:C', 45)

    expect(httpClient.post).toHaveBeenCalledWith('/api/control/green-duration', {
      intersection_id: 'C',
      seconds: 45,
    })
  })

  it('maps Orion URN in control read paths', async () => {
    await getIntersectionState('urn:ngsi-ld:Intersection:D')

    expect(httpClient.get).toHaveBeenCalledWith('/api/control/intersections/D/state')
  })
})
