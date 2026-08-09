import { describe, expect, it } from 'vitest'

import { toGoldIntersectionId } from '@/utils/analyticsIntersectionId'

describe('toGoldIntersectionId', () => {
  it('maps a Realtime Orion URN to the Gold mart business key', () => {
    expect(toGoldIntersectionId('urn:ngsi-ld:Intersection:B')).toBe('B')
  })

  it('preserves an existing Gold mart key', () => {
    expect(toGoldIntersectionId('B')).toBe('B')
  })

  it('does not collapse a malformed empty URN', () => {
    expect(toGoldIntersectionId('urn:ngsi-ld:Intersection:')).toBe('urn:ngsi-ld:Intersection:')
  })
})
