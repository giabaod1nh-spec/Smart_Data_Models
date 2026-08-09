import { describe, expect, it } from 'vitest'
import { deriveIntersectionListStatus } from '@/utils/intersectionListStatus'

describe('deriveIntersectionListStatus', () => {
  it('returns loading', () => {
    expect(deriveIntersectionListStatus(true, false, null, undefined)).toBe('loading')
  })

  it('returns empty on success with no items', () => {
    expect(deriveIntersectionListStatus(false, false, null, [])).toBe('empty')
  })

  it('returns success with items', () => {
    expect(deriveIntersectionListStatus(false, false, null, [{ id: 'A' } as never])).toBe('success')
  })

  it('returns unauthorized on 401 — not empty', () => {
    expect(deriveIntersectionListStatus(false, true, { response: { status: 401 } }, undefined)).toBe('unauthorized')
  })

  it('returns unavailable on 503', () => {
    expect(deriveIntersectionListStatus(false, true, { response: { status: 503 } }, undefined)).toBe('unavailable')
  })

  it('returns network_error when no response', () => {
    expect(deriveIntersectionListStatus(false, true, { request: {} }, undefined)).toBe('network_error')
  })
})
