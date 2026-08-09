import { describe, expect, it, beforeEach } from 'vitest'
import {
  resolveRealtimeIntersectionRoute,
  getStoredSelectedIntersectionId,
  setStoredSelectedIntersectionId,
  appendAnalyticsQuery,
} from '@/utils/navigation'

describe('resolveRealtimeIntersectionRoute', () => {
  beforeEach(() => {
    sessionStorage.clear()
  })

  it('navigates to selected intersection when available', () => {
    const result = resolveRealtimeIntersectionRoute(
      'success',
      [{ id: 'A' }, { id: 'B' }] as never[],
      'B',
    )
    expect(result).toEqual({
      kind: 'navigate',
      intersectionId: 'B',
      path: '/intersections/B',
      reason: 'selected',
    })
  })

  it('falls back to first intersection', () => {
    const result = resolveRealtimeIntersectionRoute(
      'success',
      [{ id: 'A' }, { id: 'B' }] as never[],
      null,
    )
    expect(result.kind).toBe('navigate')
    if (result.kind === 'navigate') {
      expect(result.intersectionId).toBe('A')
      expect(result.reason).toBe('first')
    }
  })

  it('uses stored selection', () => {
    setStoredSelectedIntersectionId('B')
    const result = resolveRealtimeIntersectionRoute(
      'success',
      [{ id: 'A' }, { id: 'B' }] as never[],
      null,
    )
    expect(result.kind).toBe('navigate')
    if (result.kind === 'navigate') {
      expect(result.intersectionId).toBe('B')
    }
  })

  it('returns empty when list is empty', () => {
    expect(resolveRealtimeIntersectionRoute('empty', [], null)).toEqual({ kind: 'empty' })
  })

  it('returns error on API failure — not empty', () => {
    const result = resolveRealtimeIntersectionRoute('unavailable', undefined, null, 'Orion down')
    expect(result.kind).toBe('error')
  })
})

describe('appendAnalyticsQuery', () => {
  it('preserves query params', () => {
    const params = new URLSearchParams('simulationRunId=run-1&scenarioId=normal')
    expect(appendAnalyticsQuery('/intersections/A', params)).toBe(
      '/intersections/A?simulationRunId=run-1&scenarioId=normal',
    )
  })
})

describe('session storage selected intersection', () => {
  beforeEach(() => sessionStorage.clear())

  it('stores and retrieves id', () => {
    setStoredSelectedIntersectionId('A')
    expect(getStoredSelectedIntersectionId()).toBe('A')
  })
})
