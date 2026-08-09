import { describe, expect, it } from 'vitest'
import {
  deriveAnalyticsDataState,
  deriveAnalyticsFeatureState,
  deriveAnalyticsServiceState,
} from '@/utils/analyticsStatus'

describe('Analytics status derivation', () => {
  it('pending filters when run/scenario missing', () => {
    expect(deriveAnalyticsDataState(false, false, false, undefined)).toBe('PENDING_FILTERS')
  })

  it('service disabled on 404', () => {
    expect(deriveAnalyticsServiceState(false, { response: { status: 404 } }, true)).toBe('DISABLED')
    expect(deriveAnalyticsFeatureState({ response: { status: 404 } }, true)).toBe('DISABLED')
  })

  it('data empty when service ready but no rows', () => {
    expect(deriveAnalyticsDataState(true, false, false, undefined)).toBe('EMPTY')
  })

  it('data available when rows exist', () => {
    expect(deriveAnalyticsDataState(true, false, true, undefined)).toBe('AVAILABLE')
  })

  it('service ready when no error', () => {
    expect(deriveAnalyticsServiceState(false, undefined, true)).toBe('READY')
  })
})
