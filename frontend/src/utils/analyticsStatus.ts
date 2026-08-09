import { classifyAnalyticsError, type AnalyticsErrorType } from '@/transforms/analyticsTransforms'

export type AnalyticsFeatureState = 'ENABLED' | 'DISABLED' | 'UNKNOWN'
export type AnalyticsServiceState = 'READY' | 'NOT_READY' | 'UNAVAILABLE' | 'DISABLED' | 'UNKNOWN'
export type AnalyticsDataState = 'AVAILABLE' | 'EMPTY' | 'PENDING_FILTERS' | 'UNKNOWN'

export function deriveAnalyticsFeatureState(error: unknown | undefined, filtersReady: boolean): AnalyticsFeatureState {
  if (!filtersReady) return 'UNKNOWN'
  if (!error) return 'ENABLED'
  if (classifyAnalyticsError(error) === 'ANALYTICS_DISABLED') return 'DISABLED'
  return 'ENABLED'
}

export function deriveAnalyticsServiceState(
  isLoading: boolean,
  error: unknown | undefined,
  filtersReady: boolean,
): AnalyticsServiceState {
  if (!filtersReady) return 'UNKNOWN'
  if (isLoading) return 'UNKNOWN'
  if (!error) return 'READY'
  const errType = classifyAnalyticsError(error)
  if (errType === 'ANALYTICS_DISABLED') return 'DISABLED'
  if (errType === 'ANALYTICS_NOT_READY') return 'NOT_READY'
  if (errType === 'ANALYTICS_UNAVAILABLE' || errType === 'UNAUTHORIZED' || errType === 'FORBIDDEN') return 'UNAVAILABLE'
  return 'UNKNOWN'
}

export function deriveAnalyticsDataState(
  filtersReady: boolean,
  isLoading: boolean,
  hasRows: boolean,
  error: unknown | undefined,
): AnalyticsDataState {
  if (!filtersReady) return 'PENDING_FILTERS'
  if (isLoading) return 'UNKNOWN'
  if (error && classifyAnalyticsError(error) !== 'ANALYTICS_NOT_READY') {
    return 'UNKNOWN'
  }
  return hasRows ? 'AVAILABLE' : 'EMPTY'
}

export function classifyQuick(error: unknown): AnalyticsErrorType {
  return classifyAnalyticsError(error)
}
