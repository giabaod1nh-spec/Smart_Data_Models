// analyticsApi.ts — Analytics API calls via Spring Server
// All routes require ADMIN session + analytics profile enabled.
// Routes verified from FRONTEND_ANALYTICS_SOURCE_RECHECK.md and source.
//
// IMPORTANT: Analytics is disabled by default (app.analytics.enabled=false).
// With analytics disabled, routes return 404. With analytics enabled but
// network mart not populated, /api/analytics/network/windows returns 503.

import type { ApiResponse, AnalyticsPage, AnalyticsQueryParams, MetricCode } from '@/types/common'
import type {
  IntersectionWindowDto,
  DirectionWindowDto,
  TrafficComparisonDto,
  CongestionWindowDto,
  PriorityRankingDto,
  SignalOperationWindowDto,
} from '@/types/analytics'
import { toGoldIntersectionId } from '@/utils/analyticsIntersectionId'
import { httpClient } from './httpClient'

/** Build analytics query params, filtering out undefined values */
function buildParams(
  base: AnalyticsQueryParams,
  extra?: Record<string, string | number | undefined>,
): Record<string, string | number> {
  const params: Record<string, string | number> = {
    simulationRunId: base.simulationRunId,
    scenarioId: base.scenarioId,
    windowSizeSec: base.windowSizeSec,
  }
  if (base.fromSimulationSec !== undefined) params['fromSimulationSec'] = base.fromSimulationSec
  if (base.toSimulationSec !== undefined) params['toSimulationSec'] = base.toSimulationSec
  if (base.page !== undefined) params['page'] = base.page
  if (base.pageSize !== undefined) params['pageSize'] = base.pageSize
  if (base.sort) params['sort'] = base.sort
  if (extra) {
    for (const [k, v] of Object.entries(extra)) {
      if (v !== undefined) params[k] = v
    }
  }
  return params
}

/**
 * GET /api/analytics/intersections/{id}/windows
 * Required: simulationRunId, scenarioId, windowSizeSec.
 */
export async function getIntersectionWindows(
  intersectionId: string,
  query: AnalyticsQueryParams,
): Promise<ApiResponse<AnalyticsPage<IntersectionWindowDto>>> {
  const res = await httpClient.get<ApiResponse<AnalyticsPage<IntersectionWindowDto>>>(
    `/api/analytics/intersections/${encodeURIComponent(toGoldIntersectionId(intersectionId))}/windows`,
    { params: buildParams(query) },
  )
  return res.data
}

/**
 * GET /api/analytics/intersections/{id}/directions/windows
 * Optional extra param: direction (North|South|East|West).
 */
export async function getDirectionWindows(
  intersectionId: string,
  query: AnalyticsQueryParams,
  direction?: string,
): Promise<ApiResponse<AnalyticsPage<DirectionWindowDto>>> {
  const res = await httpClient.get<ApiResponse<AnalyticsPage<DirectionWindowDto>>>(
    `/api/analytics/intersections/${encodeURIComponent(toGoldIntersectionId(intersectionId))}/directions/windows`,
    { params: buildParams(query, { direction }) },
  )
  return res.data
}

/**
 * GET /api/analytics/intersections/{id}/comparisons
 * Required extra: metricCode.
 */
export async function getIntersectionComparisons(
  intersectionId: string,
  query: AnalyticsQueryParams,
  metricCode: MetricCode,
): Promise<ApiResponse<AnalyticsPage<TrafficComparisonDto>>> {
  const res = await httpClient.get<ApiResponse<AnalyticsPage<TrafficComparisonDto>>>(
    `/api/analytics/intersections/${encodeURIComponent(toGoldIntersectionId(intersectionId))}/comparisons`,
    { params: buildParams(query, { metricCode }) },
  )
  return res.data
}

/**
 * GET /api/analytics/intersections/{id}/trends
 * Required extra: metricCode. Uses same TrafficComparisonDto as comparisons.
 */
export async function getIntersectionTrends(
  intersectionId: string,
  query: AnalyticsQueryParams,
  metricCode: MetricCode,
): Promise<ApiResponse<AnalyticsPage<TrafficComparisonDto>>> {
  const res = await httpClient.get<ApiResponse<AnalyticsPage<TrafficComparisonDto>>>(
    `/api/analytics/intersections/${encodeURIComponent(toGoldIntersectionId(intersectionId))}/trends`,
    { params: buildParams(query, { metricCode }) },
  )
  return res.data
}

/**
 * GET /api/analytics/congestion
 * Optional extra: intersectionId filter.
 */
export async function getCongestion(
  query: AnalyticsQueryParams,
  intersectionId?: string,
): Promise<ApiResponse<AnalyticsPage<CongestionWindowDto>>> {
  const res = await httpClient.get<ApiResponse<AnalyticsPage<CongestionWindowDto>>>(
    '/api/analytics/congestion',
    { params: buildParams(query, { intersectionId: intersectionId ? toGoldIntersectionId(intersectionId) : undefined }) },
  )
  return res.data
}

/**
 * GET /api/analytics/priority
 * Optional extra: intersectionId filter.
 */
export async function getPriority(
  query: AnalyticsQueryParams,
  intersectionId?: string,
): Promise<ApiResponse<AnalyticsPage<PriorityRankingDto>>> {
  const res = await httpClient.get<ApiResponse<AnalyticsPage<PriorityRankingDto>>>(
    '/api/analytics/priority',
    { params: buildParams(query, { intersectionId: intersectionId ? toGoldIntersectionId(intersectionId) : undefined }) },
  )
  return res.data
}

/**
 * GET /api/analytics/signals/operation-windows
 * Optional extra: intersectionId, direction filters.
 */
export async function getSignalOperationWindows(
  query: AnalyticsQueryParams,
  intersectionId?: string,
  direction?: string,
): Promise<ApiResponse<AnalyticsPage<SignalOperationWindowDto>>> {
  const res = await httpClient.get<ApiResponse<AnalyticsPage<SignalOperationWindowDto>>>(
    '/api/analytics/signals/operation-windows',
    { params: buildParams(query, {
      intersectionId: intersectionId ? toGoldIntersectionId(intersectionId) : undefined,
      direction,
    }) },
  )
  return res.data
}

/**
 * GET /api/analytics/network/windows
 * Expected to return 503 ANALYTICS_NOT_READY while Gold network mart is WHERE 0 scaffold.
 * This is NOT an error state — it is the expected behavior.
 */
export async function getNetworkWindows(
  query: AnalyticsQueryParams,
): Promise<ApiResponse<unknown>> {
  const res = await httpClient.get<ApiResponse<unknown>>(
    '/api/analytics/network/windows',
    { params: buildParams(query) },
  )
  return res.data
}
