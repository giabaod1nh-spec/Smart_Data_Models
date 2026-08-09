// useAnalytics.ts — TanStack Query hooks for all analytics endpoints
// All hooks require simulationRunId + scenarioId + windowSizeSec to be set.
// If params not set, queries are disabled.

import { useQuery } from '@tanstack/react-query'
import type { AnalyticsQueryParams, MetricCode } from '@/types/common'
import {
  getCongestion,
  getPriority,
  getIntersectionWindows,
  getDirectionWindows,
  getIntersectionTrends,
  getSignalOperationWindows,
} from '@/api/analyticsApi'
import type {
  CongestionWindowDto,
  PriorityRankingDto,
  IntersectionWindowDto,
  DirectionWindowDto,
  TrafficComparisonDto,
  SignalOperationWindowDto,
} from '@/types/analytics'
import type { AnalyticsPage } from '@/types/common'

const REFRESH_MS = Number(import.meta.env.VITE_ANALYTICS_REFRESH_MS ?? 60_000)

function isQueryReady(q: AnalyticsQueryParams | null): q is AnalyticsQueryParams {
  return Boolean(q && q.simulationRunId && q.scenarioId && q.windowSizeSec)
}

// ── Congestion ────────────────────────────────────────────
export function useAnalyticsCongestion(
  query: AnalyticsQueryParams | null,
  intersectionId?: string,
) {
  return useQuery<AnalyticsPage<CongestionWindowDto>>({
    queryKey: ['analytics', 'congestion', query, intersectionId],
    queryFn: async () => {
      const res = await getCongestion(query!, intersectionId)
      return res.data
    },
    enabled: isQueryReady(query),
    staleTime: REFRESH_MS,
    refetchOnWindowFocus: false,
    retry: false,
  })
}

// ── Priority ──────────────────────────────────────────────
export function useAnalyticsPriority(
  query: AnalyticsQueryParams | null,
  intersectionId?: string,
) {
  return useQuery<AnalyticsPage<PriorityRankingDto>>({
    queryKey: ['analytics', 'priority', query, intersectionId],
    queryFn: async () => {
      const res = await getPriority(query!, intersectionId)
      return res.data
    },
    enabled: isQueryReady(query),
    staleTime: REFRESH_MS,
    refetchOnWindowFocus: false,
    retry: false,
  })
}

// ── Intersection Windows ──────────────────────────────────
export function useIntersectionWindows(
  intersectionId: string | null,
  query: AnalyticsQueryParams | null,
) {
  return useQuery<AnalyticsPage<IntersectionWindowDto>>({
    queryKey: ['analytics', 'intersection-windows', intersectionId, query],
    queryFn: async () => {
      const res = await getIntersectionWindows(intersectionId!, query!)
      return res.data
    },
    enabled: isQueryReady(query) && Boolean(intersectionId),
    staleTime: REFRESH_MS,
    refetchOnWindowFocus: false,
    retry: false,
  })
}

// ── Direction Windows ─────────────────────────────────────
export function useDirectionWindows(
  intersectionId: string | null,
  query: AnalyticsQueryParams | null,
  direction?: string,
) {
  return useQuery<AnalyticsPage<DirectionWindowDto>>({
    queryKey: ['analytics', 'direction-windows', intersectionId, query, direction],
    queryFn: async () => {
      const res = await getDirectionWindows(intersectionId!, query!, direction)
      return res.data
    },
    enabled: isQueryReady(query) && Boolean(intersectionId),
    staleTime: REFRESH_MS,
    refetchOnWindowFocus: false,
    retry: false,
  })
}

// ── Trends ────────────────────────────────────────────────
export function useAnalyticsTrends(
  intersectionId: string | null,
  query: AnalyticsQueryParams | null,
  metricCode: MetricCode | null,
) {
  return useQuery<AnalyticsPage<TrafficComparisonDto>>({
    queryKey: ['analytics', 'trends', intersectionId, query, metricCode],
    queryFn: async () => {
      const res = await getIntersectionTrends(intersectionId!, query!, metricCode!)
      return res.data
    },
    enabled: isQueryReady(query) && Boolean(intersectionId) && Boolean(metricCode),
    staleTime: REFRESH_MS,
    refetchOnWindowFocus: false,
    retry: false,
  })
}

// ── Signal Operation Windows ──────────────────────────────
export function useSignalOperationWindows(
  query: AnalyticsQueryParams | null,
  intersectionId?: string,
) {
  return useQuery<AnalyticsPage<SignalOperationWindowDto>>({
    queryKey: ['analytics', 'signal-ops', query, intersectionId],
    queryFn: async () => {
      const res = await getSignalOperationWindows(query!, intersectionId)
      return res.data
    },
    enabled: isQueryReady(query),
    staleTime: REFRESH_MS,
    refetchOnWindowFocus: false,
    retry: false,
  })
}
