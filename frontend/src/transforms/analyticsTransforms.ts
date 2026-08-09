// analyticsTransforms.ts — Presentation-only transforms for analytics DTOs
// Forbidden: re-computing congestion score, priority score, quality, Gold KPIs.
// Allowed: sort, group, format, build chart series, count by status bucket.

import type { CongestionWindowDto } from '@/types/analytics'
import type { TrafficComparisonDto } from '@/types/analytics'
import type { PriorityRankingDto } from '@/types/analytics'

/** Chart data point for trend/comparison charts */
export interface TrendDataPoint {
  windowStart: number
  windowEnd: number
  currentValue: number | null
  previousValue: number | null
  changeDirection: string | null
  windowId: string
}

/** Build chart series from TrafficComparisonDto[] for Recharts */
export function buildTrendSeries(rows: TrafficComparisonDto[]): TrendDataPoint[] {
  return rows
    .map((r) => ({
      windowStart: r.currentWindowStartSimSec,
      windowEnd: r.currentWindowEndSimSec,
      currentValue: r.currentValue,
      previousValue: r.previousValue,
      changeDirection: r.changeDirection,
      windowId: r.currentWindowId,
    }))
    .sort((a, b) => a.windowStart - b.windowStart)
}

/** Congestion bar chart data: intersectionId + numericValue + status */
export interface CongestionBarItem {
  intersectionId: string
  numericValue: number
  status: string | null
  color: string
}

export function buildCongestionBarData(rows: CongestionWindowDto[]): CongestionBarItem[] {
  // Deduplicate by intersectionId — take the most recent revisionSeq
  const map = new Map<string, CongestionWindowDto>()
  for (const r of rows) {
    const existing = map.get(r.intersectionId)
    if (!existing || r.revisionSeq > existing.revisionSeq) {
      map.set(r.intersectionId, r)
    }
  }
  return Array.from(map.values())
    .filter((r) => r.numericValue !== null)
    .map((r) => ({
      intersectionId: r.intersectionId,
      numericValue: r.numericValue ?? 0,
      status: r.status,
      color: congestionStatusColor(r.status),
    }))
    .sort((a, b) => b.numericValue - a.numericValue)
}

/** Priority bar chart data */
export interface PriorityBarItem {
  intersectionId: string
  priorityScore: number
  priorityRank: number | null
  scoreStatus: string | null
}

export function buildPriorityBarData(rows: PriorityRankingDto[]): PriorityBarItem[] {
  const map = new Map<string, PriorityRankingDto>()
  for (const r of rows) {
    const existing = map.get(r.intersectionId)
    if (!existing || r.revisionSeq > existing.revisionSeq) {
      map.set(r.intersectionId, r)
    }
  }
  return Array.from(map.values())
    .filter((r) => r.priorityScore !== null)
    .map((r) => ({
      intersectionId: r.intersectionId,
      priorityScore: r.priorityScore ?? 0,
      priorityRank: r.priorityRank,
      scoreStatus: r.scoreStatus,
    }))
    .sort((a, b) => b.priorityScore - a.priorityScore)
}

/** Congestion distribution: group rows by status bucket for donut chart */
export interface CongestionBucket {
  status: string
  count: number
  percentage: number
  color: string
}

export function buildCongestionDistribution(rows: CongestionWindowDto[]): CongestionBucket[] {
  const counts = new Map<string, number>()
  for (const r of rows) {
    const key = r.status ?? 'UNKNOWN'
    counts.set(key, (counts.get(key) ?? 0) + 1)
  }
  const total = rows.length
  return Array.from(counts.entries()).map(([status, count]) => ({
    status,
    count,
    percentage: total > 0 ? Math.round((count / total) * 100) : 0,
    color: congestionStatusColor(status),
  }))
}

export function congestionStatusColor(status: string | null | undefined): string {
  if (!status) return '#71889B'
  const s = status.toUpperCase()
  if (s.includes('LOW') || s.includes('GREEN')) return '#22C55E'
  if (s.includes('MEDIUM') || s.includes('YELLOW')) return '#FACC15'
  if (s.includes('HIGH') || s.includes('ORANGE')) return '#F59E0B'
  if (s.includes('SEVERE') || s.includes('RED') || s.includes('CRITICAL')) return '#EF4444'
  return '#71889B'
}

/** Analytics error type classification */
export type AnalyticsErrorType =
  | 'ANALYTICS_DISABLED'   // 404 — analytics feature flag off
  | 'ANALYTICS_NOT_READY'  // 503 ANALYTICS_NOT_READY
  | 'ANALYTICS_UNAVAILABLE'// 503 other
  | 'INVALID_QUERY'        // 400
  | 'UNAUTHORIZED'         // 401
  | 'FORBIDDEN'            // 403
  | 'EMPTY'                // 200 items=[]
  | 'UNKNOWN'

export function classifyAnalyticsError(error: unknown): AnalyticsErrorType {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const status = (error as any)?.response?.status as number | undefined
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const msg: string = (error as any)?.response?.data?.message ?? ''
  if (status === 404) return 'ANALYTICS_DISABLED'
  if (status === 401) return 'UNAUTHORIZED'
  if (status === 403) return 'FORBIDDEN'
  if (status === 400) return 'INVALID_QUERY'
  if (status === 503) {
    if (msg.includes('NOT_READY')) return 'ANALYTICS_NOT_READY'
    return 'ANALYTICS_UNAVAILABLE'
  }
  return 'UNKNOWN'
}
