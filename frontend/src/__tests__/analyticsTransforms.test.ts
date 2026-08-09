import { describe, it, expect } from 'vitest'
import {
  buildCongestionBarData,
  buildPriorityBarData,
  buildCongestionDistribution,
  classifyAnalyticsError,
} from '@/transforms/analyticsTransforms'
import type { CongestionWindowDto, PriorityRankingDto } from '@/types/analytics'

describe('analyticsTransforms', () => {
  const mockCongestionRows: CongestionWindowDto[] = [
    {
      simulationRunId: 'run-1',
      scenarioId: 'normal',
      namespace: 'traffic',
      intersectionId: 'int-1',
      direction: null,
      windowId: 'w-1',
      windowSizeSec: 60,
      windowStartSimSec: 0,
      windowEndSimSec: 60,
      metricCode: 'CONGESTION_INDEX',
      metricVersion: '1.0',
      numericValue: 85,
      unitCode: 'SCORE',
      status: 'HIGH',
      explanationJson: null,
      qualityStatus: 'VALID',
      qualityFlags: null,
      analyticalFreshnessStatus: 'CURRENT',
      revisionSeq: 1,
      computedAt: '2026-08-06T12:00:00Z',
    },
    {
      simulationRunId: 'run-1',
      scenarioId: 'normal',
      namespace: 'traffic',
      intersectionId: 'int-2',
      direction: null,
      windowId: 'w-1',
      windowSizeSec: 60,
      windowStartSimSec: 0,
      windowEndSimSec: 60,
      metricCode: 'CONGESTION_INDEX',
      metricVersion: '1.0',
      numericValue: 35,
      unitCode: 'SCORE',
      status: 'LOW',
      explanationJson: null,
      qualityStatus: 'VALID',
      qualityFlags: null,
      analyticalFreshnessStatus: 'CURRENT',
      revisionSeq: 1,
      computedAt: '2026-08-06T12:00:00Z',
    },
  ]

  const mockPriorityRows: PriorityRankingDto[] = [
    {
      simulationRunId: 'run-1',
      scenarioId: 'normal',
      namespace: 'traffic',
      intersectionId: 'int-1',
      direction: null,
      windowId: 'w-1',
      windowSizeSec: 60,
      windowStartSimSec: 0,
      windowEndSimSec: 60,
      priorityScore: 92.5,
      priorityRank: 1,
      scoreStatus: 'VALID',
      rankStatus: 'VALID',
      explanationJson: null,
      qualityStatus: 'VALID',
      qualityFlags: null,
      analyticalFreshnessStatus: 'CURRENT',
      revisionSeq: 1,
      computedAt: '2026-08-06T12:00:00Z',
    },
  ]

  it('builds sorted congestion bar data', () => {
    const bars = buildCongestionBarData(mockCongestionRows)
    expect(bars).toHaveLength(2)
    expect(bars[0].intersectionId).toBe('int-1')
    expect(bars[0].numericValue).toBe(85)
    expect(bars[1].intersectionId).toBe('int-2')
  })

  it('builds sorted priority bar data', () => {
    const bars = buildPriorityBarData(mockPriorityRows)
    expect(bars).toHaveLength(1)
    expect(bars[0].intersectionId).toBe('int-1')
    expect(bars[0].priorityScore).toBe(92.5)
  })

  it('builds congestion distribution buckets with percentages', () => {
    const dist = buildCongestionDistribution(mockCongestionRows)
    expect(dist).toHaveLength(2)
    const highBucket = dist.find((b) => b.status === 'HIGH')
    expect(highBucket?.percentage).toBe(50)
  })

  it('classifies analytics errors correctly', () => {
    expect(classifyAnalyticsError({ response: { status: 404 } })).toBe('ANALYTICS_DISABLED')
    expect(classifyAnalyticsError({ response: { status: 503, data: { message: 'ANALYTICS_NOT_READY' } } })).toBe('ANALYTICS_NOT_READY')
    expect(classifyAnalyticsError({ response: { status: 401 } })).toBe('UNAUTHORIZED')
    expect(classifyAnalyticsError({ response: { status: 403 } })).toBe('FORBIDDEN')
  })
})
