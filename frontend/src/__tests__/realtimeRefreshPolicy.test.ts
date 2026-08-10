import { describe, expect, it } from 'vitest'
import {
  REALTIME_BURST_OFFSETS_MS,
  REALTIME_FAST_POLL_MS,
  resolveRealtimeRefetchInterval,
} from '@/hooks/realtimeRefreshPolicy'

describe('realtimeRefreshPolicy', () => {
  it('schedules burst offsets covering Kafka/Orion catch-up window', () => {
    expect([...REALTIME_BURST_OFFSETS_MS]).toEqual([0, 1000, 3000, 6000, 12_000])
  })

  it('uses fast poll during boost even after fetch failures (503 recovery)', () => {
    const interval = resolveRealtimeRefetchInterval({
      nowMs: 1_000,
      boostUntilMs: 30_000,
      fetchFailureCount: 3,
      httpStatus: 503,
      unchangedPolls: 10,
    })
    expect(interval).toBe(REALTIME_FAST_POLL_MS)
  })

  it('stops polling on auth/not-found even during boost', () => {
    expect(
      resolveRealtimeRefetchInterval({
        nowMs: 1_000,
        boostUntilMs: 30_000,
        fetchFailureCount: 1,
        httpStatus: 401,
        unchangedPolls: 0,
      }),
    ).toBe(false)
  })

  it('applies error backoff outside boost', () => {
    const interval = resolveRealtimeRefetchInterval({
      nowMs: 40_000,
      boostUntilMs: 30_000,
      fetchFailureCount: 2,
      unchangedPolls: 0,
      baseMs: 2000,
    })
    expect(interval).toBe(8000)
  })

  it('applies idle backoff outside boost after grace polls', () => {
    const interval = resolveRealtimeRefetchInterval({
      nowMs: 40_000,
      boostUntilMs: 0,
      fetchFailureCount: 0,
      unchangedPolls: 4, // 4 - 2 grace = 2 → 2s * 2^2 = 8s
      baseMs: 2000,
    })
    expect(interval).toBe(8000)
  })
})
