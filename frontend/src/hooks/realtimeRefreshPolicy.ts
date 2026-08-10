// realtimeRefreshPolicy.ts — shared timing for post-control realtime catch-up
//
// After scenario/demand apply, Kafka→projector→Orion can lag and Spring may
// briefly return 503 (mixed/stale). Burst refetch + temporary fast poll recover
// KPIs (PCU/h, vehicles, …) without a full page reload.

export const REALTIME_REFETCH_MS = Number(import.meta.env.VITE_REALTIME_REFETCH_MS ?? 2000)
export const REALTIME_MAX_BACKOFF_MS = Number(import.meta.env.VITE_REALTIME_MAX_BACKOFF_MS ?? 15000)
export const REALTIME_FAST_POLL_MS = 1000
/** Wall-clock window of aggressive polling after a control mutation. */
export const REALTIME_BOOST_WINDOW_MS = 30_000
/** One-shot refetch delays after control apply (Kafka/Orion catch-up). */
export const REALTIME_BURST_OFFSETS_MS = [0, 1000, 3000, 6000, 12_000] as const

const IDLE_GRACE_POLLS = 2

export type RefetchIntervalInput = {
  nowMs: number
  boostUntilMs: number
  fetchFailureCount: number
  httpStatus?: number
  unchangedPolls: number
  baseMs?: number
  fastMs?: number
  maxBackoffMs?: number
}

/**
 * Decide the next poll interval.
 * During boost: always fast-poll (including after 503) so KPIs catch up.
 * Outside boost: existing error + idle exponential backoff.
 */
export function resolveRealtimeRefetchInterval(input: RefetchIntervalInput): number | false {
  const baseMs = input.baseMs ?? REALTIME_REFETCH_MS
  const fastMs = input.fastMs ?? REALTIME_FAST_POLL_MS
  const maxBackoffMs = input.maxBackoffMs ?? REALTIME_MAX_BACKOFF_MS
  const status = input.httpStatus

  if (status === 401 || status === 403 || status === 404) return false

  if (input.nowMs < input.boostUntilMs) {
    return fastMs
  }

  if (input.fetchFailureCount > 0) {
    return Math.min(baseMs * 2 ** input.fetchFailureCount, maxBackoffMs)
  }

  const idlePolls = input.unchangedPolls - IDLE_GRACE_POLLS
  if (idlePolls > 0) {
    return Math.min(baseMs * 2 ** idlePolls, maxBackoffMs)
  }

  return baseMs
}
