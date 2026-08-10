// useRealtimeIntersection.ts — Poll GET /api/realtime/intersections/{id}
// Refetch interval driven by VITE_REALTIME_REFETCH_MS (default 2000ms).
// Polling pauses when document is hidden (tab not visible).
//
// ANTI-SPAM (debounce + adaptive backoff):
//   - intersectionId is debounced (300ms) so rapid intersection switching
//     fires one request for the final selection only.
//   - retry: false — the poll interval IS the retry. Per-tick retries used to
//     multiply requests ×3 while the server returned 503 (stale snapshot).
//   - Error backoff: poll interval doubles per consecutive failure
//     (2s → 4s → 8s → …, capped at VITE_REALTIME_MAX_BACKOFF_MS).
//   - Idle backoff: when the simulation is paused/stale, every poll returns an
//     identical simulationTime. After 2 unchanged polls the interval doubles
//     per unchanged poll up to the same cap, and snaps back to 2s on the first
//     poll where simulationTime advances again.
//   - refetchOnWindowFocus: false — polling already keeps data fresh; focus
//     refetches only added request bursts.
//
// POST-CONTROL CATCH-UP (scenario/demand apply):
//   - requestMetricsRefresh() resets idle counters, opens a ~30s fast-poll
//     boost (1s), and schedules burst refetches at 0/1/3/6/12s so PCU/h and
//     vehicle KPIs update without a full page reload.

import { useCallback, useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getRealtimeIntersection } from '@/api/realtimeApi'
import type { RealtimeIntersectionResponse } from '@/types/realtime'
import { useDebouncedValue } from './useDebouncedValue'
import {
  REALTIME_BOOST_WINDOW_MS,
  REALTIME_BURST_OFFSETS_MS,
  REALTIME_MAX_BACKOFF_MS,
  REALTIME_REFETCH_MS,
  resolveRealtimeRefetchInterval,
} from './realtimeRefreshPolicy'

const ID_DEBOUNCE_MS = 300

export function realtimeIntersectionKey(intersectionId: string) {
  return ['realtime', 'intersection', intersectionId] as const
}

export function useRealtimeIntersection(
  intersectionId: string | undefined,
  options?: { preferFastPoll?: boolean },
) {
  const queryClient = useQueryClient()
  const debouncedId = useDebouncedValue(intersectionId, ID_DEBOUNCE_MS)
  const lastSimTimeRef = useRef<number | null>(null)
  const unchangedPollsRef = useRef(0)
  const burstTimersRef = useRef<ReturnType<typeof setTimeout>[]>([])
  // State so React Query rebinds refetchInterval when boost starts.
  const [boostUntilMs, setBoostUntilMs] = useState(0)
  const preferFastPoll = Boolean(options?.preferFastPoll)

  const query = useQuery({
    queryKey: realtimeIntersectionKey(debouncedId ?? ''),
    queryFn: async (): Promise<RealtimeIntersectionResponse> => {
      if (!debouncedId) throw new Error('No intersectionId')
      const res = await getRealtimeIntersection(debouncedId)
      return res.data
    },
    enabled: Boolean(debouncedId),
    staleTime: REALTIME_REFETCH_MS,
    refetchInterval: (q) => {
      const nowMs = Date.now()
      // Keep 1s polls while TraCI-applied scenario is ahead of Orion feed.
      const effectiveBoostUntil = preferFastPoll
        ? Math.max(boostUntilMs, nowMs + 1)
        : boostUntilMs
      return resolveRealtimeRefetchInterval({
        nowMs,
        boostUntilMs: effectiveBoostUntil,
        fetchFailureCount: q.state.fetchFailureCount,
        httpStatus: (q.state.error as { response?: { status?: number } } | null)
          ?.response?.status,
        unchangedPolls: unchangedPollsRef.current,
        baseMs: REALTIME_REFETCH_MS,
        maxBackoffMs: REALTIME_MAX_BACKOFF_MS,
      })
    },
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })

  // Track consecutive polls where simulationTime did not advance.
  const simTime = query.data?.metadata?.simulationTime
    ?? query.data?.intersection?.simulationTime
    ?? null
  const dataUpdatedAt = query.dataUpdatedAt
  useEffect(() => {
    if (simTime === null || dataUpdatedAt === 0) return
    if (lastSimTimeRef.current === simTime) {
      unchangedPollsRef.current += 1
    } else {
      unchangedPollsRef.current = 0
      lastSimTimeRef.current = simTime
    }
  }, [simTime, dataUpdatedAt])

  // Fresh intersection selection starts with a clean idle counter.
  useEffect(() => {
    unchangedPollsRef.current = 0
    lastSimTimeRef.current = null
    setBoostUntilMs(0)
    burstTimersRef.current.forEach(clearTimeout)
    burstTimersRef.current = []
  }, [debouncedId])

  useEffect(() => {
    return () => {
      burstTimersRef.current.forEach(clearTimeout)
      burstTimersRef.current = []
    }
  }, [])

  const requestMetricsRefresh = useCallback(() => {
    const id = debouncedId || intersectionId
    if (!id) return

    unchangedPollsRef.current = 0
    lastSimTimeRef.current = null
    setBoostUntilMs(Date.now() + REALTIME_BOOST_WINDOW_MS)

    burstTimersRef.current.forEach(clearTimeout)
    burstTimersRef.current = []

    const key = realtimeIntersectionKey(id)
    for (const delay of REALTIME_BURST_OFFSETS_MS) {
      const timer = setTimeout(() => {
        void queryClient.refetchQueries({ queryKey: key })
      }, delay)
      burstTimersRef.current.push(timer)
    }
  }, [debouncedId, intersectionId, queryClient])

  return { ...query, requestMetricsRefresh }
}
