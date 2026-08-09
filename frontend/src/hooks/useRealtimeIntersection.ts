// useRealtimeIntersection.ts — Poll GET /api/realtime/intersections/{id}
// Refetch interval driven by VITE_REALTIME_REFETCH_MS (default 2000ms).
// Polling pauses when document is hidden (tab not visible).

import { useQuery } from '@tanstack/react-query'
import { getRealtimeIntersection } from '@/api/realtimeApi'
import type { RealtimeIntersectionResponse } from '@/types/realtime'

const REFETCH_MS = Number(import.meta.env.VITE_REALTIME_REFETCH_MS ?? 2000)

export function realtimeIntersectionKey(intersectionId: string) {
  return ['realtime', 'intersection', intersectionId] as const
}

export function useRealtimeIntersection(intersectionId: string | undefined) {
  return useQuery({
    queryKey: realtimeIntersectionKey(intersectionId ?? ''),
    queryFn: async (): Promise<RealtimeIntersectionResponse> => {
      if (!intersectionId) throw new Error('No intersectionId')
      const res = await getRealtimeIntersection(intersectionId)
      return res.data
    },
    enabled: Boolean(intersectionId),
    staleTime: REFETCH_MS,
    refetchInterval: REFETCH_MS,
    // Only poll when tab is visible
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: true,
    retry: (failureCount, error) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const status = (error as any)?.response?.status as number | undefined
      if (status === 401 || status === 403 || status === 404) return false
      return failureCount < 2
    },
  })
}
