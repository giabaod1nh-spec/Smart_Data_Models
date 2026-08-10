// useIntersectionList.ts — fetch list of intersections from Orion via Spring

import { useQuery } from '@tanstack/react-query'
import { getIntersections } from '@/api/realtimeApi'
import type { IntersectionResponse } from '@/types/realtime'
import {
  deriveIntersectionListStatus,
  intersectionListStatusMessage,
  type IntersectionListStatus,
} from '@/utils/intersectionListStatus'
import { apiErrorMessage } from '@/utils/apiErrors'
import { useRuntimeAlignment } from '@/hooks/useRuntimeAlignment'

export const INTERSECTION_LIST_KEY = ['intersections'] as const

export function useIntersectionList() {
  const { alignment } = useRuntimeAlignment()

  const query = useQuery({
    queryKey: INTERSECTION_LIST_KEY,
    queryFn: async (): Promise<IntersectionResponse[]> => {
      const res = await getIntersections()
      return res.data ?? []
    },
    staleTime: 60_000,
    refetchOnWindowFocus: false,
    retry: (failureCount, error) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const status = (error as any)?.response?.status as number | undefined
      if (status === 401 || status === 403) return false
      return failureCount < 2
    },
  })

  const status: IntersectionListStatus = deriveIntersectionListStatus(
    query.isLoading,
    query.isError,
    query.error,
    query.data,
    alignment,
  )

  const statusMessage = query.isError
    ? apiErrorMessage(query.error, intersectionListStatusMessage(status, alignment))
    : intersectionListStatusMessage(status, alignment)

  return {
    ...query,
    intersections: query.data,
    status,
    statusMessage,
    isEmpty: status === 'empty' || status === 'run_mismatch',
    isSuccessWithData: status === 'success',
    isUnavailable: status === 'unavailable' || status === 'network_error' || status === 'error',
  }
}
