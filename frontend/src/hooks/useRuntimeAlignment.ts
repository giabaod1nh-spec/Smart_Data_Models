// useRuntimeAlignment.ts — Poll admin health/details for TraCI vs Projector run alignment.

import { useQuery } from '@tanstack/react-query'
import { getSystemHealthDetails } from '@/api/realtimeApi'
import type { RuntimeAlignmentResponse } from '@/types/realtime'

const ALIGNMENT_POLL_MS = 15_000

export function useRuntimeAlignment(): {
  alignment: RuntimeAlignmentResponse | null | undefined
  isLoading: boolean
  isRunMismatch: boolean
} {
  const query = useQuery({
    queryKey: ['system', 'health', 'details', 'alignment'],
    queryFn: async () => {
      const res = await getSystemHealthDetails()
      return res.data?.runtimeAlignment ?? null
    },
    staleTime: ALIGNMENT_POLL_MS,
    refetchInterval: ALIGNMENT_POLL_MS,
    refetchOnWindowFocus: false,
    retry: (failureCount, error) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const status = (error as any)?.response?.status as number | undefined
      if (status === 401 || status === 403) return false
      return failureCount < 1
    },
  })

  const alignment = query.data
  const isRunMismatch = alignment?.aligned === false && alignment?.reason === 'run_mismatch'

  return {
    alignment,
    isLoading: query.isLoading,
    isRunMismatch,
  }
}
