// useRuntimeAlignment.ts — Poll admin health/details for TraCI vs Projector run alignment.

import { useQuery } from '@tanstack/react-query'
import { getSystemHealthDetails } from '@/api/realtimeApi'
import { useAuth } from '@/features/auth/AuthContext'
import type { RuntimeAlignmentResponse } from '@/types/realtime'

export const RUNTIME_ALIGNMENT_QUERY_KEY = ['system', 'health', 'details', 'alignment'] as const

const ALIGNMENT_POLL_MS = 15_000

export function useRuntimeAlignment(): {
  alignment: RuntimeAlignmentResponse | null | undefined
  isLoading: boolean
  isRunMismatch: boolean
} {
  const { isAuthenticated } = useAuth()

  const query = useQuery({
    queryKey: RUNTIME_ALIGNMENT_QUERY_KEY,
    queryFn: async () => {
      const res = await getSystemHealthDetails()
      return res.data?.runtimeAlignment ?? null
    },
    enabled: isAuthenticated,
    staleTime: ALIGNMENT_POLL_MS,
    refetchInterval: isAuthenticated ? ALIGNMENT_POLL_MS : false,
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
