// queryClient.ts — TanStack Query client configuration

import { QueryClient } from '@tanstack/react-query'

const REALTIME_STALE_MS = Number(import.meta.env.VITE_REALTIME_REFETCH_MS ?? 2000)
const ANALYTICS_STALE_MS = Number(import.meta.env.VITE_ANALYTICS_REFRESH_MS ?? 60000)

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Default stale time for analytics (60s). Realtime hooks override this.
      staleTime: ANALYTICS_STALE_MS,
      // Do not retry indefinitely on errors
      retry: (failureCount, error) => {
        // Do not retry on 401/403/404
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const status = (error as any)?.response?.status as number | undefined
        if (status === 401 || status === 403 || status === 404) return false
        // Max 2 retries for network errors and 5xx
        return failureCount < 2
      },
      retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
      // Refetch on window focus for realtime hooks only (those set refetchOnWindowFocus manually)
      refetchOnWindowFocus: false,
      refetchOnReconnect: true,
    },
    mutations: {
      // No auto-retry on POST mutations (control commands)
      retry: false,
    },
  },
})

export { REALTIME_STALE_MS, ANALYTICS_STALE_MS }
