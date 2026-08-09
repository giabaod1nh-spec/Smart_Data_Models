// useSystemStatus.ts — Poll /api/system/health to derive system connectivity.
// CRITICAL: System Status is OFFLINE if health check fails, never OPERATIONAL when unreachable.

import { useQuery } from '@tanstack/react-query'
import { getSystemHealth } from '@/api/realtimeApi'
import { deriveSystemStatus, type SystemConnectivityStatus } from '@/utils/systemStatus'

const HEALTH_POLL_MS = 15_000 // poll every 15s — health check is cheap

export function isSystemHealthUp(health: { status?: unknown; server?: unknown }): boolean {
  // Accept both Actuator-style health and this repository's canonical
  // component envelope: { server, orion, contextProvider, controlApi }.
  const aggregate = typeof health.status === 'string' ? health.status.toUpperCase() : ''
  const server = typeof health.server === 'string' ? health.server.toUpperCase() : ''
  return aggregate === 'UP' || server === 'UP'
}

export function useSystemStatus(): {
  status: SystemConnectivityStatus
  isLoading: boolean
} {
  const query = useQuery({
    queryKey: ['system', 'health'],
    queryFn: async () => {
      const health = await getSystemHealth()
      const ok = isSystemHealthUp(health)
      return { ok }
    },
    staleTime: HEALTH_POLL_MS,
    refetchInterval: HEALTH_POLL_MS,
    refetchIntervalInBackground: false,
    retry: 1,
  })

  const healthOk = query.data?.ok ?? null
  const healthError = query.isError

  const status = deriveSystemStatus(
    query.isLoading ? null : healthOk,
    healthError,
    false, // realtimeError — could be extended later
  )

  return { status, isLoading: query.isLoading }
}
