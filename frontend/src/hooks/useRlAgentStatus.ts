/**
 * useRlAgentStatus — poll Control API /rl/status (via Vite /rl proxy).
 *
 * Cheap when ADAPTIVE mode is off (backend answers without touching the
 * DQN controller); polling only runs while `enabled` is true.
 */
import { useQuery } from '@tanstack/react-query'
import type { LiveAgentStatus, LiveGlobalMetrics } from '@/types/liveTraffic'

export interface RlStatusResponse {
  enabled: boolean
  mode: string | null
  agents: LiveAgentStatus[]
  globalMetrics: LiveGlobalMetrics
}

async function fetchRlStatus(): Promise<RlStatusResponse> {
  const res = await fetch('/rl/status')
  if (!res.ok) throw new Error(`rl/status ${res.status}`)
  return (await res.json()) as RlStatusResponse
}

export function useRlAgentStatus(enabled = true) {
  const query = useQuery({
    queryKey: ['rl', 'status'],
    queryFn: fetchRlStatus,
    enabled,
    refetchInterval: 2000,
    staleTime: 1500,
    retry: 1,
  })

  return {
    enabled: query.data?.enabled ?? false,
    mode: query.data?.mode ?? null,
    agents: query.data?.agents ?? [],
    globalMetrics: query.data?.globalMetrics,
    isLoading: query.isLoading,
    isError: query.isError,
  }
}
