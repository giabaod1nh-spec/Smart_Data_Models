// useRealtimeContextBootstrap.ts — Auto-fill simulationRunId/scenarioId from Realtime aggregate.

import { useEffect } from 'react'
import { useRealtimeIntersection } from './useRealtimeIntersection'

interface BootstrapOptions {
  /** Intersection used to read metadata (first list item or selected). */
  intersectionId: string | null
  simulationRunId: string
  scenarioId: string
  setFilters: (updates: Record<string, string>) => void
}

export function buildRuntimeContextUpdates(
  simulationRunId: string,
  scenarioId: string,
  runtimeRunId: string | null | undefined,
  runtimeScenarioId: string | null | undefined,
): Record<string, string> {
  const updates: Record<string, string> = {}
  if (!simulationRunId && runtimeRunId) updates.simulationRunId = runtimeRunId
  if (!scenarioId && runtimeScenarioId) updates.scenarioId = runtimeScenarioId
  return updates
}

/**
 * When analytics filters are missing, bootstrap from GET /api/realtime/intersections/{id}.
 * Does not call Projector directly — Spring Realtime aggregate only.
 */
export function useRealtimeContextBootstrap({
  intersectionId,
  simulationRunId,
  scenarioId,
  setFilters,
}: BootstrapOptions): { isBootstrapping: boolean; bootstrapError: unknown } {
  const needsBootstrap = Boolean(intersectionId) && (!simulationRunId || !scenarioId)

  const { data, isLoading, error } = useRealtimeIntersection(needsBootstrap ? intersectionId ?? undefined : undefined)

  useEffect(() => {
    if (!needsBootstrap || !data) return

    const runId = data.metadata?.simulationRunId ?? data.intersection?.simulationRunId ?? null
    const scen = data.metadata?.scenarioId ?? data.intersection?.scenarioId ?? null
    const updates = buildRuntimeContextUpdates(simulationRunId, scenarioId, runId, scen)

    // React Router does not queue multiple setSearchParams calls in the same tick.
    // Apply run + scenario atomically so one value cannot overwrite the other.
    if (Object.keys(updates).length > 0) setFilters(updates)
  }, [needsBootstrap, data, simulationRunId, scenarioId, setFilters])

  return {
    isBootstrapping: needsBootstrap && isLoading,
    bootstrapError: needsBootstrap ? error : undefined,
  }
}
