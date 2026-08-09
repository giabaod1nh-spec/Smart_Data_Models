// useControlCommand.ts — mutation and status-polling hooks for reverse control

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState, useRef, useCallback } from 'react'
import * as controlApi from '@/api/controlApi'
import { realtimeIntersectionKey } from './useRealtimeIntersection'
import type {
  ScenarioId, PhaseId, ControlMode, ControlCommandStatusResponse,
} from '@/types/control'
import { TERMINAL_LIFECYCLE_STATUSES } from '@/types/control'

const POLL_MS = Number(import.meta.env.VITE_CONTROL_STATUS_POLL_MS ?? 1000)

// ── Proxy mutations (legacy path) ────────────────────────────────────────────

export function useSetScenario(intersectionId?: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ scenario, target }: { scenario: ScenarioId; target?: string }) =>
      controlApi.setScenario(scenario, target),
    onSuccess: () => {
      if (intersectionId) {
        void qc.invalidateQueries({ queryKey: realtimeIntersectionKey(intersectionId) })
      }
    },
    retry: false,
  })
}

export function useSetPhase(intersectionId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ phase }: { phase: PhaseId }) =>
      controlApi.setPhase(intersectionId, phase),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: realtimeIntersectionKey(intersectionId) })
    },
    retry: false,
  })
}

export function useSetGreenDuration(intersectionId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ seconds }: { seconds: number }) =>
      controlApi.setGreenDuration(intersectionId, seconds),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: realtimeIntersectionKey(intersectionId) })
    },
    retry: false,
  })
}

export function useSetControlMode(intersectionId?: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ mode }: { mode: ControlMode }) => controlApi.setControlMode(mode),
    onSuccess: () => {
      if (intersectionId) {
        void qc.invalidateQueries({ queryKey: realtimeIntersectionKey(intersectionId) })
      }
    },
    retry: false,
  })
}

export function useSetDemandProfile(intersectionId?: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ profile }: { profile: string }) => controlApi.setDemandProfile(profile),
    onSuccess: () => {
      if (intersectionId) {
        void qc.invalidateQueries({ queryKey: realtimeIntersectionKey(intersectionId) })
      }
    },
    retry: false,
  })
}

// ── Command domain status polling ─────────────────────────────────────────────

/**
 * Poll GET /api/control/commands/{commandId} until terminal state.
 * Stops polling on: COMPLETED, FAILED, EXPIRED, UNKNOWN_OUTCOME.
 * Returns 404 if command domain is disabled.
 */
export function useCommandStatus(commandId: string | null) {
  return useQuery<ControlCommandStatusResponse>({
    queryKey: ['control', 'command-status', commandId],
    queryFn: async () => {
      const res = await controlApi.getCommandStatus(commandId!)
      return res.data
    },
    enabled: Boolean(commandId),
    staleTime: 0,
    refetchInterval: (query) => {
      const data = query.state.data
      if (!data) return POLL_MS
      if (TERMINAL_LIFECYCLE_STATUSES.includes(data.lifecycleStatus)) return false
      return POLL_MS
    },
    refetchIntervalInBackground: false,
    retry: false,
  })
}

// ── Pending command tracker (client-side) ─────────────────────────────────────

export type PendingCommandState =
  | { phase: 'idle' }
  | { phase: 'submitting' }
  | { phase: 'queued'; queuedAt: number }
  | { phase: 'polling'; commandId: string }
  | { phase: 'applied' }
  | { phase: 'failed'; message: string }

/**
 * useCommandTracker provides a simple client-side state machine for control actions.
 * When command domain is disabled the proxy returns {queued:true} — we treat that
 * as "Pending" and verify via realtime refetch, NOT as "Applied".
 */
export function useCommandTracker() {
  const [state, setState] = useState<PendingCommandState>({ phase: 'idle' })
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const reset = useCallback(() => {
    if (timerRef.current) clearTimeout(timerRef.current)
    setState({ phase: 'idle' })
  }, [])

  const setSubmitting = useCallback(() => setState({ phase: 'submitting' }), [])

  const setQueued = useCallback(() => {
    setState({ phase: 'queued', queuedAt: Date.now() })
    // Auto-reset after 10s if no further info
    timerRef.current = setTimeout(reset, 10_000)
  }, [reset])

  const setPolling = useCallback(
    (commandId: string) => {
      if (timerRef.current) clearTimeout(timerRef.current)
      setState({ phase: 'polling', commandId })
    },
    [],
  )

  const setApplied = useCallback(() => {
    if (timerRef.current) clearTimeout(timerRef.current)
    setState({ phase: 'applied' })
    timerRef.current = setTimeout(reset, 5_000)
  }, [reset])

  const setFailed = useCallback((message: string) => {
    if (timerRef.current) clearTimeout(timerRef.current)
    setState({ phase: 'failed', message })
  }, [])

  return { state, reset, setSubmitting, setQueued, setPolling, setApplied, setFailed }
}
