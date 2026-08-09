// controlApi.ts — Reverse control API via Spring Server proxy
// Frontend ONLY calls Spring Server (/api/control/**).
// Spring Server proxies to Python Control API on port 9090.
// Frontend NEVER calls port 9090 directly.

import type { ApiResponse } from '@/types/common'
import type {
  ControlCommandStatusResponse,
  ControlProxyQueuedResponse,
  ScenarioId,
  PhaseId,
  ControlMode,
} from '@/types/control'
import { toGoldIntersectionId } from '@/utils/analyticsIntersectionId'
import { httpClient } from './httpClient'

/** Python Control API expects SUMO node ids (A/B/C/D), not Orion URNs. */
function toControlIntersectionId(intersectionId: string): string {
  return toGoldIntersectionId(intersectionId)
}

// ──────────────────────────────────────────────────────
// Proxy read endpoints (proxied from FastAPI via Spring)
// ──────────────────────────────────────────────────────

/**
 * GET /api/control/health — engine status via proxy.
 */
export async function getControlHealth(): Promise<unknown> {
  const res = await httpClient.get<unknown>('/api/control/health')
  return res.data
}

/**
 * GET /api/control/network-state — full network state via proxy.
 */
export async function getNetworkState(): Promise<unknown> {
  const res = await httpClient.get<unknown>('/api/control/network-state')
  return res.data
}

/**
 * GET /api/control/intersections/{id}/state — intersection engine state.
 */
export async function getIntersectionState(intersectionId: string): Promise<unknown> {
  const controlId = toControlIntersectionId(intersectionId)
  const res = await httpClient.get<unknown>(
    `/api/control/intersections/${encodeURIComponent(controlId)}/state`,
  )
  return res.data
}

/**
 * GET /api/control/snapshot/{id} — intersection snapshot from engine.
 */
export async function getSnapshot(intersectionId: string): Promise<unknown> {
  const controlId = toControlIntersectionId(intersectionId)
  const res = await httpClient.get<unknown>(
    `/api/control/snapshot/${encodeURIComponent(controlId)}`,
  )
  return res.data
}

// ──────────────────────────────────────────────────────
// Proxy mutation endpoints (proxied from FastAPI via Spring)
// When command domain is enabled, Spring routes these through
// the canonical command domain instead of legacy proxy.
// ──────────────────────────────────────────────────────

/**
 * POST /api/control/scenario
 * Body: { scenario: ScenarioId, target_intersection?: string, ... }
 *
 * IMPORTANT: queued:true means queue acceptance only, NOT SUMO application.
 */
export async function setScenario(
  scenario: ScenarioId,
  target_intersection?: string,
): Promise<ControlProxyQueuedResponse> {
  const body: { scenario: ScenarioId; target_intersection?: string } = { scenario }
  if (target_intersection) {
    body.target_intersection = toControlIntersectionId(target_intersection)
  }
  const res = await httpClient.post<ControlProxyQueuedResponse>('/api/control/scenario', body)
  return res.data
}

/**
 * POST /api/control/phase
 * Body: { intersection_id: string, phase: PhaseId }
 * phase must be one of: NS_GREEN, NS_YELLOW, EW_GREEN, EW_YELLOW
 */
export async function setPhase(
  intersectionId: string,
  phase: PhaseId,
): Promise<ControlProxyQueuedResponse> {
  const res = await httpClient.post<ControlProxyQueuedResponse>('/api/control/phase', {
    intersection_id: toControlIntersectionId(intersectionId),
    phase,
  })
  return res.data
}

/**
 * POST /api/control/green-duration
 * Body: { intersection_id: string, seconds: number (10–120) }
 */
export async function setGreenDuration(
  intersectionId: string,
  seconds: number,
): Promise<ControlProxyQueuedResponse> {
  const res = await httpClient.post<ControlProxyQueuedResponse>('/api/control/green-duration', {
    intersection_id: toControlIntersectionId(intersectionId),
    seconds,
  })
  return res.data
}

/**
 * POST /api/control/control-mode
 * Body: { mode: ControlMode }
 */
export async function setControlMode(
  mode: ControlMode,
): Promise<ControlProxyQueuedResponse> {
  const res = await httpClient.post<ControlProxyQueuedResponse>('/api/control/control-mode', {
    mode,
  })
  return res.data
}

/**
 * POST /api/control/demand-profile
 * Body: { profile: string } — profile must be in simulation registry.
 */
export async function setDemandProfile(
  profile: string,
): Promise<ControlProxyQueuedResponse> {
  const res = await httpClient.post<ControlProxyQueuedResponse>('/api/control/demand-profile', {
    profile,
  })
  return res.data
}

// ──────────────────────────────────────────────────────
// Canonical command domain (requires app.control.command-domain-enabled=true)
// Returns 404 if command domain is disabled.
// ──────────────────────────────────────────────────────

/**
 * GET /api/control/commands/{commandId}
 * Returns command status. 404 if command domain is disabled or commandId not found.
 */
export async function getCommandStatus(
  commandId: string,
): Promise<ApiResponse<ControlCommandStatusResponse>> {
  const res = await httpClient.get<ApiResponse<ControlCommandStatusResponse>>(
    `/api/control/commands/${commandId}`,
  )
  return res.data
}
