// navigation.ts — Realtime route resolution for header/sidebar navigation.

import type { IntersectionResponse } from '@/types/realtime'

const SELECTED_KEY = 'smart-traffic:selectedIntersectionId'

export function getStoredSelectedIntersectionId(): string | null {
  try {
    return sessionStorage.getItem(SELECTED_KEY)
  } catch {
    return null
  }
}

export function setStoredSelectedIntersectionId(id: string): void {
  try {
    sessionStorage.setItem(SELECTED_KEY, id)
  } catch {
    // ignore storage failures
  }
}

export type RealtimeRouteResolution =
  | { kind: 'navigate'; intersectionId: string; path: string; reason: 'selected' | 'first' }
  | { kind: 'empty' }
  | { kind: 'loading' }
  | { kind: 'error'; message: string }

export function resolveRealtimeIntersectionRoute(
  status: 'loading' | 'success' | 'empty' | 'run_mismatch' | 'unauthorized' | 'forbidden' | 'unavailable' | 'network_error' | 'error',
  intersections: IntersectionResponse[] | undefined,
  preferredId?: string | null,
  errorMessage?: string,
): RealtimeRouteResolution {
  if (status === 'loading') return { kind: 'loading' }

  if (status !== 'success' && status !== 'empty') {
    return { kind: 'error', message: errorMessage ?? 'Unable to load intersections.' }
  }

  if (!intersections || intersections.length === 0) {
    return { kind: 'empty' }
  }

  const ids = intersections.map((i) => i.id).filter(Boolean) as string[]
  const stored = getStoredSelectedIntersectionId()
  const candidate = preferredId || stored
  const target = candidate && ids.includes(candidate) ? candidate : ids[0]

  return {
    kind: 'navigate',
    intersectionId: target,
    path: `/intersections/${encodeURIComponent(target)}`,
    reason: candidate && ids.includes(candidate) ? 'selected' : 'first',
  }
}

/** Preserve analytics query string when navigating between pages. */
export function appendAnalyticsQuery(path: string, searchParams: URLSearchParams): string {
  const qs = searchParams.toString()
  return qs ? `${path}?${qs}` : path
}
