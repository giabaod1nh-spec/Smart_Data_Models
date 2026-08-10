// intersectionListStatus.ts — Distinguish empty success from API failures.

import type { IntersectionResponse } from '@/types/realtime'
import type { RuntimeAlignmentResponse } from '@/types/realtime'
import { classifyApiError, type ApiErrorKind } from './apiErrors'

export type IntersectionListStatus =
  | 'loading'
  | 'success'
  | 'empty'
  | 'run_mismatch'
  | 'unauthorized'
  | 'forbidden'
  | 'unavailable'
  | 'network_error'
  | 'error'

function isRunMismatchAlignment(
  alignment: RuntimeAlignmentResponse | null | undefined,
): boolean {
  return alignment?.aligned === false && alignment?.reason === 'run_mismatch'
}

export function buildRunMismatchMessage(
  alignment: RuntimeAlignmentResponse | null | undefined,
): string {
  const producer = alignment?.producerRunId ?? 'unknown'
  const projector = alignment?.projectorRunId ?? 'unknown'
  return (
    `Run ID mismatch: TraCI (${producer}) ≠ Projector (${projector}). ` +
    'Restart TraCI with --new-run or --simulation-run-id matching Projector, ' +
    'or wait for Projector to catch up.'
  )
}

export function deriveIntersectionListStatus(
  isLoading: boolean,
  isError: boolean,
  error: unknown,
  data: IntersectionResponse[] | undefined,
  alignment?: RuntimeAlignmentResponse | null,
): IntersectionListStatus {
  if (isLoading) return 'loading'
  if (isError) {
    const kind: ApiErrorKind = classifyApiError(error)
    if (kind === 'unauthorized') return 'unauthorized'
    if (kind === 'forbidden') return 'forbidden'
    if (kind === 'unavailable') return 'unavailable'
    if (kind === 'network') return 'network_error'
    return 'error'
  }
  if (!data || data.length === 0) {
    if (isRunMismatchAlignment(alignment)) return 'run_mismatch'
    return 'empty'
  }
  return 'success'
}

export function intersectionListStatusMessage(
  status: IntersectionListStatus,
  alignment?: RuntimeAlignmentResponse | null,
): string {
  switch (status) {
    case 'empty':
      return 'No intersections exist in Orion for the current run.'
    case 'run_mismatch':
      return buildRunMismatchMessage(alignment)
    case 'unauthorized':
      return 'Session expired. Please sign in again.'
    case 'forbidden':
      return 'You do not have permission to view intersections.'
    case 'unavailable':
      return 'Orion or upstream services are temporarily unavailable.'
    case 'network_error':
      return 'Unable to reach the server. Check that Spring is running on the configured port.'
    case 'error':
      return 'Unable to load intersections.'
    default:
      return ''
  }
}
