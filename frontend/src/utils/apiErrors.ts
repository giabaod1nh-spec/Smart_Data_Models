// apiErrors.ts — Classify Spring API errors without swallowing them as empty data.

export type ApiErrorKind =
  | 'unauthorized'
  | 'forbidden'
  | 'unavailable'
  | 'network'
  | 'not_found'
  | 'unknown'

export function classifyApiError(error: unknown): ApiErrorKind {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const err = error as any
  const status = err?.response?.status as number | undefined
  if (status === 401) return 'unauthorized'
  if (status === 403) return 'forbidden'
  if (status === 404) return 'not_found'
  if (status === 502 || status === 503 || status === 504) return 'unavailable'
  if (!status && err?.request && !err?.response) return 'network'
  return 'unknown'
}

export function apiErrorMessage(error: unknown, fallback = 'Request failed'): string {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const msg = (error as any)?.response?.data?.message as string | undefined
  return msg ?? fallback
}
