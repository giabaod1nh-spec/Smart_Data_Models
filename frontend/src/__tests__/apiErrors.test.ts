import { describe, expect, it } from 'vitest'
import { classifyApiError } from '@/utils/apiErrors'

describe('classifyApiError', () => {
  it('detects unauthorized', () => {
    expect(classifyApiError({ response: { status: 401 } })).toBe('unauthorized')
  })

  it('detects forbidden', () => {
    expect(classifyApiError({ response: { status: 403 } })).toBe('forbidden')
  })

  it('detects unavailable', () => {
    expect(classifyApiError({ response: { status: 503 } })).toBe('unavailable')
  })

  it('detects network error', () => {
    expect(classifyApiError({ request: {}, response: undefined })).toBe('network')
  })
})
