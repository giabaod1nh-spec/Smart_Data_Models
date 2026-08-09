// systemStatus.test.ts — Tests for system connectivity status utility

import { describe, it, expect } from 'vitest'
import { deriveSystemStatus, getSystemStatusInfo } from '@/utils/systemStatus'
import { isSystemHealthUp } from '@/hooks/useSystemStatus'

describe('isSystemHealthUp', () => {
  it('accepts the canonical Spring component health envelope', () => {
    expect(isSystemHealthUp({ server: 'UP' })).toBe(true)
  })

  it('continues to accept an Actuator-style aggregate status', () => {
    expect(isSystemHealthUp({ status: 'UP' })).toBe(true)
  })

  it('rejects health without a live server', () => {
    expect(isSystemHealthUp({ server: 'DOWN' })).toBe(false)
  })
})

describe('deriveSystemStatus', () => {
  it('returns UNKNOWN when health not checked yet', () => {
    expect(deriveSystemStatus(null, false, false)).toBe('UNKNOWN')
  })

  it('returns OFFLINE when health check fails (error)', () => {
    // CRITICAL: When server is unreachable, status MUST be OFFLINE — never OPERATIONAL
    expect(deriveSystemStatus(null, true, false)).toBe('OFFLINE')
  })

  it('returns OFFLINE when health ok is false', () => {
    expect(deriveSystemStatus(false, false, false)).toBe('OFFLINE')
  })

  it('returns OPERATIONAL when health ok and no realtime errors', () => {
    expect(deriveSystemStatus(true, false, false)).toBe('OPERATIONAL')
  })

  it('returns DEGRADED when health ok but realtime has errors', () => {
    expect(deriveSystemStatus(true, false, true)).toBe('DEGRADED')
  })

  it('returns OFFLINE (not OPERATIONAL or DEGRADED) when health error', () => {
    const status = deriveSystemStatus(null, true, false)
    expect(status).not.toBe('OPERATIONAL')
    expect(status).not.toBe('DEGRADED')
    expect(status).toBe('OFFLINE')
  })
})

describe('getSystemStatusInfo', () => {
  it('returns green color for OPERATIONAL', () => {
    const info = getSystemStatusInfo('OPERATIONAL')
    expect(info.status).toBe('OPERATIONAL')
    expect(info.label).toBe('Operational')
    expect(info.color).toBe('#22C55E')
  })

  it('returns red color for OFFLINE', () => {
    const info = getSystemStatusInfo('OFFLINE')
    expect(info.color).toBe('#EF4444')
    expect(info.label).toBe('Offline')
  })

  it('returns orange/yellow for DEGRADED', () => {
    const info = getSystemStatusInfo('DEGRADED')
    expect(info.label).toBe('Degraded')
    expect(info.color).toBe('#F59E0B')
  })
})
