// controlClosedLoop.test.ts — Tests for closed-loop control semantics.
//
// CLOSED LOOP RULE:
//   - queued=true means queue acceptance only — NOT SUMO application
//   - UI must NOT update optimistically
//   - UI updates only when Realtime state changes AFTER command applied by SUMO
//   - commandTracker.setQueued() must NOT set phase to 'applied'

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useCommandTracker } from '@/hooks/useControlCommand'

describe('useCommandTracker — closed loop semantics', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('starts in idle state', () => {
    const { result } = renderHook(() => useCommandTracker())
    expect(result.current.state.phase).toBe('idle')
  })

  it('transitions to submitting on setSubmitting', () => {
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setSubmitting())
    expect(result.current.state.phase).toBe('submitting')
  })

  it('CLOSED LOOP: setQueued does NOT indicate command applied (phase=queued not applied)', () => {
    // queued=true means queue acceptance — NOT SUMO application
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setSubmitting())
    act(() => result.current.setQueued())
    // CRITICAL: phase must be 'queued' — not 'applied'
    expect(result.current.state.phase).toBe('queued')
    expect(result.current.state.phase).not.toBe('applied')
  })

  it('transitions to polling with commandId', () => {
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setPolling('cmd-abc-123'))
    expect(result.current.state.phase).toBe('polling')
    if (result.current.state.phase === 'polling') {
      expect(result.current.state.commandId).toBe('cmd-abc-123')
    }
  })

  it('transitions to applied on setApplied', () => {
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setApplied())
    expect(result.current.state.phase).toBe('applied')
  })

  it('auto-resets from applied to idle after 5s', () => {
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setApplied())
    expect(result.current.state.phase).toBe('applied')
    act(() => vi.advanceTimersByTime(5001))
    expect(result.current.state.phase).toBe('idle')
  })

  it('auto-resets from queued to idle after 10s if no further update', () => {
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setQueued())
    expect(result.current.state.phase).toBe('queued')
    act(() => vi.advanceTimersByTime(10001))
    expect(result.current.state.phase).toBe('idle')
  })

  it('transitions to failed with message on setFailed', () => {
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setFailed('Command rejected by SUMO'))
    expect(result.current.state.phase).toBe('failed')
    if (result.current.state.phase === 'failed') {
      expect(result.current.state.message).toBe('Command rejected by SUMO')
    }
  })

  it('manual reset returns to idle', () => {
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setSubmitting())
    act(() => result.current.reset())
    expect(result.current.state.phase).toBe('idle')
  })

  it('CLOSED LOOP: queued phase must show correct label (not Applied)', () => {
    // The queued=true response from API is queue acceptance only
    const { result } = renderHook(() => useCommandTracker())
    act(() => result.current.setQueued())
    // Phase is 'queued' — UI must show "Queued — awaiting SUMO" not "Applied"
    expect(result.current.state.phase).toBe('queued')
  })
})
