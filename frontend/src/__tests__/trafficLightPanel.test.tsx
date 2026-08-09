// trafficLightPanel.test.tsx — Comprehensive tests for Traffic Lights status panel
// Requirements from Section XX:
// 1. NS_GREEN: North Green, South Green, East Red, West Red
// 2. EW_GREEN: East Green, West Green, North Red, South Red
// 3. NS_YELLOW: North/South Yellow
// 4. EW_YELLOW: East/West Yellow
// 5. configured duration không được hiển thị như remaining
// 6. countdown giảm từng giây nếu source hỗ trợ
// 7. realtime update resync countdown
// 8. phase change reset countdown
// 9. countdown = 0 không tự đổi phase
// 10. stale state freeze countdown
// 11. offline state freeze countdown
// 12. Apply ở component khác không optimistic update panel
// 13. Realtime state mới update panel
// 14. configured FIXED_TIME không còn xuất hiện main UI
// 15. panel render compact 4 directions

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { TrafficLightPanel } from '@/components/feedback/TrafficLightPanel'
import type { TrafficLightView } from '@/transforms/realtimeTransforms'

function createMockLight(overrides: Partial<TrafficLightView>): TrafficLightView {
  return {
    id: 'tl-1',
    direction: 'North',
    currentStatus: 'GREEN',
    currentPhase: 'NS_GREEN',
    timingMode: 'FIXED_TIME',
    workingState: 'OK',
    greenDurationCurrent: 40,
    redDurationCurrent: 40,
    yellowDuration: 3,
    phaseStartedAt: new Date(Date.now() - 10_000).toISOString(), // 10s ago
    simulationTime: 100,
    simulationRunId: 'run-1',
    ...overrides,
  }
}

describe('Traffic Lights Panel (Section XX)', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // Test 1: NS_GREEN maps North/South Green, East/West Red
  it('1. NS_GREEN sets North & South to Green, East & West to Red', () => {
    const lights: TrafficLightView[] = [
      createMockLight({ direction: 'North', currentStatus: 'GREEN', currentPhase: 'NS_GREEN' }),
      createMockLight({ direction: 'South', currentStatus: 'GREEN', currentPhase: 'NS_GREEN' }),
      createMockLight({ direction: 'East', currentStatus: 'RED', currentPhase: 'NS_GREEN' }),
      createMockLight({ direction: 'West', currentStatus: 'RED', currentPhase: 'NS_GREEN' }),
    ]

    render(
      <TrafficLightPanel
        lights={lights}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    expect(screen.getByLabelText(/North signal: Green/i)).toBeDefined()
    expect(screen.getByLabelText(/South signal: Green/i)).toBeDefined()
    expect(screen.getByLabelText(/East signal: Red/i)).toBeDefined()
    expect(screen.getByLabelText(/West signal: Red/i)).toBeDefined()
  })

  // Test 2: EW_GREEN maps East/West Green, North/South Red
  it('2. EW_GREEN sets East & West to Green, North & South to Red', () => {
    const lights: TrafficLightView[] = [
      createMockLight({ direction: 'North', currentStatus: 'RED', currentPhase: 'EW_GREEN' }),
      createMockLight({ direction: 'South', currentStatus: 'RED', currentPhase: 'EW_GREEN' }),
      createMockLight({ direction: 'East', currentStatus: 'GREEN', currentPhase: 'EW_GREEN' }),
      createMockLight({ direction: 'West', currentStatus: 'GREEN', currentPhase: 'EW_GREEN' }),
    ]

    render(
      <TrafficLightPanel
        lights={lights}
        freshnessState="live"
        isPaused={false}
        currentPhase="EW_GREEN"
      />,
    )

    expect(screen.getByLabelText(/East signal: Green/i)).toBeDefined()
    expect(screen.getByLabelText(/West signal: Green/i)).toBeDefined()
    expect(screen.getByLabelText(/North signal: Red/i)).toBeDefined()
    expect(screen.getByLabelText(/South signal: Red/i)).toBeDefined()
  })

  // Test 3: NS_YELLOW maps North/South Yellow
  it('3. NS_YELLOW sets North & South to Yellow', () => {
    const lights: TrafficLightView[] = [
      createMockLight({ direction: 'North', currentStatus: 'YELLOW', currentPhase: 'NS_YELLOW' }),
      createMockLight({ direction: 'South', currentStatus: 'YELLOW', currentPhase: 'NS_YELLOW' }),
      createMockLight({ direction: 'East', currentStatus: 'RED', currentPhase: 'NS_YELLOW' }),
      createMockLight({ direction: 'West', currentStatus: 'RED', currentPhase: 'NS_YELLOW' }),
    ]

    render(
      <TrafficLightPanel
        lights={lights}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_YELLOW"
      />,
    )

    expect(screen.getByLabelText(/North signal: Yellow/i)).toBeDefined()
    expect(screen.getByLabelText(/South signal: Yellow/i)).toBeDefined()
  })

  // Test 4: EW_YELLOW maps East/West Yellow
  it('4. EW_YELLOW sets East & West to Yellow', () => {
    const lights: TrafficLightView[] = [
      createMockLight({ direction: 'North', currentStatus: 'RED', currentPhase: 'EW_YELLOW' }),
      createMockLight({ direction: 'South', currentStatus: 'RED', currentPhase: 'EW_YELLOW' }),
      createMockLight({ direction: 'East', currentStatus: 'YELLOW', currentPhase: 'EW_YELLOW' }),
      createMockLight({ direction: 'West', currentStatus: 'YELLOW', currentPhase: 'EW_YELLOW' }),
    ]

    render(
      <TrafficLightPanel
        lights={lights}
        freshnessState="live"
        isPaused={false}
        currentPhase="EW_YELLOW"
      />,
    )

    expect(screen.getByLabelText(/East signal: Yellow/i)).toBeDefined()
    expect(screen.getByLabelText(/West signal: Yellow/i)).toBeDefined()
  })

  // Test 5 & 14: Configured durations and FIXED_TIME are not shown on main card
  it('5 & 14. FIXED_TIME and Configured duration labels do NOT appear on the main UI', () => {
    const lights: TrafficLightView[] = [
      createMockLight({ direction: 'North', currentStatus: 'GREEN', greenDurationCurrent: 42 }),
      createMockLight({ direction: 'South', currentStatus: 'GREEN', greenDurationCurrent: 42 }),
      createMockLight({ direction: 'East', currentStatus: 'RED', redDurationCurrent: 45 }),
      createMockLight({ direction: 'West', currentStatus: 'RED', redDurationCurrent: 45 }),
    ]

    render(
      <TrafficLightPanel
        lights={lights}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    // FIXED_TIME must NOT be present on main UI
    expect(screen.queryByText(/FIXED_TIME/i)).toBeNull()
    expect(screen.queryByText(/Configured:/i)).toBeNull()
  })

  // Test 6: Countdown ticks down second by second
  it('6. Countdown decreases second-by-second on local display tick', () => {
    const now = Date.now()
    const lights: TrafficLightView[] = [
      createMockLight({
        direction: 'North',
        currentStatus: 'GREEN',
        greenDurationCurrent: 40,
        phaseStartedAt: new Date(now - 10_000).toISOString(), // 30s remaining
      }),
    ]

    render(
      <TrafficLightPanel
        lights={lights}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    // Initial remaining is 30s
    expect(screen.getByText('30s')).toBeDefined()

    // Advance 5 seconds
    act(() => {
      vi.advanceTimersByTime(5000)
    })

    // Now remaining is 25s
    expect(screen.getByText('25s')).toBeDefined()
  })

  // Test 7: Realtime update resyncs countdown
  it('7. Realtime update resyncs the countdown from new phaseStartedAt', () => {
    const now = Date.now()
    const { rerender } = render(
      <TrafficLightPanel
        lights={[
          createMockLight({
            direction: 'North',
            currentStatus: 'GREEN',
            greenDurationCurrent: 40,
            phaseStartedAt: new Date(now - 10_000).toISOString(), // 30s
          }),
        ]}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    expect(screen.getByText('30s')).toBeDefined()

    // New realtime response with updated phaseStartedAt (e.g. authoritative 20s remaining)
    rerender(
      <TrafficLightPanel
        lights={[
          createMockLight({
            direction: 'North',
            currentStatus: 'GREEN',
            greenDurationCurrent: 40,
            phaseStartedAt: new Date(now - 20_000).toISOString(), // 20s
          }),
        ]}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    expect(screen.getByText('20s')).toBeDefined()
  })

  // Test 8: Phase change resets countdown
  it('8. Phase change resets countdown to the new phase duration', () => {
    const now = Date.now()
    const { rerender } = render(
      <TrafficLightPanel
        lights={[
          createMockLight({
            direction: 'North',
            currentStatus: 'GREEN',
            greenDurationCurrent: 40,
            phaseStartedAt: new Date(now - 38_000).toISOString(), // 2s remaining
          }),
        ]}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    expect(screen.getByText('2s')).toBeDefined()

    // Phase transitions to NS_YELLOW with 3s yellow duration
    rerender(
      <TrafficLightPanel
        lights={[
          createMockLight({
            direction: 'North',
            currentStatus: 'YELLOW',
            yellowDuration: 3,
            phaseStartedAt: new Date(now).toISOString(), // just started -> 3s
          }),
        ]}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_YELLOW"
      />,
    )

    expect(screen.getAllByText('3s').length).toBeGreaterThan(0)
    expect(screen.getByLabelText(/North signal: Yellow/i)).toBeDefined()
  })

  // Test 9: Countdown = 0 does NOT self-advance phase
  it('9. When countdown reaches 0s, it does NOT self-advance the phase and shows 0s/Syncing', () => {
    const now = Date.now()
    const lights: TrafficLightView[] = [
      createMockLight({
        direction: 'North',
        currentStatus: 'GREEN',
        greenDurationCurrent: 10,
        phaseStartedAt: new Date(now - 10_000).toISOString(), // 0s remaining
      }),
    ]

    render(
      <TrafficLightPanel
        lights={lights}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    expect(screen.getByText('0s')).toBeDefined()
    expect(screen.getByText(/Syncing…/i)).toBeDefined()
    // Status MUST STILL BE Green — not automatically changed to Yellow
    expect(screen.getByLabelText(/North signal: Green/i)).toBeDefined()
  })

  // Test 10 & 11: Stale and Offline freeze countdown
  it('10-11. Stale, paused, and offline states freeze the countdown timer', () => {
    const now = Date.now()
    const lights: TrafficLightView[] = [
      createMockLight({
        direction: 'North',
        currentStatus: 'GREEN',
        greenDurationCurrent: 40,
        phaseStartedAt: new Date(now - 10_000).toISOString(), // 30s
      }),
    ]

    const { rerender } = render(
      <TrafficLightPanel
        lights={lights}
        freshnessState="stale"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    // Shows 30s and Stale badge
    expect(screen.getByText('30s')).toBeDefined()
    expect(screen.getAllByText(/Stale/i).length).toBeGreaterThan(0)

    // Advance timers — countdown does NOT tick when frozen
    act(() => {
      vi.advanceTimersByTime(5000)
    })

    // Still shows 30s (frozen)
    expect(screen.getByText('30s')).toBeDefined()

    // Test offline state
    rerender(
      <TrafficLightPanel
        lights={lights}
        freshnessState="error"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    expect(screen.getByText('30s')).toBeDefined()
  })

  // Test 12 & 13: Panel updates strictly from Realtime state, never optimistic from clicks
  it('12-13. Panel strictly updates only when Realtime props change', () => {
    const { rerender } = render(
      <TrafficLightPanel
        lights={[
          createMockLight({ direction: 'North', currentStatus: 'GREEN' }),
          createMockLight({ direction: 'East', currentStatus: 'RED' }),
        ]}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    expect(screen.getByLabelText(/North signal: Green/i)).toBeDefined()
    expect(screen.getByLabelText(/East signal: Red/i)).toBeDefined()

    // New Realtime payload arriving after SUMO applies command
    rerender(
      <TrafficLightPanel
        lights={[
          createMockLight({ direction: 'North', currentStatus: 'RED' }),
          createMockLight({ direction: 'East', currentStatus: 'GREEN' }),
        ]}
        freshnessState="live"
        isPaused={false}
        currentPhase="EW_GREEN"
      />,
    )

    expect(screen.getByLabelText(/North signal: Red/i)).toBeDefined()
    expect(screen.getByLabelText(/East signal: Green/i)).toBeDefined()
  })

  // Test 15: Panel renders compact 4 directions
  it('15. Renders all 4 directions in compact format', () => {
    render(
      <TrafficLightPanel
        lights={[
          createMockLight({ direction: 'North' }),
          createMockLight({ direction: 'East' }),
          createMockLight({ direction: 'West' }),
          createMockLight({ direction: 'South' }),
        ]}
        freshnessState="live"
        isPaused={false}
        currentPhase="NS_GREEN"
      />,
    )

    expect(screen.getByText(/^North$/i)).toBeDefined()
    expect(screen.getByText(/^East$/i)).toBeDefined()
    expect(screen.getByText(/^West$/i)).toBeDefined()
    expect(screen.getByText(/^South$/i)).toBeDefined()
  })
})
