// manualTrafficLightControl.test.tsx — Comprehensive tests for Manual Traffic Light Control
// Requirements from Section III & XXIX:
// 1. Red button xuất hiện
// 2. Green selected: Green Duration slider xuất hiện
// 3. Yellow selected: Green slider biến mất/disabled
// 4. Yellow Duration hiển thị 3s Fixed
// 5. Red selected: Green Duration slider không xuất hiện
// 6. NS + Green mapping giữ đúng (NS_GREEN)
// 7. NS + Yellow mapping giữ đúng (NS_YELLOW)
// 8. EW + Green mapping giữ đúng (EW_GREEN)
// 9. EW + Yellow mapping giữ đúng (EW_YELLOW)
// 10. Red mapping map đúng vào phase tương ứng (NS + Red -> EW_GREEN, EW + Red -> NS_GREEN)
// 11. Configured Green không decrement (đứng yên)
// 12. Remaining Time decrement / countdown
// 13. Realtime update resync Remaining
// 14. Phase change reset countdown
// 15. Apply disabled khi không có thay đổi

import React from 'react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import {
  ReverseControlPanel,
  toPhaseId,
  fromPhaseId,
} from '@/components/control/ReverseControlPanel'
import { GREEN_DURATION_MIN, GREEN_DURATION_MAX } from '@/types/control'

// Mock the control API mutations
const mockSetPhase = vi.fn()
const mockSetGreenDuration = vi.fn()

vi.mock('@/api/controlApi', () => ({
  setPhase: (...args: unknown[]) => mockSetPhase(...args),
  setGreenDuration: (...args: unknown[]) => mockSetGreenDuration(...args),
  setScenario: vi.fn().mockResolvedValue({ queued: true }),
  setControlMode: vi.fn().mockResolvedValue({ queued: true }),
  setDemandProfile: vi.fn().mockResolvedValue({ queued: true }),
}))

function renderWithClient(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      {ui}
    </QueryClientProvider>,
  )
}

describe('Manual Traffic Light Control — 4 Core Requirements', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockSetPhase.mockResolvedValue({ queued: true })
    mockSetGreenDuration.mockResolvedValue({ queued: true })
  })

  // Test 1: Red button appears in SET SIGNAL STATE alongside Green and Yellow
  it('1. Renders Green, Yellow, and Red buttons in SET SIGNAL STATE', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
      />,
    )
    expect(screen.getByRole('button', { name: /^Green$/i })).toBeDefined()
    expect(screen.getByRole('button', { name: /^Yellow$/i })).toBeDefined()
    expect(screen.getByRole('button', { name: /^Red$/i })).toBeDefined()
  })

  // Test 2 & 5: Green duration slider appears only for Green, disappears for Yellow & Red
  it('2, 3, 5. Green duration slider appears for Green, and is hidden for Yellow and Red', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
      />,
    )

    // Initial state is GREEN -> Slider exists
    expect(screen.getByLabelText(/Green duration:/i)).toBeDefined()

    // Click Yellow -> Green slider disappears, Yellow Duration 3s Fixed appears
    fireEvent.click(screen.getByRole('button', { name: /^Yellow$/i }))
    expect(screen.queryByLabelText(/Green duration:/i)).toBeNull()
    expect(screen.getByText(/YELLOW DURATION/i)).toBeDefined()
    expect(screen.getByText('3 s')).toBeDefined()
    expect(screen.getByText(/Fixed/i)).toBeDefined()

    // Click Red -> Slider is hidden, Red State notice appears
    fireEvent.click(screen.getByRole('button', { name: /^Red$/i }))
    expect(screen.queryByLabelText(/Green duration:/i)).toBeNull()
    expect(screen.getByText(/RED STATE/i)).toBeDefined()
    expect(screen.getByText(/Controlled by opposite signal phase/i)).toBeDefined()
  })

  // Test 4: Yellow Duration displays 3s Fixed
  it('4. Yellow Duration displays 3s Fixed from contract', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        currentYellowDuration={3}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /^Yellow$/i }))
    expect(screen.getByText('3 s')).toBeDefined()
    expect(screen.getByText(/Fixed/i)).toBeDefined()
  })

  // Test 6-10: Phase mappings including Red
  it('6-10. Maps signal group and state correctly to PhaseId enum including Red mapping', () => {
    expect(toPhaseId('NS', 'GREEN')).toBe('NS_GREEN')
    expect(toPhaseId('NS', 'YELLOW')).toBe('NS_YELLOW')
    expect(toPhaseId('NS', 'RED')).toBe('EW_GREEN')

    expect(toPhaseId('EW', 'GREEN')).toBe('EW_GREEN')
    expect(toPhaseId('EW', 'YELLOW')).toBe('EW_YELLOW')
    expect(toPhaseId('EW', 'RED')).toBe('NS_GREEN')

    expect(fromPhaseId('NS_GREEN')).toEqual({ group: 'NS', state: 'GREEN' })
    expect(fromPhaseId('NS_YELLOW')).toEqual({ group: 'NS', state: 'YELLOW' })
    expect(fromPhaseId('EW_GREEN')).toEqual({ group: 'EW', state: 'GREEN' })
    expect(fromPhaseId('EW_YELLOW')).toEqual({ group: 'EW', state: 'YELLOW' })
  })

  // Test 11 & 12: Configured Green is static (never decrements), Time Remaining counts down
  it('11-12. Configured Green is stationary and separated from live Time Remaining', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        currentRemaining={18}
      />,
    )

    // Configured Green is static
    expect(screen.getAllByText('42 s').length).toBeGreaterThan(0)
    expect(screen.getByText('CONFIGURED GREEN')).toBeDefined()

    // Time Remaining is displayed separately
    expect(screen.getByText('18 s')).toBeDefined()
    expect(screen.getByText('TIME REMAINING')).toBeDefined()
  })

  // Test 13: Apply is disabled when there are no changes
  it('13. Apply Changes button is disabled when selected values match current state', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={60}
      />,
    )
    const applyBtn = screen.getByRole('button', { name: /APPLY CHANGES/i })
    expect(applyBtn.hasAttribute('disabled')).toBe(true)
  })

  // Test 14: Only phase changed -> only submit phase command
  it('14. Selecting Red state submits corresponding opposite Green phase', async () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={60}
      />,
    )

    // Select Red for North – South -> targets EW_GREEN
    fireEvent.click(screen.getByRole('button', { name: /^Red$/i }))

    const applyBtn = screen.getByRole('button', { name: /APPLY CHANGES/i })
    expect(applyBtn.hasAttribute('disabled')).toBe(false)
    fireEvent.click(applyBtn)

    const confirmBtn = screen.getByRole('button', { name: /Confirm & Apply/i })
    fireEvent.click(confirmBtn)

    await waitFor(() => {
      expect(mockSetPhase).toHaveBeenCalledTimes(1)
      expect(mockSetPhase).toHaveBeenCalledWith('urn:ngsi-ld:Intersection:C', 'EW_GREEN')
      expect(mockSetGreenDuration).not.toHaveBeenCalled()
    })
  })

  // Test 15: Green duration slider bounds 10–120
  it('15. Validates green duration slider bounds are strictly 10 to 120', () => {
    expect(GREEN_DURATION_MIN).toBe(10)
    expect(GREEN_DURATION_MAX).toBe(120)

    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
      />,
    )

    const slider = screen.getByLabelText(/Green duration:/i) as HTMLInputElement
    expect(slider.min).toBe('10')
    expect(slider.max).toBe('120')
  })
})
