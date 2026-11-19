// manualTrafficLightControl.test.tsx — Signal Control (Automatic ↔ Manual / Officer)
//
// Automatic: green duration configurable, countdown runs, phase force hidden.
// Manual: officer phase force (G/Y/R), countdown Held, green duration hidden.

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

const mockSetPhase = vi.fn()
const mockSetGreenDuration = vi.fn()
const mockSetControlMode = vi.fn()

vi.mock('@/api/controlApi', () => ({
  setPhase: (...args: unknown[]) => mockSetPhase(...args),
  setGreenDuration: (...args: unknown[]) => mockSetGreenDuration(...args),
  setScenario: vi.fn().mockResolvedValue({ queued: true }),
  setControlMode: (...args: unknown[]) => mockSetControlMode(...args),
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

describe('Signal Control — Automatic / Manual (Officer)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockSetPhase.mockResolvedValue({ queued: true })
    mockSetGreenDuration.mockResolvedValue({ queued: true })
    mockSetControlMode.mockResolvedValue({ queued: false, applied: true, mode: 'MANUAL' })
  })

  it('renders Automatic / Manual / DQN mode toggle', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        controlMode="FIXED"
      />,
    )
    expect(screen.getByRole('button', { name: /Automatic/i })).toBeDefined()
    expect(screen.getByRole('button', { name: /Manual \(Officer\)/i })).toBeDefined()
    expect(screen.getByRole('button', { name: /DQN Agent/i })).toBeDefined()
  })

  it('clicking DQN Agent calls setControlMode(ADAPTIVE)', async () => {
    mockSetControlMode.mockResolvedValue({ queued: false, applied: true, mode: 'ADAPTIVE' })
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        controlMode="FIXED"
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /DQN Agent/i }))
    await waitFor(() => {
      expect(mockSetControlMode).toHaveBeenCalledWith('ADAPTIVE')
    })
  })

  it('DQN Agent mode: hides green duration and officer signal buttons', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        controlMode="ADAPTIVE"
      />,
    )
    expect(screen.queryByLabelText(/Green duration:/i)).toBeNull()
    expect(screen.queryByRole('button', { name: /^Green$/i })).toBeNull()
    expect(screen.getByText(/DQN Agent: cooperative AI agents/i)).toBeDefined()
  })

  it('Automatic: shows green duration, hides officer signal buttons, countdown visible', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        currentRemaining={18}
        controlMode="FIXED"
      />,
    )
    expect(screen.getByLabelText(/Green duration:/i)).toBeDefined()
    expect(screen.queryByRole('button', { name: /^Green$/i })).toBeNull()
    expect(screen.getByText('18 s')).toBeDefined()
    expect(screen.getByText('TIME REMAINING')).toBeDefined()
  })

  it('Manual: shows Held countdown and G/Y/R controls; hides green duration', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        controlMode="MANUAL"
      />,
    )
    expect(screen.getByText(/Held \(Officer\)/i)).toBeDefined()
    expect(screen.getByRole('button', { name: /^Green$/i })).toBeDefined()
    expect(screen.getByRole('button', { name: /^Yellow$/i })).toBeDefined()
    expect(screen.getByRole('button', { name: /^Red$/i })).toBeDefined()
    expect(screen.queryByLabelText(/Green duration:/i)).toBeNull()
  })

  it('Manual: Yellow shows fixed 3s notice; Red shows opposite-phase notice', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        currentYellowDuration={3}
        controlMode="MANUAL"
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /^Yellow$/i }))
    expect(screen.getByText(/YELLOW DURATION/i)).toBeDefined()
    expect(screen.getByText('3 s')).toBeDefined()

    fireEvent.click(screen.getByRole('button', { name: /^Red$/i }))
    expect(screen.getByText(/RED STATE/i)).toBeDefined()
    expect(screen.getByText(/Controlled by opposite signal phase/i)).toBeDefined()
  })

  it('maps signal group and state correctly to PhaseId including Red', () => {
    expect(toPhaseId('NS', 'GREEN')).toBe('NS_GREEN')
    expect(toPhaseId('NS', 'YELLOW')).toBe('NS_YELLOW')
    expect(toPhaseId('NS', 'RED')).toBe('EW_GREEN')
    expect(toPhaseId('EW', 'GREEN')).toBe('EW_GREEN')
    expect(toPhaseId('EW', 'YELLOW')).toBe('EW_YELLOW')
    expect(toPhaseId('EW', 'RED')).toBe('NS_GREEN')
    expect(fromPhaseId('NS_GREEN')).toEqual({ group: 'NS', state: 'GREEN' })
  })

  it('Configured Green stays visible and static in both modes', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        controlMode="FIXED"
      />,
    )
    expect(screen.getByText('CONFIGURED GREEN')).toBeDefined()
    expect(screen.getAllByText('42 s').length).toBeGreaterThan(0)
  })

  it('Apply is disabled when Automatic and duration matches current', () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={60}
        controlMode="FIXED"
      />,
    )
    const applyBtn = screen.getByRole('button', { name: /APPLY GREEN DURATION/i })
    expect(applyBtn.hasAttribute('disabled')).toBe(true)
  })

  it('Manual: selecting Red submits opposite green phase', async () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={60}
        controlMode="MANUAL"
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: /^Red$/i }))
    const applyBtn = screen.getByRole('button', { name: /APPLY SIGNAL/i })
    expect(applyBtn.hasAttribute('disabled')).toBe(false)
    fireEvent.click(applyBtn)
    fireEvent.click(screen.getByRole('button', { name: /Confirm & Apply/i }))

    await waitFor(() => {
      expect(mockSetPhase).toHaveBeenCalledTimes(1)
      expect(mockSetPhase).toHaveBeenCalledWith('urn:ngsi-ld:Intersection:C', 'EW_GREEN')
      expect(mockSetGreenDuration).not.toHaveBeenCalled()
    })
  })

  it('clicking Manual (Officer) calls setControlMode(MANUAL)', async () => {
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        controlMode="FIXED"
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /Manual \(Officer\)/i }))
    await waitFor(() => {
      expect(mockSetControlMode).toHaveBeenCalledWith('MANUAL')
    })
  })

  it('validates green duration slider bounds 10–120 in Automatic', () => {
    expect(GREEN_DURATION_MIN).toBe(10)
    expect(GREEN_DURATION_MAX).toBe(120)
    renderWithClient(
      <ReverseControlPanel
        intersectionId="urn:ngsi-ld:Intersection:C"
        currentPhase="NS_GREEN"
        currentConfiguredGreen={42}
        controlMode="FIXED"
      />,
    )
    const slider = screen.getByLabelText(/Green duration:/i) as HTMLInputElement
    expect(slider.min).toBe('10')
    expect(slider.max).toBe('120')
  })
})
