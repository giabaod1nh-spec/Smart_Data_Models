// ReverseControlPanel.tsx — Manual Traffic Light Control
// Direct signal group, state (Green / Yellow / Red), and green duration control.
// All commands dispatch to Spring Server /api/control/**
//
// CLOSED LOOP & PRESENTATION RULES:
//   - No optimistic UI updates (authoritative Realtime state only)
//   - Current: Phase name (formatPhaseLabel)
//   - Time Remaining: Live countdown (ticks & resyncs with SUMO)
//   - Configured Green: Static configured duration (never counts down)
//   - Signal States: Green, Yellow (fixed 3s, no slider), Red (mapped to opposite phase, no slider)

import { useState, useEffect } from 'react'
import { Loader2, AlertCircle, CheckCircle2, Clock, Send, Info } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import {
  useSetPhase,
  useSetGreenDuration,
  useCommandTracker,
} from '@/hooks/useControlCommand'
import {
  GREEN_DURATION_MIN,
  GREEN_DURATION_MAX,
  type PhaseId,
} from '@/types/control'
import { formatPhaseLabel, type TrafficLightView, type FreshnessState } from '@/transforms/realtimeTransforms'
import { useCountdown } from '@/hooks/useCountdown'

export type SignalGroup = 'NS' | 'EW'
export type SignalState = 'GREEN' | 'YELLOW' | 'RED'

interface Props {
  intersectionId: string
  currentPhase?: string | null
  currentConfiguredGreen?: number | null
  currentRemaining?: number | null
  currentYellowDuration?: number | null
  freshnessState?: FreshnessState
  isPaused?: boolean
  currentScenario?: string | null
  currentMode?: string | null
  onCommandApplied?: (action: string) => void
}

/** Map SignalGroup and SignalState to valid backend PhaseId */
// eslint-disable-next-line react-refresh/only-export-components
export function toPhaseId(group: SignalGroup, state: SignalState): PhaseId {
  if (group === 'NS') {
    if (state === 'GREEN') return 'NS_GREEN'
    if (state === 'YELLOW') return 'NS_YELLOW'
    // Making North-South RED in a 2-phase controller activates East-West GREEN
    return 'EW_GREEN'
  }
  if (state === 'GREEN') return 'EW_GREEN'
  if (state === 'YELLOW') return 'EW_YELLOW'
  // Making East-West RED activates North-South GREEN
  return 'NS_GREEN'
}

/** Extract SignalGroup and SignalState from backend PhaseId */
// eslint-disable-next-line react-refresh/only-export-components
export function fromPhaseId(phase: string | null | undefined): { group: SignalGroup; state: SignalState } | null {
  if (!phase) return null
  const p = phase.toUpperCase()
  if (p === 'NS_GREEN') return { group: 'NS', state: 'GREEN' }
  if (p === 'NS_YELLOW') return { group: 'NS', state: 'YELLOW' }
  if (p === 'EW_GREEN') return { group: 'EW', state: 'GREEN' }
  if (p === 'EW_YELLOW') return { group: 'EW', state: 'YELLOW' }
  return null
}

export function ReverseControlPanel({
  intersectionId,
  currentPhase,
  currentConfiguredGreen,
  currentRemaining,
  currentYellowDuration = 3,
  freshnessState = 'live',
  isPaused = false,
  onCommandApplied,
}: Props) {
  const queryClient = useQueryClient()
  const tracker = useCommandTracker()
  const phaseMutation = useSetPhase(intersectionId)
  const durationMutation = useSetGreenDuration(intersectionId)

  // Local selection state (values the user is preparing to Apply)
  const [selectedGroup, setSelectedGroup] = useState<SignalGroup>('NS')
  const [selectedState, setSelectedState] = useState<SignalState>('GREEN')
  const [selectedGreenDuration, setSelectedGreenDuration] = useState<number>(
    currentConfiguredGreen && currentConfiguredGreen >= GREEN_DURATION_MIN && currentConfiguredGreen <= GREEN_DURATION_MAX
      ? currentConfiguredGreen
      : 60,
  )
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [partialError, setPartialError] = useState<string | null>(null)
  const [lastSubmittedPhase, setLastSubmittedPhase] = useState<PhaseId | null>(null)
  const [lastSubmittedDuration, setLastSubmittedDuration] = useState<number | null>(null)

  // Live countdown hook for active phase
  const activeLight: TrafficLightView | undefined = currentPhase ? {
    id: `tl-control-active`,
    direction: currentPhase.startsWith('NS') ? 'North' : 'East',
    currentStatus: currentPhase.includes('GREEN') ? 'GREEN' : 'YELLOW',
    currentPhase,
    timingMode: 'FIXED_TIME',
    workingState: 'OK',
    greenDurationCurrent: currentConfiguredGreen ?? null,
    redDurationCurrent: null,
    yellowDuration: currentYellowDuration ?? 3,
    phaseStartedAt: new Date().toISOString(),
    simulationTime: null,
    simulationRunId: null,
  } : undefined

  const { remainingSec: hookRemainingSec, isSyncing: hookIsSyncing } = useCountdown(
    activeLight,
    freshnessState,
    isPaused,
  )

  // Compute selected PhaseId
  const selectedPhase = toPhaseId(selectedGroup, selectedState)

  // Detect changes against authoritative current state
  const phaseChanged = Boolean(currentPhase ? selectedPhase !== currentPhase : selectedPhase)
  const durationChanged = selectedState === 'GREEN' && typeof currentConfiguredGreen === 'number'
    ? selectedGreenDuration !== currentConfiguredGreen
    : false
  const hasChanges = phaseChanged || durationChanged

  const isSubmitting = tracker.state.phase === 'submitting'

  // Closed loop: When Realtime state reflects submitted changes, transition to 'applied'
  useEffect(() => {
    if (tracker.state.phase === 'queued' || tracker.state.phase === 'polling') {
      const phaseMatched = lastSubmittedPhase ? currentPhase === lastSubmittedPhase : true
      const durationMatched = lastSubmittedDuration !== null ? currentConfiguredGreen === lastSubmittedDuration : true

      if (phaseMatched && durationMatched) {
        tracker.setApplied()
      }
    }
  }, [currentPhase, currentConfiguredGreen, lastSubmittedPhase, lastSubmittedDuration, tracker])

  // Open confirmation modal
  const handleApplyClick = () => {
    if (!hasChanges || isSubmitting) return
    setConfirmOpen(true)
  }

  // Execute command(s) upon confirmation
  const handleConfirm = async () => {
    setConfirmOpen(false)
    tracker.setSubmitting()
    setPartialError(null)

    const willChangePhase = phaseChanged
    const willChangeDuration = durationChanged

    let phaseSuccess = false
    let durationSuccess = false
    let phaseErrorMsg: string | null = null
    let durationErrorMsg: string | null = null

    if (willChangePhase && willChangeDuration) {
      // CASE 3: Both Phase and Green Duration changed
      try {
        await phaseMutation.mutateAsync({ phase: selectedPhase })
        phaseSuccess = true
      } catch (e: unknown) {
        const err = e as { response?: { data?: { detail?: { message?: string }; message?: string } }; message?: string }
        phaseErrorMsg = err?.response?.data?.detail?.message ?? err?.response?.data?.message ?? err?.message ?? 'Phase command failed.'
      }

      try {
        await durationMutation.mutateAsync({ seconds: selectedGreenDuration })
        durationSuccess = true
      } catch (e: unknown) {
        const err = e as { response?: { data?: { detail?: { message?: string }; message?: string } }; message?: string }
        durationErrorMsg = err?.response?.data?.detail?.message ?? err?.response?.data?.message ?? err?.message ?? 'Duration command failed.'
      }

      // Always refetch Realtime state to ensure true state is reflected
      queryClient.invalidateQueries({ queryKey: ['realtime', 'intersection', intersectionId] })

      if (phaseSuccess && durationSuccess) {
        tracker.setQueued()
        setLastSubmittedPhase(selectedPhase)
        setLastSubmittedDuration(selectedGreenDuration)
        onCommandApplied?.(`Phase -> ${selectedPhase}, Green Duration -> ${selectedGreenDuration}s`)
      } else if (phaseSuccess && !durationSuccess) {
        // Partial failure: Phase succeeded, Duration failed
        tracker.setQueued()
        setLastSubmittedPhase(selectedPhase)
        setPartialError(`Phase queued, but Green Duration failed: ${durationErrorMsg}`)
        onCommandApplied?.(`Phase -> ${selectedPhase} (Duration failed)`)
      } else if (!phaseSuccess && durationSuccess) {
        // Partial failure: Duration succeeded, Phase failed
        tracker.setQueued()
        setLastSubmittedDuration(selectedGreenDuration)
        setPartialError(`Green Duration queued, but Phase failed: ${phaseErrorMsg}`)
        onCommandApplied?.(`Green Duration -> ${selectedGreenDuration}s (Phase failed)`)
      } else {
        // Both failed
        tracker.setFailed(`Failed: Phase (${phaseErrorMsg}), Duration (${durationErrorMsg})`)
      }
    } else if (willChangePhase) {
      // CASE 1: Only Phase changed
      try {
        await phaseMutation.mutateAsync({ phase: selectedPhase })
        queryClient.invalidateQueries({ queryKey: ['realtime', 'intersection', intersectionId] })
        tracker.setQueued()
        setLastSubmittedPhase(selectedPhase)
        onCommandApplied?.(`Phase -> ${selectedPhase}`)
      } catch (e: unknown) {
        const err = e as { response?: { data?: { detail?: { message?: string }; message?: string } }; message?: string }
        const msg = err?.response?.data?.detail?.message ?? err?.response?.data?.message ?? err?.message ?? 'Failed to apply phase.'
        tracker.setFailed(msg)
      }
    } else if (willChangeDuration) {
      // CASE 2: Only Duration changed
      try {
        await durationMutation.mutateAsync({ seconds: selectedGreenDuration })
        queryClient.invalidateQueries({ queryKey: ['realtime', 'intersection', intersectionId] })
        tracker.setQueued()
        setLastSubmittedDuration(selectedGreenDuration)
        onCommandApplied?.(`Green Duration -> ${selectedGreenDuration}s`)
      } catch (e: unknown) {
        const err = e as { response?: { data?: { detail?: { message?: string }; message?: string } }; message?: string }
        const msg = err?.response?.data?.detail?.message ?? err?.response?.data?.message ?? err?.message ?? 'Failed to apply green duration.'
        tracker.setFailed(msg)
      }
    }
  }

  // Friendly labels
  const currentPhaseDisplay = formatPhaseLabel(currentPhase)
  const selectedPhaseDisplay = formatPhaseLabel(selectedPhase)

  // Remaining time format
  const remainingValue = typeof currentRemaining === 'number'
    ? currentRemaining
    : hookRemainingSec !== null
    ? Math.ceil(hookRemainingSec)
    : null

  const isSyncingState = hookIsSyncing || remainingValue === 0

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {/* ── SECTION: CURRENT STATE (Authoritative) ── */}
      <div style={{
        background: 'rgba(6,17,31,0.5)',
        border: '1px solid var(--border)',
        borderRadius: 8,
        padding: '10px 12px',
        display: 'flex',
        flexDirection: 'column',
        gap: 8,
      }}>
        {/* Current Phase */}
        <div>
          <div style={MUTED_LABEL}>CURRENT</div>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-primary)', marginTop: 1 }}>
            {currentPhaseDisplay}
          </div>
        </div>

        {/* Time Remaining (Countdown) */}
        <div>
          <div style={MUTED_LABEL}>TIME REMAINING</div>
          <div style={{
            fontSize: 14,
            fontWeight: 700,
            color: '#16C7E8',
            fontFamily: 'var(--font-mono, monospace)',
            fontVariantNumeric: 'tabular-nums',
            marginTop: 1,
          }}>
            {remainingValue !== null
              ? isSyncingState
                ? '0 s (Syncing…)'
                : `${remainingValue} s`
              : '—'}
          </div>
        </div>

        {/* Configured Green (Static, NEVER counts down) */}
        <div>
          <div style={MUTED_LABEL}>CONFIGURED GREEN</div>
          <div style={{
            fontSize: 13,
            fontWeight: 700,
            color: '#22C55E',
            fontFamily: 'var(--font-mono, monospace)',
            marginTop: 1,
          }}>
            {typeof currentConfiguredGreen === 'number' ? `${currentConfiguredGreen} s` : '—'}
          </div>
        </div>
      </div>

      {/* ── SECTION: SELECT SIGNAL GROUP ── */}
      <div>
        <div style={SECTION_HEADER}>SELECT SIGNAL GROUP</div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
          <button
            id="signal-group-ns-btn"
            type="button"
            onClick={() => setSelectedGroup('NS')}
            aria-pressed={selectedGroup === 'NS'}
            style={{
              padding: '8px 12px',
              borderRadius: 6,
              fontSize: 12,
              fontWeight: selectedGroup === 'NS' ? 700 : 500,
              cursor: 'pointer',
              border: `1.5px solid ${selectedGroup === 'NS' ? '#168CFF' : 'var(--border)'}`,
              background: selectedGroup === 'NS' ? 'rgba(22,140,255,0.2)' : 'rgba(10,26,40,0.4)',
              color: selectedGroup === 'NS' ? '#FFFFFF' : 'var(--text-secondary)',
              transition: 'all 0.15s ease',
            }}
          >
            North – South
          </button>
          <button
            id="signal-group-ew-btn"
            type="button"
            onClick={() => setSelectedGroup('EW')}
            aria-pressed={selectedGroup === 'EW'}
            style={{
              padding: '8px 12px',
              borderRadius: 6,
              fontSize: 12,
              fontWeight: selectedGroup === 'EW' ? 700 : 500,
              cursor: 'pointer',
              border: `1.5px solid ${selectedGroup === 'EW' ? '#168CFF' : 'var(--border)'}`,
              background: selectedGroup === 'EW' ? 'rgba(22,140,255,0.2)' : 'rgba(10,26,40,0.4)',
              color: selectedGroup === 'EW' ? '#FFFFFF' : 'var(--text-secondary)',
              transition: 'all 0.15s ease',
            }}
          >
            East – West
          </button>
        </div>
      </div>

      {/* ── SECTION: SET SIGNAL STATE (Green / Yellow / Red) ── */}
      <div>
        <div style={SECTION_HEADER}>SET SIGNAL STATE</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8 }}>
          <button
            id="signal-state-green-btn"
            type="button"
            onClick={() => setSelectedState('GREEN')}
            aria-pressed={selectedState === 'GREEN'}
            style={{
              padding: '8px 12px',
              borderRadius: 6,
              fontSize: 12,
              fontWeight: selectedState === 'GREEN' ? 700 : 500,
              cursor: 'pointer',
              border: `1.5px solid ${selectedState === 'GREEN' ? '#22C55E' : 'var(--border)'}`,
              background: selectedState === 'GREEN' ? 'rgba(34,197,94,0.22)' : 'rgba(10,26,40,0.4)',
              color: selectedState === 'GREEN' ? '#22C55E' : 'var(--text-secondary)',
              transition: 'all 0.15s ease',
            }}
          >
            Green
          </button>
          <button
            id="signal-state-yellow-btn"
            type="button"
            onClick={() => setSelectedState('YELLOW')}
            aria-pressed={selectedState === 'YELLOW'}
            style={{
              padding: '8px 12px',
              borderRadius: 6,
              fontSize: 12,
              fontWeight: selectedState === 'YELLOW' ? 700 : 500,
              cursor: 'pointer',
              border: `1.5px solid ${selectedState === 'YELLOW' ? '#FACC15' : 'var(--border)'}`,
              background: selectedState === 'YELLOW' ? 'rgba(250,204,21,0.22)' : 'rgba(10,26,40,0.4)',
              color: selectedState === 'YELLOW' ? '#FACC15' : 'var(--text-secondary)',
              transition: 'all 0.15s ease',
            }}
          >
            Yellow
          </button>
          <button
            id="signal-state-red-btn"
            type="button"
            onClick={() => setSelectedState('RED')}
            aria-pressed={selectedState === 'RED'}
            style={{
              padding: '8px 12px',
              borderRadius: 6,
              fontSize: 12,
              fontWeight: selectedState === 'RED' ? 700 : 500,
              cursor: 'pointer',
              border: `1.5px solid ${selectedState === 'RED' ? '#EF4444' : 'var(--border)'}`,
              background: selectedState === 'RED' ? 'rgba(239,68,68,0.22)' : 'rgba(10,26,40,0.4)',
              color: selectedState === 'RED' ? '#EF4444' : 'var(--text-secondary)',
              transition: 'all 0.15s ease',
            }}
          >
            Red
          </button>
        </div>
      </div>

      {/* ── SECTION: DURATION CONTROLS (Green slider vs Yellow 3s Fixed vs Red State) ── */}
      {selectedState === 'GREEN' && (
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
            <div style={SECTION_HEADER}>GREEN DURATION</div>
            <div style={{ fontSize: 13, fontWeight: 700, color: '#22C55E', fontFamily: 'var(--font-mono, monospace)' }}>
              {selectedGreenDuration} s
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{GREEN_DURATION_MIN}s</span>
            <input
              id="green-duration-slider"
              type="range"
              min={GREEN_DURATION_MIN}
              max={GREEN_DURATION_MAX}
              value={selectedGreenDuration}
              onChange={(e) => setSelectedGreenDuration(Number(e.target.value))}
              style={{ flex: 1, accentColor: '#22C55E', cursor: 'pointer' }}
              aria-label={`Green duration: ${selectedGreenDuration} seconds`}
            />
            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{GREEN_DURATION_MAX}s</span>
          </div>
        </div>
      )}

      {selectedState === 'YELLOW' && (
        <div>
          <div style={SECTION_HEADER}>YELLOW DURATION</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 4 }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: '#FACC15', fontFamily: 'var(--font-mono, monospace)' }}>
              {currentYellowDuration ?? 3} s
            </div>
            <span style={{
              fontSize: 10,
              fontWeight: 600,
              color: '#71889B',
              background: 'rgba(24,58,82,0.4)',
              padding: '2px 6px',
              borderRadius: 4,
              textTransform: 'uppercase',
            }}>
              Fixed
            </span>
          </div>
        </div>
      )}

      {selectedState === 'RED' && (
        <div>
          <div style={SECTION_HEADER}>RED STATE</div>
          <div style={{ fontSize: 11, color: '#EF4444', marginTop: 4, lineHeight: 1.4 }}>
            Controlled by opposite signal phase
          </div>
        </div>
      )}

      {/* ── SECTION: APPLY CHANGES BUTTON ── */}
      <div>
        <button
          id="apply-changes-btn"
          type="button"
          onClick={handleApplyClick}
          disabled={!hasChanges || isSubmitting}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 8,
            width: '100%',
            padding: '10px 16px',
            borderRadius: 6,
            background: !hasChanges || isSubmitting ? 'rgba(22,140,255,0.25)' : '#168CFF',
            color: '#FFFFFF',
            fontSize: 13,
            fontWeight: 700,
            letterSpacing: '0.04em',
            textTransform: 'uppercase',
            border: 'none',
            cursor: !hasChanges || isSubmitting ? 'not-allowed' : 'pointer',
            transition: 'background 0.15s ease',
          }}
        >
          <Send size={14} />
          APPLY CHANGES
        </button>

        {/* ── STATUS FEEDBACK ── */}
        {tracker.state.phase === 'submitting' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#168CFF', marginTop: 8 }}>
            <Loader2 size={13} className="animate-spin" /> Submitting command…
          </div>
        )}
        {tracker.state.phase === 'queued' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#FACC15', marginTop: 8 }}>
            <Clock size={13} /> Queued — awaiting SUMO realtime update…
          </div>
        )}
        {tracker.state.phase === 'applied' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#22C55E', marginTop: 8 }}>
            <CheckCircle2 size={13} /> Realtime state updated.
          </div>
        )}
        {partialError && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#F59E0B', marginTop: 8 }}>
            <AlertCircle size={13} /> Partially Applied: {partialError}
          </div>
        )}
        {tracker.state.phase === 'failed' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#EF4444', marginTop: 8 }}>
            <AlertCircle size={13} /> {tracker.state.message}
          </div>
        )}
      </div>

      {/* Info notice about closed loop */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: 5,
        fontSize: 10,
        color: 'var(--text-muted)',
        marginTop: -4,
      }}>
        <Info size={11} style={{ flexShrink: 0 }} />
        <span>Commands are queued — state updates only after SUMO confirmation.</span>
      </div>

      {/* ── CONFIRMATION MODAL ── */}
      {confirmOpen && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            zIndex: 300,
            background: 'rgba(4,17,31,0.85)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
          role="dialog"
          aria-modal="true"
          aria-label="Confirm Traffic Light Changes"
        >
          <div style={{
            background: '#0C2235',
            border: '1px solid #183A52',
            borderRadius: 12,
            padding: '22px 26px',
            width: 380,
            maxWidth: '90vw',
            boxShadow: '0 24px 48px rgba(0,0,0,0.6)',
            display: 'flex',
            flexDirection: 'column',
            gap: 16,
          }}>
            <div style={{ fontSize: 16, fontWeight: 700, color: '#F8FAFC' }}>
              Confirm Traffic Light Changes
            </div>

            <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
              You are about to apply the following signal parameters:
            </div>

            <div style={{
              background: 'rgba(6,17,31,0.7)',
              border: '1px solid rgba(24,58,82,0.6)',
              borderRadius: 8,
              padding: '12px 14px',
              fontSize: 12,
              display: 'flex',
              flexDirection: 'column',
              gap: 8,
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ color: 'var(--text-muted)' }}>Target Signal:</span>
                <span style={{ fontWeight: 700, color: '#F8FAFC' }}>
                  {selectedGroup === 'NS' ? 'North – South' : 'East – West'}
                </span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ color: 'var(--text-muted)' }}>Selected State:</span>
                <span style={{
                  fontWeight: 700,
                  color: selectedState === 'GREEN' ? '#22C55E' : selectedState === 'YELLOW' ? '#FACC15' : '#EF4444',
                }}>
                  {selectedState}
                </span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ color: 'var(--text-muted)' }}>Target Phase:</span>
                <span style={{ fontWeight: 700, color: '#168CFF' }}>{selectedPhaseDisplay}</span>
              </div>
              {selectedState === 'GREEN' && (
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: 'var(--text-muted)' }}>Green Duration:</span>
                  <span style={{ fontWeight: 700, color: '#22C55E' }}>{selectedGreenDuration} s</span>
                </div>
              )}
              {selectedState === 'YELLOW' && (
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: 'var(--text-muted)' }}>Yellow Duration:</span>
                  <span style={{ fontWeight: 700, color: '#FACC15' }}>{currentYellowDuration ?? 3} s (Fixed)</span>
                </div>
              )}
            </div>

            <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end', marginTop: 4 }}>
              <button
                type="button"
                onClick={() => setConfirmOpen(false)}
                style={{
                  padding: '8px 16px',
                  borderRadius: 6,
                  border: '1px solid var(--border)',
                  background: 'rgba(255,255,255,0.05)',
                  color: 'var(--text-secondary)',
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
              >
                Cancel
              </button>
              <button
                id="confirm-apply-btn"
                type="button"
                onClick={handleConfirm}
                style={{
                  padding: '8px 18px',
                  borderRadius: 6,
                  border: 'none',
                  background: '#168CFF',
                  color: '#FFFFFF',
                  fontSize: 12,
                  fontWeight: 700,
                  cursor: 'pointer',
                }}
              >
                Confirm & Apply
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const SECTION_HEADER: React.CSSProperties = {
  fontSize: 10,
  fontWeight: 700,
  color: 'var(--text-muted)',
  textTransform: 'uppercase',
  letterSpacing: '0.06em',
  marginBottom: 6,
}

const MUTED_LABEL: React.CSSProperties = {
  fontSize: 10,
  color: 'var(--text-muted)',
  fontWeight: 600,
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
}
