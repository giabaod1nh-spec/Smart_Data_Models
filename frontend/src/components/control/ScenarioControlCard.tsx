// ScenarioControlCard.tsx — Dedicated Scenario Control panel for Left Column
// Approach B (scenario):
//   - POST waits for TraCI; success { applied: true } → show Applied immediately
//   - Parent may update badge from onCommandApplied before realtime catches up
//   - Confirmation modal still required before dispatch

import { useState } from 'react'
import { Activity, Send, CheckCircle2, AlertCircle, Loader2 } from 'lucide-react'
import { useSetScenario, useCommandTracker } from '@/hooks/useControlCommand'
import { SCENARIO_IDS, SCENARIO_LABELS, type ScenarioId } from '@/types/control'
import { formatScenarioLabel } from '@/transforms/realtimeTransforms'

interface Props {
  intersectionId: string
  currentScenarioId: string | null | undefined
  onCommandApplied?: (scenario: string) => void
}

export function ScenarioControlCard({
  intersectionId,
  currentScenarioId,
  onCommandApplied,
}: Props) {
  const [selectedScenario, setSelectedScenario] = useState<ScenarioId | ''>('')
  const [confirmOpen, setConfirmOpen] = useState(false)
  const tracker = useCommandTracker()
  const mutation = useSetScenario(intersectionId)

  const isSubmitting = tracker.state.phase === 'submitting'
  const currentDisplay = formatScenarioLabel(currentScenarioId)

  const handleApplyClick = () => {
    if (!selectedScenario) return
    setConfirmOpen(true)
  }

  const handleConfirm = async () => {
    if (!selectedScenario) return
    setConfirmOpen(false)
    tracker.setSubmitting()

    try {
      const res = await mutation.mutateAsync({
        scenario: selectedScenario,
        target: intersectionId,
      })
      // Approach B: HTTP returns only after TraCI applied the scenario.
      if (res?.applied === true || res?.queued === false) {
        tracker.setApplied()
      } else {
        tracker.setQueued()
      }
      if (onCommandApplied) {
        onCommandApplied(selectedScenario)
      }
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: { message?: string } | string; message?: string } }; message?: string }
      const detail = err?.response?.data?.detail
      const detailMsg = typeof detail === 'string' ? detail : detail?.message
      tracker.setFailed(detailMsg ?? err?.response?.data?.message ?? err?.message ?? 'Failed to apply scenario.')
    }
  }

  return (
    <div className="card" style={{ padding: 12 }}>
      {/* Header */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: 10,
      }}>
        <div style={{
          fontSize: 12, fontWeight: 700, color: 'var(--text-primary)',
          display: 'flex', alignItems: 'center', gap: 6,
          textTransform: 'uppercase', letterSpacing: '0.04em',
        }}>
          <Activity size={13} style={{ color: '#168CFF' }} />
          Scenario Control
        </div>
        <span style={{
          fontSize: 10, padding: '2px 6px', borderRadius: 4,
          background: 'rgba(22,140,255,0.1)', color: '#168CFF', fontWeight: 600,
        }}>
          {currentDisplay}
        </span>
      </div>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 8, lineHeight: 1.35 }}>
        Applies to this intersection only (not the whole network).
      </div>

      {/* Scenario Grid */}
      <div style={{
        display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)',
        gap: 6, marginBottom: 10,
      }}>
        {SCENARIO_IDS.map((id) => {
          const isSelected = selectedScenario === id
          const isCurrent = currentScenarioId === id
          const label = SCENARIO_LABELS[id] ?? id

          return (
            <button
              key={id}
              id={`scenario-btn-${id}`}
              type="button"
              onClick={() => setSelectedScenario(id)}
              aria-pressed={isSelected}
              style={{
                padding: '6px 8px',
                borderRadius: 6,
                fontSize: 11,
                fontWeight: isSelected ? 700 : isCurrent ? 600 : 400,
                textAlign: 'center',
                cursor: 'pointer',
                border: `1px solid ${isSelected ? '#168CFF' : isCurrent ? 'rgba(22,199,232,0.4)' : 'rgba(24,58,82,0.7)'}`,
                background: isSelected
                  ? 'rgba(22,140,255,0.22)'
                  : isCurrent
                  ? 'rgba(22,199,232,0.08)'
                  : 'rgba(10,26,40,0.4)',
                color: isSelected
                  ? '#FFFFFF'
                  : isCurrent
                  ? '#16C7E8'
                  : 'var(--text-secondary)',
                transition: 'all 0.15s ease',
              }}
            >
              {label}
            </button>
          )
        })}
      </div>

      {/* Action Footer */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <button
          id="apply-scenario-btn"
          type="button"
          onClick={handleApplyClick}
          disabled={!selectedScenario || isSubmitting}
          style={{
            display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            width: '100%', padding: '8px 12px', borderRadius: 6,
            background: !selectedScenario || isSubmitting ? 'rgba(22,140,255,0.3)' : '#168CFF',
            color: '#FFFFFF', fontSize: 12, fontWeight: 700,
            border: 'none', cursor: !selectedScenario || isSubmitting ? 'not-allowed' : 'pointer',
            transition: 'background 0.15s ease',
          }}
        >
          <Send size={12} />
          APPLY SCENARIO
        </button>

        {/* Status notice */}
        {tracker.state.phase === 'submitting' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#168CFF', marginTop: 2 }}>
            <Loader2 size={12} className="animate-spin" /> Applying in SUMO…
          </div>
        )}
        {tracker.state.phase === 'queued' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#FACC15', marginTop: 2 }}>
            Queued — awaiting engine confirmation…
          </div>
        )}
        {tracker.state.phase === 'applied' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#22C55E', marginTop: 2 }}>
            <CheckCircle2 size={12} /> Scenario applied in SUMO.
          </div>
        )}
        {tracker.state.phase === 'failed' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#EF4444', marginTop: 2 }}>
            <AlertCircle size={12} /> {tracker.state.message}
          </div>
        )}
      </div>

      {/* Confirmation Dialog */}
      {confirmOpen && (
        <div
          style={{
            position: 'fixed', inset: 0, zIndex: 300,
            background: 'rgba(4,17,31,0.85)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}
          role="dialog"
          aria-modal="true"
          aria-label="Confirm Scenario Change"
        >
          <div style={{
            background: '#0C2235', border: '1px solid #183A52',
            borderRadius: 12, padding: '22px 26px',
            width: 360, maxWidth: '90vw',
            boxShadow: '0 24px 48px rgba(0,0,0,0.6)',
          }}>
            <div style={{ fontSize: 15, fontWeight: 700, color: '#F8FAFC', marginBottom: 8 }}>
              Change Scenario
            </div>
            <div style={{ fontSize: 12, color: '#94A3B8', marginBottom: 18, lineHeight: 1.5 }}>
              Change intersection scenario from <strong>{currentDisplay}</strong> to <strong>{formatScenarioLabel(selectedScenario)}</strong>?
              <div style={{ marginTop: 8, fontSize: 11, color: '#71889B' }}>
                The request waits until SUMO applies the scenario, then the UI updates immediately.
              </div>
            </div>
            <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end' }}>
              <button
                type="button"
                onClick={() => setConfirmOpen(false)}
                style={{
                  padding: '6px 14px', borderRadius: 6,
                  border: '1px solid #183A52', background: 'transparent',
                  color: '#94A3B8', fontSize: 12, cursor: 'pointer',
                }}
              >
                Cancel
              </button>
              <button
                id="confirm-scenario-action"
                type="button"
                onClick={handleConfirm}
                style={{
                  padding: '6px 16px', borderRadius: 6,
                  background: '#168CFF', border: 'none',
                  color: '#FFFFFF', fontSize: 12, fontWeight: 600,
                  cursor: 'pointer',
                }}
                autoFocus
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
