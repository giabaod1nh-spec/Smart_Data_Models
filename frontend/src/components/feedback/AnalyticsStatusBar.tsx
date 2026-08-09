// AnalyticsStatusBar.tsx — Compact analytics status with popover detail.
// Reduces clutter: single primary badge + hover popover for full details.

import { useState } from 'react'
import { StatusBadge } from './StatusBadge'
import type {
  AnalyticsDataState,
  AnalyticsFeatureState,
  AnalyticsServiceState,
} from '@/utils/analyticsStatus'
import { Info } from 'lucide-react'

interface Props {
  featureState: AnalyticsFeatureState
  serviceState: AnalyticsServiceState
  dataState: AnalyticsDataState
  networkMartDeferred?: boolean
}

function getPrimaryStatus(
  featureState: AnalyticsFeatureState,
  serviceState: AnalyticsServiceState,
  dataState: AnalyticsDataState,
): { label: string; variant: 'green' | 'yellow' | 'red' | 'muted' | 'blue' | 'orange' | 'cyan' | 'purple'; detail: string } {
  if (featureState === 'DISABLED') return { label: 'Analytics Disabled', variant: 'muted', detail: 'Analytics feature is disabled in server config.' }
  if (serviceState === 'UNAVAILABLE') return { label: 'Analytics Unavailable', variant: 'red', detail: 'Analytics service cannot be reached.' }
  if (serviceState === 'NOT_READY') return { label: 'Analytics Not Ready', variant: 'yellow', detail: 'Gold windows are still processing. Try again later.' }
  if (featureState === 'UNKNOWN' || serviceState === 'UNKNOWN') return { label: 'Analytics', variant: 'muted', detail: 'Waiting for filter selection (Run + Scenario).' }
  if (dataState === 'AVAILABLE') return { label: 'Analytics Ready', variant: 'green', detail: 'Data is available for the selected filters.' }
  if (dataState === 'EMPTY') return { label: 'Analytics Empty', variant: 'yellow', detail: 'No rows for current filters.' }
  if (dataState === 'PENDING_FILTERS') return { label: 'Analytics', variant: 'muted', detail: 'Set Run and Scenario to load analytics.' }
  return { label: 'Analytics', variant: 'muted', detail: 'Status unknown.' }
}

export function AnalyticsStatusBar({
  featureState,
  serviceState,
  dataState,
  networkMartDeferred = true,
}: Props) {
  const [showPopover, setShowPopover] = useState(false)
  const { label, variant, detail } = getPrimaryStatus(featureState, serviceState, dataState)

  return (
    <div style={{ position: 'relative', display: 'inline-flex', alignItems: 'center', gap: 6 }} role="status" aria-label="Analytics readiness">
      <StatusBadge
        label={label}
        variant={variant}
        dot={variant === 'green'}
        pulse={variant === 'green'}
        size="md"
      />
      <button
        type="button"
        onClick={() => setShowPopover((v) => !v)}
        style={{
          background: 'transparent', border: 'none', cursor: 'pointer',
          color: 'var(--text-muted)', display: 'flex', alignItems: 'center',
          padding: 2,
        }}
        aria-label="Analytics status details"
        title="View analytics status details"
      >
        <Info size={13} />
      </button>

      {showPopover && (
        <>
          {/* Backdrop */}
          <div
            style={{ position: 'fixed', inset: 0, zIndex: 90 }}
            onClick={() => setShowPopover(false)}
          />
          {/* Popover */}
          <div style={{
            position: 'absolute', top: '100%', right: 0, zIndex: 100, marginTop: 6,
            background: '#0C2235', border: '1px solid #183A52', borderRadius: 10,
            padding: '14px 16px', minWidth: 240,
            boxShadow: '0 12px 32px rgba(0,0,0,0.5)',
          }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: '#F8FAFC', marginBottom: 10 }}>
              Analytics Status Details
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 11 }}>
              <DetailRow label="Feature" value={featureState} ok={featureState === 'ENABLED'} unknown={featureState === 'UNKNOWN'} />
              <DetailRow label="Service" value={serviceState} ok={serviceState === 'READY'} unknown={serviceState === 'UNKNOWN'} />
              <DetailRow label="Data" value={dataState === 'AVAILABLE' ? 'Available' : dataState === 'EMPTY' ? 'Empty' : dataState === 'PENDING_FILTERS' ? 'Awaiting filters' : 'Unknown'} ok={dataState === 'AVAILABLE'} unknown={dataState === 'UNKNOWN'} />
              {networkMartDeferred && (
                <DetailRow label="Network Overview" value="Deferred" ok={false} unknown={false} muted />
              )}
            </div>
            <div style={{ fontSize: 10, color: '#71889B', marginTop: 10, lineHeight: 1.5 }}>
              {detail}
            </div>
          </div>
        </>
      )}
    </div>
  )
}

function DetailRow({ label, value, ok, unknown, muted }: {
  label: string; value: string; ok: boolean; unknown: boolean; muted?: boolean
}) {
  const color = muted ? '#71889B' : ok ? '#22C55E' : unknown ? '#71889B' : '#F59E0B'
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
      <span style={{ color: '#A8BDCE' }}>{label}</span>
      <span style={{ color, fontWeight: 600 }}>{value}</span>
    </div>
  )
}
