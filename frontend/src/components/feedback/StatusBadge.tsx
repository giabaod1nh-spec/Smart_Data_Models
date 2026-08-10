// StatusBadge.tsx — Reusable status badge with icon + label
// Rule: status is NEVER communicated by color alone — always includes text label.

import React from 'react'
import { CheckCircle2, AlertTriangle, XCircle, Info, Clock, Wifi, WifiOff } from 'lucide-react'

export type BadgeVariant = 'green' | 'yellow' | 'orange' | 'red' | 'blue' | 'purple' | 'muted' | 'cyan'

interface Props {
  label: string
  variant: BadgeVariant
  icon?: React.ReactNode
  size?: 'sm' | 'md'
  dot?: boolean
  pulse?: boolean
}

const VARIANT_STYLES: Record<BadgeVariant, { bg: string; text: string; border: string }> = {
  green:  { bg: 'rgba(34,197,94,0.12)',   text: '#22C55E', border: 'rgba(34,197,94,0.3)' },
  yellow: { bg: 'rgba(250,204,21,0.12)',  text: '#FACC15', border: 'rgba(250,204,21,0.3)' },
  orange: { bg: 'rgba(245,158,11,0.12)',  text: '#F59E0B', border: 'rgba(245,158,11,0.3)' },
  red:    { bg: 'rgba(239,68,68,0.12)',   text: '#EF4444', border: 'rgba(239,68,68,0.3)' },
  blue:   { bg: 'rgba(22,140,255,0.12)',  text: '#168CFF', border: 'rgba(22,140,255,0.3)' },
  purple: { bg: 'rgba(139,92,246,0.12)', text: '#8B5CF6', border: 'rgba(139,92,246,0.3)' },
  cyan:   { bg: 'rgba(22,199,232,0.12)', text: '#16C7E8', border: 'rgba(22,199,232,0.3)' },
  muted:  { bg: 'rgba(113,136,155,0.12)', text: '#71889B', border: 'rgba(113,136,155,0.3)' },
}

export function StatusBadge({ label, variant, icon, size = 'sm', dot, pulse }: Props) {
  const s = VARIANT_STYLES[variant]
  const fontSize = size === 'md' ? '12px' : '11px'
  const px = size === 'md' ? '10px' : '7px'
  const py = size === 'md' ? '4px' : '2px'

  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 4,
        background: s.bg,
        color: s.text,
        border: `1px solid ${s.border}`,
        borderRadius: 5,
        padding: `${py} ${px}`,
        fontSize,
        fontWeight: 600,
        letterSpacing: '0.03em',
        textTransform: 'uppercase',
        whiteSpace: 'nowrap',
      }}
      role="status"
      aria-label={label}
    >
      {dot && (
        <span
          className={pulse ? 'animate-pulse' : ''}
          style={{
            width: 6,
            height: 6,
            borderRadius: '50%',
            background: s.text,
            flexShrink: 0,
          }}
        />
      )}
      {icon && !dot && <span style={{ display: 'flex', alignItems: 'center' }}>{icon}</span>}
      {label}
    </span>
  )
}

/** Convenience: live / stale / error badge */
export function FreshnessBadge({ state }: { state: 'live' | 'stale' | 'error' | 'idle' }) {
  if (state === 'live') return <StatusBadge label="Live" variant="green" dot pulse />
  if (state === 'stale') return <StatusBadge label="Stale" variant="yellow" dot />
  if (state === 'error') return <StatusBadge label="Error" variant="red" icon={<WifiOff size={10} />} />
  return <StatusBadge label="Idle" variant="muted" icon={<Clock size={10} />} />
}

/** Convenience: analytics ready/not-ready badge */
export function AnalyticsReadinessBadge({ status }: { status: 'READY' | 'NOT_READY' | 'UNAVAILABLE' | 'DISABLED' | 'UNKNOWN' }) {
  if (status === 'READY') return <StatusBadge label="Analytics Ready" variant="purple" icon={<CheckCircle2 size={10} />} />
  if (status === 'NOT_READY') return <StatusBadge label="Not Ready" variant="yellow" icon={<Clock size={10} />} />
  if (status === 'UNAVAILABLE') return <StatusBadge label="Unavailable" variant="red" icon={<XCircle size={10} />} />
  if (status === 'DISABLED') return <StatusBadge label="Disabled" variant="muted" icon={<Info size={10} />} />
  return <StatusBadge label="Unknown" variant="muted" icon={<AlertTriangle size={10} />} />
}

export function TrafficStatusBadge({ status }: { status: string | null | undefined }) {
  if (!status) return <StatusBadge label="Unknown" variant="muted" />
  const s = status.toUpperCase()
  if (s.includes('LOW') || s.includes('FREE') || s.includes('NORMAL'))
    return <StatusBadge label={status} variant="green" dot />
  if (s.includes('MEDIUM') || s.includes('SLOW'))
    return <StatusBadge label={status} variant="yellow" dot />
  if (s.includes('HIGH') || s.includes('CONGESTED'))
    return <StatusBadge label={status} variant="orange" dot />
  if (s.includes('SEVERE') || s.includes('JAM') || s.includes('BLOCKED'))
    return <StatusBadge label={status} variant="red" dot />
  return <StatusBadge label={status} variant="muted" />
}

/** Random Forest anomaly label badge (NORMAL / CONGESTION / ACCIDENT). */
export function AnomalyLabelBadge({ label }: { label: string | null | undefined }) {
  if (!label) return <StatusBadge label="—" variant="muted" />
  const s = label.toUpperCase()
  if (s === 'NORMAL') return <StatusBadge label="NORMAL" variant="green" dot />
  if (s === 'CONGESTION') return <StatusBadge label="CONGESTION" variant="orange" dot />
  if (s === 'ACCIDENT') return <StatusBadge label="ACCIDENT" variant="red" dot />
  return <StatusBadge label={label} variant="muted" />
}

export { CheckCircle2, AlertTriangle, XCircle, Info, Clock, Wifi, WifiOff }
