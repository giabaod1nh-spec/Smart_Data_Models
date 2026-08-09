// LoadingStates.tsx — Skeleton loaders and empty/error state components

import React from 'react'
import { AlertCircle, PackageOpen, RefreshCw, WifiOff, Lock } from 'lucide-react'

// ── Skeleton ──────────────────────────────────────────────────────────────────
export function Skeleton({
  width,
  height = 16,
  className,
  style,
}: {
  width?: string | number
  height?: string | number
  className?: string
  style?: React.CSSProperties
}) {
  return (
    <span
      className={className}
      aria-hidden="true"
      style={{
        display: 'inline-block',
        width: width ?? '100%',
        height,
        borderRadius: 6,
        background: 'linear-gradient(90deg, #0C2235 25%, #102A40 50%, #0C2235 75%)',
        backgroundSize: '800px 100%',
        animation: 'shimmer 1.5s infinite',
        ...style,
      }}
    />
  )
}

export function SkeletonCard({ rows = 3 }: { rows?: number }) {
  return (
    <div className="card" style={{ padding: 16 }}>
      <Skeleton height={14} width="40%" />
      <div style={{ marginTop: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
        {Array.from({ length: rows }).map((_, i) => (
          <Skeleton key={i} height={12} width={`${70 + (i % 3) * 10}%`} />
        ))}
      </div>
    </div>
  )
}

export function SkeletonChart({ height = 180 }: { height?: number }) {
  return (
    <div className="card" style={{ padding: 16 }}>
      <Skeleton height={14} width="50%" />
      <div style={{ marginTop: 16, height }}>
        <Skeleton height={height} width="100%" />
      </div>
    </div>
  )
}

export function SkeletonKpiCard() {
  return (
    <div className="card" style={{ padding: '14px 18px' }}>
      <Skeleton height={12} width="60%" />
      <Skeleton height={28} width="50%" style={{ marginTop: 8 }} />
      <Skeleton height={10} width="40%" style={{ marginTop: 8 }} />
    </div>
  )
}

// ── Empty state ────────────────────────────────────────────────────────────────
interface EmptyStateProps {
  title: string
  description?: string
  icon?: React.ReactNode
  action?: React.ReactNode
}

export function EmptyState({ title, description, icon, action }: EmptyStateProps) {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '32px 16px',
        textAlign: 'center',
        gap: 8,
      }}
    >
      <div style={{ color: 'var(--text-muted)', marginBottom: 4 }}>
        {icon ?? <PackageOpen size={32} />}
      </div>
      <div style={{ color: 'var(--text-secondary)', fontWeight: 600, fontSize: 14 }}>{title}</div>
      {description && (
        <div style={{ color: 'var(--text-muted)', fontSize: 12, maxWidth: 320 }}>{description}</div>
      )}
      {action && <div style={{ marginTop: 8 }}>{action}</div>}
    </div>
  )
}

// ── Error state ────────────────────────────────────────────────────────────────
interface ErrorStateProps {
  title?: string
  message?: string
  retryable?: boolean
  onRetry?: () => void
  variant?: 'error' | 'forbidden' | 'not-ready' | 'disabled'
}

export function ErrorState({
  title,
  message,
  retryable,
  onRetry,
  variant = 'error',
}: ErrorStateProps) {
  const icon =
    variant === 'forbidden' ? <Lock size={28} /> :
    variant === 'not-ready' ? <RefreshCw size={28} /> :
    variant === 'disabled' ? <WifiOff size={28} /> :
    <AlertCircle size={28} />

  const defaultTitle =
    variant === 'forbidden' ? 'Access Denied' :
    variant === 'not-ready' ? 'Data Not Ready' :
    variant === 'disabled' ? 'Feature Disabled' :
    'Error'

  const color =
    variant === 'not-ready' ? 'var(--yellow)' :
    variant === 'disabled' ? 'var(--text-muted)' :
    variant === 'forbidden' ? 'var(--orange)' :
    'var(--red)'

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '24px 16px',
        textAlign: 'center',
        gap: 6,
      }}
    >
      <div style={{ color, marginBottom: 4 }}>{icon}</div>
      <div style={{ color: 'var(--text-secondary)', fontWeight: 600, fontSize: 13 }}>
        {title ?? defaultTitle}
      </div>
      {message && (
        <div style={{ color: 'var(--text-muted)', fontSize: 11, maxWidth: 320 }}>{message}</div>
      )}
      {retryable && onRetry && (
        <button
          onClick={onRetry}
          style={{
            marginTop: 8,
            padding: '6px 14px',
            borderRadius: 6,
            border: '1px solid var(--border)',
            background: 'transparent',
            color: 'var(--blue)',
            fontSize: 12,
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: 4,
          }}
        >
          <RefreshCw size={12} /> Retry
        </button>
      )}
    </div>
  )
}

// ── Analytics not ready panel ──────────────────────────────────────────────────
export function AnalyticsNotReadyPanel({ compact }: { compact?: boolean }) {
  return (
    <div
      style={{
        borderRadius: 8,
        border: '1px solid rgba(250,204,21,0.2)',
        background: 'rgba(250,204,21,0.05)',
        padding: compact ? '12px 16px' : '20px 24px',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <RefreshCw size={16} style={{ color: 'var(--yellow)', flexShrink: 0 }} />
        <div>
          <div style={{ color: 'var(--yellow)', fontSize: 13, fontWeight: 600 }}>
            Network analytics is not ready yet.
          </div>
          {!compact && (
            <div style={{ color: 'var(--text-muted)', fontSize: 11, marginTop: 4 }}>
              The Gold network mart is not populated. Intersection-level analytics may still be available.
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Analytics disabled panel ───────────────────────────────────────────────────
export function AnalyticsDisabledPanel() {
  return (
    <div
      style={{
        borderRadius: 8,
        border: '1px solid rgba(113,136,155,0.2)',
        background: 'rgba(113,136,155,0.05)',
        padding: '20px 24px',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <WifiOff size={16} style={{ color: 'var(--text-muted)', flexShrink: 0 }} />
        <div>
          <div style={{ color: 'var(--text-secondary)', fontSize: 13, fontWeight: 600 }}>
            Analytics feature is disabled.
          </div>
          <div style={{ color: 'var(--text-muted)', fontSize: 11, marginTop: 4 }}>
            Enable with Spring profile <code style={{ color: 'var(--cyan)' }}>analytics</code> or set{' '}
            <code style={{ color: 'var(--cyan)' }}>app.analytics.enabled=true</code>.
          </div>
        </div>
      </div>
    </div>
  )
}
