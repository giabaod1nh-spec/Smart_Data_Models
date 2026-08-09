// KpiCard.tsx — KPI metric card matching Image 1 reference design
// Large metric number, label, subtitle, icon, optional status indicator

import React from 'react'
import { Skeleton } from '@/components/feedback/LoadingStates'

interface KpiCardProps {
  id: string
  label: string
  value: React.ReactNode
  subtitle?: string
  icon?: React.ReactNode
  iconBg?: string
  isLoading?: boolean
  unavailable?: boolean
  trend?: { direction: 'up' | 'down' | 'neutral'; label: string }
  statusDot?: 'green' | 'yellow' | 'orange' | 'red' | 'muted'
  'aria-label'?: string
}

const DOT_COLORS = {
  green: '#22C55E',
  yellow: '#FACC15',
  orange: '#F59E0B',
  red: '#EF4444',
  muted: '#71889B',
}

export function KpiCard({
  id,
  label,
  value,
  subtitle,
  icon,
  iconBg = 'rgba(22,140,255,0.15)',
  isLoading,
  unavailable,
  trend,
  statusDot,
  'aria-label': ariaLabel,
}: KpiCardProps) {
  return (
    <div
      id={id}
      className="card"
      role="region"
      aria-label={ariaLabel ?? label}
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: 6,
        padding: '14px 16px',
        minWidth: 0,
        flex: 1,
      }}
    >
      {/* Header: icon + label */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        {icon && (
          <div
            style={{
              width: 30,
              height: 30,
              borderRadius: 7,
              background: iconBg,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexShrink: 0,
            }}
            aria-hidden="true"
          >
            {icon}
          </div>
        )}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {statusDot && (
            <span
              style={{
                width: 7,
                height: 7,
                borderRadius: '50%',
                background: DOT_COLORS[statusDot],
                flexShrink: 0,
              }}
            />
          )}
          <span
            style={{
              fontSize: 11,
              fontWeight: 600,
              letterSpacing: '0.06em',
              textTransform: 'uppercase',
              color: 'var(--text-muted)',
            }}
          >
            {label}
          </span>
        </div>
      </div>

      {/* Value */}
      {isLoading ? (
        <Skeleton height={28} width="60%" />
      ) : unavailable ? (
        <div style={{ fontSize: 22, fontWeight: 700, color: 'var(--text-muted)' }} title={subtitle}>
          Unavailable
        </div>
      ) : (
        <div
          style={{
            fontSize: 26,
            fontWeight: 700,
            letterSpacing: '-0.02em',
            color: 'var(--text-primary)',
            lineHeight: 1.1,
          }}
        >
          {value}
        </div>
      )}

      {/* Subtitle / trend */}
      {isLoading ? (
        <Skeleton height={10} width="45%" />
      ) : (
        (subtitle || trend) && (
          <div style={{ fontSize: 11, color: 'var(--text-muted)', display: 'flex', gap: 6, alignItems: 'center' }}>
            {trend && (
              <span
                style={{
                  color:
                    trend.direction === 'up' ? '#22C55E' :
                    trend.direction === 'down' ? '#EF4444' :
                    'var(--text-muted)',
                  fontWeight: 600,
                }}
              >
                {trend.direction === 'up' ? '▲' : trend.direction === 'down' ? '▼' : '→'}{' '}
                {trend.label}
              </span>
            )}
            {subtitle && <span>{subtitle}</span>}
          </div>
        )
      )}
    </div>
  )
}
