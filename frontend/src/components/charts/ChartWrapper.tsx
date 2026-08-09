// ChartWrapper.tsx — Wrapper providing title, loading, empty, error states for all Recharts charts

import type React from 'react'
import { Skeleton, EmptyState, ErrorState } from '@/components/feedback/LoadingStates'
import { classifyAnalyticsError } from '@/transforms/analyticsTransforms'

interface ChartWrapperProps {
  id: string
  title: string
  subtitle?: string
  isLoading?: boolean
  isEmpty?: boolean
  error?: unknown
  onRetry?: () => void
  height?: number
  headerRight?: React.ReactNode
  children: React.ReactNode
}

export function ChartWrapper({
  id,
  title,
  subtitle,
  isLoading,
  isEmpty,
  error,
  onRetry,
  height = 200,
  headerRight,
  children,
}: ChartWrapperProps) {
  return (
    <div id={id} className="card" style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>{title}</div>
          {subtitle && (
            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>{subtitle}</div>
          )}
        </div>
        {headerRight}
      </div>

      {/* Chart area */}
      <div
        role="img"
        aria-label={`Chart: ${title}`}
        style={{ minHeight: height }}
      >
        {isLoading ? (
          <Skeleton height={height} width="100%" />
        ) : error ? (
          (() => {
            const errType = classifyAnalyticsError(error)
            if (errType === 'ANALYTICS_NOT_READY') {
              return (
                <ErrorState
                  variant="not-ready"
                  title="Analytics Not Ready"
                  message="Network analytics mart is not populated yet."
                />
              )
            }
            if (errType === 'ANALYTICS_DISABLED') {
              return (
                <ErrorState
                  variant="disabled"
                  title="Analytics Disabled"
                  message="Enable the analytics Spring profile to access this data."
                />
              )
            }
            return (
              <ErrorState
                variant="error"
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                message={(error as any)?.response?.data?.message ?? 'Failed to load chart data.'}
                retryable={Boolean(onRetry)}
                onRetry={onRetry}
              />
            )
          })()
        ) : isEmpty ? (
          <EmptyState
            title="No data"
            description="No analytics rows match the selected run, scenario and window."
          />
        ) : (
          children
        )}
      </div>
    </div>
  )
}
