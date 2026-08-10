// SchematicMap.tsx — Schematic intersection overview map

import { useNavigate, useSearchParams } from 'react-router-dom'
import type { IntersectionMapNode } from '@/transforms/realtimeTransforms'
import { AlertTriangle, RefreshCw } from 'lucide-react'
import type { IntersectionListStatus } from '@/utils/intersectionListStatus'
import { intersectionListStatusMessage } from '@/utils/intersectionListStatus'
import { appendAnalyticsQuery, setStoredSelectedIntersectionId } from '@/utils/navigation'
import { Skeleton } from '@/components/feedback/LoadingStates'

const NODE_COLOR: Record<string, { bg: string; border: string; text: string }> = {
  green:  { bg: 'rgba(34,197,94,0.15)',  border: '#22C55E', text: '#22C55E' },
  yellow: { bg: 'rgba(250,204,21,0.15)', border: '#FACC15', text: '#FACC15' },
  orange: { bg: 'rgba(245,158,11,0.15)', border: '#F59E0B', text: '#F59E0B' },
  red:    { bg: 'rgba(239,68,68,0.15)',  border: '#EF4444', text: '#EF4444' },
  muted:  { bg: 'rgba(113,136,155,0.1)', border: '#183A52', text: '#71889B' },
}

interface SchematicMapProps {
  nodes: IntersectionMapNode[]
  listStatus: IntersectionListStatus
  statusMessage?: string
  selectedId?: string | null
  onSelect?: (id: string) => void
  onRetry?: () => void
}

export function SchematicMap({
  nodes,
  listStatus,
  statusMessage,
  selectedId,
  onSelect,
  onRetry,
}: SchematicMapProps) {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()

  const handleClick = (id: string) => {
    onSelect?.(id)
    setStoredSelectedIntersectionId(id)
    navigate(appendAnalyticsQuery(`/intersections/${encodeURIComponent(id)}`, searchParams))
  }

  const cols = Math.max(Math.min(nodes.length, 4), 1)
  const isLoading = listStatus === 'loading'
  const isRunMismatch = listStatus === 'run_mismatch'
  const isError = listStatus !== 'loading' && listStatus !== 'success' && listStatus !== 'empty' && !isRunMismatch
  const isEmpty = listStatus === 'empty' || isRunMismatch

  return (
    <div style={{ position: 'relative', width: '100%', minHeight: 280 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 12 }}>
        <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
          Traffic Overview Map
        </div>
        <span
          style={{
            fontSize: 10,
            padding: '1px 7px',
            borderRadius: 4,
            background: 'rgba(22,140,255,0.1)',
            color: 'var(--blue)',
            fontWeight: 600,
          }}
        >
          Schematic Overview
        </span>
      </div>

      <div style={{ display: 'flex', gap: 12, marginBottom: 12 }}>
        {[
          { label: 'Severe', color: '#EF4444' },
          { label: 'High', color: '#F59E0B' },
          { label: 'Medium', color: '#FACC15' },
          { label: 'Low', color: '#22C55E' },
        ].map(({ label, color }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ width: 8, height: 8, borderRadius: '50%', background: color }} />
            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{label}</span>
          </div>
        ))}
      </div>

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: `repeat(${cols}, 1fr)`,
          gap: 12,
          padding: 16,
          background: 'rgba(6,17,31,0.6)',
          borderRadius: 8,
          border: '1px solid var(--border)',
          minHeight: 220,
        }}
        role="list"
        aria-label="Intersection schematic overview"
      >
        {isLoading ? (
          <div style={{ gridColumn: `1 / -1`, padding: 24 }}>
            <Skeleton height={160} width="100%" />
          </div>
        ) : isError ? (
          <div style={{ gridColumn: `1 / -1`, textAlign: 'center', padding: 32 }}>
            <AlertTriangle size={28} color="#EF4444" style={{ marginBottom: 8 }} />
            <div style={{ color: 'var(--text-secondary)', fontSize: 13, fontWeight: 600 }}>
              Unable to load intersections.
            </div>
            <div style={{ color: 'var(--text-muted)', fontSize: 12, marginTop: 6 }}>
              {statusMessage ?? intersectionListStatusMessage(listStatus)}
            </div>
            {onRetry && (
              <button
                onClick={onRetry}
                style={{
                  marginTop: 12,
                  padding: '6px 14px',
                  borderRadius: 6,
                  border: '1px solid var(--border)',
                  background: 'transparent',
                  color: 'var(--blue)',
                  fontSize: 12,
                  cursor: 'pointer',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                }}
              >
                <RefreshCw size={12} /> Retry
              </button>
            )}
          </div>
        ) : isEmpty ? (
          <div style={{ gridColumn: `1 / -1`, textAlign: 'center', padding: 32 }}>
            {isRunMismatch && (
              <AlertTriangle size={28} color="#FACC15" style={{ marginBottom: 8 }} />
            )}
            <div style={{ color: isRunMismatch ? 'var(--text-secondary)' : 'var(--text-muted)', fontSize: 13, fontWeight: isRunMismatch ? 600 : 400 }}>
              {statusMessage ?? intersectionListStatusMessage(listStatus)}
            </div>
          </div>
        ) : (
          nodes.map((node) => {
            const colors = NODE_COLOR[node.statusColor] ?? NODE_COLOR.muted
            const isSelected = node.id === selectedId
            return (
              <button
                key={node.id}
                role="listitem"
                onClick={() => handleClick(node.id)}
                title={`View ${node.displayName} — ${node.overallTrafficStatus ?? 'Unknown'}\n${node.id}`}
                aria-label={`${node.displayName}, traffic status: ${node.overallTrafficStatus ?? 'Unknown'}. Click to view details.`}
                style={{
                  background: isSelected ? colors.bg : 'rgba(12,34,53,0.8)',
                  border: `1.5px solid ${isSelected ? colors.border : `${colors.border}80`}`,
                  borderRadius: 10,
                  padding: '12px 10px',
                  cursor: 'pointer',
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  gap: 6,
                  transition: 'all 0.15s',
                  outline: 'none',
                  position: 'relative',
                  textAlign: 'center',
                  boxShadow: isSelected ? `0 0 12px ${colors.border}40` : 'none',
                }}
              >
                <span
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: '50%',
                    background: colors.border,
                    boxShadow: `0 0 6px ${colors.border}80`,
                  }}
                  aria-hidden="true"
                />
                <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-primary)', lineHeight: 1.3 }}>
                  {node.displayName}
                </span>
                <span style={{ fontSize: 10, color: colors.text }}>
                  {node.overallTrafficStatus ?? 'Unknown'}
                </span>
                {node.totalVehicleCount !== null && (
                  <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>
                    {node.totalVehicleCount} veh
                  </span>
                )}
                {node.hasActiveIncident && (
                  <span style={{ position: 'absolute', top: 6, right: 6 }} title="Active incident">
                    <AlertTriangle size={12} color="#EF4444" />
                  </span>
                )}
              </button>
            )
          })
        )}
      </div>
    </div>
  )
}
