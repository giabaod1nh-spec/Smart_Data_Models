/** HUD for cooperative DQN agents on Live Traffic view */
import type { LiveAgentStatus, LiveGlobalMetrics } from '@/types/liveTraffic'

interface Props {
  agents?: LiveAgentStatus[]
  globalMetrics?: LiveGlobalMetrics
}

export function AgentStatusPanel({ agents, globalMetrics }: Props) {
  if (!agents?.length) return null

  return (
    <div
      style={{
        position: 'absolute',
        top: 8,
        right: 8,
        zIndex: 20,
        maxWidth: 280,
        background: 'rgba(6,17,31,0.92)',
        border: '1px solid rgba(24,58,82,0.8)',
        borderRadius: 8,
        padding: '10px 12px',
        fontSize: 11,
        color: '#E2E8F0',
        pointerEvents: 'none',
      }}
    >
      <div style={{ fontWeight: 700, fontSize: 10, letterSpacing: '0.06em', marginBottom: 8, color: '#16C7E8' }}>
        DQN AGENTS
      </div>
      {globalMetrics && (
        <div style={{ marginBottom: 8, paddingBottom: 8, borderBottom: '1px solid rgba(24,58,82,0.6)' }}>
          <div>Global reward: {globalMetrics.globalReward ?? '—'}</div>
          <div>Avg queue: {globalMetrics.averageQueue ?? '—'}</div>
          {(globalMetrics.spillbackPenalty ?? 0) > 0 && (
            <div style={{ color: '#F59E0B', marginTop: 4 }}>Spillback warning</div>
          )}
        </div>
      )}
      {agents.map((a) => (
        <div key={a.id} style={{ marginBottom: 6 }}>
          <div style={{ fontWeight: 700 }}>Agent {a.id}</div>
          <div style={{ color: '#94A3B8', lineHeight: 1.4 }}>
            {a.phase ?? '—'} · {a.action ?? '—'}
            <br />
            Q:{a.queue ?? 0} W:{a.waiting ?? 0} R:{a.reward ?? 0}
            {a.spillback_detected && <span style={{ color: '#F59E0B' }}> · spillback</span>}
          </div>
        </div>
      ))}
    </div>
  )
}
