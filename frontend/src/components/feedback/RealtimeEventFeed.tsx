// RealtimeEventFeed.tsx — Operations center realtime event feed
// Shows session-only event history derived from Realtime snapshot diffs.
// Color codes:
//   green: control commands applied / normal
//   blue: traffic light phase / scenario change
//   yellow/orange: approach congestion / status change
//   red: spillback / box blocked / incident

import { Activity, Trash2 } from 'lucide-react'
import type { RealtimeFeedEvent } from '@/hooks/useRealtimeEventFeed'

interface Props {
  events: RealtimeFeedEvent[]
  onClear?: () => void
}

const SEVERITY_COLORS: Record<string, { dot: string; glow: string; text: string }> = {
  green:  { dot: '#22C55E', glow: 'rgba(34,197,94,0.4)',   text: '#22C55E' },
  blue:   { dot: '#168CFF', glow: 'rgba(22,140,255,0.4)',  text: '#16C7E8' },
  yellow: { dot: '#FACC15', glow: 'rgba(250,204,21,0.4)', text: '#FACC15' },
  orange: { dot: '#F59E0B', glow: 'rgba(245,158,11,0.4)', text: '#F59E0B' },
  red:    { dot: '#EF4444', glow: 'rgba(239,68,68,0.4)',   text: '#EF4444' },
}

export function RealtimeEventFeed({ events, onClear }: Props) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
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
          <span>Realtime Event Feed</span>
          <span style={{
            fontSize: 10, padding: '1px 6px', borderRadius: 10,
            background: 'rgba(22,140,255,0.12)', color: '#168CFF', fontWeight: 600,
          }}>
            {events.length}
          </span>
        </div>

        {events.length > 0 && onClear && (
          <button
            onClick={onClear}
            style={{
              display: 'flex', alignItems: 'center', gap: 4,
              fontSize: 10, color: 'var(--text-muted)', background: 'transparent',
              border: 'none', cursor: 'pointer', padding: '2px 6px', borderRadius: 4,
            }}
            title="Clear session events"
            aria-label="Clear event feed"
          >
            <Trash2 size={11} /> Clear
          </button>
        )}
      </div>

      {/* Event list */}
      <div
        id="realtime-event-feed-list"
        style={{
          display: 'flex', flexDirection: 'column', gap: 6,
          maxHeight: 240, overflowY: 'auto', paddingRight: 4,
        }}
      >
        {events.length === 0 ? (
          <div style={{
            padding: '24px 12px', textAlign: 'center',
            color: 'var(--text-muted)', fontSize: 11,
          }}>
            No events detected in this session yet.
          </div>
        ) : (
          events.map((evt) => {
            const sc = SEVERITY_COLORS[evt.severity] ?? SEVERITY_COLORS.blue
            return (
              <div
                key={evt.id}
                style={{
                  display: 'flex', alignItems: 'flex-start', gap: 8,
                  padding: '6px 8px', borderRadius: 6,
                  background: 'rgba(10,26,40,0.5)',
                  border: '1px solid rgba(24,58,82,0.4)',
                  fontSize: 11, lineHeight: 1.4,
                  transition: 'background 0.15s ease',
                }}
              >
                {/* Timestamp */}
                <span style={{
                  fontFamily: 'var(--font-mono, monospace)',
                  fontSize: 10, color: 'var(--text-muted)',
                  flexShrink: 0, marginTop: 1,
                }}>
                  {evt.timestamp}
                </span>

                {/* Severity Dot */}
                <span style={{
                  width: 7, height: 7, borderRadius: '50%',
                  background: sc.dot,
                  boxShadow: `0 0 6px ${sc.glow}`,
                  flexShrink: 0, marginTop: 5,
                }} />

                {/* Content */}
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ color: 'var(--text-primary)', fontWeight: 500 }}>
                    {evt.title}
                  </div>
                  {evt.detail && (
                    <div style={{ fontSize: 10, color: 'var(--text-secondary)', marginTop: 1 }}>
                      {evt.detail}
                    </div>
                  )}
                </div>
              </div>
            )
          })
        )}
      </div>
    </div>
  )
}
