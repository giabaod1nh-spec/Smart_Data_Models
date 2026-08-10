// TrafficLightPanel.tsx — Streamlined Traffic Lights Status Panel
// Matching target mockup (Image 2):
// - 4 direction cards in 2x2 grid (North, East, West, South)
// - Visual traffic light housing with active lamp glowing
// - Big bold remaining countdown seconds
// - Direction name with directional arrow (colored green/yellow/red)
// - Realtime countdown synchronization:
//     * Green: configured green - elapsed
//     * Yellow: yellow duration (3s) - elapsed
//     * Red: opposing green remaining + yellow duration, or redDurationCurrent - elapsed, or "—" if not derivable
// - NO FIXED_TIME clutter, NO Configured G/Y/R clutter on main UI
// - Countdown freeze on paused/stale/offline
// - Never self-advances phase when countdown reaches 0s

import { useMemo } from 'react'
import { useCountdown } from '@/hooks/useCountdown'
import type { TrafficLightView, FreshnessState } from '@/transforms/realtimeTransforms'
import { computeCountdownSec } from '@/transforms/realtimeTransforms'

const DIR_ARROWS: Record<string, string> = {
  North: '↑',
  South: '↑',
  East: '→',
  West: '→',
}

const ORDERED_DIRS = ['North', 'East', 'West', 'South']

interface TrafficLightCardProps {
  light: TrafficLightView | undefined
  dir: string
  freshnessState: FreshnessState
  isPaused: boolean
  fallbackStatus: 'GREEN' | 'YELLOW' | 'RED' | null
  opposingGreenLight?: TrafficLightView
  currentPhase?: string | null
}

function TrafficLightCard({
  light,
  dir,
  freshnessState,
  isPaused,
  fallbackStatus,
  opposingGreenLight,
  currentPhase,
}: TrafficLightCardProps) {
  const effectiveLight: TrafficLightView | undefined = light ?? (fallbackStatus ? {
    id: `tl-${dir.toLowerCase()}`,
    direction: dir,
    currentStatus: fallbackStatus,
    currentPhase: currentPhase ?? null,
    timingMode: 'FIXED_TIME',
    workingState: 'OK',
    greenDurationCurrent: null,
    redDurationCurrent: null,
    yellowDuration: 3,
    phaseStartedAt: null,
    simulationTime: null,
    simulationRunId: null,
  } : undefined)

  const { remainingSec: directRemainingSec, isSyncing: directIsSyncing, isFrozen } = useCountdown(
    effectiveLight,
    freshnessState,
    isPaused,
  )

  const rawStatus = (light?.currentStatus ?? fallbackStatus ?? '').toUpperCase()
  const isGreen = rawStatus.includes('GREEN')
  const isYellow = rawStatus.includes('YELLOW')
  const isRed = rawStatus.includes('RED') || (!isGreen && !isYellow && rawStatus !== '')

  // Calculate remaining countdown
  const derivedRemaining = useMemo(() => {
    // 1. Direct countdown from light if available
    if (directRemainingSec !== null && !isNaN(directRemainingSec)) {
      return directRemainingSec
    }

    // 2. If this light is RED, derive remaining time until next Green from active cycle if possible
    if (isRed && opposingGreenLight && opposingGreenLight.phaseStartedAt) {
      const opposingGreenSec = computeCountdownSec(opposingGreenLight)
      if (opposingGreenSec !== null && !isNaN(opposingGreenSec)) {
        const yellowDur = opposingGreenLight.yellowDuration ?? 3
        const oppStatus = (opposingGreenLight.currentStatus ?? '').toUpperCase()
        if (oppStatus.includes('GREEN')) {
          // East/West Red remaining = opposing green remaining + yellow duration
          return opposingGreenSec + yellowDur
        } else if (oppStatus.includes('YELLOW')) {
          // East/West Red remaining = opposing yellow remaining
          return opposingGreenSec
        }
      }
    }

    return null
  }, [directRemainingSec, isRed, opposingGreenLight])

  const arrow = DIR_ARROWS[dir] ?? '↑'
  const arrowColor = isGreen ? '#22C55E' : isYellow ? '#FACC15' : isRed ? '#EF4444' : '#71889B'

  // Format remaining seconds
  let countdownText = '—'
  const isSyncing = directIsSyncing || (derivedRemaining !== null && derivedRemaining === 0)
  if (derivedRemaining !== null) {
    countdownText = isSyncing ? '0s' : `${Math.ceil(derivedRemaining)}s`
  }

  return (
    <div
      style={{
        background: 'rgba(6, 17, 31, 0.65)',
        border: '1px solid rgba(24, 58, 82, 0.7)',
        borderRadius: 10,
        padding: '12px 14px',
        display: 'flex',
        flexDirection: 'column',
        gap: 10,
        minHeight: 110,
        position: 'relative',
      }}
    >
      {/* Direction Header: NORTH ↑ */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div style={{
          fontSize: 12,
          fontWeight: 700,
          color: '#F8FAFC',
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          display: 'flex',
          alignItems: 'center',
          gap: 6,
        }}>
          <span>{dir}</span>
          <span style={{ color: arrowColor, fontSize: 14, fontWeight: 800 }}>{arrow}</span>
        </div>

        {/* Small freeze badge if paused or stale */}
        {isFrozen && (
          <span style={{
            fontSize: 9,
            fontWeight: 600,
            color: effectiveLight?.timingMode === 'MANUAL' ? '#F59E0B' : '#FACC15',
            background: effectiveLight?.timingMode === 'MANUAL'
              ? 'rgba(245,158,11,0.12)'
              : 'rgba(250,204,21,0.12)',
            padding: '1px 5px',
            borderRadius: 3,
            textTransform: 'uppercase',
          }}>
            {effectiveLight?.timingMode === 'MANUAL'
              ? 'Manual'
              : isPaused
              ? 'Delayed'
              : 'Stale'}
          </span>
        )}
      </div>

      {/* Body: Vertical Traffic Light Housing + Remaining Countdown */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginTop: 'auto', marginBottom: 'auto' }}>
        {/* Sleek Vertical Traffic Light Housing */}
        <div
          style={{
            background: '#040C16',
            border: '1.5px solid #10263C',
            borderRadius: 16,
            padding: '5px 4px',
            display: 'flex',
            flexDirection: 'column',
            gap: 5,
            boxShadow: 'inset 0 2px 4px rgba(0,0,0,0.6)',
            flexShrink: 0,
          }}
          aria-label={`${dir} signal: ${isGreen ? 'Green' : isYellow ? 'Yellow' : 'Red'}`}
        >
          {/* Red Lamp */}
          <div
            style={{
              width: 14,
              height: 14,
              borderRadius: '50%',
              background: isRed ? '#EF4444' : '#152232',
              boxShadow: isRed ? '0 0 10px rgba(239,68,68,0.85), inset 0 0 2px #FFFFFF' : 'none',
              border: isRed ? '1px solid rgba(255,255,255,0.4)' : '1px solid rgba(255,255,255,0.04)',
              transition: 'all 0.25s ease',
            }}
          />
          {/* Yellow Lamp */}
          <div
            style={{
              width: 14,
              height: 14,
              borderRadius: '50%',
              background: isYellow ? '#FACC15' : '#152232',
              boxShadow: isYellow ? '0 0 10px rgba(250,204,21,0.85), inset 0 0 2px #FFFFFF' : 'none',
              border: isYellow ? '1px solid rgba(255,255,255,0.4)' : '1px solid rgba(255,255,255,0.04)',
              transition: 'all 0.25s ease',
            }}
          />
          {/* Green Lamp */}
          <div
            style={{
              width: 14,
              height: 14,
              borderRadius: '50%',
              background: isGreen ? '#22C55E' : '#152232',
              boxShadow: isGreen ? '0 0 10px rgba(34,197,94,0.85), inset 0 0 2px #FFFFFF' : 'none',
              border: isGreen ? '1px solid rgba(255,255,255,0.4)' : '1px solid rgba(255,255,255,0.04)',
              transition: 'all 0.25s ease',
            }}
          />
        </div>

        {/* Big Remaining Countdown */}
        <div style={{ display: 'flex', flexDirection: 'column' }}>
          <div
            style={{
              fontSize: 22,
              fontWeight: 800,
              color: isFrozen ? '#71889B' : '#F8FAFC',
              fontFamily: 'var(--font-mono, monospace)',
              fontVariantNumeric: 'tabular-nums',
              lineHeight: 1.1,
            }}
          >
            {countdownText}
          </div>
          {isSyncing && (
            <div style={{ fontSize: 9, color: '#16C7E8', marginTop: 2 }}>
              Syncing…
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export interface TrafficLightPanelProps {
  lights: TrafficLightView[]
  freshnessState: FreshnessState
  isPaused: boolean
  currentPhase?: string | null
  intersectionDisplayName?: string
}

export function TrafficLightPanel({
  lights,
  freshnessState,
  isPaused,
  currentPhase,
}: TrafficLightPanelProps) {
  // Determine fallback status per direction from currentPhase
  const phaseUpper = (currentPhase ?? '').toUpperCase()
  const isNSGreen = phaseUpper === 'NS_GREEN'
  const isNSYellow = phaseUpper === 'NS_YELLOW'
  const isEWGreen = phaseUpper === 'EW_GREEN'
  const isEWYellow = phaseUpper === 'EW_YELLOW'

  const getFallback = (dir: string): 'GREEN' | 'YELLOW' | 'RED' | null => {
    if (dir === 'North' || dir === 'South') {
      if (isNSGreen) return 'GREEN'
      if (isNSYellow) return 'YELLOW'
      if (isEWGreen || isEWYellow) return 'RED'
    } else if (dir === 'East' || dir === 'West') {
      if (isEWGreen) return 'GREEN'
      if (isEWYellow) return 'YELLOW'
      if (isNSGreen || isNSYellow) return 'RED'
    }
    return null
  }

  // Find currently active green/yellow light for cycle-derived red timing
  const activeGreenLight = lights.find((l) => {
    const s = (l.currentStatus ?? '').toUpperCase()
    return s.includes('GREEN') || s.includes('YELLOW')
  })

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, position: 'relative' }}>
      {/* 2x2 Grid matching Image 2 mockup: North | East / West | South */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(2, 1fr)',
          gap: 10,
          position: 'relative',
        }}
      >
        {ORDERED_DIRS.map((dir) => {
          const light = lights.find((l) => l.direction === dir)
          const fallback = getFallback(dir)
          return (
            <TrafficLightCard
              key={dir}
              dir={dir}
              light={light}
              fallbackStatus={fallback}
              freshnessState={freshnessState}
              isPaused={isPaused}
              opposingGreenLight={activeGreenLight}
              currentPhase={currentPhase}
            />
          )
        })}
      </div>
    </div>
  )
}
