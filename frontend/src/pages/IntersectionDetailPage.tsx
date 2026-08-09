// IntersectionDetailPage.tsx — Realtime detail for one intersection
// Route: /intersections/:intersectionId
// Data: GET /api/realtime/intersections/{id} polled every 2s
// Reverse control: POST /api/control/** (Spring proxy only)
//
// LAYOUT (desktop ≥1440px):
//   ROW 1: KPI row (6 cards)
//   ROW 2: [Summary + Direction Counts + Scenario Control 20%] | [Live Canvas 58%] | [Traffic Lights 22%]
//   ROW 3: [Realtime Event Feed 32%] | [Signal Control Tabs 36%] | [Vehicle Sensor Table 32%]
//
// AVG SPEED FIX: only uses VehicleSensor.averageSpeed — NEVER trafficStatus/derivedTrafficState
// OCCUPANCY FIX: formatOccupancyRate scale 0-100 (47.0 → 47.0%, not 4700%)
// FRIENDLY NAME: getIntersectionDisplayName(id, name) — URN only in tooltip
// NO OPTIMISTIC CONTROL: UI waits for Realtime state after command
// COUNTDOWN: live wall-clock countdown, resync on each Realtime response

import { useMemo, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams, useLocation } from 'react-router-dom'
import {
  ArrowLeft, Car, Gauge, MapPin, AlertTriangle,
  Activity, LayoutGrid, Clock, Calendar,
} from 'lucide-react'
import { useRealtimeIntersection } from '@/hooks/useRealtimeIntersection'
import { useSimulationPauseDetector } from '@/hooks/useSimulationPauseDetector'
import { useRealtimeEventFeed } from '@/hooks/useRealtimeEventFeed'
import {
  mapSensorsToDirections,
  mapTrafficLights,
  sumVehicleCount,
  formatAvgSpeed,
  formatSpeedKmh,
  formatOccupancyRate,
  formatPhaseLabel,
  formatScenarioLabel,
  getFreshnessState,
  formatSimSec,
  deriveRealtimePageStatus,
} from '@/transforms/realtimeTransforms'
import { getIntersectionDisplayName } from '@/utils/intersectionDisplayName'
import { KpiCard } from '@/components/cards/KpiCard'
import { IntersectionScene } from '@/components/canvas/IntersectionScene'
import { ReverseControlPanel } from '@/components/control/ReverseControlPanel'
import { ScenarioControlCard } from '@/components/control/ScenarioControlCard'
import { TrafficLightPanel } from '@/components/feedback/TrafficLightPanel'
import { RealtimeEventFeed } from '@/components/feedback/RealtimeEventFeed'
import { TrafficStatusBadge } from '@/components/feedback/StatusBadge'
import { SkeletonKpiCard, ErrorState, Skeleton } from '@/components/feedback/LoadingStates'
import { setStoredSelectedIntersectionId } from '@/utils/navigation'
import { classifyApiError } from '@/utils/apiErrors'

type Dir = 'North' | 'South' | 'East' | 'West'
const DIRECTIONS: Dir[] = ['North', 'South', 'East', 'West']
const DIR_ARROW: Record<string, string> = { North: '↑', South: '↓', East: '→', West: '←' }

// Page status badge
function PageStatusBadge({ status }: { status: string }) {
  const cfg: Record<string, { color: string; bg: string; border: string; pulse: boolean }> = {
    LIVE:    { color: '#22C55E', bg: 'rgba(34,197,94,0.12)',   border: 'rgba(34,197,94,0.3)',   pulse: true  },
    PAUSED:  { color: '#FACC15', bg: 'rgba(250,204,21,0.12)',  border: 'rgba(250,204,21,0.3)',  pulse: false },
    STALE:   { color: '#F59E0B', bg: 'rgba(245,158,11,0.12)',  border: 'rgba(245,158,11,0.3)',  pulse: false },
    WAITING: { color: '#71889B', bg: 'rgba(113,136,155,0.1)',  border: 'rgba(113,136,155,0.25)', pulse: false },
    OFFLINE: { color: '#EF4444', bg: 'rgba(239,68,68,0.12)',   border: 'rgba(239,68,68,0.3)',   pulse: false },
    ERROR:   { color: '#EF4444', bg: 'rgba(239,68,68,0.12)',   border: 'rgba(239,68,68,0.3)',   pulse: false },
    UNKNOWN: { color: '#71889B', bg: 'rgba(113,136,155,0.1)',  border: 'rgba(113,136,155,0.25)', pulse: false },
  }
  const c = cfg[status] ?? cfg.UNKNOWN
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      padding: '4px 10px', borderRadius: 20,
      background: c.bg, border: `1px solid ${c.border}`,
      fontSize: 11, fontWeight: 700, color: c.color,
      textTransform: 'uppercase', letterSpacing: '0.06em',
    }}>
      <span style={{
        width: 7, height: 7, borderRadius: '50%',
        background: c.color, flexShrink: 0,
        animation: c.pulse ? 'pulse 2s ease infinite' : 'none',
      }} />
      {status}
    </span>
  )
}

export function IntersectionDetailPage() {
  const { intersectionId } = useParams<{ intersectionId: string }>()
  const navigate = useNavigate()
  const location = useLocation()
  const [searchParams] = useSearchParams()

  const { data, isLoading, isError, error, refetch } = useRealtimeIntersection(intersectionId)

  // Realtime Event Feed Hook
  const { events, addCommandEvent, clearFeed } = useRealtimeEventFeed(data)

  useEffect(() => {
    if (intersectionId) setStoredSelectedIntersectionId(intersectionId)
  }, [intersectionId])

  // Sync run/scenario to URL from realtime metadata for Back → Analytics
  useEffect(() => {
    if (!data?.metadata) return
    const params = new URLSearchParams(searchParams)
    let changed = false
    const runId = data.metadata.simulationRunId
    const scen = data.metadata.scenarioId
    if (runId && !params.get('simulationRunId')) { params.set('simulationRunId', runId); changed = true }
    if (scen && !params.get('scenarioId')) { params.set('scenarioId', scen); changed = true }
    if (intersectionId && !params.get('intersectionId')) { params.set('intersectionId', intersectionId); changed = true }
    if (changed) navigate({ pathname: location.pathname, search: params.toString() }, { replace: true })
  }, [data?.metadata, intersectionId, searchParams, navigate, location.pathname])

  const intersection = data?.intersection
  const sensors = useMemo(() => data?.vehicleSensors ?? [], [data?.vehicleSensors])
  const lights = useMemo(() => data?.trafficLights ?? [], [data?.trafficLights])
  const metadata = data?.metadata

  const dirSensors = useMemo(() => mapSensorsToDirections(sensors), [sensors])
  const lightViews = useMemo(() => mapTrafficLights(lights), [lights])

  // AVG SPEED FIX: only from averageSpeed numeric values, never traffic status strings
  const avgSpeedDisplay = formatAvgSpeed(sensors)

  const totalVehicles = useMemo(
    () => intersection?.totalVehicleCount ?? sumVehicleCount(sensors),
    [intersection, sensors],
  )

  const freshnessState = useMemo(
    () => getFreshnessState(metadata, isError),
    [metadata, isError],
  )

  // Simulation pause detection (feeds into countdown freeze and animation)
  const isPaused = useSimulationPauseDetector(metadata?.simulationTime)

  const pageStatus = useMemo(
    () => deriveRealtimePageStatus(freshnessState, isPaused),
    [freshnessState, isPaused],
  )

  // Friendly display name — URN only in tooltip/technical details
  const displayName = getIntersectionDisplayName(intersectionId, intersection?.name)

  // Current phase (from intersection or first light)
  const currentPhase = intersection?.currentPhase ?? lights[0]?.currentPhase ?? null
  const currentPhaseFriendly = formatPhaseLabel(currentPhase)

  // Total queue length (presentation aggregate)
  const totalQueueLength = dirSensors.reduce((acc, s) => acc + (s.queueLength ?? 0), 0)

  // Scenario friendly label
  const rawScenario = intersection?.scenarioId ?? metadata?.scenarioId ?? null
  const scenarioDisplay = formatScenarioLabel(rawScenario)

  // Configured green duration from first green light
  const currentConfiguredGreen = lights[0]?.greenDurationCurrent ?? null

  const buildBackUrl = () => {
    const queryStr = searchParams.toString()
    return `/analytics${queryStr ? '?' + queryStr : ''}`
  }

  if (!intersectionId) {
    return (
      <div style={{ padding: 24 }}>
        <ErrorState title="Invalid route" message="Intersection ID is missing from the URL." />
      </div>
    )
  }

  if (isError && !data) {
    const errKind = classifyApiError(error)
    return (
      <div style={{ padding: 24 }}>
        <BackButton backUrl={buildBackUrl()} navigate={navigate} />
        <div className="card" style={{ marginTop: 16, padding: 32 }}>
          <ErrorState
            title={errKind === 'not_found' ? 'Intersection not found' : 'Failed to load intersection'}
            message={(error as { response?: { data?: { message?: string } } })?.response?.data?.message ?? 'Could not load realtime data.'}
            retryable={errKind !== 'not_found' && errKind !== 'forbidden'}
            onRetry={() => void refetch()}
          />
        </div>
      </div>
    )
  }

  return (
    <div style={{ padding: '14px 18px', display: 'flex', flexDirection: 'column', gap: 14 }}>

      {/* ── HEADER / PAGE TITLE ── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <BackButton backUrl={buildBackUrl()} navigate={navigate} />
        <div style={{ flex: 1, minWidth: 0 }}>
          {isLoading ? (
            <Skeleton height={22} width={280} />
          ) : (
            <h1 style={{ fontSize: 18, fontWeight: 700, color: 'var(--text-primary)', margin: 0, display: 'flex', alignItems: 'center', gap: 10 }}>
              {displayName}
              <PageStatusBadge status={pageStatus} />
            </h1>
          )}
          {!isLoading && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginTop: 4, flexWrap: 'wrap' }}>
              {/* Full URN in tooltip */}
              {intersection?.id && (
                <span
                  style={{ fontSize: 10, color: 'var(--text-muted)', cursor: 'help', display: 'flex', alignItems: 'center', gap: 3 }}
                  title={`Full URN: ${intersection.id}`}
                >
                  <MapPin size={10} />
                  {intersection.id}
                </span>
              )}
              {scenarioDisplay !== '—' && (
                <span style={{ fontSize: 11, padding: '2px 7px', borderRadius: 4, background: 'rgba(22,140,255,0.12)', color: '#168CFF', fontWeight: 600 }}>
                  Scenario: {scenarioDisplay}
                </span>
              )}
              {metadata?.simulationTime !== null && metadata?.simulationTime !== undefined && (
                <span style={{ fontSize: 10, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 3 }}>
                  <Clock size={10} /> Sim Time: {formatSimSec(metadata.simulationTime)}
                </span>
              )}
              <span style={{ fontSize: 10, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 3 }}>
                <Clock size={10} /> Last Update: {new Date().toLocaleTimeString()}
              </span>
              {metadata?.consistent === false && (
                <span style={{ fontSize: 10, color: '#F59E0B', display: 'flex', alignItems: 'center', gap: 3 }}>
                  <AlertTriangle size={10} /> Inconsistent
                </span>
              )}
            </div>
          )}
        </div>
      </div>

      {/* ── ROW 1: KPI CARDS (6 Cards) ── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 10 }}>
        {isLoading ? (
          Array.from({ length: 6 }).map((_, i) => <SkeletonKpiCard key={i} />)
        ) : (
          <>
            <KpiCard
              id="kpi-detail-vehicles"
              label="Total Vehicles"
              value={totalVehicles !== null && totalVehicles !== undefined ? totalVehicles.toLocaleString() : '—'}
              icon={<Car size={16} color="#16C7E8" />}
              iconBg="rgba(22,199,232,0.15)"
            />
            {/* AVG SPEED — strictly numeric km/h from averageSpeed */}
            <KpiCard
              id="kpi-detail-speed"
              label="Average Speed"
              value={avgSpeedDisplay}
              subtitle="Average across sensors"
              icon={<Gauge size={16} color="#22C55E" />}
              iconBg="rgba(34,197,94,0.15)"
            />
            <KpiCard
              id="kpi-detail-queue"
              label="Queue Length"
              value={totalQueueLength > 0 ? `${totalQueueLength.toFixed(0)} m` : '—'}
              subtitle="Aggregated from directional sensors"
              icon={<Activity size={16} color="#F59E0B" />}
              iconBg="rgba(245,158,11,0.15)"
            />
            <KpiCard
              id="kpi-detail-traffic-status"
              label="Traffic Status"
              value={<TrafficStatusBadge status={intersection?.overallTrafficStatus} />}
              subtitle="Overall intersection status"
              icon={<Activity size={16} color="#EF4444" />}
              iconBg="rgba(239,68,68,0.15)"
            />
            <KpiCard
              id="kpi-detail-scenario"
              label="Current Scenario"
              value={scenarioDisplay}
              subtitle="Active"
              icon={<Calendar size={16} color="#168CFF" />}
              iconBg="rgba(22,140,255,0.15)"
            />
            <KpiCard
              id="kpi-detail-phase"
              label="Current Phase"
              value={currentPhaseFriendly}
              subtitle={currentPhase ?? '—'}
              icon={<LayoutGrid size={16} color="#8B5CF6" />}
              iconBg="rgba(139,92,246,0.15)"
            />
          </>
        )}
      </div>

      {/* ── ROW 2: MAIN REALTIME AREA (Left 20% | Center 58% | Right 22%) ── */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'minmax(200px, 20fr) minmax(0, 58fr) minmax(220px, 22fr)',
        gap: 14, alignItems: 'start',
      }}>
        {/* Left: Summary + Direction Counts + Scenario Control */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {/* Intersection summary */}
          <div className="card">
            <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 10, display: 'flex', alignItems: 'center', gap: 6 }}>
              <MapPin size={13} style={{ color: 'var(--blue)' }} />
              Summary
            </div>
            {isLoading ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} height={12} width={`${60 + i * 8}%`} />)}
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
                {([
                  ['Vehicles', totalVehicles ?? '—'],
                  ['Traffic Status', intersection?.overallTrafficStatus ?? '—'],
                  ['Derived State', intersection?.derivedTrafficState ?? '—'],
                  ['Spillback', intersection?.hasSpillback ? '⚠ Yes' : 'No'],
                  ['Box Blocked', intersection?.isBoxBlocked ? '⚠ Yes' : 'No'],
                  ['Incident', intersection?.hasActiveIncident ? '⚠ Yes' : 'None'],
                  ['Scenario', scenarioDisplay],
                  ['Simulation Time', formatSimSec(metadata?.simulationTime)],
                  ['Last Seen', metadata?.freshnessSeconds != null ? `${metadata.freshnessSeconds.toFixed(1)}s ago` : '—'],
                ] as [string, string | number][]).map(([label, value]) => (
                  <div key={label} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, gap: 4 }}>
                    <span style={{ color: 'var(--text-muted)' }}>{label}</span>
                    <span style={{
                      color: String(value).includes('⚠') ? '#EF4444' : 'var(--text-primary)',
                      fontWeight: 500, textAlign: 'right',
                    }}>
                      {String(value)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Vehicle count by direction */}
          <div className="card">
            <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 10 }}>
              Vehicle Count by Direction
            </div>
            {DIRECTIONS.map((dir) => {
              const s = dirSensors.find((d) => d.direction === dir)
              const count = s?.vehicleCount ?? 0
              const maxCount = Math.max(...dirSensors.map((d) => d.vehicleCount ?? 0), 1)
              const pct = maxCount > 0 ? (count / maxCount) * 100 : 0
              return (
                <div key={dir} style={{ marginBottom: 8 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, marginBottom: 3 }}>
                    <span style={{ color: 'var(--text-secondary)' }}>
                      {DIR_ARROW[dir]} {dir}
                    </span>
                    <span style={{ color: 'var(--text-primary)', fontWeight: 600 }}>{count}</span>
                  </div>
                  <div style={{ height: 6, background: 'rgba(24,58,82,0.6)', borderRadius: 3 }}>
                    <div style={{
                      height: '100%', width: `${pct}%`, borderRadius: 3,
                      background: '#168CFF',
                      transition: 'width 0.4s ease',
                    }} />
                  </div>
                </div>
              )
            })}
          </div>

          {/* Scenario Control Panel (Left column restoration) */}
          <ScenarioControlCard
            intersectionId={intersectionId ?? ''}
            currentScenarioId={rawScenario}
            onCommandApplied={(scenario) => {
              addCommandEvent(`Scenario command queued: ${scenario}`, 'Awaiting realtime update from SUMO', 'blue')
            }}
          />
        </div>

        {/* Center: Live Traffic View (React Konva 2D Canvas) */}
        <div className="card" style={{ padding: 10 }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ color: '#16C7E8' }}>●</span>
            Live Traffic View
            {isPaused && <span style={{ fontSize: 10, color: '#FACC15', marginLeft: 4 }}>PAUSED</span>}
          </div>
          {isLoading ? (
            <Skeleton height={520} width="100%" />
          ) : (
            <IntersectionScene
              sensors={sensors}
              lights={lights}
              currentPhase={currentPhase}
              simulationRunId={metadata?.simulationRunId}
              stale={freshnessState === 'stale' || freshnessState === 'error' || isPaused}
            />
          )}
        </div>

        {/* Right: Traffic Light Panel */}
        <div className="card">
          <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 10 }}>
            Traffic Lights — {displayName}
          </div>
          {isLoading ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {[1, 2, 3, 4].map((i) => <Skeleton key={i} height={90} width="100%" />)}
            </div>
          ) : (
            <TrafficLightPanel
              lights={lightViews}
              freshnessState={freshnessState}
              isPaused={isPaused}
              currentPhase={currentPhase}
            />
          )}
        </div>
      </div>

      {/* ── ROW 3: BOTTOM PANELS (Event Feed 32% | Signal Control 36% | Sensor Table 32%) ── */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'minmax(240px, 32fr) minmax(260px, 36fr) minmax(280px, 32fr)',
        gap: 14, alignItems: 'start',
      }}>
        {/* Left: Realtime Event Feed */}
        <div className="card" style={{ minHeight: 280 }}>
          <RealtimeEventFeed events={events} onClear={clearFeed} />
        </div>

        {/* Center: Signal Control Tabs */}
        <div className="card" style={{ minHeight: 280 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
            Manual Signal Control
          </div>
          <ReverseControlPanel
            intersectionId={intersectionId ?? ''}
            currentPhase={currentPhase}
            currentConfiguredGreen={currentConfiguredGreen}
            currentYellowDuration={lights[0]?.yellowDuration ?? 3}
            freshnessState={freshnessState}
            isPaused={isPaused}
            currentScenario={rawScenario}
            currentMode={intersection?.overallTrafficStatus}
            onCommandApplied={(action) => {
              addCommandEvent(`Control command queued: ${action}`, 'Awaiting realtime update from SUMO', 'green')
            }}
          />
        </div>

        {/* Right: Vehicle Sensor Data Table */}
        <div className="card" style={{ minHeight: 280 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 10, display: 'flex', alignItems: 'center', gap: 6, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
            Vehicle Sensor (Aggregated)
            <span
              title="Data from directional vehicle sensors. Aggregated from SUMO via Realtime pipeline."
              style={{ fontSize: 11, color: 'var(--text-muted)', cursor: 'help' }}
            >
              ⓘ
            </span>
          </div>

          {isLoading ? (
            <Skeleton height={140} width="100%" />
          ) : sensors.length === 0 ? (
            <p style={{ color: 'var(--text-muted)', fontSize: 12 }}>No vehicle sensor data available.</p>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid var(--border)' }}>
                    {['Direction', 'Left', 'Straight', 'Right', 'Total', 'Avg Speed', 'Queue', 'Waiting', 'Occupancy', 'Status', 'Spillback'].map((h) => (
                      <th key={h} style={{ padding: '5px 6px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 600, fontSize: 10, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {dirSensors.map((s) => (
                    <tr key={s.direction} style={{ borderBottom: '1px solid rgba(24,58,82,0.5)' }}>
                      <td style={{ padding: '6px', color: 'var(--text-primary)', fontWeight: 600, whiteSpace: 'nowrap' }}>
                        {DIR_ARROW[s.direction] ?? ''} {s.direction}
                      </td>
                      <td style={{ padding: '6px', color: 'var(--text-secondary)' }}>{s.leftTurnCount ?? '—'}</td>
                      <td style={{ padding: '6px', color: 'var(--text-secondary)' }}>{s.straightCount ?? '—'}</td>
                      <td style={{ padding: '6px', color: 'var(--text-secondary)' }}>{s.rightTurnCount ?? '—'}</td>
                      <td style={{ padding: '6px', color: 'var(--text-primary)', fontWeight: 700 }}>{s.vehicleCount ?? '—'}</td>
                      {/* AVG SPEED: numeric only */}
                      <td style={{ padding: '6px', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>
                        {formatSpeedKmh(s.averageSpeed)}
                      </td>
                      <td style={{ padding: '6px', color: 'var(--text-secondary)' }}>
                        {s.queueLength !== null ? `${s.queueLength.toFixed(0)} m` : '—'}
                      </td>
                      <td style={{ padding: '6px', color: 'var(--text-secondary)' }}>{s.waitingVehicleCount ?? '—'}</td>
                      {/* OCCUPANCY FIX: formatOccupancyRate scale 0-100 */}
                      <td style={{ padding: '6px', color: 'var(--text-secondary)', fontWeight: 500 }}>
                        {formatOccupancyRate(s.occupancyRate)}
                      </td>
                      <td style={{ padding: '6px' }}>
                        <TrafficStatusBadge status={s.trafficStatus} />
                      </td>
                      <td style={{ padding: '6px', color: s.spillbackRisk ? '#EF4444' : 'var(--text-muted)' }}>
                        {s.spillbackRisk ? '⚠ Risk' : 'No'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Technical metadata in footer */}
          {metadata && (
            <div style={{ marginTop: 10, fontSize: 10, color: 'var(--text-muted)', display: 'flex', gap: 14, flexWrap: 'wrap', borderTop: '1px solid rgba(24,58,82,0.5)', paddingTop: 8 }}>
              <span>Run: {metadata.simulationRunId ?? '—'}</span>
              <span>Sim: {formatSimSec(metadata.simulationTime)}</span>
              <span>Freshness: {metadata.freshnessSeconds != null ? `${metadata.freshnessSeconds.toFixed(1)}s` : '—'}</span>
              <span>Consistent: {metadata.consistent !== null ? (metadata.consistent ? 'Yes' : 'No') : '—'}</span>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Back button ──────────────────────────────────────────────────────────────
function BackButton({ backUrl, navigate }: { backUrl: string; navigate: ReturnType<typeof useNavigate> }) {
  return (
    <button
      id="detail-back-btn"
      type="button"
      onClick={() => navigate(backUrl)}
      style={{
        display: 'flex', alignItems: 'center', gap: 6,
        padding: '6px 14px', borderRadius: 7,
        border: '1px solid var(--border)', background: 'transparent',
        color: 'var(--text-secondary)', fontSize: 12, cursor: 'pointer',
        transition: 'all 0.15s',
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = 'rgba(22,140,255,0.08)'
        e.currentTarget.style.borderColor = 'var(--blue)'
        e.currentTarget.style.color = 'var(--blue)'
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = 'transparent'
        e.currentTarget.style.borderColor = 'var(--border)'
        e.currentTarget.style.color = 'var(--text-secondary)'
      }}
      aria-label="Back to Analytics Overview"
    >
      <ArrowLeft size={14} />
      Back to Overview
    </button>
  )
}
