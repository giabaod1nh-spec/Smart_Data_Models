import { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { Activity, Car, Clock3, Gauge, Rows3 } from 'lucide-react'
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { useIntersectionList } from '@/hooks/useIntersectionList'
import { useRealtimeIntersection } from '@/hooks/useRealtimeIntersection'
import { useLiveVehicleStream } from '@/hooks/useLiveVehicleStream'
import { useRlAgentStatus } from '@/hooks/useRlAgentStatus'
import { useAnalyticsCongestion, useAnalyticsTrends, useSignalOperationWindows } from '@/hooks/useAnalytics'
import { buildCongestionBarData, buildTrendSeries } from '@/transforms/analyticsTransforms'
import { getIntersectionDisplayName } from '@/utils/intersectionDisplayName'
import { toGoldIntersectionId } from '@/utils/analyticsIntersectionId'
import type { AnalyticsQueryParams } from '@/types/common'
import type { SignalOperationWindowDto } from '@/types/analytics'
import { DataNotice, MetricStrip, PageHeader, Panel, PanelHeader, StatusPill, type StatusTone } from '@/components/operations/OperationalUI'
import { formatSimulationTime } from '@/utils/operationsFormatting'

function trafficTone(status: string | null | undefined): StatusTone {
  const value = (status ?? '').toUpperCase()
  if (value.includes('CRITICAL') || value.includes('SEVERE') || value.includes('RED')) return 'critical'
  if (value.includes('HIGH') || value.includes('CONGEST') || value.includes('ORANGE')) return 'warning'
  if (value.includes('NORMAL') || value.includes('LOW') || value.includes('GREEN') || value.includes('SMOOTH')) return 'healthy'
  return 'neutral'
}

function mergeTrendSeries(speedRows: ReturnType<typeof buildTrendSeries>, queueRows: ReturnType<typeof buildTrendSeries>) {
  const points = new Map<number, { simulationTime: number; speed: number | null; queue: number | null }>()
  speedRows.forEach((row) => points.set(row.windowStart, { simulationTime: row.windowStart, speed: row.currentValue, queue: null }))
  queueRows.forEach((row) => {
    const existing = points.get(row.windowStart) ?? { simulationTime: row.windowStart, speed: null, queue: null }
    existing.queue = row.currentValue
    points.set(row.windowStart, existing)
  })
  return [...points.values()].sort((a, b) => a.simulationTime - b.simulationTime)
}

export function OperationsPage() {
  const navigate = useNavigate()
  const { intersections, isLoading: intersectionsLoading, isError: intersectionsError, statusMessage } = useIntersectionList()
  const primaryIntersectionId = intersections?.[0]?.id ?? undefined
  const realtime = useRealtimeIntersection(primaryIntersectionId)
  const live = useLiveVehicleStream(true)
  const rl = useRlAgentStatus(true)
  const metadata = realtime.data?.metadata
  const query: AnalyticsQueryParams | null = metadata?.simulationRunId && metadata.scenarioId
    ? { simulationRunId: metadata.simulationRunId, scenarioId: metadata.scenarioId, windowSizeSec: 60 }
    : null

  const speedQuery = useAnalyticsTrends(primaryIntersectionId ?? null, query, 'AVG_SPEED_KMH')
  const queueQuery = useAnalyticsTrends(primaryIntersectionId ?? null, query, 'AVG_QUEUE_LENGTH_M')
  const congestionQuery = useAnalyticsCongestion(query)
  const signalQuery = useSignalOperationWindows(query)

  const trendData = useMemo(
    () => mergeTrendSeries(buildTrendSeries(speedQuery.data?.items ?? []), buildTrendSeries(queueQuery.data?.items ?? [])),
    [speedQuery.data, queueQuery.data],
  )

  const congestionById = useMemo(() => new Map(buildCongestionBarData(congestionQuery.data?.items ?? []).map((row) => [toGoldIntersectionId(row.intersectionId), row])), [congestionQuery.data])
  const ranked = useMemo(() => [...(intersections ?? [])].sort((a, b) => {
    const scoreA = congestionById.get(toGoldIntersectionId(a.id ?? ''))?.numericValue ?? 0
    const scoreB = congestionById.get(toGoldIntersectionId(b.id ?? ''))?.numericValue ?? 0
    if (scoreA !== scoreB) return scoreB - scoreA
    return (b.totalVehicleCount ?? 0) - (a.totalVehicleCount ?? 0)
  }).slice(0, 6), [intersections, congestionById])

  const pressuredCount = (intersections ?? []).filter((item) => ['warning', 'critical'].includes(trafficTone(item.overallTrafficStatus ?? item.derivedTrafficState))).length
  const simTime = live.simulationTime ?? metadata?.simulationTime ?? null
  const freshness = metadata?.freshnessSeconds
  const streamTone: StatusTone = live.status === 'connected' ? 'healthy' : live.status === 'error' ? 'critical' : 'warning'

  const signalRows = useMemo(() => {
    const latest = new Map<string, SignalOperationWindowDto>()
    for (const row of signalQuery.data?.items ?? []) {
      const key = toGoldIntersectionId(row.intersectionId)
      const current = latest.get(key)
      if (!current || row.windowEndSimSec > current.windowEndSimSec) latest.set(key, row)
    }
    return [...latest.values()].slice(0, 5)
  }, [signalQuery.data])

  return (
    <div className="ops-page ops-overview">
      <PageHeader
        title="Traffic operations"
        description="Live network performance, congestion pressure and adaptive-control evidence for the active simulation."
        meta={<><StatusPill tone={streamTone} pulse={live.status === 'connected'}>{live.status === 'connected' ? 'Live TraCI stream' : `Stream ${live.status}`}</StatusPill><span>Run <code>{metadata?.simulationRunId ?? 'not resolved'}</code></span></>}
      />

      <div className="simulation-context" aria-label="Active simulation context">
        <StatusPill tone={streamTone} pulse={live.status === 'connected'}>{live.status === 'connected' ? 'Running' : 'Connection pending'}</StatusPill>
        <div><span>Simulation time</span><strong>{formatSimulationTime(simTime)}</strong></div>
        <div><span>Scenario</span><strong>{metadata?.scenarioId ?? '—'}</strong></div>
        <div><span>Control strategy</span><strong>{rl.mode ?? (rl.enabled ? 'Adaptive' : 'Not reported')}</strong></div>
        <div><span>Data freshness</span><strong>{freshness !== null && freshness !== undefined ? `${freshness.toFixed(1)} s` : '—'}</strong></div>
      </div>

      <MetricStrip metrics={[
        { label: 'Active vehicles', value: live.stats ? live.stats.vehicleCount.toLocaleString() : '—', unit: 'veh', icon: <Car size={17} />, meta: 'Whole SUMO network' },
        { label: 'Average speed', value: live.stats ? live.stats.averageSpeed.toFixed(1) : '—', unit: 'km/h', icon: <Gauge size={17} />, meta: 'Whole SUMO network' },
        { label: 'Waiting vehicles', value: live.stats ? live.stats.waitingVehicles.toLocaleString() : '—', unit: 'veh', icon: <Clock3 size={17} />, tone: live.stats && live.stats.waitingVehicles > 0 ? 'warning' : 'neutral', meta: 'TraCI stream' },
        { label: 'Average queue', value: rl.globalMetrics?.averageQueue !== undefined ? Number(rl.globalMetrics.averageQueue).toFixed(1) : '—', unit: 'veh', icon: <Rows3 size={17} />, meta: rl.enabled ? 'Adaptive controller' : 'Controller metric unavailable', unavailable: rl.globalMetrics?.averageQueue === undefined },
        { label: 'Under pressure', value: String(pressuredCount), unit: `/ ${intersections?.length ?? 0}`, icon: <Activity size={17} />, tone: pressuredCount > 0 ? 'warning' : 'healthy', meta: 'From reported traffic states' },
      ]} />

      <div className="ops-primary-grid">
        <Panel className="ops-chart-panel">
          <PanelHeader title="Network performance" description={primaryIntersectionId ? `Gold 60 s windows · reference intersection ${getIntersectionDisplayName(primaryIntersectionId, undefined)}` : 'Waiting for current-run context'} actions={<button className="text-button" onClick={() => navigate('/analytics')}>Open Gold analytics</button>} />
          {!query ? (
            <DataNotice state="empty" title="Run context not resolved" description="Start the simulation and wait for a current-run realtime snapshot before Gold trend queries can run." />
          ) : speedQuery.isLoading || queueQuery.isLoading ? (
            <DataNotice state="loading" title="Loading analytical windows" description="Fetching speed and queue trends for the active run." />
          ) : speedQuery.isError || queueQuery.isError ? (
            <DataNotice state="warning" title="Gold analytics unavailable" description="Realtime operation can continue. Enable and populate the analytics profile to display historical trends." />
          ) : trendData.length === 0 ? (
            <DataNotice state="empty" title="No analytical windows yet" description="The active run has not produced matching 60-second speed and queue windows." />
          ) : (
            <div className="ops-main-chart" role="img" aria-label="Average speed and queue length over simulation time">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={trendData} margin={{ top: 12, right: 18, left: 2, bottom: 4 }}>
                  <CartesianGrid stroke="rgba(34,49,64,.72)" vertical={false} />
                  <XAxis dataKey="simulationTime" tickFormatter={(value) => `${value}s`} stroke="#718294" tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="speed" stroke="#35BCE8" tick={{ fontSize: 11 }} unit=" km/h" />
                  <YAxis yAxisId="queue" orientation="right" stroke="#6E8BFF" tick={{ fontSize: 11 }} unit=" m" />
                  <Tooltip contentStyle={{ background: '#111C27', border: '1px solid #223140', borderRadius: 8 }} labelFormatter={(value) => `Simulation ${value}s`} />
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                  <Line yAxisId="speed" type="monotone" dataKey="speed" name="Average speed" stroke="#35BCE8" strokeWidth={2.4} dot={false} connectNulls />
                  <Line yAxisId="queue" type="monotone" dataKey="queue" name="Average queue" stroke="#6E8BFF" strokeWidth={2.2} dot={false} connectNulls />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </Panel>

        <Panel>
          <PanelHeader title="Congestion ranking" description="Current-run intersections enriched with Gold congestion score when available" />
          {intersectionsLoading ? <DataNotice state="loading" title="Loading intersections" description="Reading current-run entities from Orion-LD." /> : intersectionsError ? <DataNotice state="error" title="Intersection inventory unavailable" description={statusMessage} /> : ranked.length === 0 ? <DataNotice state="empty" title="No current-run intersections" description="No intersection entities match the active simulation run." /> : (
            <div className="ranking-list">{ranked.map((item, index) => {
              const id = item.id ?? `intersection-${index}`
              const score = congestionById.get(toGoldIntersectionId(id))?.numericValue
              const status = item.overallTrafficStatus ?? item.derivedTrafficState ?? 'Unknown'
              return <button className="ranking-row" key={id} onClick={() => navigate(`/intersections/${encodeURIComponent(id)}`)}>
                <span className="ranking-index">{index + 1}</span>
                <span className="ranking-main"><strong>{getIntersectionDisplayName(id, item.name)}</strong><small>{item.totalVehicleCount ?? '—'} vehicles · {item.currentPhase ?? 'phase unavailable'}</small><span className="ranking-track"><i style={{ width: score !== undefined ? `${Math.max(4, Math.min(100, score))}%` : '4%' }} /></span></span>
                <span className="ranking-state"><StatusPill tone={trafficTone(status)}>{status}</StatusPill><small>{score !== undefined ? `${score.toFixed(1)} score` : 'No Gold score'}</small></span>
              </button>
            })}</div>
          )}
        </Panel>
      </div>

      <div className="ops-secondary-grid">
        <Panel>
          <PanelHeader title="Adaptive control activity" description="Current agent outputs; no decision reason is inferred" actions={<button className="text-button" onClick={() => navigate('/adaptive-control')}>Inspect controller</button>} />
          {rl.isLoading ? <DataNotice state="loading" title="Reading controller status" description="Waiting for the DQN runtime status endpoint." /> : rl.isError ? <DataNotice state="warning" title="Controller status unavailable" description="The read-only /rl/status endpoint is not reachable." /> : !rl.enabled || rl.agents.length === 0 ? <DataNotice state="empty" title="Adaptive mode is not active" description="Activate the adaptive controller to display live agent actions and rewards." /> : (
            <div className="agent-table" role="table" aria-label="Adaptive agent activity">
              <div className="agent-row agent-head" role="row"><span>Agent</span><span>Action</span><span>Phase</span><span>Queue</span><span>Reward</span></div>
              {rl.agents.slice(0, 6).map((agent) => <div className="agent-row" role="row" key={agent.id}><strong>{agent.id}</strong><span className="action-code">{agent.action ?? '—'}</span><span>{agent.phase ?? '—'}</span><span>{agent.queue ?? '—'}</span><span className={(agent.reward ?? 0) < 0 ? 'value-critical' : 'value-positive'}>{agent.reward !== undefined ? Number(agent.reward).toFixed(2) : '—'}</span></div>)}
            </div>
          )}
        </Panel>

        <Panel>
          <PanelHeader title="Signal allocation" description="Latest Gold signal-operation window by intersection" />
          {signalQuery.isLoading ? <DataNotice state="loading" title="Loading phase shares" description="Reading signal-operation windows." /> : signalQuery.isError ? <DataNotice state="warning" title="Signal analytics unavailable" description="The realtime signal state remains available in intersection detail." /> : signalRows.length === 0 ? <DataNotice state="empty" title="No phase-share windows" description="No signal-operation windows match the active run and scenario." /> : (
            <div className="allocation-list">{signalRows.map((row) => {
              const green = Math.max(0, row.greenSharePct ?? 0)
              const yellow = Math.max(0, row.yellowSharePct ?? 0)
              const red = Math.max(0, row.redSharePct ?? 0)
              return <div className="allocation-row" key={`${row.intersectionId}-${row.windowId}`}><span>{getIntersectionDisplayName(row.intersectionId, undefined)}</span><div className="allocation-bar" aria-label={`${green}% green, ${yellow}% yellow, ${red}% red`}><i className="is-green" style={{ width: `${green}%` }} /><i className="is-yellow" style={{ width: `${yellow}%` }} /><i className="is-red" style={{ width: `${red}%` }} /></div><strong>{row.dominantPhase ?? '—'}</strong></div>
            })}</div>
          )}
        </Panel>

        <Panel>
          <PanelHeader title="Decision impact & data quality" description="Evidence that qualifies the live dashboard" />
          <div className="evidence-list">
            <div><span>Latest controller output</span><strong>{rl.agents[0]?.action ?? 'No decision exposed'}</strong><small>{rl.agents[0] ? `Agent ${rl.agents[0].id} · ${rl.agents[0].phase ?? 'phase unavailable'}` : 'Decision-history endpoint is not available'}</small></div>
            <div><span>Realtime consistency</span><strong>{metadata?.consistent === true ? 'Consistent' : metadata?.consistent === false ? 'Review required' : 'Not reported'}</strong><small>{metadata?.consistencyIssues?.length ? metadata.consistencyIssues.join(', ') : 'No consistency issue reported'}</small></div>
            <div><span>Projector freshness</span><strong>{freshness !== null && freshness !== undefined ? `${freshness.toFixed(1)} seconds` : 'Not reported'}</strong><small>{metadata?.projectorStatus ?? 'Projector status unavailable'}</small></div>
            <button className="secondary-action" onClick={() => navigate('/experiments')}>Compare Fixed-Time and Adaptive runs</button>
          </div>
        </Panel>
      </div>
    </div>
  )
}
