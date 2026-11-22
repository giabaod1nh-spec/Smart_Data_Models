import { useMemo, useState } from 'react'
import { CheckCircle2, FlaskConical } from 'lucide-react'
import { useIntersectionList } from '@/hooks/useIntersectionList'
import { useDirectionWindows } from '@/hooks/useAnalytics'
import { SCENARIO_IDS, SCENARIO_LABELS, type ScenarioId } from '@/types/control'
import type { AnalyticsQueryParams } from '@/types/common'
import type { DirectionWindowDto } from '@/types/analytics'
import { getIntersectionDisplayName } from '@/utils/intersectionDisplayName'
import { DataNotice, PageHeader, Panel, PanelHeader, StatusPill } from '@/components/operations/OperationalUI'

interface Summary {
  speed: number | null
  queue: number | null
  vehicles: number | null
  spillback: number | null
}

function average(values: Array<number | null>): number | null {
  const valid = values.filter((value): value is number => value !== null && Number.isFinite(value))
  return valid.length ? valid.reduce((sum, value) => sum + value, 0) / valid.length : null
}

function summarize(rows: DirectionWindowDto[]): Summary | null {
  if (!rows.length) return null
  const latest = new Map<string, DirectionWindowDto>()
  for (const row of rows) {
    const current = latest.get(row.direction)
    if (!current || row.windowEndSimSec > current.windowEndSimSec) latest.set(row.direction, row)
  }
  const values = [...latest.values()]
  return {
    speed: average(values.map((row) => row.avgSpeedKmh)),
    queue: average(values.map((row) => row.avgQueueLengthM)),
    vehicles: average(values.map((row) => row.avgVehicleCount)),
    spillback: average(values.map((row) => row.spillbackRatioPct)),
  }
}

function improvement(fixed: number | null, adaptive: number | null, higherIsBetter: boolean): number | null {
  if (fixed === null || adaptive === null || fixed === 0) return null
  return (higherIsBetter ? (adaptive - fixed) : (fixed - adaptive)) / fixed * 100
}

const METRICS = [
  { key: 'queue' as const, label: 'Average queue', unit: 'm', higherIsBetter: false },
  { key: 'speed' as const, label: 'Average speed', unit: 'km/h', higherIsBetter: true },
  { key: 'vehicles' as const, label: 'Average vehicles', unit: 'veh', higherIsBetter: false },
  { key: 'spillback' as const, label: 'Spillback ratio', unit: '%', higherIsBetter: false },
]

export function ExperimentsPage() {
  const { intersections } = useIntersectionList()
  const [fixedRun, setFixedRun] = useState('')
  const [adaptiveRun, setAdaptiveRun] = useState('')
  const [scenario, setScenario] = useState<ScenarioId>('normal')
  const [intersectionId, setIntersectionId] = useState('')
  const [windowSize, setWindowSize] = useState<60 | 300>(60)
  const selectedIntersection = intersectionId || intersections?.[0]?.id || ''
  const fixedQuery: AnalyticsQueryParams | null = fixedRun.trim() ? { simulationRunId: fixedRun.trim(), scenarioId: scenario, windowSizeSec: windowSize } : null
  const adaptiveQuery: AnalyticsQueryParams | null = adaptiveRun.trim() ? { simulationRunId: adaptiveRun.trim(), scenarioId: scenario, windowSizeSec: windowSize } : null
  const fixedData = useDirectionWindows(selectedIntersection || null, fixedQuery)
  const adaptiveData = useDirectionWindows(selectedIntersection || null, adaptiveQuery)
  const fixedSummary = useMemo(() => summarize(fixedData.data?.items ?? []), [fixedData.data])
  const adaptiveSummary = useMemo(() => summarize(adaptiveData.data?.items ?? []), [adaptiveData.data])
  const ready = Boolean(fixedQuery && adaptiveQuery && selectedIntersection)
  const comparable = Boolean(fixedSummary && adaptiveSummary)

  return <div className="ops-page">
    <PageHeader title="Experiment comparison" description="Compare Fixed-Time and Adaptive runs only under the same selected scenario, intersection and analytical window." meta={<StatusPill tone={comparable ? 'healthy' : 'neutral'}>{comparable ? 'Comparable data loaded' : 'Awaiting run evidence'}</StatusPill>} />
    <Panel>
      <PanelHeader title="Comparison contract" description="Run IDs are supplied explicitly because the backend does not expose a trustworthy experiment-run catalog." />
      <div className="experiment-filters">
        <label><span>Fixed-Time run ID</span><input value={fixedRun} onChange={(event) => setFixedRun(event.target.value)} placeholder="simulationRunId" /></label>
        <label><span>Adaptive run ID</span><input value={adaptiveRun} onChange={(event) => setAdaptiveRun(event.target.value)} placeholder="simulationRunId" /></label>
        <label><span>Scenario</span><select value={scenario} onChange={(event) => setScenario(event.target.value as ScenarioId)}>{SCENARIO_IDS.map((id) => <option key={id} value={id}>{SCENARIO_LABELS[id]}</option>)}</select></label>
        <label><span>Intersection</span><select value={selectedIntersection} onChange={(event) => setIntersectionId(event.target.value)}>{(intersections ?? []).map((item) => <option key={item.id ?? ''} value={item.id ?? ''}>{getIntersectionDisplayName(item.id, item.name)}</option>)}</select></label>
        <label><span>Window</span><select value={windowSize} onChange={(event) => setWindowSize(Number(event.target.value) as 60 | 300)}><option value={60}>60 seconds</option><option value={300}>300 seconds</option></select></label>
      </div>
      <div className="comparison-conditions"><CheckCircle2 size={16} /><span>Shared conditions enforced in the query: scenario <strong>{scenario}</strong>, intersection <strong>{getIntersectionDisplayName(selectedIntersection, undefined)}</strong>, window <strong>{windowSize}s</strong>. Network, duration and random seed are not exposed by this API and must be verified outside this view.</span></div>
    </Panel>

    {!ready ? <DataNotice state="empty" title="Enter both experiment run IDs" description="No comparison is executed until a Fixed-Time run and an Adaptive run are explicitly selected." /> : fixedData.isLoading || adaptiveData.isLoading ? <DataNotice state="loading" title="Loading comparable Gold windows" description="Reading direction-level windows for both runs." /> : fixedData.isError || adaptiveData.isError ? <DataNotice state="error" title="Comparison data unavailable" description="One or both run IDs did not return matching Gold direction windows. Check analytics availability and experiment metadata." /> : !comparable ? <DataNotice state="empty" title="No equivalent windows found" description="The selected runs do not currently contain comparable direction windows for this scenario and intersection." /> : (
      <div className="experiment-results">
        <Panel className="experiment-chart-panel">
          <PanelHeader title="Fixed-Time vs Adaptive" description="Latest direction window averaged across available approaches; absolute values remain visible" />
          <div className="comparison-bars">{METRICS.map((metric) => {
            const fixed = fixedSummary?.[metric.key] ?? null
            const adaptive = adaptiveSummary?.[metric.key] ?? null
            const max = Math.max(fixed ?? 0, adaptive ?? 0, 1)
            const delta = improvement(fixed, adaptive, metric.higherIsBetter)
            return <div className="comparison-metric" key={metric.key}>
              <div className="comparison-metric-title"><strong>{metric.label}</strong><span className={delta !== null && delta >= 0 ? 'value-positive' : 'value-critical'}>{delta === null ? 'Invalid comparison' : `${delta >= 0 ? '+' : ''}${delta.toFixed(1)}% improvement`}</span></div>
              <div className="comparison-line"><span>Fixed-Time</span><div><i className="fixed" style={{ width: `${((fixed ?? 0) / max) * 100}%` }} /></div><strong>{fixed !== null ? fixed.toFixed(1) : '—'} {metric.unit}</strong></div>
              <div className="comparison-line"><span>Adaptive</span><div><i className="adaptive" style={{ width: `${((adaptive ?? 0) / max) * 100}%` }} /></div><strong>{adaptive !== null ? adaptive.toFixed(1) : '—'} {metric.unit}</strong></div>
            </div>
          })}</div>
        </Panel>
        <Panel>
          <PanelHeader title="Scientific validity" description="What this view can support in a report" />
          <div className="evidence-list">
            <div><span>Absolute values</span><strong>Available</strong><small>Both run values are shown for every valid metric.</small></div>
            <div><span>Percentage formula</span><strong>Direction-aware</strong><small>Lower-is-better and higher-is-better metrics use different formulas; zero denominators are rejected.</small></div>
            <div><span>Travel time / throughput / waiting time</span><strong>Not exposed here</strong><small>The current DirectionWindow contract does not provide these experiment metrics.</small></div>
            <div><span>Conclusion</span><strong>Requires metadata review</strong><small>Verify network, duration and random seed before citing improvements.</small></div>
          </div>
        </Panel>
      </div>
    )}
    <Panel><PanelHeader title="Experiment readiness" description="A trustworthy run catalog is the remaining backend gap" /><div className="readiness-row"><FlaskConical size={22} /><div><strong>Comparison UI is contract-ready</strong><p>Add an experiment-run lookup endpoint containing strategy, network digest, demand, duration and seed to remove manual run-ID entry and automate equivalence checks.</p></div></div></Panel>
  </div>
}
