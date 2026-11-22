import { useMemo, useState } from 'react'
import { Search } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { useIntersectionList } from '@/hooks/useIntersectionList'
import { getIntersectionDisplayName } from '@/utils/intersectionDisplayName'
import { DataNotice, MetricStrip, PageHeader, Panel, PanelHeader, StatusPill, type StatusTone } from '@/components/operations/OperationalUI'

function tone(status: string | null | undefined): StatusTone {
  const value = (status ?? '').toUpperCase()
  if (value.includes('CRITICAL') || value.includes('SEVERE')) return 'critical'
  if (value.includes('HIGH') || value.includes('CONGEST')) return 'warning'
  if (value.includes('NORMAL') || value.includes('LOW') || value.includes('SMOOTH')) return 'healthy'
  return 'neutral'
}

export function IntersectionsPage() {
  const navigate = useNavigate()
  const { intersections, isLoading, isError, statusMessage, refetch } = useIntersectionList()
  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState('ALL')
  const filtered = useMemo(() => (intersections ?? []).filter((item) => {
    const label = `${item.id ?? ''} ${item.name ?? ''}`.toLowerCase()
    const status = (item.overallTrafficStatus ?? item.derivedTrafficState ?? 'UNKNOWN').toUpperCase()
    return label.includes(search.toLowerCase()) && (filter === 'ALL' || status.includes(filter))
  }), [intersections, search, filter])
  const critical = (intersections ?? []).filter((item) => tone(item.overallTrafficStatus ?? item.derivedTrafficState) === 'critical').length
  const congested = (intersections ?? []).filter((item) => tone(item.overallTrafficStatus ?? item.derivedTrafficState) === 'warning').length
  const vehicles = (intersections ?? []).reduce((sum, item) => sum + (item.totalVehicleCount ?? 0), 0)

  return <div className="ops-page">
    <PageHeader title="Intersections" description="Current-run Orion-LD inventory. Select an intersection for realtime sensors, signals, controls and the spatial SUMO view." />
    <MetricStrip metrics={[
      { label: 'Current-run intersections', value: String(intersections?.length ?? 0), meta: 'Filtered by simulationRunId' },
      { label: 'Reported vehicles', value: vehicles.toLocaleString(), unit: 'veh', meta: 'Sum of intersection aggregates' },
      { label: 'Congested', value: String(congested), tone: congested ? 'warning' : 'healthy', meta: 'Reported traffic state' },
      { label: 'Critical', value: String(critical), tone: critical ? 'critical' : 'healthy', meta: 'Reported traffic state' },
    ]} />
    <Panel>
      <PanelHeader title="Intersection inventory" description="Queue and speed remain in the detail view because they are approach-level sensor metrics." actions={<button className="text-button" onClick={() => void refetch()}>Refresh</button>} />
      <div className="table-toolbar">
        <label className="search-field"><Search size={16} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search ID or name" aria-label="Search intersections" /></label>
        <select value={filter} onChange={(event) => setFilter(event.target.value)} aria-label="Filter by traffic state"><option value="ALL">All states</option><option value="CRITICAL">Critical</option><option value="HIGH">High / congested</option><option value="NORMAL">Normal</option></select>
      </div>
      {isLoading ? <DataNotice state="loading" title="Loading current-run intersections" description="Reading the Spring realtime discovery endpoint." /> : isError ? <DataNotice state="error" title="Intersection inventory unavailable" description={statusMessage} action={<button className="secondary-action" onClick={() => void refetch()}>Retry</button>} /> : filtered.length === 0 ? <DataNotice state="empty" title="No intersections match" description={intersections?.length ? 'Adjust the search or state filter.' : 'No current-run intersection entities are available.'} /> : (
        <div className="ops-table-wrap"><table className="ops-table"><thead><tr><th>Intersection</th><th>Traffic state</th><th>Vehicles</th><th>Current phase</th><th>Scenario</th><th>Observed</th><th /></tr></thead><tbody>{filtered.map((item) => {
          const id = item.id ?? ''
          const status = item.overallTrafficStatus ?? item.derivedTrafficState ?? 'Unknown'
          return <tr key={id}><td><strong>{getIntersectionDisplayName(id, item.name)}</strong><small title={id}>{id}</small></td><td><StatusPill tone={tone(status)}>{status}</StatusPill></td><td className="numeric">{item.totalVehicleCount ?? '—'}</td><td><code>{item.currentPhase ?? '—'}</code></td><td>{item.scenarioId ?? '—'}</td><td>{item.dateObserved ? new Date(item.dateObserved).toLocaleTimeString('vi-VN') : '—'}</td><td><button className="text-button" onClick={() => navigate(`/intersections/${encodeURIComponent(id)}`)}>Inspect</button></td></tr>
        })}</tbody></table></div>
      )}
    </Panel>
  </div>
}
