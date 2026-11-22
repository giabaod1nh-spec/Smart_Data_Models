import { useQuery } from '@tanstack/react-query'
import { RefreshCw, ServerCog } from 'lucide-react'
import { getSystemHealth } from '@/api/realtimeApi'
import { DataNotice, PageHeader, Panel, PanelHeader, StatusPill, type StatusTone } from '@/components/operations/OperationalUI'

function healthTone(value: unknown): StatusTone {
  if (String(value).toUpperCase() === 'UP') return 'healthy'
  if (String(value).toUpperCase() === 'DOWN') return 'critical'
  return 'neutral'
}

export function SystemMonitoringPage() {
  const query = useQuery({ queryKey: ['system', 'health', 'details-page'], queryFn: getSystemHealth, refetchInterval: 15_000, retry: 1 })
  const health = query.data as Record<string, unknown> | undefined
  const components = [
    { name: 'Spring API', key: 'server', purpose: 'Authenticated dashboard API boundary' },
    { name: 'FIWARE Orion-LD', key: 'orion', purpose: 'Current traffic context and entity projection' },
    { name: 'Context provider', key: 'contextProvider', purpose: 'Context-source dependency reported by Spring' },
    { name: 'Control API / TraCI', key: 'controlApi', purpose: 'SUMO runtime and reverse-control path' },
  ]
  const upCount = components.filter((component) => String(health?.[component.key]).toUpperCase() === 'UP').length

  return <div className="ops-page">
    <PageHeader title="System monitoring" description="Infrastructure and data-path health is isolated here so it cannot be confused with congestion or traffic performance." actions={<button className="secondary-action" onClick={() => void query.refetch()} disabled={query.isFetching}><RefreshCw size={15} />{query.isFetching ? 'Refreshing…' : 'Refresh health'}</button>} />
    {query.isLoading ? <DataNotice state="loading" title="Checking system health" description="Spring is probing its configured dependencies." /> : query.isError ? <DataNotice state="error" title="Spring API is unreachable" description="The dashboard cannot retrieve system health. Check the server process and the Vite proxy." action={<button className="secondary-action" onClick={() => void query.refetch()}>Retry</button>} /> : (
      <>
        <div className="system-summary"><ServerCog size={22} /><div><strong>{upCount} of {components.length} reported components are up</strong><span>Last checked {new Date(query.dataUpdatedAt).toLocaleTimeString('vi-VN')}</span></div><StatusPill tone={upCount === components.length ? 'healthy' : upCount === 0 ? 'critical' : 'warning'}>{upCount === components.length ? 'Operational' : upCount === 0 ? 'Offline' : 'Degraded'}</StatusPill></div>
        <Panel>
          <PanelHeader title="Component health" description="Direct Spring health results; traffic KPIs are intentionally absent" />
          <div className="component-grid">{components.map((component) => {
            const value = health?.[component.key]
            return <div className="component-health" key={component.key}><div><strong>{component.name}</strong><p>{component.purpose}</p></div><StatusPill tone={healthTone(value)}>{String(value ?? 'Unknown')}</StatusPill></div>
          })}</div>
        </Panel>
        <div className="system-grid">
          <Panel><PanelHeader title="Not exposed by this endpoint" description="Avoids false-green infrastructure claims" /><div className="evidence-list"><div><span>Kafka broker and topic</span><strong>Not reported</strong><small>No broker status, message rate or consumer lag is returned by /api/system/health.</small></div><div><span>Projector offset / lag</span><strong>Not reported</strong><small>Realtime metadata exposes freshness and projector state per intersection, not infrastructure lag.</small></div><div><span>Gold analytics services</span><strong>Feature-dependent</strong><small>Analytics errors are displayed on the pages that query those contracts.</small></div></div></Panel>
          <Panel><PanelHeader title="Diagnostic boundary" description="How to interpret this page" /><div className="evidence-list"><div><span>Traffic congestion</span><strong>Operations / Intersections</strong><small>Never inferred from component health.</small></div><div><span>Simulation runtime</span><strong>Control API status</strong><small>A healthy Spring server does not prove SUMO is running.</small></div><div><span>Data freshness</span><strong>Realtime metadata</strong><small>Freshness qualifies traffic observations and appears next to those observations.</small></div></div></Panel>
        </div>
      </>
    )}
  </div>
}
