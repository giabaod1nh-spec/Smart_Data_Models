// AnalyticsOverviewPage.tsx — Analytics Overview screen (route: /analytics)

import React, { useCallback, useMemo, useState } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import {
  BarChart3, Car, Gauge, Wifi, Filter, ChevronRight, ChevronDown,
  Activity, TrendingUp, AlertTriangle, RefreshCw,
} from 'lucide-react'
import { useIntersectionList } from '@/hooks/useIntersectionList'
import { useRealtimeContextBootstrap } from '@/hooks/useRealtimeContextBootstrap'
import {
  useAnalyticsCongestion,
  useAnalyticsPriority,
  useAnalyticsTrends,
  useSignalOperationWindows,
} from '@/hooks/useAnalytics'
import { mapIntersectionToNode, trafficStatusColor } from '@/transforms/realtimeTransforms'
import {
  buildCongestionBarData,
  buildPriorityBarData,
  buildTrendSeries,
  buildCongestionDistribution,
  classifyAnalyticsError,
} from '@/transforms/analyticsTransforms'
import { KpiCard } from '@/components/cards/KpiCard'
import { SchematicMap } from '@/components/cards/SchematicMap'
import {
  CongestionScoreByIntersectionChart,
  PriorityIntersectionRanking,
  TrafficVolumeChart,
  AverageSpeedTrendChart,
  CongestionDistributionChart,
} from '@/components/charts/AnalyticsCharts'
import { SkeletonKpiCard, AnalyticsDisabledPanel, ErrorState } from '@/components/feedback/LoadingStates'
import { AnalyticsStatusBar } from '@/components/feedback/AnalyticsStatusBar'
import {
  deriveAnalyticsDataState,
  deriveAnalyticsFeatureState,
  deriveAnalyticsServiceState,
} from '@/utils/analyticsStatus'
import { FreshnessBadge, TrafficStatusBadge } from '@/components/feedback/StatusBadge'
import type { AnalyticsQueryParams, MetricCode, WindowSize } from '@/types/common'
import { METRIC_CODES, WINDOW_SIZES } from '@/types/common'
import { appendAnalyticsQuery, setStoredSelectedIntersectionId } from '@/utils/navigation'
import { getIntersectionDisplayName } from '@/utils/intersectionDisplayName'

function useAnalyticsFilters() {
  const [params, setParams] = useSearchParams()

  const simulationRunId = params.get('simulationRunId') ?? ''
  const scenarioId = params.get('scenarioId') ?? ''
  const windowSizeSec = (Number(params.get('windowSizeSec')) || 60) as WindowSize
  const fromSimulationSec = params.get('fromSimulationSec') ? Number(params.get('fromSimulationSec')) : undefined
  const toSimulationSec = params.get('toSimulationSec') ? Number(params.get('toSimulationSec')) : undefined
  const metricCode = (params.get('metricCode') ?? 'AVG_VEHICLE_COUNT') as MetricCode
  const selectedIntersectionId = params.get('intersectionId') ?? ''

  const analyticsQuery: AnalyticsQueryParams | null =
    simulationRunId && scenarioId
      ? { simulationRunId, scenarioId, windowSizeSec, fromSimulationSec, toSimulationSec }
      : null

  const setFilter = useCallback(
    (key: string, value: string) => {
      setParams((prev) => {
        const next = new URLSearchParams(prev)
        if (value) next.set(key, value)
        else next.delete(key)
        return next
      })
    },
    [setParams],
  )

  const setFilters = useCallback(
    (updates: Record<string, string>) => {
      setParams((prev) => {
        const next = new URLSearchParams(prev)
        for (const [key, value] of Object.entries(updates)) {
          if (value) next.set(key, value)
          else next.delete(key)
        }
        return next
      })
    },
    [setParams],
  )

  return {
    simulationRunId,
    scenarioId,
    windowSizeSec,
    fromSimulationSec,
    toSimulationSec,
    metricCode,
    selectedIntersectionId,
    analyticsQuery,
    setFilter,
    setFilters,
    searchParams: params,
  }
}

export function AnalyticsOverviewPage() {
  const navigate = useNavigate()
  const [advancedOpen, setAdvancedOpen] = useState(false)

  const {
    simulationRunId, scenarioId, windowSizeSec,
    fromSimulationSec, toSimulationSec,
    metricCode, selectedIntersectionId, analyticsQuery, setFilter, setFilters,
    searchParams,
  } = useAnalyticsFilters()

  const {
    intersections,
    status: listStatus,
    statusMessage,
    isLoading: intLoading,
    isUnavailable: intUnavailable,
    isEmpty: intEmpty,
    refetch: refetchIntersections,
  } = useIntersectionList()

  const bootstrapIntersectionId =
    selectedIntersectionId || intersections?.[0]?.id || null

  const { isBootstrapping, bootstrapError } = useRealtimeContextBootstrap({
    intersectionId: bootstrapIntersectionId,
    simulationRunId,
    scenarioId,
    setFilters,
  })

  const filtersReady = Boolean(analyticsQuery)

  const congestionQuery = useAnalyticsCongestion(analyticsQuery)
  const priorityQuery = useAnalyticsPriority(analyticsQuery)
  const trendIntersectionId =
    selectedIntersectionId ||
    priorityQuery.data?.items?.[0]?.intersectionId ||
    congestionQuery.data?.items?.[0]?.intersectionId ||
    intersections?.[0]?.id ||
    null
  const trendVolumeQuery = useAnalyticsTrends(trendIntersectionId, analyticsQuery, 'AVG_VEHICLE_COUNT')
  const trendSpeedQuery = useAnalyticsTrends(trendIntersectionId, analyticsQuery, 'AVG_SPEED_KMH')
  const signalQuery = useSignalOperationWindows(analyticsQuery)

  const nodes = useMemo(
    () => (intersections ?? []).map(mapIntersectionToNode),
    [intersections],
  )

  const congestionBars = useMemo(
    () => buildCongestionBarData(congestionQuery.data?.items ?? []),
    [congestionQuery.data],
  )
  const priorityBars = useMemo(
    () => buildPriorityBarData(priorityQuery.data?.items ?? []),
    [priorityQuery.data],
  )
  const volumeTrend = useMemo(
    () => buildTrendSeries(trendVolumeQuery.data?.items ?? []),
    [trendVolumeQuery.data],
  )
  const speedTrend = useMemo(
    () => buildTrendSeries(trendSpeedQuery.data?.items ?? []),
    [trendSpeedQuery.data],
  )
  const congestionDist = useMemo(
    () => buildCongestionDistribution(congestionQuery.data?.items ?? []),
    [congestionQuery.data],
  )

  const hasAnalyticsRows =
    congestionBars.length > 0 ||
    priorityBars.length > 0 ||
    volumeTrend.length > 0 ||
    speedTrend.length > 0

  const featureState = deriveAnalyticsFeatureState(congestionQuery.error, filtersReady)
  const serviceState = deriveAnalyticsServiceState(
    congestionQuery.isLoading,
    congestionQuery.error,
    filtersReady,
  )
  const dataState = deriveAnalyticsDataState(
    filtersReady,
    congestionQuery.isLoading,
    hasAnalyticsRows,
    congestionQuery.error,
  )

  const avgCongestion = useMemo(() => {
    const items = congestionQuery.data?.items ?? []
    if (items.length === 0) return null
    const valid = items.filter((i) => i.numericValue !== null)
    if (valid.length === 0) return null
    return (valid.reduce((s, i) => s + (i.numericValue ?? 0), 0) / valid.length).toFixed(1)
  }, [congestionQuery.data])

  const navigateToIntersection = (id: string) => {
    setStoredSelectedIntersectionId(id)
    setFilter('intersectionId', id)
    navigate(appendAnalyticsQuery(`/intersections/${encodeURIComponent(id)}`, searchParams))
  }

  const showSetupBanner = !filtersReady && !isBootstrapping

  return (
    <div style={{ padding: '14px 18px', display: 'flex', flexDirection: 'column', gap: 14, minHeight: '100%' }}>

      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
        <div>
          <h1 style={{ fontSize: 17, fontWeight: 700, color: 'var(--text-primary)', margin: 0 }}>
            Analytics Overview
          </h1>
          <p style={{ fontSize: 12, color: 'var(--text-muted)', margin: '4px 0 0' }}>
            Network KPIs, priority ranking, and Gold window analytics
          </p>
        </div>
        <AnalyticsStatusBar
          featureState={featureState}
          serviceState={serviceState}
          dataState={dataState}
          networkMartDeferred
        />
      </div>

      {(showSetupBanner || isBootstrapping) && (
        <div
          style={{
            padding: '12px 16px',
            borderRadius: 8,
            border: '1px solid rgba(22,140,255,0.25)',
            background: 'rgba(22,140,255,0.06)',
            display: 'flex',
            gap: 10,
            alignItems: 'flex-start',
          }}
          role="status"
        >
          <RefreshCw size={16} style={{ color: 'var(--blue)', marginTop: 2, flexShrink: 0 }} />
          <div>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
              {isBootstrapping
                ? 'Loading simulation context from Realtime aggregate…'
                : 'Analytics filters not set yet'}
            </div>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>
              {isBootstrapping
                ? `Reading run/scenario from intersection ${bootstrapIntersectionId ?? '—'}.`
                : bootstrapIntersectionId
                ? 'Waiting for Realtime metadata (simulationRunId, scenarioId). Start SUMO or select an intersection.'
                : 'Load intersections from Orion first, or enter Run and Scenario manually below.'}
            </div>
            {Boolean(bootstrapError) && (
              <div style={{ fontSize: 11, color: 'var(--orange)', marginTop: 6 }}>
                Bootstrap failed — enter Run and Scenario manually.
              </div>
            )}
          </div>
        </div>
      )}

      {congestionQuery.error && classifyAnalyticsError(congestionQuery.error) === 'ANALYTICS_DISABLED' && (
        <AnalyticsDisabledPanel />
      )}

      {/* Filter bar */}
      <div
        className="card"
        style={{ padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 10 }}
        role="search"
        aria-label="Analytics filters"
      >
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, alignItems: 'flex-end' }}>
          <Filter size={15} style={{ color: 'var(--text-muted)', marginBottom: 6 }} />
          <FilterField id="filter-runId" label="Run" value={simulationRunId} placeholder="auto from Realtime" onChange={(v) => setFilter('simulationRunId', v)} wide />
          <FilterField id="filter-scenarioId" label="Scenario" value={scenarioId} placeholder="e.g. normal" onChange={(v) => setFilter('scenarioId', v)} />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <label htmlFor="filter-windowSize" style={LABEL_STYLE}>Window</label>
            <select id="filter-windowSize" value={windowSizeSec} onChange={(e) => setFilter('windowSizeSec', e.target.value)} style={SELECT_STYLE}>
              {WINDOW_SIZES.map((w) => <option key={w} value={w}>{w}s</option>)}
            </select>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <label htmlFor="filter-intersection" style={LABEL_STYLE}>Intersection</label>
            <select
              id="filter-intersection"
              value={selectedIntersectionId}
              onChange={(e) => setFilter('intersectionId', e.target.value)}
              style={{ ...SELECT_STYLE, minWidth: 120 }}
            >
              <option value="">All / first</option>
              {(intersections ?? []).map((i) => (
                <option key={i.id ?? ''} value={i.id ?? ''}>
                  {getIntersectionDisplayName(i.id, i.name)}
                </option>
              ))}
            </select>
          </div>
          <button
            type="button"
            onClick={() => {
              void congestionQuery.refetch()
              void priorityQuery.refetch()
            }}
            disabled={!filtersReady}
            style={{
              padding: '6px 12px',
              borderRadius: 6,
              border: '1px solid var(--border)',
              background: 'rgba(22,140,255,0.1)',
              color: 'var(--blue)',
              fontSize: 12,
              fontWeight: 600,
              cursor: filtersReady ? 'pointer' : 'not-allowed',
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              marginBottom: 2,
            }}
          >
            <RefreshCw size={13} /> Refresh
          </button>
          <button
            type="button"
            onClick={() => setAdvancedOpen((v) => !v)}
            style={{
              padding: '6px 10px',
              borderRadius: 6,
              border: '1px solid var(--border)',
              background: 'transparent',
              color: 'var(--text-secondary)',
              fontSize: 12,
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: 4,
              marginBottom: 2,
            }}
          >
            Advanced {advancedOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </button>
        </div>

        {advancedOpen && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, paddingTop: 8, borderTop: '1px solid var(--border)' }}>
            <FilterField id="filter-from" label="From Sim Sec" value={fromSimulationSec?.toString() ?? ''} placeholder="optional" type="number" onChange={(v) => setFilter('fromSimulationSec', v)} />
            <FilterField id="filter-to" label="To Sim Sec" value={toSimulationSec?.toString() ?? ''} placeholder="optional" type="number" onChange={(v) => setFilter('toSimulationSec', v)} />
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              <label htmlFor="filter-metric" style={LABEL_STYLE}>Metric</label>
              <select id="filter-metric" value={metricCode} onChange={(e) => setFilter('metricCode', e.target.value)} style={SELECT_STYLE}>
                {METRIC_CODES.map((m) => <option key={m} value={m}>{m}</option>)}
              </select>
            </div>
          </div>
        )}
      </div>

      {/* KPI row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 10 }}>
        {intLoading ? (
          <SkeletonKpiCard />
        ) : (
          <KpiCard
            id="kpi-total-intersections"
            label="Total Intersections"
            value={intUnavailable ? '—' : intEmpty ? 0 : (intersections?.length ?? 0)}
            unavailable={intUnavailable}
            subtitle={intUnavailable ? statusMessage : intEmpty ? 'Orion list empty' : 'from Orion entity list'}
            icon={<BarChart3 size={16} color="#168CFF" />}
            iconBg="rgba(22,140,255,0.15)"
          />
        )}
        <KpiCard
          id="kpi-analytics-service"
          label="Analytics Service"
          value={serviceState === 'READY' ? 'Ready' : serviceState === 'NOT_READY' ? 'Not Ready' : serviceState === 'DISABLED' ? 'Disabled' : '—'}
          subtitle={filtersReady ? 'Congestion mart probe' : 'Awaiting filters'}
          icon={<Wifi size={16} color="#16C7E8" />}
          iconBg="rgba(22,199,232,0.15)"
          isLoading={filtersReady && congestionQuery.isLoading}
        />
        <KpiCard
          id="kpi-avg-congestion"
          label="Avg Congestion"
          value={!filtersReady ? '—' : congestionQuery.isLoading ? '—' : (avgCongestion ?? '—')}
          subtitle={dataState === 'EMPTY' ? 'No rows for filters' : 'Selected window'}
          icon={<Gauge size={16} color="#F59E0B" />}
          iconBg="rgba(245,158,11,0.15)"
          isLoading={congestionQuery.isLoading && filtersReady}
        />
        <KpiCard
          id="kpi-vehicles-window"
          label="Vehicles in Window"
          value={(() => {
            if (!filtersReady) return '—'
            const items = trendVolumeQuery.data?.items ?? []
            if (!items.length) return '—'
            return items[items.length - 1]?.currentValue?.toFixed(0) ?? '—'
          })()}
          icon={<Car size={16} color="#22C55E" />}
          iconBg="rgba(34,197,94,0.15)"
          isLoading={trendVolumeQuery.isLoading && filtersReady}
        />
        <KpiCard
          id="kpi-avg-speed"
          label="Avg Speed"
          value={(() => {
            if (!filtersReady) return '—'
            const items = trendSpeedQuery.data?.items ?? []
            if (!items.length) return '—'
            const v = items[items.length - 1]?.currentValue
            return v != null ? `${v.toFixed(1)} km/h` : '—'
          })()}
          icon={<TrendingUp size={16} color="#16C7E8" />}
          iconBg="rgba(22,199,232,0.15)"
          isLoading={trendSpeedQuery.isLoading && filtersReady}
        />
        <KpiCard
          id="kpi-congested"
          label="Congested"
          value={intUnavailable ? '—' : (intersections ?? []).filter((i) => {
            const c = trafficStatusColor(i.overallTrafficStatus)
            return c === 'orange' || c === 'red'
          }).length}
          unavailable={intUnavailable}
          subtitle="overallTrafficStatus"
          icon={<Activity size={16} color="#EF4444" />}
          iconBg="rgba(239,68,68,0.15)"
          isLoading={intLoading}
        />
      </div>

      {/* Map + priority — 62/38 split */}
      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.65fr) minmax(280px, 1fr)', gap: 14, minHeight: 320 }}>
        <div className="card" style={{ padding: 14 }}>
          <SchematicMap
            nodes={nodes}
            listStatus={listStatus}
            statusMessage={statusMessage}
            selectedId={selectedIntersectionId || null}
            onSelect={(id) => setFilter('intersectionId', id)}
            onRetry={() => void refetchIntersections()}
          />
        </div>

        <div className="card" style={{ padding: 14 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 12 }}>
            Top Priority Intersections
          </div>
          {!filtersReady ? (
            <p style={{ color: 'var(--text-muted)', fontSize: 12 }}>Set Run and Scenario to load priority ranking.</p>
          ) : priorityQuery.isLoading ? (
            <div style={{ height: 120, background: '#0C2235', borderRadius: 6 }} />
          ) : priorityQuery.error ? (
            <ErrorState
              title="Priority unavailable"
              message={classifyAnalyticsError(priorityQuery.error) === 'ANALYTICS_NOT_READY'
                ? 'Mart not ready for selected filters.'
                : 'Unable to load priority data.'}
              retryable
              onRetry={() => void priorityQuery.refetch()}
            />
          ) : priorityBars.length === 0 ? (
            <p style={{ color: 'var(--text-muted)', fontSize: 12 }}>No priority rows for Run={simulationRunId}, Scenario={scenarioId}, Window={windowSizeSec}s.</p>
          ) : (
            <div>
              {priorityBars.slice(0, 8).map((item, idx) => (
                <button
                  key={item.intersectionId}
                  onClick={() => navigateToIntersection(item.intersectionId)}
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '24px 1fr 64px',
                    gap: 8,
                    width: '100%',
                    padding: '8px 0',
                    borderBottom: '1px solid rgba(24,58,82,0.5)',
                    background: 'transparent',
                    border: 'none',
                    cursor: 'pointer',
                    textAlign: 'left',
                  }}
                >
                  <span style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>{idx + 1}</span>
                  <span style={{ fontSize: 12, color: 'var(--text-primary)' }}
                    title={item.intersectionId}
                  >
                    {getIntersectionDisplayName(
                      item.intersectionId,
                      (intersections ?? []).find((x) => x.id === item.intersectionId || x.id?.endsWith(`:${item.intersectionId}`))?.name,
                    )}
                  </span>
                  <span style={{
                    fontSize: 12, fontWeight: 700, textAlign: 'right',
                    color: item.priorityScore > 70 ? '#EF4444' : item.priorityScore > 50 ? '#F59E0B' : '#22C55E',
                  }}>
                    {item.priorityScore.toFixed(1)}
                  </span>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Charts — only when filters ready */}
      {filtersReady ? (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: 14 }}>
          <CongestionScoreByIntersectionChart
            data={congestionBars}
            isLoading={congestionQuery.isLoading}
            error={congestionQuery.error}
            onRetry={() => void congestionQuery.refetch()}
          />
          <PriorityIntersectionRanking
            data={priorityBars}
            isLoading={priorityQuery.isLoading}
            error={priorityQuery.error}
            onRetry={() => void priorityQuery.refetch()}
          />
          <TrafficVolumeChart
            data={volumeTrend}
            intersectionId={trendIntersectionId ?? '—'}
            isLoading={trendVolumeQuery.isLoading}
            error={trendVolumeQuery.error}
            onRetry={() => void trendVolumeQuery.refetch()}
          />
          <AverageSpeedTrendChart
            data={speedTrend}
            intersectionId={trendIntersectionId ?? '—'}
            isLoading={trendSpeedQuery.isLoading}
            error={trendSpeedQuery.error}
            onRetry={() => void trendSpeedQuery.refetch()}
          />
          <CongestionDistributionChart
            data={congestionDist}
            isLoading={congestionQuery.isLoading}
            error={congestionQuery.error}
            onRetry={() => void congestionQuery.refetch()}
          />
        </div>
      ) : null}

      {/* Intersection table */}
      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <h2 style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>Intersection Status</h2>
          <FreshnessBadge state={intLoading ? 'idle' : intUnavailable ? 'error' : 'live'} />
        </div>
        {intUnavailable ? (
          <ErrorState
            title="Unable to load intersections"
            message={statusMessage}
            retryable
            onRetry={() => void refetchIntersections()}
          />
        ) : intEmpty ? (
          <p style={{ color: listStatus === 'run_mismatch' ? 'var(--text-secondary)' : 'var(--text-muted)', fontSize: 13, padding: '12px 0' }}>
            {statusMessage}
          </p>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border)' }}>
                  {['Intersection', 'Status', 'State', 'Vehicles', 'Scenario', 'Phase', 'Incident', ''].map((h) => (
                    <th key={h} style={{ padding: '6px 10px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 600, fontSize: 10, textTransform: 'uppercase' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(intersections ?? []).map((i) => (
                  <tr key={i.id ?? i.name} style={{ borderBottom: '1px solid rgba(24,58,82,0.5)' }}>
                    <td style={{ padding: '8px 10px', color: 'var(--text-primary)', fontWeight: 500 }}
                      title={i.id ?? ''}>
                      {getIntersectionDisplayName(i.id, i.name)}
                    </td>
                    <td style={{ padding: '8px 10px' }}><TrafficStatusBadge status={i.overallTrafficStatus} /></td>
                    <td style={{ padding: '8px 10px', color: 'var(--text-secondary)' }}>{i.derivedTrafficState ?? '—'}</td>
                    <td style={{ padding: '8px 10px', fontWeight: 600 }}>{i.totalVehicleCount ?? '—'}</td>
                    <td style={{ padding: '8px 10px', color: 'var(--text-secondary)' }}>{i.scenarioId ?? '—'}</td>
                    <td style={{ padding: '8px 10px', color: 'var(--text-secondary)' }}>{i.currentPhase ?? '—'}</td>
                    <td style={{ padding: '8px 10px' }}>
                      {i.hasActiveIncident ? (
                        <span style={{ color: '#EF4444', display: 'flex', alignItems: 'center', gap: 4 }}><AlertTriangle size={12} /> Yes</span>
                      ) : 'None'}
                    </td>
                    <td style={{ padding: '8px 10px' }}>
                      {i.id && (
                        <button
                          onClick={() => navigateToIntersection(i.id!)}
                          style={{
                            padding: '4px 10px', borderRadius: 5, fontSize: 11, fontWeight: 600,
                            background: 'rgba(22,140,255,0.1)', border: '1px solid rgba(22,140,255,0.3)',
                            color: 'var(--blue)', cursor: 'pointer',
                          }}
                        >
                          View <ChevronRight size={12} style={{ display: 'inline', verticalAlign: 'middle' }} />
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Signal table */}
      {filtersReady && (
        <div className="card">
          <h2 style={{ fontSize: 13, fontWeight: 600, marginBottom: 12 }}>Signal Operation Performance</h2>
          {signalQuery.isLoading ? (
            <div style={{ height: 60, background: '#0C2235', borderRadius: 6 }} />
          ) : (signalQuery.data?.items ?? []).length === 0 ? (
            <p style={{ color: 'var(--text-muted)', fontSize: 12 }}>No signal operation rows for current filters.</p>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid var(--border)' }}>
                    {['Intersection', 'Direction', 'Window', 'Green%', 'Red%', 'Phase', 'Quality'].map((h) => (
                      <th key={h} style={{ padding: '6px 10px', textAlign: 'left', color: 'var(--text-muted)', fontSize: 10, textTransform: 'uppercase' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(signalQuery.data?.items ?? []).slice(0, 20).map((row, idx) => (
                    <tr key={idx} style={{ borderBottom: '1px solid rgba(24,58,82,0.5)' }}>
                      <td style={{ padding: '7px 10px' }}>{row.intersectionId}</td>
                      <td style={{ padding: '7px 10px' }}>{row.direction ?? '—'}</td>
                      <td style={{ padding: '7px 10px', color: 'var(--text-muted)' }}>{row.windowStartSimSec.toFixed(0)}–{row.windowEndSimSec.toFixed(0)}s</td>
                      <td style={{ padding: '7px 10px', color: '#22C55E' }}>{row.greenSharePct?.toFixed(1) ?? '—'}%</td>
                      <td style={{ padding: '7px 10px', color: '#EF4444' }}>{row.redSharePct?.toFixed(1) ?? '—'}%</td>
                      <td style={{ padding: '7px 10px' }}>{row.dominantPhase ?? '—'}</td>
                      <td style={{ padding: '7px 10px' }}>{row.qualityStatus ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

const LABEL_STYLE: React.CSSProperties = {
  fontSize: 10,
  color: 'var(--text-muted)',
  fontWeight: 600,
  textTransform: 'uppercase',
}

const SELECT_STYLE: React.CSSProperties = {
  background: 'var(--bg-elevated)',
  border: '1px solid var(--border)',
  borderRadius: 6,
  padding: '5px 10px',
  color: 'var(--text-primary)',
  fontSize: 12,
  outline: 'none',
  cursor: 'pointer',
}

function FilterField({
  id, label, value, placeholder, type = 'text', onChange, wide,
}: {
  id: string
  label: string
  value: string
  placeholder?: string
  type?: string
  onChange: (v: string) => void
  wide?: boolean
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <label htmlFor={id} style={LABEL_STYLE}>{label}</label>
      <input
        id={id}
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        style={{
          background: 'var(--bg-elevated)',
          border: '1px solid var(--border)',
          borderRadius: 6,
          padding: '5px 10px',
          color: 'var(--text-primary)',
          fontSize: 12,
          outline: 'none',
          width: wide ? 180 : 120,
        }}
      />
    </div>
  )
}
