// AnalyticsCharts.tsx — All 6 analytics charts using Recharts
// Data sourced only from verified analytics APIs. No hard-coded data.

import {
  ResponsiveContainer,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, Cell,
  LineChart, Line,
  AreaChart, Area,
  PieChart, Pie,
} from 'recharts'
import { ChartWrapper } from './ChartWrapper'
import type { CongestionBarItem, CongestionBucket, TrendDataPoint, PriorityBarItem } from '@/transforms/analyticsTransforms'

const GRID_STROKE = 'rgba(24,58,82,0.8)'
const AXIS_STYLE = { fill: '#71889B', fontSize: 11 }
const TOOLTIP_STYLE = {
  backgroundColor: '#0C2235',
  border: '1px solid #183A52',
  borderRadius: 8,
  color: '#F8FAFC',
  fontSize: 12,
}

// ── 1. Congestion Score by Intersection (horizontal bar) ──────────────────────
interface CongestionBarChartProps {
  data: CongestionBarItem[]
  isLoading?: boolean
  error?: unknown
  onRetry?: () => void
}

export function CongestionScoreByIntersectionChart({
  data, isLoading, error, onRetry,
}: CongestionBarChartProps) {
  return (
    <ChartWrapper
      id="chart-congestion-bar"
      title="Congestion Score by Intersection"
      isLoading={isLoading}
      isEmpty={!isLoading && !error && data.length === 0}
      error={error}
      onRetry={onRetry}
      height={Math.max(180, data.length * 28)}
    >
      <ResponsiveContainer width="100%" height={Math.max(180, data.length * 28)}>
        <BarChart data={data} layout="vertical" margin={{ top: 0, right: 40, left: 8, bottom: 0 }}>
          <CartesianGrid horizontal={false} stroke={GRID_STROKE} />
          <XAxis type="number" domain={[0, 100]} tick={AXIS_STYLE} />
          <YAxis
            type="category"
            dataKey="intersectionId"
            tick={AXIS_STYLE}
            width={90}
          />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            formatter={(value: unknown, _name: unknown, entry: { payload?: { status?: string } }) => [
              `${typeof value === 'number' ? value.toFixed(1) : String(value ?? '—')} — ${entry?.payload?.status ?? ''}`,
              'Congestion Score',
            ]}
          />
          <Bar dataKey="numericValue" radius={[0, 4, 4, 0]} label={{ position: 'right', fill: '#A8BDCE', fontSize: 11 }}>
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.color} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartWrapper>
  )
}

// ── 2. Priority Intersection Ranking ──────────────────────────────────────────
interface PriorityChartProps {
  data: PriorityBarItem[]
  isLoading?: boolean
  error?: unknown
  onRetry?: () => void
}

export function PriorityIntersectionRanking({ data, isLoading, error, onRetry }: PriorityChartProps) {
  return (
    <ChartWrapper
      id="chart-priority"
      title="Priority Intersection Ranking"
      subtitle="Source: Gold priority mart"
      isLoading={isLoading}
      isEmpty={!isLoading && !error && data.length === 0}
      error={error}
      onRetry={onRetry}
      height={Math.max(180, data.length * 28)}
    >
      <ResponsiveContainer width="100%" height={Math.max(180, data.length * 28)}>
        <BarChart data={data} layout="vertical" margin={{ top: 0, right: 40, left: 8, bottom: 0 }}>
          <CartesianGrid horizontal={false} stroke={GRID_STROKE} />
          <XAxis type="number" tick={AXIS_STYLE} />
          <YAxis type="category" dataKey="intersectionId" tick={AXIS_STYLE} width={90} />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            formatter={(value: unknown, _name: unknown, entry: { payload?: { priorityRank?: number } }) => [
              `${typeof value === 'number' ? value.toFixed(2) : String(value ?? '—')} (Rank: ${entry?.payload?.priorityRank ?? '—'})`,
              'Priority Score',
            ]}
          />
          <Bar dataKey="priorityScore" fill="#8B5CF6" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </ChartWrapper>
  )
}

// ── 3. Traffic Volume Chart (AreaChart, VEHICLE_COUNT) ───────────────────────
interface TrendChartProps {
  data: TrendDataPoint[]
  intersectionId: string
  metricLabel?: string
  unit?: string
  isLoading?: boolean
  error?: unknown
  onRetry?: () => void
}

export function TrafficVolumeChart({ data, intersectionId, isLoading, error, onRetry }: TrendChartProps) {
  return (
    <ChartWrapper
      id="chart-traffic-volume"
      title="Traffic Volume"
      subtitle={`Intersection: ${intersectionId} — metric: AVG_VEHICLE_COUNT`}
      isLoading={isLoading}
      isEmpty={!isLoading && !error && data.length === 0}
      error={error}
      onRetry={onRetry}
      height={180}
    >
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={data} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="areaGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#168CFF" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#168CFF" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
          <XAxis dataKey="windowStart" tick={AXIS_STYLE} tickFormatter={(v) => `${Math.round(v)}s`} />
          <YAxis tick={AXIS_STYLE} />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            labelFormatter={(v) => `Window start: ${v}s`}
            formatter={(value: unknown) => [value !== null && value !== undefined ? Number(value).toFixed(0) : '—', 'Vehicle Count (current window)']}
          />
          <Legend wrapperStyle={{ fontSize: 11, color: '#A8BDCE' }} />
          <Area
            type="monotone"
            dataKey="currentValue"
            stroke="#168CFF"
            fill="url(#areaGradient)"
            name="Current"
            dot={false}
            activeDot={{ r: 4 }}
            connectNulls
          />
          {data.some((d) => d.previousValue !== null) && (
            <Area
              type="monotone"
              dataKey="previousValue"
              stroke="#71889B"
              fill="none"
              strokeDasharray="4 4"
              name="Previous window"
              dot={false}
              connectNulls
            />
          )}
        </AreaChart>
      </ResponsiveContainer>
    </ChartWrapper>
  )
}

// ── 4. Average Speed Trend (LineChart, SPEED_KMH) ────────────────────────────
export function AverageSpeedTrendChart({ data, intersectionId, isLoading, error, onRetry }: TrendChartProps) {
  return (
    <ChartWrapper
      id="chart-speed-trend"
      title="Average Speed Trend"
      subtitle={`${intersectionId} — km/h`}
      isLoading={isLoading}
      isEmpty={!isLoading && !error && data.length === 0}
      error={error}
      onRetry={onRetry}
      height={180}
    >
      <ResponsiveContainer width="100%" height={180}>
        <LineChart data={data} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
          <XAxis dataKey="windowStart" tick={AXIS_STYLE} tickFormatter={(v) => `${Math.round(v)}s`} />
          <YAxis tick={AXIS_STYLE} unit=" km/h" />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            formatter={(value: unknown) => [value !== null && value !== undefined ? `${Number(value).toFixed(1)} km/h` : '—', 'Avg Speed']}
          />
          <Legend wrapperStyle={{ fontSize: 11, color: '#A8BDCE' }} />
          <Line
            type="monotone"
            dataKey="currentValue"
            stroke="#16C7E8"
            dot={false}
            activeDot={{ r: 4 }}
            name="Speed (km/h)"
            connectNulls
          />
        </LineChart>
      </ResponsiveContainer>
    </ChartWrapper>
  )
}

// ── 5. Congestion Distribution (Donut) ────────────────────────────────────────
interface CongestionDistributionProps {
  data: CongestionBucket[]
  isLoading?: boolean
  error?: unknown
  onRetry?: () => void
}

interface PieLabelRenderProps {
  cx?: number
  cy?: number
  midAngle?: number
  innerRadius?: number
  outerRadius?: number
  percentage?: number
}

export function CongestionDistributionChart({ data, isLoading, error, onRetry }: CongestionDistributionProps) {
  const RADIAN = Math.PI / 180
  const renderLabel = (props: PieLabelRenderProps) => {
    const { cx = 0, cy = 0, midAngle = 0, innerRadius = 0, outerRadius = 0, percentage } = props
    if (!percentage || percentage < 5) return null
    const radius = innerRadius + (outerRadius - innerRadius) * 0.5
    const x = cx + radius * Math.cos(-midAngle * RADIAN)
    const y = cy + radius * Math.sin(-midAngle * RADIAN)
    return (
      <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central" fontSize={10} fontWeight={600}>
        {`${percentage}%`}
      </text>
    )
  }

  return (
    <ChartWrapper
      id="chart-congestion-distribution"
      title="Congestion Distribution"
      isLoading={isLoading}
      isEmpty={!isLoading && !error && data.length === 0}
      error={error}
      onRetry={onRetry}
      height={200}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
        <ResponsiveContainer width={200} height={200}>
          <PieChart>
            <Pie
              data={data}
              dataKey="count"
              nameKey="status"
              cx="50%"
              cy="50%"
              innerRadius={55}
              outerRadius={85}
              labelLine={false}
              label={renderLabel}
            >
              {data.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              formatter={(value: unknown, _name: unknown, entry: { payload?: { status?: string; percentage?: number } }) => [
                `${String(value ?? '')} (${entry?.payload?.percentage ?? 0}%)`,
                entry?.payload?.status ?? '',
              ]}
            />
          </PieChart>
        </ResponsiveContainer>
        {/* Legend */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 11 }}>
          {data.map((d) => (
            <div key={d.status} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 10, height: 10, borderRadius: 2, background: d.color, flexShrink: 0 }} />
              <span style={{ color: 'var(--text-secondary)' }}>{d.status}</span>
              <span style={{ color: 'var(--text-muted)', marginLeft: 'auto' }}>{d.percentage}%</span>
            </div>
          ))}
        </div>
      </div>
    </ChartWrapper>
  )
}
