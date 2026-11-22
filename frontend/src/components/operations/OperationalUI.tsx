import type { ReactNode } from 'react'
import { AlertTriangle, Database, LoaderCircle } from 'lucide-react'

export type StatusTone = 'neutral' | 'info' | 'healthy' | 'warning' | 'critical'

export function StatusPill({ children, tone = 'neutral', pulse = false }: { children: ReactNode; tone?: StatusTone; pulse?: boolean }) {
  return <span className={`ops-status ops-status-${tone}${pulse ? ' is-pulsing' : ''}`}><span className="ops-status-dot" />{children}</span>
}

export function PageHeader({ title, description, actions, meta }: { title: string; description: string; actions?: ReactNode; meta?: ReactNode }) {
  return <div className="ops-page-header"><div><h1>{title}</h1><p>{description}</p>{meta && <div className="ops-page-meta">{meta}</div>}</div>{actions && <div className="ops-page-actions">{actions}</div>}</div>
}

export function Panel({ children, className = '', ariaLabel }: { children: ReactNode; className?: string; ariaLabel?: string }) {
  return <section className={`ops-panel ${className}`} aria-label={ariaLabel}>{children}</section>
}

export function PanelHeader({ title, description, actions }: { title: string; description?: string; actions?: ReactNode }) {
  return <div className="ops-panel-header"><div><h2>{title}</h2>{description && <p>{description}</p>}</div>{actions && <div className="ops-panel-actions">{actions}</div>}</div>
}

export interface MetricItem {
  label: string
  value: string
  unit?: string
  meta?: string
  icon?: ReactNode
  tone?: StatusTone
  unavailable?: boolean
}

export function MetricStrip({ metrics }: { metrics: MetricItem[] }) {
  return <div className="ops-metric-strip">{metrics.map((metric) => <div className={`ops-metric${metric.unavailable ? ' is-unavailable' : ''}`} key={metric.label}>{metric.icon && <span className="ops-metric-icon">{metric.icon}</span>}<div><span className="ops-metric-label">{metric.label}</span><div className={`ops-metric-value tone-${metric.tone ?? 'info'}`}>{metric.value}{metric.unit && <small>{metric.unit}</small>}</div>{metric.meta && <span className="ops-metric-meta">{metric.meta}</span>}</div></div>)}</div>
}

export function DataNotice({ state, title, description, action }: { state: 'loading' | 'empty' | 'warning' | 'error'; title: string; description: string; action?: ReactNode }) {
  const Icon = state === 'loading' ? LoaderCircle : state === 'empty' ? Database : AlertTriangle
  return <div className={`ops-data-notice is-${state}`}><Icon size={20} className={state === 'loading' ? 'animate-spin' : ''} /><div><strong>{title}</strong><p>{description}</p></div>{action && <div className="ops-data-notice-action">{action}</div>}</div>
}

