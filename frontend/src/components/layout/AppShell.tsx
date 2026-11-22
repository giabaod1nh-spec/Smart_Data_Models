import { useEffect, useMemo, useState } from 'react'
import { NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { Activity, BrainCircuit, ChevronLeft, ChevronRight, FlaskConical, LogOut, Menu, Network, ServerCog, X } from 'lucide-react'
import { useAuth } from '@/features/auth/AuthContext'
import { useSystemStatus } from '@/hooks/useSystemStatus'
import { getSystemStatusInfo } from '@/utils/systemStatus'
import { UI_PREVIEW_MODE } from '@/utils/uiPreviewMode'

const NAV_ITEMS = [
  { to: '/operations', label: 'Operations', description: 'Network performance', icon: Activity },
  { to: '/intersections', label: 'Intersections', description: 'Current-run inventory', icon: Network },
  { to: '/adaptive-control', label: 'Adaptive Control', description: 'DQN decisions', icon: BrainCircuit },
  { to: '/experiments', label: 'Experiments', description: 'Run comparison', icon: FlaskConical },
  { to: '/system', label: 'System', description: 'Pipeline health', icon: ServerCog },
] as const

function pageTitle(pathname: string): string {
  if (pathname.startsWith('/intersections/')) return 'Intersection detail'
  if (pathname.startsWith('/intersections')) return 'Intersections'
  if (pathname.startsWith('/adaptive-control')) return 'Adaptive control'
  if (pathname.startsWith('/experiments')) return 'Experiments'
  if (pathname.startsWith('/system')) return 'System monitoring'
  if (pathname.startsWith('/analytics')) return 'Gold analytics'
  return 'Traffic operations'
}

export function AppShell() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()
  const { status } = useSystemStatus()
  const statusInfo = getSystemStatusInfo(status)
  const [collapsed, setCollapsed] = useState(false)
  const [mobileOpen, setMobileOpen] = useState(false)
  const [loggingOut, setLoggingOut] = useState(false)
  const [now, setNow] = useState(() => new Date())

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000)
    return () => window.clearInterval(timer)
  }, [])

  const timeLabel = useMemo(
    () => now.toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
    [now],
  )

  const handleLogout = async () => {
    setLoggingOut(true)
    await logout()
    navigate('/login', { replace: true })
    setLoggingOut(false)
  }

  return (
    <div className="app-shell">
      {mobileOpen && <button className="app-sidebar-scrim" aria-label="Close navigation" onClick={() => setMobileOpen(false)} />}
      <aside className={`app-sidebar${collapsed ? ' is-collapsed' : ''}${mobileOpen ? ' is-open' : ''}`}>
        <div className="app-brand">
          <div className="app-brand-mark" aria-hidden="true"><Network size={18} /></div>
          {!collapsed && <div className="app-brand-copy"><strong>Smart Traffic</strong><span>Operations Center</span></div>}
          <button className="icon-button app-mobile-close" onClick={() => setMobileOpen(false)} aria-label="Close navigation"><X size={18} /></button>
        </div>
        <nav className="app-nav" aria-label="Primary navigation">
          {NAV_ITEMS.map(({ to, label, description, icon: Icon }) => (
            <NavLink key={to} to={to} onClick={() => setMobileOpen(false)} className={({ isActive }) => `app-nav-item${isActive || (to === '/intersections' && location.pathname.startsWith('/intersections/')) ? ' is-active' : ''}`} title={collapsed ? label : undefined}>
              <Icon size={19} aria-hidden="true" />
              {!collapsed && <span><strong>{label}</strong><small>{description}</small></span>}
            </NavLink>
          ))}
        </nav>
        <div className="app-sidebar-footer">
          <div className="sidebar-health" title={statusInfo.detail}>
            <span className={`status-dot ${status === 'OPERATIONAL' ? 'live' : status === 'DEGRADED' ? 'stale' : status === 'OFFLINE' ? 'offline' : 'unknown'}`} />
            {!collapsed && <span><small>Platform</small><strong>{statusInfo.label}</strong></span>}
          </div>
          <button className="app-collapse-button" onClick={() => setCollapsed((value) => !value)} aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'}>
            {collapsed ? <ChevronRight size={16} /> : <><ChevronLeft size={16} /><span>Collapse</span></>}
          </button>
        </div>
      </aside>
      <section className="app-workspace">
        <header className="app-topbar">
          <button className="icon-button app-mobile-menu" onClick={() => setMobileOpen(true)} aria-label="Open navigation"><Menu size={20} /></button>
          <div className="app-page-title"><span>Smart Traffic Operations Center</span><strong>{pageTitle(location.pathname)}</strong></div>
          {UI_PREVIEW_MODE && <span className="preview-mode-badge">UI preview · mock data</span>}
          <div className="app-topbar-meta">
            <div className="topbar-clock"><span>Local time</span><strong>{timeLabel}</strong></div>
            <div className="topbar-user" title={user?.username ?? 'Administrator'}>{user?.username?.[0]?.toUpperCase() ?? 'A'}</div>
            <button className="icon-button" onClick={() => void handleLogout()} disabled={loggingOut} aria-label="Sign out"><LogOut size={17} /></button>
          </div>
        </header>
        <main className="app-content"><Outlet /></main>
      </section>
    </div>
  )
}
