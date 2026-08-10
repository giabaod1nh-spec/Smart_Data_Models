// AppShell.tsx — Main layout shell: header + sidebar + content area

import React, { useCallback, useState } from 'react'
import { Outlet, useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import {
  LayoutDashboard,
  Map,
  TrafficCone,
  Activity,
  BarChart3,
  Settings,
  LogOut,
  Menu,
  X,
  Radio,
  ChevronRight,
  AlertTriangle,
} from 'lucide-react'
import { useAuth } from '@/features/auth/AuthContext'
import { useIntersectionList } from '@/hooks/useIntersectionList'
import { useSystemStatus } from '@/hooks/useSystemStatus'
import { getSystemStatusInfo } from '@/utils/systemStatus'
import { buildRunMismatchMessage } from '@/utils/intersectionListStatus'
import {
  appendAnalyticsQuery,
  resolveRealtimeIntersectionRoute,
  setStoredSelectedIntersectionId,
} from '@/utils/navigation'

interface NavItem {
  icon: React.ReactNode
  label: string
  action?: 'analytics' | 'realtime'
  disabled?: boolean
  badge?: string
}

const SIDEBAR_ITEMS: NavItem[] = [
  { icon: <LayoutDashboard size={18} />, label: 'Overview', action: 'analytics' },
  { icon: <Map size={18} />, label: 'Map', disabled: true, badge: 'Soon' },
  { icon: <TrafficCone size={18} />, label: 'Intersections', action: 'realtime' },
  { icon: <Activity size={18} />, label: 'Incidents', disabled: true, badge: 'Soon' },
  { icon: <Radio size={18} />, label: 'Signal Control', disabled: true, badge: 'Soon' },
  { icon: <BarChart3 size={18} />, label: 'Analytics', action: 'analytics' },
  { icon: <Settings size={18} />, label: 'System', disabled: true, badge: 'Soon' },
]

const NAV_TABS = [
  { label: 'Real-time Overview', action: 'realtime' as const },
  { label: 'Analytics', action: 'analytics' as const },
  { label: 'Signal Control', disabled: true },
  { label: 'Reports', disabled: true },
  { label: 'Settings', disabled: true },
]

export function AppShell() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()
  const [searchParams] = useSearchParams()
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [loggingOut, setLoggingOut] = useState(false)
  const [navNotice, setNavNotice] = useState<string | null>(null)

  const {
    intersections,
    status: listStatus,
    statusMessage,
    isLoading: listLoading,
    alignment,
    isRunMismatch,
  } = useIntersectionList()

  const { status: sysStatus } = useSystemStatus()
  const sysStatusInfo = getSystemStatusInfo(sysStatus)

  const selectedFromUrl = searchParams.get('intersectionId')
    ?? (location.pathname.startsWith('/intersections/')
      ? decodeURIComponent(location.pathname.split('/')[2] ?? '')
      : null)

  const navigateToRealtime = useCallback(() => {
    setNavNotice(null)
    const resolution = resolveRealtimeIntersectionRoute(
      listStatus,
      intersections,
      selectedFromUrl,
      statusMessage,
    )

    if (resolution.kind === 'loading') {
      setNavNotice('Loading intersections…')
      return
    }
    if (resolution.kind === 'empty') {
      setNavNotice('No realtime intersections are currently available.')
      return
    }
    if (resolution.kind === 'error') {
      setNavNotice(resolution.message)
      return
    }

    setStoredSelectedIntersectionId(resolution.intersectionId)
    const path = appendAnalyticsQuery(resolution.path, searchParams)
    navigate(path)
  }, [listStatus, intersections, selectedFromUrl, statusMessage, searchParams, navigate])

  const navigateToAnalytics = useCallback(() => {
    setNavNotice(null)
    const path = appendAnalyticsQuery('/analytics', searchParams)
    navigate(path)
  }, [navigate, searchParams])

  const handleNavAction = (action: 'analytics' | 'realtime') => {
    if (action === 'realtime') navigateToRealtime()
    else navigateToAnalytics()
  }

  const handleLogout = async () => {
    setLoggingOut(true)
    await logout()
    navigate('/login', { replace: true })
    setLoggingOut(false)
  }

  const isAnalyticsActive = location.pathname.startsWith('/analytics') || location.pathname === '/'
  const isRealtimeActive = location.pathname.startsWith('/intersections/')

  const now = new Date()
  const timeStr = now.toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  const dateStr = now.toLocaleDateString('vi-VN', { day: '2-digit', month: '2-digit', year: 'numeric' })

  return (
    <div className="flex h-screen overflow-hidden" style={{ background: 'var(--bg-base)' }}>
      <aside
        className="flex flex-col flex-shrink-0 transition-all duration-200"
        style={{
          width: sidebarOpen ? 220 : 56,
          background: '#071020',
          borderRight: '1px solid var(--border)',
        }}
      >
        <div className="flex items-center gap-3 px-4 py-3 border-b" style={{ borderColor: 'var(--border)' }}>
          <div
            className="flex-shrink-0 w-8 h-8 rounded-lg flex items-center justify-center"
            style={{ background: 'linear-gradient(135deg, #168CFF 0%, #16C7E8 100%)' }}
          >
            <TrafficCone size={16} className="text-white" />
          </div>
          {sidebarOpen && (
            <div className="min-w-0">
              <div className="text-xs font-bold truncate" style={{ color: 'var(--text-primary)' }}>Smart Traffic</div>
              <div className="text-[10px] truncate" style={{ color: 'var(--text-muted)' }}>Operations Center</div>
            </div>
          )}
        </div>

        <nav className="flex-1 py-3 overflow-y-auto">
          {SIDEBAR_ITEMS.map((item) => {
            const isActive =
              (item.action === 'analytics' && isAnalyticsActive) ||
              (item.action === 'realtime' && isRealtimeActive)
            return (
              <button
                key={item.label}
                onClick={() => !item.disabled && item.action && handleNavAction(item.action)}
                disabled={item.disabled}
                title={!sidebarOpen ? item.label : undefined}
                className="w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors"
                style={{
                  color: item.disabled
                    ? 'var(--text-muted)'
                    : isActive
                    ? 'var(--blue)'
                    : 'var(--text-secondary)',
                  background: isActive ? 'rgba(22,140,255,0.1)' : 'transparent',
                  borderLeft: isActive ? '2px solid var(--blue)' : '2px solid transparent',
                  cursor: item.disabled ? 'not-allowed' : 'pointer',
                }}
              >
                <span className="flex-shrink-0">{item.icon}</span>
                {sidebarOpen && (
                  <>
                    <span className="text-sm flex-1 truncate">{item.label}</span>
                    {item.badge && (
                      <span
                        className="text-[9px] px-1.5 py-0.5 rounded font-semibold"
                        style={{ background: 'rgba(113,136,155,0.2)', color: 'var(--text-muted)' }}
                      >
                        {item.badge}
                      </span>
                    )}
                  </>
                )}
              </button>
            )
          })}
        </nav>

        {sidebarOpen && (
          <div className="px-4 py-3 border-t" style={{ borderColor: 'var(--border)' }}>
            <div className="text-[10px] font-semibold mb-1" style={{ color: 'var(--text-muted)' }}>SYSTEM STATUS</div>
            <div className="flex items-center gap-1.5">
              <span
                className={`w-2 h-2 rounded-full${sysStatus === 'OPERATIONAL' ? ' animate-pulse' : ''}`}
                style={{ background: sysStatusInfo.color }}
              />
              <span className="text-xs" style={{ color: sysStatusInfo.color }}>
                {sysStatusInfo.label}
              </span>
            </div>
            {sysStatus !== 'OPERATIONAL' && sysStatus !== 'UNKNOWN' && (
              <div style={{ fontSize: 9, color: 'var(--text-muted)', marginTop: 4, lineHeight: 1.4 }}>
                {sysStatusInfo.detail}
              </div>
            )}
          </div>
        )}
      </aside>

      <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
        <header
          className="flex-shrink-0 flex flex-col border-b z-10"
          style={{ background: '#071020', borderColor: 'var(--border)' }}
        >
          <div className="flex items-center px-4" style={{ height: 48 }}>
            <button
              onClick={() => setSidebarOpen((v) => !v)}
              className="mr-3 p-1.5 rounded transition-colors"
              style={{ color: 'var(--text-muted)' }}
              aria-label="Toggle sidebar"
            >
              {sidebarOpen ? <X size={18} /> : <Menu size={18} />}
            </button>

            <div className="flex items-center gap-2 mr-6">
              <div
                className="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0"
                style={{ background: 'linear-gradient(135deg, #168CFF, #16C7E8)' }}
              >
                <TrafficCone size={14} className="text-white" />
              </div>
              <div className="hidden lg:block">
                <div className="text-sm font-bold leading-none" style={{ color: 'var(--text-primary)' }}>
                  Smart Traffic System
                </div>
                <div className="text-[10px]" style={{ color: 'var(--text-muted)' }}>
                  Traffic Intelligence Dashboard
                </div>
              </div>
            </div>

            <nav className="flex items-center gap-0 flex-1" role="navigation" aria-label="Main navigation">
              {NAV_TABS.map((tab) => {
                const isActive =
                  (tab.action === 'analytics' && isAnalyticsActive) ||
                  (tab.action === 'realtime' && isRealtimeActive)
                return (
                  <button
                    key={tab.label}
                    onClick={() => !tab.disabled && tab.action && handleNavAction(tab.action)}
                    disabled={tab.disabled}
                    className="relative px-4 text-sm font-medium transition-colors flex items-center"
                    style={{
                      height: 48,
                      color: tab.disabled
                        ? 'var(--text-muted)'
                        : isActive
                        ? 'var(--blue)'
                        : 'var(--text-secondary)',
                      borderBottom: isActive ? '2px solid var(--blue)' : '2px solid transparent',
                      cursor: tab.disabled ? 'not-allowed' : 'pointer',
                      background: 'transparent',
                    }}
                  >
                    {tab.label}
                    {tab.disabled && <ChevronRight size={10} style={{ color: 'var(--text-muted)', marginLeft: 4 }} />}
                  </button>
                )
              })}
            </nav>

            <div className="flex items-center gap-4">
              <div className="text-right hidden md:block">
                <div className="text-sm font-mono font-semibold" style={{ color: 'var(--text-primary)' }}>{timeStr}</div>
                <div className="text-[10px]" style={{ color: 'var(--text-muted)' }}>{dateStr}</div>
              </div>
              <div className="flex items-center gap-2">
                <div
                  className="w-8 h-8 rounded-full flex items-center justify-center text-sm font-semibold"
                  style={{ background: 'rgba(22,140,255,0.2)', color: 'var(--blue)' }}
                >
                  {user?.username?.[0]?.toUpperCase() ?? 'A'}
                </div>
                <span className="hidden lg:block text-sm" style={{ color: 'var(--text-secondary)' }}>
                  {user?.username ?? 'Admin'}
                </span>
                <button
                  onClick={handleLogout}
                  disabled={loggingOut}
                  className="p-1.5 rounded transition-colors"
                  style={{ color: 'var(--text-muted)' }}
                  aria-label="Logout"
                  title="Logout"
                >
                  <LogOut size={16} />
                </button>
              </div>
            </div>
          </div>

          {navNotice && (
            <div
              className="flex items-center gap-2 px-4 py-2 text-xs"
              style={{
                background: 'rgba(245,158,11,0.08)',
                borderTop: '1px solid rgba(245,158,11,0.2)',
                color: 'var(--orange)',
              }}
              role="alert"
            >
              <AlertTriangle size={14} />
              {navNotice}
              {listLoading && <span style={{ color: 'var(--text-muted)' }}>(loading…)</span>}
            </div>
          )}

          {isRunMismatch && (
            <div
              className="flex items-center gap-2 px-4 py-2 text-xs"
              style={{
                background: 'rgba(250,204,21,0.1)',
                borderTop: '1px solid rgba(250,204,21,0.25)',
                color: '#FACC15',
              }}
              role="alert"
            >
              <AlertTriangle size={14} />
              {buildRunMismatchMessage(alignment)}
            </div>
          )}
        </header>

        <main className="flex-1 overflow-auto" style={{ background: 'var(--bg-base)' }}>
          <Outlet />
        </main>
      </div>
    </div>
  )
}
