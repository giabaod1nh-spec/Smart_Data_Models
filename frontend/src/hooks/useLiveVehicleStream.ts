/**
 * useLiveVehicleStream — WebSocket client for Control API /ws/live.
 *
 * Keeps the latest frame in a ref for the canvas rAF loop; only bumps React
 * state for connection status + lightweight statistics HUD.
 */
import { useEffect, useRef, useState } from 'react'
import type { MutableRefObject } from 'react'
import type {
  LiveFrame,
  LiveNetworkGeometry,
  LiveStatistics,
  LiveWsStatus,
} from '@/types/liveTraffic'
import { enrichNetworkGeometry, getBundledNetwork } from '@/utils/liveNetworkGeometry'

const DEFAULT_PATH = '/live/ws'
const FALLBACK_PATHS = ['/live/ws', '/ws/live']
const MAX_BACKOFF_MS = 15_000
const BASE_BACKOFF_MS = 500

function resolveWsUrl(path: string): string {
  const configured = (import.meta.env.VITE_LIVE_WS_URL as string | undefined)?.trim()
  if (configured) return configured

  const wsPath =
    (import.meta.env.VITE_LIVE_WS_PATH as string | undefined)?.trim() || path || DEFAULT_PATH
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}${wsPath.startsWith('/') ? wsPath : `/${wsPath}`}`
}

function candidateWsUrls(): string[] {
  const configured = (import.meta.env.VITE_LIVE_WS_URL as string | undefined)?.trim()
  if (configured) return [configured]

  const preferred =
    (import.meta.env.VITE_LIVE_WS_PATH as string | undefined)?.trim() || DEFAULT_PATH
  const paths = [preferred, ...FALLBACK_PATHS.filter((p) => p !== preferred)]
  const urls = paths.map((p) => resolveWsUrl(p))
  // Dev fallback: Control API direct (when Vite proxy WS is misconfigured / not restarted)
  if (import.meta.env.DEV) {
    urls.push('ws://127.0.0.1:9090/live/ws', 'ws://127.0.0.1:9090/ws/live')
  }
  return [...new Set(urls)]
}

export interface UseLiveVehicleStreamResult {
  status: LiveWsStatus
  /** Latest frame (also mirrored in frameRef for canvas). */
  frame: LiveFrame | null
  frameRef: MutableRefObject<LiveFrame | null>
  network: LiveNetworkGeometry | null
  stats: LiveStatistics | null
  simulationTime: number | null
}

export function useLiveVehicleStream(enabled = true): UseLiveVehicleStreamResult {
  const [status, setStatus] = useState<LiveWsStatus>('disconnected')
  const [frame, setFrame] = useState<LiveFrame | null>(null)
  const [network, setNetwork] = useState<LiveNetworkGeometry | null>(() => getBundledNetwork())
  const [stats, setStats] = useState<LiveStatistics | null>(null)
  const [simulationTime, setSimulationTime] = useState<number | null>(null)

  const frameRef = useRef<LiveFrame | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const closedByUser = useRef(false)
  const attemptRef = useRef(0)
  const urlIndexRef = useRef(0)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  // Throttle React state updates so hundreds of vehicles do not re-render the page at 10Hz
  const lastHudAt = useRef(0)

  useEffect(() => {
    if (!enabled) {
      setStatus('disconnected')
      return
    }

    closedByUser.current = false
    urlIndexRef.current = 0
    const urls = candidateWsUrls()

    const clearReconnect = () => {
      if (reconnectTimer.current) {
        clearTimeout(reconnectTimer.current)
        reconnectTimer.current = null
      }
    }

    const connect = () => {
      clearReconnect()
      const url = urls[urlIndexRef.current % urls.length]
      setStatus((prev) => (prev === 'connected' ? prev : 'connecting'))

      let ws: WebSocket
      try {
        ws = new WebSocket(url)
      } catch {
        setStatus('error')
        scheduleReconnect()
        return
      }

      wsRef.current = ws

      ws.onopen = () => {
        attemptRef.current = 0
        setStatus('connected')
      }

      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(String(ev.data)) as {
            type?: string
            network?: LiveNetworkGeometry
            seq?: number
            simulationTime?: number
            vehicles?: LiveFrame['vehicles']
            trafficLights?: LiveFrame['trafficLights']
            statistics?: LiveStatistics
          }
          if (msg.type === 'network' && msg.network) {
            setNetwork(enrichNetworkGeometry(msg.network))
            return
          }
          if (msg.type === 'frame' || Array.isArray(msg.vehicles)) {
            const next: LiveFrame = {
              type: 'frame',
              seq: Number(msg.seq ?? 0),
              simulationTime: Number(msg.simulationTime ?? 0),
              vehicles: msg.vehicles ?? [],
              trafficLights: msg.trafficLights ?? [],
              statistics: msg.statistics ?? {
                vehicleCount: 0,
                averageSpeed: 0,
                waitingVehicles: 0,
              },
            }
            frameRef.current = next
            const now = performance.now()
            // HUD / React state at ~4 Hz; canvas reads frameRef every rAF
            if (now - lastHudAt.current >= 250) {
              lastHudAt.current = now
              setFrame(next)
              setStats(next.statistics)
              setSimulationTime(next.simulationTime)
            }
          }
        } catch {
          // ignore malformed payloads
        }
      }

      ws.onerror = () => {
        setStatus('error')
      }

      ws.onclose = () => {
        wsRef.current = null
        if (closedByUser.current) {
          setStatus('disconnected')
          return
        }
        // Rotate URL candidates when proxy path fails
        urlIndexRef.current = (urlIndexRef.current + 1) % urls.length
        setStatus('disconnected')
        scheduleReconnect()
      }
    }

    const scheduleReconnect = () => {
      if (closedByUser.current) return
      const attempt = attemptRef.current++
      const jitter = Math.random() * 300
      const delay = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS * 2 ** Math.min(attempt, 5) + jitter)
      reconnectTimer.current = setTimeout(connect, delay)
    }

    connect()

    return () => {
      closedByUser.current = true
      clearReconnect()
      try {
        wsRef.current?.close()
      } catch {
        /* ignore */
      }
      wsRef.current = null
    }
  }, [enabled])

  // HTTP fallback for geometry (same-origin proxy, then direct Control API in dev)
  useEffect(() => {
    if (!enabled) return
    let cancelled = false
    const paths = [
      (import.meta.env.VITE_LIVE_NETWORK_PATH as string | undefined)?.trim() || '/live/network',
      '/live-network.json',
    ]
    if (import.meta.env.DEV) {
      paths.push('http://127.0.0.1:9090/live/network')
    }

    const tryFetch = async () => {
      for (const path of paths) {
        try {
          const r = await fetch(path)
          if (!r.ok) continue
          const raw = (await r.json()) as LiveNetworkGeometry & { network?: LiveNetworkGeometry }
          const geom = enrichNetworkGeometry(raw.network ?? raw)
          if (!cancelled && geom?.bounds && Array.isArray(geom.edges) && geom.edges.length > 0) {
            setNetwork(geom)
            return
          }
        } catch {
          /* try next */
        }
      }
    }

    void tryFetch()
    const retry = window.setInterval(() => {
      if (!cancelled) void tryFetch()
    }, 8000)

    return () => {
      cancelled = true
      window.clearInterval(retry)
    }
  }, [enabled])

  return { status, frame, frameRef, network, stats, simulationTime }
}
