/**
 * LiveSumoTrafficView — Konva canvas mapped to real SUMO cartesian coordinates.
 *
 * Vehicles come from TraCI via WebSocket (/live/ws). Positions are interpolated
 * between frames for smooth motion. Network is not geo-referenced (proj=!):
 * we draw SUMO x/y directly — never invent lat/lon.
 *
 * When `intersectionId` (A/B/C/D) is set, the canvas crops to that junction's
 * quadrant of the 2×2 grid so the detail page shows only that intersection.
 */
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Stage, Layer, Line, Rect, Circle, Text, Group } from 'react-konva'
import type {
  LiveFrame,
  LiveNetworkGeometry,
  LiveStatistics,
  LiveVehicle,
  LiveWsStatus,
} from '@/types/liveTraffic'
import { toGoldIntersectionId } from '@/utils/analyticsIntersectionId'
import { SumoVehicleSprite, resolveVehicleDims, useVehicleSprites } from '@/components/canvas/liveVehicleRender'
import { SumoRoadLayer } from '@/components/canvas/sumoRoadLayer'
import { INTERSECTION_APPROACH_M, mpx, WORLD } from '@/utils/canvasWorldScale'
import { expandEdgeLanes } from '@/utils/liveNetworkGeometry'
import {
  clampVehicleBeforeStopLine,
  computeStopLines,
  pairOpposingEdges,
  stopLineForLane,
} from '@/utils/stopLineGeometry'

const NODE_LABEL: Record<string, string> = {
  J1: 'A',
  J2: 'B',
  J3: 'C',
  J4: 'D',
}

/** Detail-page intersection letter → SUMO traffic-light junction id */
const INTERSECTION_TO_TLS: Record<string, string> = {
  A: 'J1',
  B: 'J2',
  C: 'J3',
  D: 'J4',
}

/** Normalize route/URN ids like urn:ngsi-ld:Intersection:A → A */
function toIntersectionLetter(intersectionId: string | null | undefined): string {
  const raw = (intersectionId || '').trim()
  if (!raw) return ''
  const gold = toGoldIntersectionId(raw).trim().toUpperCase()
  // Bare A–D, or trailing letter after URN / path
  if (/^[A-D]$/.test(gold)) return gold
  const m = gold.match(/([A-D])$/)
  return m?.[1] ?? ''
}

interface ViewBounds {
  minX: number
  minY: number
  maxX: number
  maxY: number
}

interface InterpVehicle {
  id: string
  x: number
  y: number
  angle: number
  speed: number
  type: string
  length: number
  width: number
  lane: string
}

interface LiveSumoTrafficViewProps {
  /** A / B / C / D — crops view to that intersection only */
  intersectionId?: string | null
  status: LiveWsStatus
  frameRef: React.MutableRefObject<LiveFrame | null>
  frame?: LiveFrame | null
  network: LiveNetworkGeometry | null
  stats: LiveStatistics | null
  simulationTime: number | null
  width?: number
  height?: number
}

type SignalLamp = 'RED' | 'YELLOW' | 'GREEN'
/** Map SUMO/phase color string for one approach → active lamp. */
function lampFromColor(raw: string | undefined | null): SignalLamp {
  const c = (raw || '').toUpperCase()
  if (c.includes('GREEN') || c === 'G') return 'GREEN'
  if (c.includes('YELLOW') || c.includes('AMBER') || c === 'Y') return 'YELLOW'
  return 'RED'
}

/** Fallback when stream only has phase name. */
function lampsFromPhase(phase: string | null | undefined): Record<string, SignalLamp> {
  const p = (phase || '').toUpperCase()
  if (p === 'NS_GREEN') {
    return { North: 'GREEN', South: 'GREEN', East: 'RED', West: 'RED' }
  }
  if (p === 'NS_YELLOW') {
    return { North: 'YELLOW', South: 'YELLOW', East: 'RED', West: 'RED' }
  }
  if (p === 'EW_GREEN') {
    return { North: 'RED', South: 'RED', East: 'GREEN', West: 'GREEN' }
  }
  if (p === 'EW_YELLOW') {
    return { North: 'RED', South: 'RED', East: 'YELLOW', West: 'YELLOW' }
  }
  return { North: 'RED', South: 'RED', East: 'RED', West: 'RED' }
}

function resolveApproachLamps(
  colors: Record<string, string> | undefined,
  phase: string | null | undefined,
): Record<'North' | 'South' | 'East' | 'West', SignalLamp> {
  const fromPhase = lampsFromPhase(phase)
  const dirs = ['North', 'South', 'East', 'West'] as const
  const out = { ...fromPhase } as Record<(typeof dirs)[number], SignalLamp>
  for (const d of dirs) {
    if (colors && colors[d]) out[d] = lampFromColor(colors[d])
  }
  return out
}

/** World-space offsets (SUMO m) from junction center → signal posts. */
const SIGNAL_OFFSETS: Record<'North' | 'South' | 'East' | 'West', { dx: number; dy: number }> = {
  North: { dx: -14, dy: 24 },
  South: { dx: 14, dy: -24 },
  East: { dx: 24, dy: 14 },
  West: { dx: -24, dy: -14 },
}

function SignalHead({
  x,
  y,
  lamp,
  label,
  scale,
}: {
  x: number
  y: number
  lamp: SignalLamp
  label: string
  scale: number
}) {
  const boxW = mpx(WORLD.signalWidth, scale)
  const boxH = mpx(WORLD.signalHeight, scale)
  const lampR = mpx(WORLD.signalLampRadius, scale, 0.6)
  const strokeW = mpx(0.04, scale, 0.5)
  const fontSize = mpx(WORLD.signalLabelFont, scale, 6)
  const yRed = boxH * 0.22
  const yYel = boxH * 0.5
  const yGrn = boxH * 0.78

  return (
    <Group x={x - boxW / 2} y={y - boxH / 2}>
      <Rect
        x={0}
        y={0}
        width={boxW}
        height={boxH}
        fill="#061320"
        stroke="#183A52"
        strokeWidth={strokeW}
        cornerRadius={mpx(0.08, scale, 1)}
      />
      <Circle
        x={boxW / 2}
        y={yRed}
        radius={lampR}
        fill={lamp === 'RED' ? '#EF4444' : '#2A1010'}
      />
      <Circle
        x={boxW / 2}
        y={yYel}
        radius={lampR}
        fill={lamp === 'YELLOW' ? '#FACC15' : '#2A2005'}
      />
      <Circle
        x={boxW / 2}
        y={yGrn}
        radius={lampR}
        fill={lamp === 'GREEN' ? '#22C55E' : '#082515'}
      />
      <Text
        x={0}
        y={boxH + mpx(0.15, scale, 1)}
        width={boxW}
        text={label}
        fontSize={fontSize}
        fill="#94A3B8"
        align="center"
        fontStyle="bold"
      />
    </Group>
  )
}

function lerp(a: number, b: number, t: number) {
  return a + (b - a) * t
}

function lerpAngle(a: number, b: number, t: number) {
  let d = ((b - a + 540) % 360) - 180
  return a + d * t
}

function pointInBounds(x: number, y: number, b: ViewBounds, margin = 0): boolean {
  return (
    x >= b.minX - margin &&
    x <= b.maxX + margin &&
    y >= b.minY - margin &&
    y <= b.maxY + margin
  )
}

/**
 * Crop the 800×800 2×2 grid into the quadrant owned by the focus TLS.
 * Midlines sit halfway between junctions (x=400, y=400 in conv coords).
 */
export function viewBoundsForIntersection(
  intersectionId: string | null | undefined,
  network: LiveNetworkGeometry | null,
): ViewBounds {
  const full: ViewBounds = network?.bounds
    ? {
        minX: network.bounds.minX,
        minY: network.bounds.minY,
        maxX: network.bounds.maxX,
        maxY: network.bounds.maxY,
      }
    : { minX: 0, minY: 0, maxX: 800, maxY: 800 }

  const letter = toIntersectionLetter(intersectionId)
  const tlsId = INTERSECTION_TO_TLS[letter]
  if (!tlsId || !network) return full

  const junc = network.junctions.find((j) => j.id === tlsId)
  if (!junc) return full

  const midX = (full.minX + full.maxX) / 2
  const midY = (full.minY + full.maxY) / 2

  // SW A=J1, SE B=J2, NW C=J3, NE D=J4
  let quad: ViewBounds
  if (letter === 'A') quad = { minX: full.minX, minY: full.minY, maxX: midX, maxY: midY }
  else if (letter === 'B') quad = { minX: midX, minY: full.minY, maxX: full.maxX, maxY: midY }
  else if (letter === 'C') quad = { minX: full.minX, minY: midY, maxX: midX, maxY: full.maxY }
  else if (letter === 'D') quad = { minX: midX, minY: midY, maxX: full.maxX, maxY: full.maxY }
  else return full

  // Tighter crop around junction → larger px/m scale, better vehicle visibility
  const r = INTERSECTION_APPROACH_M
  return {
    minX: Math.max(quad.minX, junc.x - r),
    minY: Math.max(quad.minY, junc.y - r),
    maxX: Math.min(quad.maxX, junc.x + r),
    maxY: Math.min(quad.maxY, junc.y + r),
  }
}

function edgeTouchesBounds(
  edge: LiveNetworkGeometry['edges'][number],
  bounds: ViewBounds,
  focusTls: string | null,
): boolean {
  if (focusTls && (edge.from === focusTls || edge.to === focusTls)) return true
  const laneShapes = edge.lanes?.length
    ? edge.lanes.map((l) => l.shape)
    : [edge.shape]
  for (const shape of laneShapes) {
    for (const [x, y] of shape) {
      if (pointInBounds(x, y, bounds, 1)) return true
    }
  }
  return false
}


function localStatsFromVehicles(vehicles: InterpVehicle[]): LiveStatistics {
  const n = vehicles.length
  if (n === 0) {
    return { vehicleCount: 0, averageSpeed: 0, waitingVehicles: 0 }
  }
  let speedSum = 0
  let waiting = 0
  for (const v of vehicles) {
    speedSum += Math.max(0, v.speed)
    if (v.speed < 0.1) waiting += 1
  }
  return {
    vehicleCount: n,
    averageSpeed: (speedSum / n) * 3.6,
    waitingVehicles: waiting,
  }
}

export function LiveSumoTrafficView({
  intersectionId = null,
  status,
  frameRef,
  frame = null,
  network,
  stats,
  simulationTime,
  width: propW,
  height: propH,
}: LiveSumoTrafficViewProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [size, setSize] = useState({ w: propW ?? 640, h: propH ?? 520 })
  const [vehicles, setVehicles] = useState<InterpVehicle[]>([])
  const [localStats, setLocalStats] = useState<LiveStatistics | null>(null)
  /** Camera: scale + stage offset; wheel zooms toward pointer. */
  const [camera, setCamera] = useState({ scale: 1, x: 0, y: 0 })

  const prevFrameRef = useRef<LiveFrame | null>(null)
  const currFrameRef = useRef<LiveFrame | null>(null)
  const frameAtRef = useRef(0)
  const lastSeqRef = useRef(-1)
  const lastPaintRef = useRef(0)
  const viewBoundsRef = useRef<ViewBounds>(viewBoundsForIntersection(intersectionId, network))
  const cameraRef = useRef(camera)
  cameraRef.current = camera

  const focusLetter = toIntersectionLetter(intersectionId)
  const focusTls = INTERSECTION_TO_TLS[focusLetter] ?? null
  const vehicleSprites = useVehicleSprites()
  const viewBounds = useMemo(
    () => viewBoundsForIntersection(intersectionId, network),
    [intersectionId, network],
  )
  viewBoundsRef.current = viewBounds

  // Reset camera when switching intersection
  useEffect(() => {
    setCamera({ scale: 1, x: 0, y: 0 })
  }, [intersectionId])

  const MIN_ZOOM = 1
  const MAX_ZOOM = 8
  const ZOOM_STEP = 1.12

  // Wheel zoom toward cursor (on container so it works even when Konva layers are non-listening)
  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    const onWheel = (e: WheelEvent) => {
      e.preventDefault()
      e.stopPropagation()
      const rect = el.getBoundingClientRect()
      const pointer = {
        x: e.clientX - rect.left,
        y: e.clientY - rect.top,
      }
      const { scale: oldScale, x: oldX, y: oldY } = cameraRef.current
      const direction = e.deltaY > 0 ? -1 : 1
      const next = direction > 0 ? oldScale * ZOOM_STEP : oldScale / ZOOM_STEP
      const newScale = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, next))
      if (Math.abs(newScale - oldScale) < 1e-6) {
        if (newScale <= MIN_ZOOM) setCamera({ scale: 1, x: 0, y: 0 })
        return
      }
      const mousePointTo = {
        x: (pointer.x - oldX) / oldScale,
        y: (pointer.y - oldY) / oldScale,
      }
      const newPos = {
        x: pointer.x - mousePointTo.x * newScale,
        y: pointer.y - mousePointTo.y * newScale,
      }
      if (newScale <= MIN_ZOOM + 1e-6) {
        setCamera({ scale: 1, x: 0, y: 0 })
      } else {
        setCamera({ scale: newScale, x: newPos.x, y: newPos.y })
      }
    }

    el.addEventListener('wheel', onWheel, { passive: false })
    return () => el.removeEventListener('wheel', onWheel)
  }, [])

  const resetCamera = useCallback(() => {
    setCamera({ scale: 1, x: 0, y: 0 })
  }, [])

  useEffect(() => {
    if (propW && propH) {
      setSize({ w: propW, h: propH })
      return
    }
    const el = containerRef.current
    if (!el) return
    const obs = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (!entry) return
      const width = entry.contentRect.width
      if (width < 1) return
      const w = Math.round(width)
      setSize({ w, h: Math.max(480, Math.min(640, Math.round(w * 0.78))) })
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [propW, propH])

  // rAF interpolation — filter vehicles to the focused intersection bounds
  useEffect(() => {
    let raf = 0
    const tick = () => {
      const latest = frameRef.current
      if (latest && latest.seq !== lastSeqRef.current) {
        prevFrameRef.current = currFrameRef.current
        currFrameRef.current = latest
        frameAtRef.current = performance.now()
        lastSeqRef.current = latest.seq
      }

      const curr = currFrameRef.current
      const prev = prevFrameRef.current
      if (!curr) {
        raf = requestAnimationFrame(tick)
        return
      }

      const elapsed = performance.now() - frameAtRef.current
      const t = Math.min(1, elapsed / 100)
      const vb = viewBoundsRef.current

      const prevMap = new Map<string, LiveVehicle>()
      if (prev) {
        for (const v of prev.vehicles) prevMap.set(v.id, v)
      }

      const next: InterpVehicle[] = []
      const sc = stopContextRef.current
      for (const v of curr.vehicles) {
        const p = prevMap.get(v.id)
        let x = !p || t >= 1 ? v.x : lerp(p.x, v.x, t)
        let y = !p || t >= 1 ? v.y : lerp(p.y, v.y, t)
        const speed = !p || t >= 1 ? v.speed : lerp(p.speed, v.speed, t)
        const length = v.length ?? p?.length ?? 4.5
        const lane = v.lane ?? p?.lane ?? ''

        if (sc.focusTls && lane && speed < 0.2) {
          const stop = stopLineForLane(lane, sc.stopLines, sc.edges, sc.jx, sc.jy)
          if (stop) {
            const lamp = sc.lamps[stop.approach]
            const mustStop = lamp === 'RED' || lamp === 'YELLOW'
            const clamped = clampVehicleBeforeStopLine(x, y, length, stop, mustStop)
            x = clamped.x
            y = clamped.y
          }
        }

        if (!pointInBounds(x, y, vb, 2)) continue
        next.push({
          id: v.id,
          x,
          y,
          angle: !p || t >= 1 ? v.angle : lerpAngle(p.angle, v.angle, t),
          speed,
          type: v.type,
          length,
          width: v.width ?? p?.width ?? 0,
          lane,
        })
      }

      next.sort((a, b) => a.y - b.y || a.x - b.x)

      const now = performance.now()
      if (now - lastPaintRef.current >= 50) {
        lastPaintRef.current = now
        setVehicles(next)
        setLocalStats(localStatsFromVehicles(next))
      }
      raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
  }, [frameRef, intersectionId])

  const bounds = viewBounds
  const pad = 28
  const { w, h } = size
  const worldW = Math.max(1, bounds.maxX - bounds.minX)
  const worldH = Math.max(1, bounds.maxY - bounds.minY)
  // Never allow non-positive scale (narrow containers used to go negative and blank the stage)
  const scale = Math.max(
    0.05,
    Math.min((Math.max(w, pad * 2 + 8) - pad * 2) / worldW, (Math.max(h, pad * 2 + 8) - pad * 2) / worldH),
  )

  const toCanvas = useMemo(() => {
    return (x: number, y: number) => {
      const cx = pad + (x - bounds.minX) * scale
      const cy = pad + (bounds.maxY - y) * scale
      return { x: cx, y: cy }
    }
  }, [bounds.minX, bounds.maxY, bounds.minY, scale, pad])

  const toRotation = (sumoAngle: number) => -sumoAngle

  const visibleEdges = useMemo(() => {
    const edges = network?.edges ?? []
    if (!focusTls) return edges.map((e) => ({ ...e, lanes: expandEdgeLanes(e) }))
    return edges
      .filter((e) => e.from === focusTls || e.to === focusTls)
      .map((e) => ({ ...e, lanes: expandEdgeLanes(e) }))
  }, [network, focusTls])

  const focusJunction = useMemo(() => {
    if (!focusTls) return null
    const j = (network?.junctions ?? []).find((jn) => jn.id === focusTls)
    if (j) return j
    const known: Record<string, { x: number; y: number }> = {
      J1: { x: 150, y: 150 },
      J2: { x: 650, y: 150 },
      J3: { x: 150, y: 650 },
      J4: { x: 650, y: 650 },
    }
    const p = known[focusTls]
    return p ? { id: focusTls, x: p.x, y: p.y, type: 'traffic_light' } : null
  }, [network, focusTls])

  const allTls = frame?.trafficLights ?? frameRef.current?.trafficLights ?? []
  const focusTl =
    (focusTls ? allTls.find((t) => t.id === focusTls) : null) ??
    allTls.find((t) => t.intersectionId === focusLetter) ??
    null
  const approachLamps = resolveApproachLamps(focusTl?.colors, focusTl?.phase ?? null)

  const stopLines = useMemo(() => {
    if (!focusTls || !focusJunction) return []
    const pairs = pairOpposingEdges(visibleEdges, focusTls, focusJunction.x, focusJunction.y)
    return computeStopLines(pairs, focusJunction.x, focusJunction.y)
  }, [visibleEdges, focusTls, focusJunction])

  const stopContextRef = useRef({
    stopLines,
    edges: visibleEdges,
    focusTls,
    jx: 0,
    jy: 0,
    lamps: {} as Record<'North' | 'South' | 'East' | 'West', SignalLamp>,
  })
  stopContextRef.current = {
    stopLines,
    edges: visibleEdges,
    focusTls,
    jx: focusJunction?.x ?? 0,
    jy: focusJunction?.y ?? 0,
    lamps: approachLamps,
  }

  const visibleJunctions = useMemo(() => {
    const junctions = network?.junctions ?? []
    if (!focusTls) return junctions
    const filtered = junctions.filter((j) => pointInBounds(j.x, j.y, viewBounds, 1))
    // Fallback hub if geometry missing but we know the TLS id
    if (filtered.length === 0 && focusTls && focusLetter) {
      const known: Record<string, { x: number; y: number }> = {
        J1: { x: 150, y: 150 },
        J2: { x: 650, y: 150 },
        J3: { x: 150, y: 650 },
        J4: { x: 650, y: 650 },
      }
      const p = known[focusTls]
      if (p) {
        return [{ id: focusTls, x: p.x, y: p.y, type: 'traffic_light' }]
      }
    }
    return filtered
  }, [network, viewBounds, focusTls, focusLetter])

  const hudStats = localStats ?? stats
  const titleSuffix = focusLetter ? ` — ${focusLetter}` : ''
  const phaseLabel = focusTl?.phase ?? null

  const statusColor =
    status === 'connected' ? '#22C55E' : status === 'connecting' ? '#FACC15' : '#EF4444'
  const statusLabel =
    status === 'connected'
      ? 'Connected'
      : status === 'connecting'
      ? 'Connecting…'
      : status === 'error'
      ? 'Error'
      : 'Disconnected'

  return (
    <div
      ref={containerRef}
      style={{
        position: 'relative',
        width: propW ? `${propW}px` : '100%',
        height: propH ? `${propH}px` : `${h}px`,
        borderRadius: 10,
        overflow: 'hidden',
        background: '#04111E',
        border: '1px solid var(--border)',
        userSelect: 'none',
        cursor: camera.scale > 1 ? 'zoom-in' : 'default',
      }}
      role="img"
      aria-label={
        focusLetter
          ? `Live SUMO traffic view for intersection ${focusLetter}`
          : 'Live SUMO traffic view with real vehicle positions'
      }
      title="Scroll to zoom toward cursor · double-click to reset"
    >
      <div
        style={{
          position: 'absolute',
          zIndex: 5,
          top: 10,
          left: 10,
          right: 10,
          display: 'flex',
          flexWrap: 'wrap',
          gap: 10,
          pointerEvents: 'none',
        }}
      >
        <div
          style={{
            background: 'rgba(6,17,31,0.92)',
            border: '1px solid #183A52',
            borderRadius: 8,
            padding: '8px 12px',
            fontSize: 11,
            color: '#94A3B8',
            minWidth: 160,
          }}
        >
          <div style={{ fontWeight: 700, color: '#F8FAFC', marginBottom: 4, fontSize: 12 }}>
            LIVE TRAFFIC{titleSuffix}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
            <span style={{ width: 8, height: 8, borderRadius: 99, background: statusColor }} />
            <span style={{ color: statusColor, fontWeight: 600 }}>{statusLabel}</span>
          </div>
          <div>Vehicles: {hudStats?.vehicleCount ?? '—'}</div>
          <div>
            Average speed:{' '}
            {hudStats ? `${hudStats.averageSpeed.toFixed(1)} km/h` : '—'}
          </div>
          <div>Waiting vehicles: {hudStats?.waitingVehicles ?? '—'}</div>
          <div>
            Simulation time:{' '}
            {simulationTime != null ? `${simulationTime.toFixed(1)} s` : '—'}
          </div>
          {phaseLabel && (
            <div style={{ marginTop: 4, color: '#16C7E8' }}>Phase: {phaseLabel}</div>
          )}
          <div style={{ marginTop: 4, color: '#64748B' }}>
            Zoom: {camera.scale.toFixed(1)}× · scroll / dbl-click reset
          </div>
        </div>
      </div>

      <Stage
        key={`stage-${focusLetter || 'all'}-${network ? 'geo' : 'nogeo'}`}
        width={Math.max(1, w)}
        height={Math.max(1, h)}
        onDblClick={resetCamera}
        onDblTap={resetCamera}
      >
        {/* Camera transform on Group — more reliable than Stage scale props */}
        <Layer listening={false}>
          <Group x={camera.x} y={camera.y} scaleX={camera.scale} scaleY={camera.scale}>
            <SumoRoadLayer
              edges={visibleEdges}
              junction={focusJunction}
              focusTls={focusTls}
              approachLamps={approachLamps}
              toCanvas={toCanvas}
              scale={scale}
              width={w}
              height={h}
            />

            {visibleJunctions.map((j) => {
              const p = toCanvas(j.x, j.y)
              const isFocusTls = focusTls != null && j.id === focusTls
              if (isFocusTls) {
                const r = mpx(WORLD.junctionLabelRadius, scale)
                return (
                  <Group key={j.id}>
                    <Circle
                      x={p.x}
                      y={p.y}
                      radius={r}
                      fill="#0F2740"
                      stroke="#38BDF8"
                      strokeWidth={mpx(0.08, scale, 0.8)}
                    />
                    <Text
                      x={p.x - r * 0.55}
                      y={p.y - mpx(0.28, scale, 4)}
                      text={NODE_LABEL[j.id] ?? focusLetter}
                      fontSize={mpx(0.55, scale, 8)}
                      fontStyle="bold"
                      fill="#F8FAFC"
                    />
                  </Group>
                )
              }
              return (
                <Group key={j.id}>
                  <Circle
                    x={p.x}
                    y={p.y}
                    radius={mpx(0.45, scale, 3)}
                    fill="#334155"
                    stroke="#1E293B"
                    strokeWidth={mpx(0.05, scale, 0.5)}
                  />
                </Group>
              )
            })}

            {focusJunction &&
              (['North', 'South', 'East', 'West'] as const).map((dir) => {
                const off = SIGNAL_OFFSETS[dir]
                const p = toCanvas(focusJunction.x + off.dx, focusJunction.y + off.dy)
                return (
                  <SignalHead
                    key={`tl-${dir}`}
                    x={p.x}
                    y={p.y}
                    lamp={approachLamps[dir]}
                    label={dir[0]}
                    scale={scale}
                  />
                )
              })}

            {vehicles.map((v) => {
              const p = toCanvas(v.x, v.y)
              if (!Number.isFinite(p.x) || !Number.isFinite(p.y)) return null
              const dims = resolveVehicleDims(v.type, v.length, v.width, network)
              return (
                <SumoVehicleSprite
                  key={v.id}
                  type={v.type}
                  lengthM={dims.length}
                  widthM={dims.width}
                  speed={v.speed}
                  scale={scale}
                  rotation={toRotation(v.angle)}
                  x={p.x}
                  y={p.y}
                  images={vehicleSprites}
                />
              )
            })}
          </Group>
        </Layer>
      </Stage>

      {visibleEdges.length === 0 && (
        <div
          style={{
            position: 'absolute',
            left: 12,
            bottom: 12,
            zIndex: 6,
            padding: '6px 10px',
            borderRadius: 6,
            background: 'rgba(127,29,29,0.85)',
            color: '#FECACA',
            fontSize: 11,
            pointerEvents: 'none',
          }}
        >
          Network geometry missing — retrying /live/network…
        </div>
      )}

      {status !== 'connected' && vehicles.length === 0 && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: '#64748B',
            fontSize: 13,
            pointerEvents: 'none',
          }}
        >
          Waiting for SUMO live stream…
        </div>
      )}
    </div>
  )
}
