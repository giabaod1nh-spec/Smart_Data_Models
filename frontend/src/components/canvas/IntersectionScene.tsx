// IntersectionScene.tsx — Live Aggregate Traffic Digital Twin (React Konva 2D Canvas)
//
// DISCLAIMER: Vehicle sprites are illustrative aggregates representing traffic sensor data.
// Sources: VehicleSensor.vehicleCount, averageSpeed, queueLength, waitingVehicleCount.
//
// Features:
//   - Asphalt road geometry, shoulders, lane dividers, double yellow center lines
//   - Direction arrows painted on approaches (North, South, East, West)
//   - Zebra crosswalk markings and crisp white stop lines
//   - 4 Traffic light clusters at the stop lines (Red/Yellow/Green with glow halos)
//   - Top-view vehicle sprites with windshield and headlights
//   - Vehicles STOP at red lights, slow on yellow, and cross/loop on green
//   - Queue bands reflecting queueLength
//   - Interactive detector zone overlays with hover tooltips
//   - Center phase indicator and simulation status

import React, { useMemo, useRef, useState, useEffect, useCallback } from 'react'
import { Stage, Layer, Rect, Line, Text, Group, Circle } from 'react-konva'
import type { VehicleSensorResponse, TrafficLightResponse } from '@/types/realtime'
import {
  useVehicleAnimation,
  signalColorForDirection,
  type Direction,
  type SensorInput,
} from './useVehicleAnimation'
import { normalizeCardinalDirection, formatOccupancyRate, formatSpeedKmh } from '@/transforms/realtimeTransforms'

// ─── Canvas layout ratios ───────────────────────────────────────────────────
const CX_RATIO = 0.5
const CY_RATIO = 0.5
const BOX_RATIO = 0.13      // Half-size of intersection center box
const ARM_W_RATIO = 0.22    // Total width of each 4-lane approach arm

// ─── Color Palette ──────────────────────────────────────────────────────────
const C = {
  background:      '#04111E',
  roadSurface:     '#0B1D2E',
  roadShoulder:    '#071524',
  intersectionBox: '#081726',
  laneDivider:     'rgba(255,255,255,0.22)',
  laneSolid:       'rgba(255,255,255,0.4)',
  centerLine:      'rgba(250,204,21,0.45)',
  crosswalk:       'rgba(255,255,255,0.18)',
  stopLine:        'rgba(255,255,255,0.85)',
  arrowFill:       'rgba(255,255,255,0.25)',
  queueBand:       'rgba(239,68,68,0.14)',
  queueBandBorder: 'rgba(239,68,68,0.4)',
  detectorZone:    'rgba(22,140,255,0.06)',
  detectorBorder:  'rgba(22,140,255,0.3)',
  tlHousing:       '#061320',
  tlHousingBorder: '#183A52',
  labelBg:         'rgba(6,17,31,0.92)',
  labelText:       '#94A3B8',
  phaseBadgeBg:    'rgba(22,140,255,0.18)',
  phaseBadgeText:  '#16C7E8',
  carBodyFree:     '#1B4332',
  carBodySlow:     '#3B2A0E',
  carBodyStopped:  '#3A1616',
  carBodyNormal:   '#163A58',
  windshield:      'rgba(22,199,232,0.6)',
  headlightOn:     '#FEF08A',
  headlightDim:    '#71889B',
  taillightRed:    '#EF4444',
}

const DIRS: Direction[] = ['North', 'South', 'East', 'West']

function getSensor(sensors: VehicleSensorResponse[], dir: Direction): VehicleSensorResponse | undefined {
  return sensors.find((s) => normalizeCardinalDirection(s.trafficDirection) === dir)
}

function getLight(lights: TrafficLightResponse[], dir: Direction): TrafficLightResponse | undefined {
  return lights.find((l) => normalizeCardinalDirection(l.trafficDirection) === dir)
}

interface TooltipData {
  dir: Direction
  x: number
  y: number
  sensor: VehicleSensorResponse | undefined
}

interface IntersectionSceneProps {
  sensors: VehicleSensorResponse[]
  lights: TrafficLightResponse[]
  currentPhase?: string | null
  simulationRunId?: string | null
  stale?: boolean
  width?: number
  height?: number
}

// eslint-disable-next-line react-refresh/only-export-components
export function measuredSceneSize(width: number): { w: number; h: number } | null {
  if (!Number.isFinite(width) || width < 1) return null
  const w = Math.max(1, Math.round(width))
  return { w, h: Math.max(480, Math.min(620, Math.round(w * 0.8))) }
}

export function IntersectionScene({
  sensors,
  lights,
  currentPhase,
  simulationRunId,
  stale = false,
  width: propW,
  height: propH,
}: IntersectionSceneProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [size, setSize] = useState({ w: propW ?? 620, h: propH ?? 540 })
  const [tooltip, setTooltip] = useState<TooltipData | null>(null)

  // Sizing observer
  useEffect(() => {
    if (propW && propH) return
    const obs = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (!entry) return
      const { width } = entry.contentRect
      const measured = measuredSceneSize(width)
      // A hidden route/layout reports width=0 during its first observer tick.
      // Keep the safe initial canvas size until the container is measurable;
      // Konva cannot draw a zero-width backing canvas.
      if (measured) setSize(measured)
    })
    if (containerRef.current) obs.observe(containerRef.current)
    return () => obs.disconnect()
  }, [propW, propH])

  const { w, h } = size
  const CX = w * CX_RATIO
  const CY = h * CY_RATIO
  const minDim = Math.min(w, h)
  const BOX = minDim * BOX_RATIO
  const ARM_W = minDim * ARM_W_RATIO
  const LANE_W = ARM_W / 2

  // Sensor inputs for animation
  const sensorInputs = useMemo<SensorInput[]>(() =>
    DIRS.map((dir) => {
      const s = getSensor(sensors, dir)
      return {
        dir,
        vehicleCount: s?.vehicleCount ?? null,
        averageSpeed: s?.averageSpeed ?? null,
        queueLength: s?.queueLength ?? null,
        waitingVehicleCount: s?.waitingVehicleCount ?? null,
        trafficStatus: s?.trafficStatus ?? null,
      }
    }),
    [sensors],
  )

  // Vehicle animation sprites
  const { sprites } = useVehicleAnimation(
    sensorInputs,
    currentPhase,
    simulationRunId,
    stale,
    w,
    h,
  )

  // Arm geometry rectangles
  const arms = useMemo(() => ({
    North: { x: CX - ARM_W / 2, y: 0, w: ARM_W, h: CY - BOX },
    South: { x: CX - ARM_W / 2, y: CY + BOX, w: ARM_W, h: h - (CY + BOX) },
    West:  { x: 0, y: CY - ARM_W / 2, w: CX - BOX, h: ARM_W },
    East:  { x: CX + BOX, y: CY - ARM_W / 2, w: w - (CX + BOX), h: ARM_W },
  }), [CX, CY, BOX, ARM_W, w, h])

  // Stop lines
  const stopLines = useMemo(() => ({
    North: { x1: CX - ARM_W / 2, y1: CY - BOX, x2: CX + ARM_W / 2, y2: CY - BOX },
    South: { x1: CX - ARM_W / 2, y1: CY + BOX, x2: CX + ARM_W / 2, y2: CY + BOX },
    West:  { x1: CX - BOX, y1: CY - ARM_W / 2, x2: CX - BOX, y2: CY + ARM_W / 2 },
    East:  { x1: CX + BOX, y1: CY - ARM_W / 2, x2: CX + BOX, y2: CY + ARM_W / 2 },
  }), [CX, CY, BOX, ARM_W])

  // Traffic light cluster locations (positioned at stoplines)
  const tlPositions = useMemo(() => ({
    North: { x: CX + ARM_W / 2 + 10, y: CY - BOX - 8 },
    South: { x: CX - ARM_W / 2 - 28, y: CY + BOX + 8 },
    East:  { x: CX + BOX + 8, y: CY + ARM_W / 2 + 10 },
    West:  { x: CX - BOX - 8, y: CY - ARM_W / 2 - 28 },
  }), [CX, CY, BOX, ARM_W])

  // Detector zones for hover inspection
  const detectorZones = useMemo(() => ({
    North: { x: CX - ARM_W / 2, y: CY - BOX - ARM_W * 1.2, w: ARM_W, h: ARM_W },
    South: { x: CX - ARM_W / 2, y: CY + BOX + 10, w: ARM_W, h: ARM_W },
    West:  { x: CX - BOX - ARM_W * 1.2, y: CY - ARM_W / 2, w: ARM_W, h: ARM_W },
    East:  { x: CX + BOX + 10, y: CY - ARM_W / 2, w: ARM_W, h: ARM_W },
  }), [CX, CY, BOX, ARM_W])

  // Pixel coordinates for vehicle sprite
  const getSpritePixel = (dir: Direction, progress: number, lane: number) => {
    // lane: -1 = inbound left, 0 = inbound right, 1 = outbound
    const laneOffset = lane * (LANE_W / 2.2)
    const armLenN = CY - BOX
    const armLenS = h - CY - BOX
    const armLenE = w - CX - BOX
    const armLenW = CX - BOX

    switch (dir) {
      case 'North':
        return {
          x: CX - LANE_W / 2 + laneOffset,
          y: CY - BOX - progress * armLenN,
          angle: 180, // Facing South (inbound)
        }
      case 'South':
        return {
          x: CX + LANE_W / 2 - laneOffset,
          y: CY + BOX + progress * armLenS,
          angle: 0, // Facing North (inbound)
        }
      case 'East':
        return {
          x: CX + BOX + progress * armLenE,
          y: CY - LANE_W / 2 + laneOffset,
          angle: 270, // Facing West (inbound)
        }
      case 'West':
        return {
          x: CX - BOX - progress * armLenW,
          y: CY + LANE_W / 2 - laneOffset,
          angle: 90, // Facing East (inbound)
        }
    }
  }

  const handleDetectorHover = useCallback((dir: Direction, stageX: number, stageY: number) => {
    const sensor = getSensor(sensors, dir)
    setTooltip({ dir, x: stageX, y: stageY, sensor })
  }, [sensors])

  const handleDetectorLeave = useCallback(() => setTooltip(null), [])

  return (
    <div
      ref={containerRef}
      style={{
        position: 'relative',
        width: propW ? `${propW}px` : '100%',
        height: propH ? `${propH}px` : `${h}px`,
        borderRadius: 10,
        overflow: 'hidden',
        background: C.background,
        border: '1px solid var(--border)',
        userSelect: 'none',
      }}
      role="img"
      aria-label="Live aggregate intersection digital twin showing road layout, signals, vehicle movements, and queues"
    >
      {/* Top right status badge — only when API feed is hard-failed */}
      {stale && (
        <div style={{
          position: 'absolute', top: 10, right: 10, zIndex: 20,
          padding: '3px 10px', borderRadius: 6,
          background: 'rgba(239,68,68,0.18)', border: '1px solid rgba(239,68,68,0.4)',
          color: '#EF4444', fontSize: 11, fontWeight: 700,
        }}>
          Offline
        </div>
      )}

      {/* Konva Stage */}
      <Stage width={Math.max(1, w)} height={Math.max(1, h)}>
        {/* ── 1. BACKGROUND & ROAD SURFACE ── */}
        <Layer listening={false}>
          <Rect x={0} y={0} width={w} height={h} fill={C.background} />

          {/* Road Arm Surfaces */}
          {(Object.entries(arms) as [Direction, { x: number; y: number; w: number; h: number }][]).map(([dir, a]) => (
            <Rect key={dir} x={a.x} y={a.y} width={a.w} height={a.h} fill={C.roadSurface} />
          ))}

          {/* Center Intersection Box */}
          <Rect
            x={CX - BOX}
            y={CY - BOX}
            width={BOX * 2}
            height={BOX * 2}
            fill={C.intersectionBox}
          />
        </Layer>

        {/* ── 2. LANE MARKINGS & CENTER DIVIDERS ── */}
        <Layer listening={false}>
          {/* Double Yellow Center Lines */}
          {/* North Center */}
          <Line points={[CX - 1.5, 0, CX - 1.5, CY - BOX]} stroke={C.centerLine} strokeWidth={1.5} />
          <Line points={[CX + 1.5, 0, CX + 1.5, CY - BOX]} stroke={C.centerLine} strokeWidth={1.5} />
          {/* South Center */}
          <Line points={[CX - 1.5, CY + BOX, CX - 1.5, h]} stroke={C.centerLine} strokeWidth={1.5} />
          <Line points={[CX + 1.5, CY + BOX, CX + 1.5, h]} stroke={C.centerLine} strokeWidth={1.5} />
          {/* East Center */}
          <Line points={[CX + BOX, CY - 1.5, w, CY - 1.5]} stroke={C.centerLine} strokeWidth={1.5} />
          <Line points={[CX + BOX, CY + 1.5, w, CY + 1.5]} stroke={C.centerLine} strokeWidth={1.5} />
          {/* West Center */}
          <Line points={[0, CY - 1.5, CX - BOX, CY - 1.5]} stroke={C.centerLine} strokeWidth={1.5} />
          <Line points={[0, CY + 1.5, CX - BOX, CY + 1.5]} stroke={C.centerLine} strokeWidth={1.5} />

          {/* White Dashed Lane Dividers */}
          {/* North Lanes */}
          <Line points={[CX - LANE_W / 2, 0, CX - LANE_W / 2, CY - BOX]} stroke={C.laneDivider} strokeWidth={1} dash={[8, 8]} />
          <Line points={[CX + LANE_W / 2, 0, CX + LANE_W / 2, CY - BOX]} stroke={C.laneDivider} strokeWidth={1} dash={[8, 8]} />
          {/* South Lanes */}
          <Line points={[CX - LANE_W / 2, CY + BOX, CX - LANE_W / 2, h]} stroke={C.laneDivider} strokeWidth={1} dash={[8, 8]} />
          <Line points={[CX + LANE_W / 2, CY + BOX, CX + LANE_W / 2, h]} stroke={C.laneDivider} strokeWidth={1} dash={[8, 8]} />
          {/* East Lanes */}
          <Line points={[CX + BOX, CY - LANE_W / 2, w, CY - LANE_W / 2]} stroke={C.laneDivider} strokeWidth={1} dash={[8, 8]} />
          <Line points={[CX + BOX, CY + LANE_W / 2, w, CY + LANE_W / 2]} stroke={C.laneDivider} strokeWidth={1} dash={[8, 8]} />
          {/* West Lanes */}
          <Line points={[0, CY - LANE_W / 2, CX - BOX, CY - LANE_W / 2]} stroke={C.laneDivider} strokeWidth={1} dash={[8, 8]} />
          <Line points={[0, CY + LANE_W / 2, CX - BOX, CY + LANE_W / 2]} stroke={C.laneDivider} strokeWidth={1} dash={[8, 8]} />

          {/* Road Edge Solid Lines */}
          {/* North Arm Edges */}
          <Line points={[CX - ARM_W / 2, 0, CX - ARM_W / 2, CY - BOX]} stroke={C.laneSolid} strokeWidth={1.5} />
          <Line points={[CX + ARM_W / 2, 0, CX + ARM_W / 2, CY - BOX]} stroke={C.laneSolid} strokeWidth={1.5} />
          {/* South Arm Edges */}
          <Line points={[CX - ARM_W / 2, CY + BOX, CX - ARM_W / 2, h]} stroke={C.laneSolid} strokeWidth={1.5} />
          <Line points={[CX + ARM_W / 2, CY + BOX, CX + ARM_W / 2, h]} stroke={C.laneSolid} strokeWidth={1.5} />
          {/* West Arm Edges */}
          <Line points={[0, CY - ARM_W / 2, CX - BOX, CY - ARM_W / 2]} stroke={C.laneSolid} strokeWidth={1.5} />
          <Line points={[0, CY + ARM_W / 2, CX - BOX, CY + ARM_W / 2]} stroke={C.laneSolid} strokeWidth={1.5} />
          {/* East Arm Edges */}
          <Line points={[CX + BOX, CY - ARM_W / 2, w, CY - ARM_W / 2]} stroke={C.laneSolid} strokeWidth={1.5} />
          <Line points={[CX + BOX, CY + ARM_W / 2, w, CY + ARM_W / 2]} stroke={C.laneSolid} strokeWidth={1.5} />
        </Layer>

        {/* ── 3. ZEBRA CROSSWALKS ── */}
        <Layer listening={false}>
          {Array.from({ length: 6 }).map((_, i) => {
            const stripeW = (ARM_W - 8) / 6
            return (
              <React.Fragment key={i}>
                {/* North Crosswalk */}
                <Rect x={CX - ARM_W / 2 + 4 + i * stripeW} y={CY - BOX - 18} width={stripeW - 3} height={14} fill={C.crosswalk} />
                {/* South Crosswalk */}
                <Rect x={CX - ARM_W / 2 + 4 + i * stripeW} y={CY + BOX + 4} width={stripeW - 3} height={14} fill={C.crosswalk} />
                {/* West Crosswalk */}
                <Rect x={CX - BOX - 18} y={CY - ARM_W / 2 + 4 + i * stripeW} width={14} height={stripeW - 3} fill={C.crosswalk} />
                {/* East Crosswalk */}
                <Rect x={CX + BOX + 4} y={CY - ARM_W / 2 + 4 + i * stripeW} width={14} height={stripeW - 3} fill={C.crosswalk} />
              </React.Fragment>
            )
          })}
        </Layer>

        {/* ── 4. STOP LINES ── */}
        <Layer listening={false}>
          {(Object.entries(stopLines) as [Direction, { x1: number; y1: number; x2: number; y2: number }][]).map(([dir, sl]) => (
            <Line key={dir} points={[sl.x1, sl.y1, sl.x2, sl.y2]} stroke={C.stopLine} strokeWidth={3} />
          ))}
        </Layer>

        {/* ── 5. QUEUE BANDS ── */}
        <Layer listening={false}>
          {DIRS.map((dir) => {
            const sensor = getSensor(sensors, dir)
            const ql = sensor?.queueLength ?? 0
            if (ql <= 0) return null

            const armLenN = CY - BOX
            const armLenS = h - CY - BOX
            const armLenE = w - CX - BOX
            const armLenW = CX - BOX

            let qx = 0, qy = 0, qw = 0, qh = 0
            if (dir === 'North') {
              const qPx = Math.min(armLenN * 0.85, (ql / 100) * armLenN)
              qx = CX - ARM_W / 2; qy = CY - BOX - qPx; qw = ARM_W / 2; qh = qPx
            } else if (dir === 'South') {
              const qPx = Math.min(armLenS * 0.85, (ql / 100) * armLenS)
              qx = CX; qy = CY + BOX; qw = ARM_W / 2; qh = qPx
            } else if (dir === 'East') {
              const qPx = Math.min(armLenE * 0.85, (ql / 100) * armLenE)
              qx = CX + BOX; qy = CY - ARM_W / 2; qw = qPx; qh = ARM_W / 2
            } else if (dir === 'West') {
              const qPx = Math.min(armLenW * 0.85, (ql / 100) * armLenW)
              qx = CX - BOX - qPx; qy = CY; qw = qPx; qh = ARM_W / 2
            }

            return (
              <Rect
                key={dir}
                x={qx} y={qy} width={qw} height={qh}
                fill={C.queueBand}
                stroke={C.queueBandBorder}
                strokeWidth={1}
                cornerRadius={3}
              />
            )
          })}
        </Layer>

        {/* ── 6. DETECTOR ZONES (HOVERABLE) ── */}
        <Layer>
          {DIRS.map((dir) => {
            const zone = detectorZones[dir]
            return (
              <Rect
                key={dir}
                x={zone.x} y={zone.y} width={zone.w} height={zone.h}
                fill={C.detectorZone}
                stroke={C.detectorBorder}
                strokeWidth={1}
                cornerRadius={4}
                onMouseEnter={(e) => {
                  const stage = e.target.getStage()
                  if (stage) {
                    const pos = stage.getPointerPosition()
                    if (pos) handleDetectorHover(dir, pos.x, pos.y)
                  }
                }}
                onMouseMove={(e) => {
                  const stage = e.target.getStage()
                  if (stage) {
                    const pos = stage.getPointerPosition()
                    if (pos) handleDetectorHover(dir, pos.x, pos.y)
                  }
                }}
                onMouseLeave={handleDetectorLeave}
              />
            )
          })}
        </Layer>

        {/* ── 7. TRAFFIC LIGHT CLUSTERS AT STOPLINES ── */}
        {/* Bulbs follow currentPhase (same source as vehicle stop logic), not stale per-light status. */}
        <Layer listening={false}>
          {DIRS.map((dir) => {
            const pos = tlPositions[dir]
            // Prefer phase; fall back to entity status only when phase is missing
            const phaseColor = currentPhase
              ? signalColorForDirection(dir, currentPhase)
              : null
            const fallback = (getLight(lights, dir)?.currentStatus ?? '').toUpperCase()
            const isGreen = phaseColor ? phaseColor === 'GREEN' : fallback.includes('GREEN')
            const isYellow = phaseColor ? phaseColor === 'YELLOW' : fallback.includes('YELLOW')
            const isRed = phaseColor ? phaseColor === 'RED' : fallback.includes('RED') || (!isGreen && !isYellow)

            const isVert = dir === 'North' || dir === 'South'
            const boxW = isVert ? 16 : 42
            const boxH = isVert ? 42 : 16

            return (
              <Group key={dir} x={pos.x} y={pos.y}>
                <Rect
                  x={0} y={0}
                  width={boxW} height={boxH}
                  fill={C.tlHousing}
                  stroke={C.tlHousingBorder}
                  strokeWidth={1}
                  cornerRadius={4}
                  shadowColor="rgba(0,0,0,0.5)"
                  shadowBlur={6}
                />
                <Circle
                  x={isVert ? 8 : 8}
                  y={isVert ? 8 : 8}
                  radius={4}
                  fill={isRed ? '#EF4444' : '#2A1010'}
                  shadowColor={isRed ? '#EF4444' : 'transparent'}
                  shadowBlur={isRed ? 10 : 0}
                />
                <Circle
                  x={isVert ? 8 : 21}
                  y={isVert ? 21 : 8}
                  radius={4}
                  fill={isYellow ? '#FACC15' : '#2A2005'}
                  shadowColor={isYellow ? '#FACC15' : 'transparent'}
                  shadowBlur={isYellow ? 10 : 0}
                />
                <Circle
                  x={isVert ? 8 : 34}
                  y={isVert ? 34 : 8}
                  radius={4}
                  fill={isGreen ? '#22C55E' : '#082515'}
                  shadowColor={isGreen ? '#22C55E' : 'transparent'}
                  shadowBlur={isGreen ? 10 : 0}
                />
              </Group>
            )
          })}
        </Layer>

        {/* ── 8. ANIMATED VEHICLE SPRITES ── */}
        <Layer listening={false}>
          {sprites.map((sprite) => {
            const pos = getSpritePixel(sprite.dir, sprite.progress, sprite.lane)
            const cat = sprite.animCategory
            const isStopped = sprite.queued || cat === 'STOPPED'
            const carColor =
              cat === 'STOPPED' || isStopped
                ? C.carBodyStopped
                : cat === 'CRAWL' || cat === 'SLOW'
                ? C.carBodySlow
                : cat === 'FAST'
                ? C.carBodyFree
                : C.carBodyNormal

            const vW = Math.max(9, ARM_W * 0.12)
            const vH = vW * 1.85

            return (
              <Group
                key={sprite.id}
                x={pos.x}
                y={pos.y}
                rotation={pos.angle}
                offsetX={vW / 2}
                offsetY={vH / 2}
              >
                <Rect
                  x={0} y={0}
                  width={vW} height={vH}
                  fill={carColor}
                  stroke="rgba(255,255,255,0.18)"
                  strokeWidth={0.8}
                  cornerRadius={vW * 0.3}
                />
                <Rect
                  x={vW * 0.12} y={vH * 0.22}
                  width={vW * 0.76} height={vH * 0.22}
                  fill={C.windshield}
                  cornerRadius={2}
                />
                <Rect
                  x={vW * 0.15} y={vH * 0.65}
                  width={vW * 0.7} height={vH * 0.14}
                  fill="rgba(22,140,255,0.3)"
                  cornerRadius={1}
                />
                <Circle
                  x={vW * 0.22} y={vH * 0.05}
                  radius={1.5}
                  fill={isStopped ? C.headlightDim : C.headlightOn}
                />
                <Circle
                  x={vW * 0.78} y={vH * 0.05}
                  radius={1.5}
                  fill={isStopped ? C.headlightDim : C.headlightOn}
                />
                <Circle
                  x={vW * 0.2} y={vH * 0.95}
                  radius={1.2}
                  fill={C.taillightRed}
                />
                <Circle
                  x={vW * 0.8} y={vH * 0.95}
                  radius={1.2}
                  fill={C.taillightRed}
                />
              </Group>
            )
          })}
        </Layer>

        {/* ── 9. OVERLAY & LABELS ── */}
        <Layer listening={false}>
          {/* Approach Cardinal Labels */}
          <Text x={CX - 6} y={10} text="N" fontSize={13} fill="#71889B" fontStyle="bold" />
          <Text x={CX - 6} y={h - 22} text="S" fontSize={13} fill="#71889B" fontStyle="bold" />
          <Text x={10} y={CY - 7} text="W" fontSize={13} fill="#71889B" fontStyle="bold" />
          <Text x={w - 20} y={CY - 7} text="E" fontSize={13} fill="#71889B" fontStyle="bold" />

          {/* Direction Metric Badges */}
          {DIRS.map((dir) => {
            const sensor = getSensor(sensors, dir)
            if (!sensor) return null

            let bx = 0, by = 0
            if (dir === 'North') { bx = CX - ARM_W / 2 - 82; by = 12 }
            else if (dir === 'South') { bx = CX + ARM_W / 2 + 12; by = h - 64 }
            else if (dir === 'West') { bx = 12; by = CY + ARM_W / 2 + 12 }
            else if (dir === 'East') { bx = w - 88; by = CY - ARM_W / 2 - 58 }

            const lines = [
              sensor.vehicleCount !== null ? `${sensor.vehicleCount} veh` : null,
              sensor.averageSpeed !== null ? `${sensor.averageSpeed.toFixed(0)} km/h` : null,
              sensor.queueLength !== null ? `Q: ${sensor.queueLength.toFixed(0)}m` : null,
            ].filter(Boolean) as string[]

            if (lines.length === 0) return null

            return (
              <Group key={dir} x={bx} y={by}>
                <Rect
                  x={0} y={0} width={74} height={lines.length * 15 + 6}
                  fill={C.labelBg} stroke="rgba(24,58,82,0.8)" strokeWidth={1}
                  cornerRadius={5}
                />
                {lines.map((line, i) => (
                  <Text
                    key={i} x={6} y={5 + i * 15} text={line}
                    fontSize={10} fill={i === 0 ? '#F8FAFC' : C.labelText}
                    fontStyle={i === 0 ? 'bold' : 'normal'}
                  />
                ))}
              </Group>
            )
          })}

          {/* Center Phase Badge */}
          {currentPhase && (
            <Group x={CX - 50} y={CY - 12}>
              <Rect
                x={0} y={0} width={100} height={24}
                fill={C.phaseBadgeBg} stroke="#168CFF" strokeWidth={1}
                cornerRadius={12}
              />
              <Text
                x={6} y={6} width={88}
                text={currentPhase}
                fontSize={10} fill={C.phaseBadgeText}
                fontStyle="bold" align="center"
              />
            </Group>
          )}
        </Layer>
      </Stage>

      {/* ── 10. DETECTOR HOVER TOOLTIP (HTML OVERLAY) ── */}
      {tooltip && (
        <div
          className="detector-tooltip"
          style={{
            position: 'absolute',
            left: Math.min(tooltip.x + 14, w - 190),
            top: Math.min(tooltip.y + 14, h - 170),
            background: '#0C2235',
            border: '1px solid #183A52',
            borderRadius: 8,
            padding: '10px 14px',
            fontSize: 11,
            color: '#94A3B8',
            pointerEvents: 'none',
            zIndex: 100,
            boxShadow: '0 12px 32px rgba(0,0,0,0.6)',
            minWidth: 170,
          }}
        >
          <div style={{ fontWeight: 700, color: '#F8FAFC', marginBottom: 6, fontSize: 12 }}>
            {tooltip.dir} Approach Detector
          </div>
          {tooltip.sensor ? (
            <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 10 }}>
              <tbody>
                {([
                  ['Vehicles', tooltip.sensor.vehicleCount ?? '—'],
                  ['Waiting', tooltip.sensor.waitingVehicleCount ?? '—'],
                  ['Avg Speed', formatSpeedKmh(tooltip.sensor.averageSpeed)],
                  ['Queue Length', tooltip.sensor.queueLength !== null ? `${tooltip.sensor.queueLength.toFixed(0)} m` : '—'],
                  ['Occupancy', formatOccupancyRate(tooltip.sensor.occupancyRate)],
                  ['Traffic Status', tooltip.sensor.trafficStatus ?? '—'],
                  ['Spillback', tooltip.sensor.spillbackRisk ? '⚠ Risk' : 'No'],
                ] as [string, string | number][]).map(([k, v]) => (
                  <tr key={k}>
                    <td style={{ color: '#A8BDCE', paddingRight: 8, paddingBottom: 2 }}>{k}</td>
                    <td style={{ color: '#F8FAFC', fontWeight: 600, textAlign: 'right' }}>{String(v)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div style={{ color: '#71889B' }}>No detector data</div>
          )}
        </div>
      )}
    </div>
  )
}
