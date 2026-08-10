/**
 * SUMO-GUI style road rendering.
 * Each lane is a filled polygon (not a thick stroke) so 3 lanes stay visible.
 */
import React, { useMemo } from 'react'
import { Line, Rect } from 'react-konva'
import type { NetworkEdge, NetworkJunction, NetworkLane } from '@/types/liveTraffic'
import { expandEdgeLanes } from '@/utils/liveNetworkGeometry'
import { mpx, WORLD } from '@/utils/canvasWorldScale'
import {
  junctionInteriorRect,
  pairOpposingEdges,
  stopLineSegment,
  type Approach,
  type CorridorPair,
} from '@/utils/stopLineGeometry'

const DEFAULT_LANE_WIDTH_M = 3.2
const LANE_SURFACE = '#56C8E6'
const LANE_EDGE = '#3AA8C8'
const GRASS = '#4A9B52'
const SOLID_LINE = '#FFFFFF'
const DASH_LINE = '#FFFFFF'
const STOP_RED = '#E53935'
const STOP_GREEN = '#43A047'
const STOP_YELLOW = '#FDD835'
const INTERSECTION_FILL = '#111111'

type SignalLamp = 'RED' | 'YELLOW' | 'GREEN'

type Box = { minX: number; minY: number; maxX: number; maxY: number }

export interface SumoRoadLayerProps {
  edges: NetworkEdge[]
  junction: NetworkJunction | null
  focusTls: string | null
  approachLamps: Record<Approach, SignalLamp>
  toCanvas: (x: number, y: number) => { x: number; y: number }
  scale: number
  width: number
  height: number
}

function dist(ax: number, ay: number, bx: number, by: number) {
  return Math.hypot(ax - bx, ay - by)
}

function shapeToCanvasPoints(
  shape: [number, number][],
  toCanvas: (x: number, y: number) => { x: number; y: number },
): number[] {
  const pts: number[] = []
  for (const [x, y] of shape) {
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue
    const p = toCanvas(x, y)
    pts.push(p.x, p.y)
  }
  return pts
}

/** Build a closed lane strip polygon from centerline + width. */
export function lanePolygon(shape: [number, number][], widthM: number): [number, number][] {
  if (shape.length < 2) return []
  const hw = widthM / 2
  const left: [number, number][] = []
  const right: [number, number][] = []

  for (let i = 0; i < shape.length; i++) {
    let dx: number
    let dy: number
    if (i === 0) {
      dx = shape[1][0] - shape[0][0]
      dy = shape[1][1] - shape[0][1]
    } else if (i === shape.length - 1) {
      dx = shape[i][0] - shape[i - 1][0]
      dy = shape[i][1] - shape[i - 1][1]
    } else {
      dx = shape[i + 1][0] - shape[i - 1][0]
      dy = shape[i + 1][1] - shape[i - 1][1]
    }
    const len = Math.hypot(dx, dy) || 1
    const nx = -dy / len
    const ny = dx / len
    const [x, y] = shape[i]
    left.push([x + nx * hw, y + ny * hw])
    right.push([x - nx * hw, y - ny * hw])
  }

  return [...left, ...right.reverse()]
}

/** Trim lane centerline so it stops at junction box (SUMO stop area). */
export function trimShapeAtJunction(
  shape: [number, number][],
  box: Box,
  jx: number,
  jy: number,
): [number, number][] {
  if (shape.length < 2) return shape
  const out = shape.map((p) => [p[0], p[1]] as [number, number])
  const start = out[0]
  const end = out[out.length - 1]
  const startNear = dist(start[0], start[1], jx, jy)
  const endNear = dist(end[0], end[1], jx, jy)
  if (startNear < endNear) {
    out[0] = clipPointToBoxEdge(end, start, box)
  } else {
    out[out.length - 1] = clipPointToBoxEdge(start, end, box)
  }
  return out
}

function clipPointToBoxEdge(far: [number, number], near: [number, number], box: Box): [number, number] {
  const dx = near[0] - far[0]
  const dy = near[1] - far[1]
  const eps = 1e-9
  if (Math.abs(dx) < eps && Math.abs(dy) < eps) return near

  let tMin = 0
  let tMax = 1
  const ts: number[] = []

  if (Math.abs(dx) > eps) {
    ts.push((box.minX - far[0]) / dx, (box.maxX - far[0]) / dx)
  }
  if (Math.abs(dy) > eps) {
    ts.push((box.minY - far[1]) / dy, (box.maxY - far[1]) / dy)
  }

  for (const t of ts) {
    if (t < 0 || t > 1) continue
    const px = far[0] + dx * t
    const py = far[1] + dy * t
    const onEdge =
      (Math.abs(px - box.minX) < 0.01 || Math.abs(px - box.maxX) < 0.01) &&
      py >= box.minY - 0.01 &&
      py <= box.maxY + 0.01
      ||
      (Math.abs(py - box.minY) < 0.01 || Math.abs(py - box.maxY) < 0.01) &&
      px >= box.minX - 0.01 &&
      px <= box.maxX + 0.01
    if (onEdge) return [px, py]
  }

  // Fallback: clamp near point to box
  return [
    Math.min(box.maxX, Math.max(box.minX, near[0])),
    Math.min(box.maxY, Math.max(box.minY, near[1])),
  ]
}

function laneDividerShape(a: [number, number][], b: [number, number][]): [number, number][] {
  const n = Math.max(a.length, b.length)
  const out: [number, number][] = []
  for (let i = 0; i < n; i++) {
    const ap = a[Math.min(i, a.length - 1)]
    const bp = b[Math.min(i, b.length - 1)]
    out.push([(ap[0] + bp[0]) / 2, (ap[1] + bp[1]) / 2])
  }
  return out
}

function junctionEndPoint(shape: [number, number][], jx: number, jy: number): [number, number] {
  const a = shape[0]
  const b = shape[shape.length - 1]
  return dist(a[0], a[1], jx, jy) <= dist(b[0], b[1], jx, jy) ? a : b
}

function stopLineColor(lamp: SignalLamp): string {
  if (lamp === 'GREEN') return STOP_GREEN
  if (lamp === 'YELLOW') return STOP_YELLOW
  return STOP_RED
}

function pickFacingLanes(inLanes: NetworkLane[], outLanes: NetworkLane[], jx: number, jy: number) {
  let bestIn = inLanes[0]
  let bestOut = outLanes[0]
  let bestGap = Infinity
  for (const il of inLanes) {
    const ip = junctionEndPoint(il.shape, jx, jy)
    for (const ol of outLanes) {
      const op = junctionEndPoint(ol.shape, jx, jy)
      const g = dist(ip[0], ip[1], op[0], op[1])
      if (g < bestGap) {
        bestGap = g
        bestIn = il
        bestOut = ol
      }
    }
  }
  return { inLane: bestIn, outLane: bestOut }
}

function centerLineShape(inbound: NetworkEdge, outbound: NetworkEdge, jx: number, jy: number): [number, number][] {
  const { inLane, outLane } = pickFacingLanes(inbound.lanes!, outbound.lanes!, jx, jy)
  return laneDividerShape(inLane.shape, outLane.shape)
}

function trimDividerAtJunction(
  shape: [number, number][],
  box: Box | null,
  jx: number,
  jy: number,
): [number, number][] {
  if (!box || shape.length < 2) return shape
  return trimShapeAtJunction(shape, box, jx, jy)
}

export function SumoRoadLayer({
  edges,
  junction,
  focusTls,
  approachLamps,
  toCanvas,
  scale,
  width,
  height,
}: SumoRoadLayerProps) {
  const jx = junction?.x ?? 0
  const jy = junction?.y ?? 0

  const roadData = useMemo(() => {
    if (!focusTls || !junction) {
      return { pairs: [] as CorridorPair[], junctionBox: null as Box | null }
    }
    const enriched = edges.map((e) => ({ ...e, lanes: expandEdgeLanes(e) }))
    const pairs = pairOpposingEdges(enriched, focusTls, jx, jy)
    const junctionBox = junctionInteriorRect(pairs, jx, jy)
    return { pairs, junctionBox }
  }, [edges, focusTls, junction, jx, jy])

  const dashLen = mpx(WORLD.markDashLength, scale)
  const gapLen = mpx(WORLD.markDashGap, scale)
  const lineW = mpx(WORLD.markDashWidth, scale)
  const stopW = mpx(WORLD.markStopWidth, scale)
  const solidW = mpx(WORLD.markSolidWidth, scale)
  const laneStrokeW = mpx(0.06, scale, 0.5)

  const junctionBox = roadData.junctionBox

  const edgesWithLanes = useMemo(
    () =>
      edges.map((edge) => ({
        ...edge,
        lanes: expandEdgeLanes(edge),
      })),
    [edges],
  )

  return (
    <>
      <Rect x={0} y={0} width={width} height={height} fill={GRASS} />

      {/* Filled lane polygons — one per SUMO lane */}
      {edgesWithLanes.flatMap((edge) => {
        const lanes = edge.lanes ?? []
        return lanes.map((lane) => {
          const widthM = lane.width || DEFAULT_LANE_WIDTH_M
          let center = lane.shape
          if (junctionBox && focusTls && edge.from === focusTls) {
            center = trimShapeAtJunction(center, junctionBox, jx, jy)
          }
          const poly = lanePolygon(center, widthM)
          if (poly.length < 4) return null
          const pts = shapeToCanvasPoints(poly, toCanvas)
          if (pts.length < 6) return null
          return (
            <Line
              key={`lane-fill-${lane.id}`}
              points={pts}
              closed
              fill={LANE_SURFACE}
              stroke={LANE_EDGE}
              strokeWidth={laneStrokeW}
              lineJoin="miter"
            />
          )
        })
      })}

      {/* Black junction interior — before markings so lines stay visible on top */}
      {junctionBox && (() => {
        const tl = toCanvas(junctionBox.minX, junctionBox.maxY)
        const br = toCanvas(junctionBox.maxX, junctionBox.minY)
        return (
          <Rect
            x={tl.x}
            y={tl.y}
            width={br.x - tl.x}
            height={br.y - tl.y}
            fill={INTERSECTION_FILL}
          />
        )
      })()}

      {/* Solid white — splits two directions */}
      {roadData.pairs.map(({ inbound, outbound }) => {
        let shape = centerLineShape(inbound, outbound, jx, jy)
        shape = trimDividerAtJunction(shape, junctionBox, jx, jy)
        const pts = shapeToCanvasPoints(shape, toCanvas)
        if (pts.length < 4) return null
        return (
          <Line
            key={`center-${inbound.id}-${outbound.id}`}
            points={pts}
            stroke={SOLID_LINE}
            strokeWidth={solidW}
            lineCap="square"
          />
        )
      })}

      {/* Dashed white — 3 lanes within each direction */}
      {edgesWithLanes.flatMap((edge) => {
        const lanes = edge.lanes ?? []
        if (lanes.length < 2) return []
        const marks: React.ReactNode[] = []
        for (let i = 0; i < lanes.length - 1; i++) {
          let divider = laneDividerShape(lanes[i].shape, lanes[i + 1].shape)
          divider = trimDividerAtJunction(divider, junctionBox, jx, jy)
          const pts = shapeToCanvasPoints(divider, toCanvas)
          if (pts.length < 4) continue
          marks.push(
            <Line
              key={`dash-${edge.id}-${i}`}
              points={pts}
              stroke={DASH_LINE}
              strokeWidth={lineW}
              dash={[dashLen, gapLen]}
              lineCap="butt"
            />,
          )
        }
        return marks
      })}

      {/* Stop lines — TLS colored */}
      {roadData.pairs.map(({ inbound, approach }) => {
          const seg = stopLineSegment(inbound, jx, jy)
          if (!seg) return null
          const pts = shapeToCanvasPoints(seg, toCanvas)
          if (pts.length < 4) return null
          return (
            <Line
              key={`stop-${inbound.id}`}
              points={pts}
              stroke={stopLineColor(approachLamps[approach])}
              strokeWidth={stopW}
              lineCap="square"
            />
          )
        })}
    </>
  )
}
