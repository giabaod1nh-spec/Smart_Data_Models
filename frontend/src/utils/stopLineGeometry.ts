/**
 * Stop-line geometry shared by road layer + vehicle clamping.
 */
import type { NetworkEdge } from '@/types/liveTraffic'

const DEFAULT_LANE_WIDTH_M = 3.2

export type Approach = 'North' | 'South' | 'East' | 'West'

export interface StopLineInfo {
  approach: Approach
  inboundEdgeId: string
  travelUnit: [number, number]
  stopPoint: [number, number]
  segment: [[number, number], [number, number]]
}

export interface CorridorPair {
  inbound: NetworkEdge
  outbound: NetworkEdge
  approach: Approach
}

function dist(ax: number, ay: number, bx: number, by: number) {
  return Math.hypot(ax - bx, ay - by)
}

function junctionEndPoint(shape: [number, number][], jx: number, jy: number): [number, number] {
  const a = shape[0]
  const b = shape[shape.length - 1]
  return dist(a[0], a[1], jx, jy) <= dist(b[0], b[1], jx, jy) ? a : b
}

function edgeTravelUnit(shape: [number, number][], jx: number, jy: number): [number, number] {
  const start = shape[0]
  const end = shape[shape.length - 1]
  const towardJ = dist(end[0], end[1], jx, jy) < dist(start[0], start[1], jx, jy)
  const dx = towardJ ? end[0] - start[0] : start[0] - end[0]
  const dy = towardJ ? end[1] - start[1] : start[1] - end[1]
  const len = Math.hypot(dx, dy) || 1
  return [dx / len, dy / len]
}

export function approachForInbound(inbound: NetworkEdge, jx: number, jy: number): Approach {
  const shape = inbound.lanes?.[0]?.shape ?? inbound.shape
  const start = shape[0]
  const end = shape[shape.length - 1]
  const from = dist(start[0], start[1], jx, jy) >= dist(end[0], end[1], jx, jy) ? start : end
  const dx = jx - from[0]
  const dy = jy - from[1]
  if (Math.abs(dx) > Math.abs(dy)) return dx > 0 ? 'West' : 'East'
  return dy > 0 ? 'South' : 'North'
}

export function pairOpposingEdges(
  edges: NetworkEdge[],
  tlsId: string,
  jx: number,
  jy: number,
): CorridorPair[] {
  const inbound = edges.filter((e) => e.to === tlsId && (e.lanes?.length ?? 0) > 0)
  const outbound = edges.filter((e) => e.from === tlsId && (e.lanes?.length ?? 0) > 0)
  const pairs: CorridorPair[] = []
  const usedOut = new Set<string>()

  for (const inc of inbound) {
    const incShape = inc.lanes![0].shape
    const [itx, ity] = edgeTravelUnit(incShape, jx, jy)
    let best: NetworkEdge | null = null
    let bestGap = Infinity

    for (const out of outbound) {
      if (usedOut.has(out.id)) continue
      const outShape = out.lanes![0].shape
      const [otx, oty] = edgeTravelUnit(outShape, jx, jy)
      if (itx * otx + ity * oty > -0.85) continue
      const incEnd = junctionEndPoint(incShape, jx, jy)
      const outEnd = junctionEndPoint(outShape, jx, jy)
      const gap = dist(incEnd[0], incEnd[1], outEnd[0], outEnd[1])
      if (gap < bestGap) {
        bestGap = gap
        best = out
      }
    }
    if (best) {
      usedOut.add(best.id)
      pairs.push({ inbound: inc, outbound: best, approach: approachForInbound(inc, jx, jy) })
    }
  }
  return pairs
}

export function stopLineSegment(
  inbound: NetworkEdge,
  jx: number,
  jy: number,
): [[number, number], [number, number]] | null {
  const lanes = inbound.lanes
  if (!lanes?.length) return null

  const ref = lanes[0].shape
  const px = -edgeTravelUnit(ref, jx, jy)[1]
  const py = edgeTravelUnit(ref, jx, jy)[0]
  const halfW = (lanes[0].width || DEFAULT_LANE_WIDTH_M) / 2

  const ends = lanes.map((l) => junctionEndPoint(l.shape, jx, jy))
  let stopX = 0
  let stopY = 0
  for (const p of ends) {
    stopX += p[0]
    stopY += p[1]
  }
  stopX /= ends.length
  stopY /= ends.length

  let minT = Infinity
  let maxT = -Infinity
  for (const lane of lanes) {
    const p = junctionEndPoint(lane.shape, jx, jy)
    const t = p[0] * px + p[1] * py
    minT = Math.min(minT, t - halfW)
    maxT = Math.max(maxT, t + halfW)
  }

  const baseT = stopX * px + stopY * py
  return [
    [stopX + px * (minT - baseT), stopY + py * (minT - baseT)],
    [stopX + px * (maxT - baseT), stopY + py * (maxT - baseT)],
  ]
}

export function computeStopLines(pairs: CorridorPair[], jx: number, jy: number): StopLineInfo[] {
  const out: StopLineInfo[] = []
  for (const { inbound, approach } of pairs) {
    const seg = stopLineSegment(inbound, jx, jy)
    if (!seg) continue
    const shape = inbound.lanes![0].shape
    out.push({
      approach,
      inboundEdgeId: inbound.id,
      travelUnit: edgeTravelUnit(shape, jx, jy),
      stopPoint: [(seg[0][0] + seg[1][0]) / 2, (seg[0][1] + seg[1][1]) / 2],
      segment: seg,
    })
  }
  return out
}

export function junctionInteriorRect(
  pairs: CorridorPair[],
  jx: number,
  jy: number,
): { minX: number; minY: number; maxX: number; maxY: number } | null {
  const ends: [number, number][] = []
  for (const { inbound } of pairs) {
    for (const lane of inbound.lanes ?? []) {
      ends.push(junctionEndPoint(lane.shape, jx, jy))
    }
  }
  if (ends.length === 0) return null

  let minX = Infinity
  let minY = Infinity
  let maxX = -Infinity
  let maxY = -Infinity
  for (const [x, y] of ends) {
    minX = Math.min(minX, x)
    minY = Math.min(minY, y)
    maxX = Math.max(maxX, x)
    maxY = Math.max(maxY, y)
  }

  const inset = DEFAULT_LANE_WIDTH_M
  return {
    minX: minX + inset,
    minY: minY + inset,
    maxX: maxX - inset,
    maxY: maxY - inset,
  }
}

export function clampVehicleBeforeStopLine(
  x: number,
  y: number,
  lengthM: number,
  stop: StopLineInfo,
  mustStop: boolean,
): { x: number; y: number } {
  if (!mustStop || lengthM <= 0) return { x, y }

  const len = Math.max(1.5, lengthM)
  const [ux, uy] = stop.travelUnit
  const frontProj = (x + ux * (len / 2)) * ux + (y + uy * (len / 2)) * uy
  const stopProj = stop.stopPoint[0] * ux + stop.stopPoint[1] * uy

  if (frontProj > stopProj + 0.05) {
    const d = frontProj - stopProj
    return { x: x - ux * d, y: y - uy * d }
  }
  return { x, y }
}

export function edgeIdFromLane(laneId: string): string {
  return laneId.replace(/_\d+$/, '')
}

export function stopLineForLane(
  laneId: string,
  stopLines: StopLineInfo[],
  edges: NetworkEdge[],
  jx: number,
  jy: number,
): StopLineInfo | null {
  const edgeId = edgeIdFromLane(laneId)
  const direct = stopLines.find((s) => s.inboundEdgeId === edgeId)
  if (direct) return direct
  const edge = edges.find((e) => e.id === edgeId)
  if (!edge || !edge.to) return null
  const approach = approachForInbound(edge, jx, jy)
  return stopLines.find((s) => s.approach === approach) ?? null
}
