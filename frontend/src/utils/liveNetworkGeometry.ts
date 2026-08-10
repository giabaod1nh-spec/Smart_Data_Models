import type { LiveNetworkGeometry, NetworkEdge } from '@/types/liveTraffic'
import bundledNetwork from '@/data/liveNetwork.json'

const DEFAULT_LANE_WIDTH_M = 3.2

/** Bundled SUMO net geometry (always has 3 lanes per edge). */
export function getBundledNetwork(): LiveNetworkGeometry {
  return bundledNetwork as LiveNetworkGeometry
}

/** Synthesize lane centerlines when API returns legacy geometry (no lanes[]). */
export function expandEdgeLanes(edge: NetworkEdge) {
  if (edge.lanes && edge.lanes.length > 0) return edge.lanes
  const count = Math.max(1, edge.numLanes || 3)
  const width = DEFAULT_LANE_WIDTH_M
  const shape = edge.shape
  if (!shape || shape.length < 2) return []

  const dx = shape[1][0] - shape[0][0]
  const dy = shape[1][1] - shape[0][1]
  const len = Math.hypot(dx, dy) || 1
  const nx = -dy / len
  const ny = dx / len

  const lanes = []
  for (let i = 0; i < count; i++) {
    const offset = i * width
    lanes.push({
      id: `${edge.id}_${i}`,
      index: i,
      width,
      shape: shape.map(([x, y]) => [x + nx * offset, y + ny * offset] as [number, number]),
    })
  }
  return lanes
}

/** Merge lane-level detail from bundled net into API geometry. */
export function enrichNetworkGeometry(api: LiveNetworkGeometry | null): LiveNetworkGeometry {
  const bundled = getBundledNetwork()
  if (!api?.edges?.length) return bundled

  const byId = new Map(bundled.edges.map((e) => [e.id, e]))
  const edges = api.edges.map((edge) => {
    const full = byId.get(edge.id)
    const lanes =
      edge.lanes && edge.lanes.length > 0
        ? edge.lanes
        : full?.lanes && full.lanes.length > 0
          ? full.lanes
          : expandEdgeLanes({ ...edge, numLanes: edge.numLanes || full?.numLanes || 3 })
    return { ...edge, numLanes: lanes.length || edge.numLanes, lanes, shape: full?.shape ?? edge.shape }
  })

  return {
    bounds: api.bounds ?? bundled.bounds,
    junctions: api.junctions?.length ? api.junctions : bundled.junctions,
    edges,
    vTypes: api.vTypes ?? bundled.vTypes,
  }
}

export function enrichNetworkEdges(edges: NetworkEdge[]): NetworkEdge[] {
  return enrichNetworkGeometry({ ...getBundledNetwork(), edges }).edges
}
