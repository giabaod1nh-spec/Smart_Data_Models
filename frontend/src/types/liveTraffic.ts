/** Live TraCI vehicle stream DTOs (Control API /ws/live). */

export interface LiveVehicle {
  id: string
  type: string
  x: number
  y: number
  speed: number
  angle: number
  lane: string
  road: string
  lanePos?: number
  length?: number
  width?: number
}

export interface LiveTrafficLight {
  id: string
  intersectionId: string
  state: string
  phase: string | null
  colors: Record<string, string>
}

export interface LiveStatistics {
  vehicleCount: number
  averageSpeed: number
  waitingVehicles: number
}

export interface LiveFrame {
  type?: 'frame'
  seq: number
  simulationTime: number
  vehicles: LiveVehicle[]
  trafficLights: LiveTrafficLight[]
  statistics: LiveStatistics
}

export interface NetworkBounds {
  minX: number
  minY: number
  maxX: number
  maxY: number
  projParameter: string
  geoReferenced: boolean
}

export interface NetworkJunction {
  id: string
  x: number
  y: number
  type: string
}

export interface NetworkLane {
  id: string
  index: number
  shape: [number, number][]
  width: number
  length?: number
}

export interface NetworkVType {
  length: number
  width: number
  vClass?: string
  guiShape?: string
}

export interface NetworkEdge {
  id: string
  from: string | null
  to: string | null
  numLanes: number
  shape: [number, number][]
  lanes?: NetworkLane[]
}

export interface LiveNetworkGeometry {
  bounds: NetworkBounds
  junctions: NetworkJunction[]
  edges: NetworkEdge[]
  vTypes?: Record<string, NetworkVType>
}

export interface LiveNetworkMessage {
  type: 'network'
  network: LiveNetworkGeometry
}

export type LiveWsStatus = 'connecting' | 'connected' | 'disconnected' | 'error'

export type LiveWsMessage = LiveFrame | LiveNetworkMessage

/** Cooperative DQN agent status (Control API /rl/status). */
export interface LiveAgentStatus {
  id: string
  phase?: string | null
  action?: string | null
  action_id?: number
  queue?: number
  waiting?: number
  reward?: number
  neighbors?: string[]
  spillback_detected?: boolean
}

export interface LiveGlobalMetrics {
  globalReward?: number
  spillbackPenalty?: number
  averageQueue?: number
  averageWaitingTime?: number
}
