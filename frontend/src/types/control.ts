// control.ts — Types from verified Spring control domain DTOs and enums

/** ControlCommandType — matches ControlCommandType.java enum */
export const CONTROL_COMMAND_TYPES = [
  'FORCE_PHASE',
  'SET_GREEN_DURATION',
  'SET_SCENARIO',
  'SET_DEMAND_PROFILE',
  'ADD_OVERLAY',
  'REMOVE_OVERLAY',
  'SET_CONTROL_MODE',
  'EMERGENCY_PREEMPTION',
] as const
export type ControlCommandType = (typeof CONTROL_COMMAND_TYPES)[number]

/** ControlLifecycleStatus — matches ControlLifecycleStatus.java */
export const CONTROL_LIFECYCLE_STATUSES = [
  'RECEIVED',
  'VALIDATED',
  'QUEUED',
  'APPLYING',
  'COMPLETED',
  'FAILED',
  'EXPIRED',
  'UNKNOWN_OUTCOME',
] as const
export type ControlLifecycleStatus = (typeof CONTROL_LIFECYCLE_STATUSES)[number]

/** Terminal lifecycle statuses — polling stops here */
export const TERMINAL_LIFECYCLE_STATUSES: ControlLifecycleStatus[] = [
  'COMPLETED',
  'FAILED',
  'EXPIRED',
  'UNKNOWN_OUTCOME',
]

/** ControlDispatchStatus — matches ControlDispatchStatus.java */
export type ControlDispatchStatus = 'PENDING' | 'DISPATCHING' | 'ACCEPTED' | 'FAILED' | 'UNKNOWN'

/** ControlExecutionStatus — matches ControlExecutionStatus.java */
export type ControlExecutionStatus =
  | 'NOT_STARTED'
  | 'QUEUED'
  | 'EXECUTING'
  | 'TRANSITIONING'
  | 'APPLIED_AT_SUMO'
  | 'FAILED_AT_RUNTIME'

/** ControlObservationStatus — matches ControlObservationStatus.java */
export type ControlObservationStatus =
  | 'NOT_REQUESTED'
  | 'PENDING'
  | 'CONFIRMED'
  | 'MISMATCH'
  | 'TIMED_OUT'
  | 'UNAVAILABLE'
  | 'NOT_OBSERVABLE'

/**
 * ControlCommandStatusResponse — matches ControlCommandStatusResponse.java record.
 * Returned by GET /api/control/commands/{commandId}.
 */
export interface ControlCommandStatusResponse {
  commandId: string
  lifecycleStatus: ControlLifecycleStatus
  dispatchStatus: ControlDispatchStatus | null
  executionStatus: ControlExecutionStatus | null
  observationStatus: ControlObservationStatus | null
  expectedRunId: string | null
  acceptedRunId: string | null
  createdAt: string | null
  updatedAt: string | null
  error: { code: string; message: string } | null
  links: Record<string, string> | null
}

/**
 * Scenario enum values — verified from Visualize/configuration/config.py SCENARIO_IDS.
 * UI labels are presentation transforms; payload must use these exact values.
 */
export const SCENARIO_IDS = [
  'normal',
  'morning_peak',
  'evening_peak',
  'heavy_traffic',
  'oversaturated',
  'rain',
  'heavy_rain',
  'accident',
  'emergency',
  'blocked_intersection',
  'spillback',
] as const
export type ScenarioId = (typeof SCENARIO_IDS)[number]

/** Scenario display labels — UI presentation transform only */
export const SCENARIO_LABELS: Record<ScenarioId, string> = {
  normal: 'Normal',
  morning_peak: 'Morning Peak',
  evening_peak: 'Evening Peak',
  heavy_traffic: 'Heavy Traffic',
  oversaturated: 'Oversaturated',
  rain: 'Rain',
  heavy_rain: 'Heavy Rain',
  accident: 'Accident',
  emergency: 'Emergency',
  blocked_intersection: 'Blocked Intersection',
  spillback: 'Spillback',
}

/**
 * Phase enum values — verified from config.py PHASE_SEQUENCE.
 * Frontend sends these exact strings in the payload.
 */
export const PHASE_IDS = ['NS_GREEN', 'NS_YELLOW', 'EW_GREEN', 'EW_YELLOW'] as const
export type PhaseId = (typeof PHASE_IDS)[number]

/** Phase display labels */
export const PHASE_LABELS: Record<PhaseId, string> = {
  NS_GREEN: 'N/S Green',
  NS_YELLOW: 'N/S Yellow',
  EW_GREEN: 'E/W Green',
  EW_YELLOW: 'E/W Yellow',
}

/** Control modes — FIXED=auto cycle, MANUAL=officer hold, PREEMPTION=EV */
export const CONTROL_MODES = ['FIXED', 'PREEMPTION_ENABLED', 'MANUAL'] as const
export type ControlMode = (typeof CONTROL_MODES)[number]

export const CONTROL_MODE_LABELS: Record<ControlMode, string> = {
  FIXED: 'Automatic',
  MANUAL: 'Manual (Officer)',
  PREEMPTION_ENABLED: 'Preemption',
}

/** Green duration bounds — verified from GreenDurationRequest Field(ge=10, le=120) */
export const GREEN_DURATION_MIN = 10
export const GREEN_DURATION_MAX = 120

/**
 * Legacy proxy mutation response.
 * - Most endpoints: queued=true means queue acceptance only (do NOT treat as applied).
 * - POST /scenario (Approach B): waits for TraCI; applied=true means already executed.
 */
export interface ControlProxyQueuedResponse {
  queued: boolean
  /** Present when the engine waited for TraCI and confirmed execution (scenario apply). */
  applied?: boolean
  [key: string]: unknown
}
