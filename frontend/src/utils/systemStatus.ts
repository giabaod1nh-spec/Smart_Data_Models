// systemStatus.ts — System-level connectivity status derived from API health.
// Status is NEVER shown as OPERATIONAL if API is unreachable.

export type SystemConnectivityStatus = 'OPERATIONAL' | 'DEGRADED' | 'OFFLINE' | 'UNKNOWN'

export interface SystemStatusInfo {
  status: SystemConnectivityStatus
  label: string
  detail: string
  color: string
}

const STATUS_MAP: Record<SystemConnectivityStatus, SystemStatusInfo> = {
  OPERATIONAL: {
    status: 'OPERATIONAL',
    label: 'Operational',
    detail: 'All systems reachable and responding normally.',
    color: '#22C55E',
  },
  DEGRADED: {
    status: 'DEGRADED',
    label: 'Degraded',
    detail: 'Server reachable but some features may be unavailable.',
    color: '#F59E0B',
  },
  OFFLINE: {
    status: 'OFFLINE',
    label: 'Offline',
    detail: 'Cannot reach the server. Check network connectivity.',
    color: '#EF4444',
  },
  UNKNOWN: {
    status: 'UNKNOWN',
    label: 'Unknown',
    detail: 'System status has not been determined yet.',
    color: '#71889B',
  },
}

export function getSystemStatusInfo(status: SystemConnectivityStatus): SystemStatusInfo {
  return STATUS_MAP[status]
}

/**
 * Derive system status from health query state.
 * CRITICAL: Never returns OPERATIONAL if server is unreachable.
 * Priority: healthError → OFFLINE > healthOk=false → OFFLINE > healthOk=null → UNKNOWN > realtimeError → DEGRADED > OPERATIONAL
 */
export function deriveSystemStatus(
  healthOk: boolean | null,
  healthError: boolean,
  realtimeError: boolean,
): SystemConnectivityStatus {
  // If health check has thrown an error → server is unreachable → OFFLINE
  if (healthError) return 'OFFLINE'
  // Health check returned but server is DOWN
  if (healthOk === false) return 'OFFLINE'
  // Health check hasn't completed yet
  if (healthOk === null) return 'UNKNOWN'
  // Server is UP but realtime has issues
  if (realtimeError) return 'DEGRADED'
  return 'OPERATIONAL'
}
