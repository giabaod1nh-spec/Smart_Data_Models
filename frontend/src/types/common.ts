// common.ts — Shared types derived from ApiResponse<T> contract

/**
 * Generic API response envelope matching Spring ApiResponse<T>.
 * { status: int, data: T, message: string }
 */
export interface ApiResponse<T> {
  status: number
  data: T
  message: string
}

/**
 * Analytics page envelope: items + pagination + metadata.
 * hasNext replaces total count (backend does not return total).
 */
export interface AnalyticsPage<T> {
  items: T[]
  page: number
  pageSize: number
  hasNext: boolean
  metadata?: AnalyticsPageMetadata
}

export interface AnalyticsPageMetadata {
  source: string
  namespace: string
  simulationRunId: string
  scenarioId: string
  windowSizeSec: number
  generatedAt?: string
}

export interface GeoPoint {
  type: string
  coordinates: number[]
}

/** Common query params shared by all analytics endpoints */
export interface AnalyticsQueryParams {
  simulationRunId: string
  scenarioId: string
  windowSizeSec: 60 | 300
  fromSimulationSec?: number
  toSimulationSec?: number
  page?: number
  pageSize?: number
  sort?: string
}

/** Analytics metric codes — verified from AbstractAnalyticsService allowlist */
export const METRIC_CODES = [
  'AVG_SPEED_KMH',
  'AVG_QUEUE_LENGTH_M',
  'MAX_QUEUE_LENGTH_M',
  'AVG_OCCUPANCY_PCT',
  'AVG_VEHICLE_COUNT',
  'AVG_ARRIVAL_RATE_PCU_PER_SEC',
] as const
export type MetricCode = (typeof METRIC_CODES)[number]

/** Directions — verified from Visualize/configuration/config.py */
export const DIRECTIONS = ['North', 'South', 'East', 'West'] as const
export type Direction = (typeof DIRECTIONS)[number]

/** Window size options */
export const WINDOW_SIZES = [60, 300] as const
export type WindowSize = (typeof WINDOW_SIZES)[number]
