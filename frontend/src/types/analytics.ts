// analytics.ts — Types matching verified Spring analytics DTOs exactly

/** IntersectionWindowDto — matches IntersectionWindowDto.java record */
export interface IntersectionWindowDto {
  simulationRunId: string
  scenarioId: string
  namespace: string
  intersectionId: string
  windowId: string
  windowSizeSec: number
  windowStartSimSec: number
  windowEndSimSec: number
  avgTotalVehicleCount: number
  maxTotalVehicleCount: number | null
  latestTotalVehicleCount: number | null
  latestOverallTrafficStatus: string | null
  latestDerivedTrafficState: string | null
  latestPhase: string | null
  incidentOccurrence: number
  spillbackOccurrence: number
  boxBlockedOccurrence: number
  qualityStatus: string | null
  qualityFlags: string | null
  analyticalFreshnessStatus: string | null
  revisionSeq: number
  computedAt: string | null
}

/** DirectionWindowDto — matches DirectionWindowDto.java record */
export interface DirectionWindowDto {
  simulationRunId: string
  scenarioId: string
  namespace: string
  intersectionId: string
  direction: string
  sourceDirection: string | null
  windowId: string
  windowSizeSec: number
  windowStartSimSec: number
  windowEndSimSec: number
  avgVehicleCount: number | null
  maxVehicleCount: number | null
  latestVehicleCount: number | null
  avgPcuEquivalent: number | null
  avgSpeedKmh: number | null
  avgQueueLengthM: number | null
  maxQueueLengthM: number | null
  latestQueueLengthM: number | null
  avgOccupancyPct: number | null
  spillbackRatioPct: number | null
  latestTrafficStatus: string | null
  qualityStatus: string | null
  qualityFlags: string | null
  analyticalFreshnessStatus: string | null
  revisionSeq: number
  computedAt: string | null
}

/** TrafficComparisonDto — matches TrafficComparisonDto.java record */
export interface TrafficComparisonDto {
  simulationRunId: string
  scenarioId: string
  namespace: string
  intersectionId: string
  direction: string | null
  metricCode: string
  currentWindowId: string
  currentWindowSizeSec: number
  currentWindowStartSimSec: number
  currentWindowEndSimSec: number
  previousWindowId: string | null
  previousWindowStartSimSec: number
  previousWindowEndSimSec: number
  currentValue: number | null
  previousValue: number | null
  absoluteChange: number | null
  percentChange: number | null
  changeDirection: string | null
  comparisonStatus: string | null
  qualityStatus: string | null
  qualityFlags: string | null
  analyticalFreshnessStatus: string | null
  revisionSeq: number
  computedAt: string | null
}

/** CongestionWindowDto — matches CongestionWindowDto.java record */
export interface CongestionWindowDto {
  simulationRunId: string
  scenarioId: string
  namespace: string
  intersectionId: string
  direction: string | null
  windowId: string
  windowSizeSec: number
  windowStartSimSec: number
  windowEndSimSec: number
  metricCode: string | null
  metricVersion: string | null
  numericValue: number | null
  unitCode: string | null
  status: string | null
  explanationJson: string | null
  qualityStatus: string | null
  qualityFlags: string | null
  analyticalFreshnessStatus: string | null
  revisionSeq: number
  computedAt: string | null
}

/** PriorityRankingDto — matches PriorityRankingDto.java record */
export interface PriorityRankingDto {
  simulationRunId: string
  scenarioId: string
  namespace: string
  intersectionId: string
  direction: string | null
  windowId: string
  windowSizeSec: number
  windowStartSimSec: number
  windowEndSimSec: number
  priorityScore: number | null
  priorityRank: number | null
  scoreStatus: string | null
  rankStatus: string | null
  explanationJson: string | null
  qualityStatus: string | null
  qualityFlags: string | null
  analyticalFreshnessStatus: string | null
  revisionSeq: number
  computedAt: string | null
}

/** SignalOperationWindowDto — matches SignalOperationWindowDto.java record */
export interface SignalOperationWindowDto {
  simulationRunId: string
  scenarioId: string
  namespace: string
  intersectionId: string
  direction: string | null
  windowId: string
  windowSizeSec: number
  windowStartSimSec: number
  windowEndSimSec: number
  observationCount: number
  greenObservationCount: number
  redObservationCount: number
  yellowObservationCount: number
  otherStatusCount: number
  greenSharePct: number | null
  redSharePct: number | null
  yellowSharePct: number | null
  dominantSignalStatus: string | null
  dominantPhase: string | null
  latestTimingMode: string | null
  avgConfiguredGreenDurationSec: number | null
  qualityStatus: string | null
  qualityFlags: string | null
  analyticalFreshnessStatus: string | null
  revisionSeq: number
  computedAt: string | null
}
