import { describe, expect, it } from 'vitest'
import { buildRuntimeContextUpdates } from '@/hooks/useRealtimeContextBootstrap'

describe('runtime analytics context bootstrap', () => {
  it('fills run and scenario atomically when both filters are missing', () => {
    expect(buildRuntimeContextUpdates('', '', 'run-123', 'morning_peak')).toEqual({
      simulationRunId: 'run-123',
      scenarioId: 'morning_peak',
    })
  })

  it('preserves a user-selected run while filling only the missing scenario', () => {
    expect(buildRuntimeContextUpdates('manual-run', '', 'runtime-run', 'normal')).toEqual({
      scenarioId: 'normal',
    })
  })

  it('does not overwrite complete filters', () => {
    expect(buildRuntimeContextUpdates('manual-run', 'manual-scenario', 'runtime-run', 'normal')).toEqual({})
  })
})
