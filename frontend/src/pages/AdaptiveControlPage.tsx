import { useState } from 'react'
import { BrainCircuit, CircleGauge, Clock3, Rows3 } from 'lucide-react'
import { useRlAgentStatus } from '@/hooks/useRlAgentStatus'
import { useSetControlMode } from '@/hooks/useControlCommand'
import { DataNotice, MetricStrip, PageHeader, Panel, PanelHeader, StatusPill } from '@/components/operations/OperationalUI'
import { apiErrorMessage } from '@/utils/apiErrors'

export function AdaptiveControlPage() {
  const rl = useRlAgentStatus(true)
  const modeMutation = useSetControlMode()
  const [feedback, setFeedback] = useState<string | null>(null)

  const activateAdaptive = async () => {
    setFeedback(null)
    try {
      const response = await modeMutation.mutateAsync({ mode: 'ADAPTIVE' })
      setFeedback(response.queued ? 'Adaptive mode command queued. Wait for the runtime status to confirm the switch.' : 'Adaptive mode command accepted by the control runtime.')
    } catch (error) {
      setFeedback(apiErrorMessage(error, 'Could not activate adaptive mode. Check the Control API and SUMO runtime.'))
    }
  }

  const global = rl.globalMetrics
  const spillbackAgents = rl.agents.filter((agent) => agent.spillback_detected).length

  return <div className="ops-page">
    <PageHeader
      title="Adaptive control"
      description="Current cooperative DQN outputs and controller metrics. The UI reports runtime evidence without inventing decision explanations."
      meta={<StatusPill tone={rl.enabled ? 'healthy' : rl.isError ? 'critical' : 'neutral'} pulse={rl.enabled}>{rl.enabled ? `Active · ${rl.mode ?? 'Adaptive'}` : rl.isError ? 'Controller unavailable' : 'Not active'}</StatusPill>}
      actions={<button className="primary-action" disabled={rl.enabled || modeMutation.isPending} onClick={() => void activateAdaptive()}>{modeMutation.isPending ? 'Sending command…' : rl.enabled ? 'Adaptive mode active' : 'Activate adaptive mode'}</button>}
    />
    {feedback && <div className="inline-feedback" role="status">{feedback}</div>}
    <MetricStrip metrics={[
      { label: 'Active agents', value: String(rl.agents.length), icon: <BrainCircuit size={17} />, meta: 'Reported by /rl/status' },
      { label: 'Global reward', value: global?.globalReward !== undefined ? Number(global.globalReward).toFixed(3) : '—', icon: <CircleGauge size={17} />, meta: 'Controller-provided metric', unavailable: global?.globalReward === undefined },
      { label: 'Average queue', value: global?.averageQueue !== undefined ? Number(global.averageQueue).toFixed(1) : '—', unit: 'veh', icon: <Rows3 size={17} />, meta: 'Controller-provided metric', unavailable: global?.averageQueue === undefined },
      { label: 'Average waiting', value: global?.averageWaitingTime !== undefined ? Number(global.averageWaitingTime).toFixed(1) : '—', unit: 's', icon: <Clock3 size={17} />, meta: 'Controller-provided metric', unavailable: global?.averageWaitingTime === undefined },
      { label: 'Spillback flags', value: String(spillbackAgents), tone: spillbackAgents ? 'warning' : 'healthy', meta: 'Current agent snapshots' },
    ]} />

    <div className="adaptive-layout">
      <Panel className="adaptive-agents-panel">
        <PanelHeader title="Agent state" description="Current action, phase, queue, waiting time, reward and neighbor coordination" />
        {rl.isLoading ? <DataNotice state="loading" title="Loading adaptive state" description="Polling the controller status endpoint." /> : rl.isError ? <DataNotice state="error" title="Adaptive status unavailable" description="The read-only controller endpoint is not reachable. Traffic realtime data may still be available." /> : !rl.enabled || rl.agents.length === 0 ? <DataNotice state="empty" title="No active agent outputs" description="Adaptive mode must be active before per-agent decisions are exposed." /> : (
          <div className="ops-table-wrap"><table className="ops-table"><thead><tr><th>Agent</th><th>Action</th><th>Phase</th><th>Queue</th><th>Waiting</th><th>Reward</th><th>Neighbors</th><th>Spillback</th></tr></thead><tbody>{rl.agents.map((agent) => <tr key={agent.id}><td><strong>{agent.id}</strong></td><td><span className="action-code">{agent.action ?? '—'}</span></td><td><code>{agent.phase ?? '—'}</code></td><td className="numeric">{agent.queue ?? '—'}</td><td className="numeric">{agent.waiting ?? '—'}</td><td className={`numeric ${(agent.reward ?? 0) < 0 ? 'value-critical' : 'value-positive'}`}>{agent.reward !== undefined ? Number(agent.reward).toFixed(3) : '—'}</td><td>{agent.neighbors?.join(', ') || '—'}</td><td>{agent.spillback_detected ? <StatusPill tone="warning">Detected</StatusPill> : <StatusPill tone="healthy">Clear</StatusPill>}</td></tr>)}</tbody></table></div>
        )}
      </Panel>

      <Panel>
        <PanelHeader title="Decision interpretation" description="What the current contract can and cannot prove" />
        <div className="evidence-list">
          <div><span>Observed output</span><strong>KEEP / SWITCH / EXTEND</strong><small>Actions are reported directly by the controller status endpoint.</small></div>
          <div><span>Inputs available</span><strong>Queue, waiting, phase, spillback</strong><small>These values can be displayed beside an action but do not by themselves prove causation.</small></div>
          <div><span>Decision reason</span><strong>Not exposed</strong><small>No backend decision-log or explanation field is currently available.</small></div>
        </div>
      </Panel>

      <Panel>
        <PanelHeader title="Decision history" description="Traceable controller actions over simulation time" />
        <DataNotice state="empty" title="Decision-history endpoint not available" description="The current runtime exposes only the latest per-agent snapshot. Add a backend decision log before presenting historical explanations or action timelines." />
      </Panel>
    </div>
  </div>
}
