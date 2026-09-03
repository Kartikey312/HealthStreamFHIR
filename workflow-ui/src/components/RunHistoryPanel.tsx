import { useEffect, useState } from "react";
import { useRunHistory, useRunPoll } from "../api/client";
import { useWorkflowStore } from "../store/workflowStore";
import { JsonViewer } from "./JsonViewer";
import type { NodeStatus } from "../types/workflow";

export function RunHistoryPanel() {
  const workflowId = useWorkflowStore((s) => s.workflowId);
  const activeRunId = useWorkflowStore((s) => s.activeRunId);
  const setActiveRunId = useWorkflowStore((s) => s.setActiveRunId);
  const updateNodeStatuses = useWorkflowStore((s) => s.updateNodeStatuses);

  const { data: history } = useRunHistory(workflowId);
  const { data: activeRun } = useRunPoll(activeRunId);
  const [selectedStepNodeId, setSelectedStepNodeId] = useState<string | null>(null);

  // Push each poll's step statuses onto the canvas nodes - this is what makes
  // nodes light up live while a run is in progress.
  useEffect(() => {
    if (!activeRun) return;
    const statusByNodeId: Record<string, NodeStatus> = {};
    for (const step of activeRun.steps) statusByNodeId[step.node_id] = step.status;
    updateNodeStatuses(statusByNodeId);
  }, [activeRun, updateNodeStatuses]);

  const selectedStep = activeRun?.steps.find((s) => s.node_id === selectedStepNodeId);

  return (
    <div className="run-history-panel">
      {activeRun && (
        <div className="active-run">
          <h3>
            Run #{activeRun.id} - <span className={`run-status run-status-${activeRun.status.toLowerCase()}`}>{activeRun.status}</span>
          </h3>
          {activeRun.error && <div className="run-error">{activeRun.error}</div>}
          <div className="run-steps">
            {activeRun.steps.map((step) => (
              <div
                key={step.node_id}
                className={`run-step run-step-${step.status.toLowerCase()} ${step.node_id === selectedStepNodeId ? "selected" : ""}`}
                onClick={() => setSelectedStepNodeId(step.node_id)}
              >
                <span className="run-step-name">{step.node_id}</span>
                <span className="run-step-type">{step.node_type}</span>
                <span className="run-step-badge">{step.status}</span>
              </div>
            ))}
          </div>
          {selectedStep && (
            <div className="step-detail">
              <div className="step-detail-section">
                <h4>Input</h4>
                <JsonViewer data={selectedStep.input} />
              </div>
              <div className="step-detail-section">
                <h4>Output</h4>
                <JsonViewer data={selectedStep.output} />
              </div>
              {selectedStep.error && (
                <div className="step-detail-section">
                  <h4>Error</h4>
                  <div className="run-error">{selectedStep.error}</div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      <h3>History</h3>
      <div className="run-history-list">
        {history?.runs.length === 0 && <div className="run-history-empty">No runs yet</div>}
        {history?.runs.map((run) => (
          <div
            key={run.id}
            className={`run-history-item ${run.id === activeRunId ? "active" : ""}`}
            onClick={() => setActiveRunId(run.id)}
          >
            <span>#{run.id}</span>
            <span className={`run-status run-status-${run.status.toLowerCase()}`}>{run.status}</span>
            <span className="run-history-time">{new Date(run.started_at).toLocaleTimeString()}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
