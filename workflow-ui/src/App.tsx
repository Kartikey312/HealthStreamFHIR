import { useEffect, useState } from "react";
import { ReactFlowProvider, type Node } from "@xyflow/react";
import "./App.css";
import { Canvas } from "./components/Canvas";
import { NodePalette } from "./components/NodePalette";
import { ConfigPanel } from "./components/ConfigPanel";
import { RunHistoryPanel } from "./components/RunHistoryPanel";
import { useWorkflowStore } from "./store/workflowStore";
import {
  useWorkflows,
  useWorkflow,
  useCreateWorkflow,
  useSaveWorkflow,
  useRunWorkflow,
  useNodeTypes,
} from "./api/client";
import type { WorkflowNodeData, WorkflowDefinition } from "./types/workflow";

function App() {
  const [selectedWorkflowId, setSelectedWorkflowId] = useState<number | null>(null);
  const [rightTab, setRightTab] = useState<"config" | "runs">("config");

  const { data: workflowsList, refetch: refetchWorkflows } = useWorkflows();
  const { data: loadedWorkflow } = useWorkflow(selectedWorkflowId);
  const { data: nodeTypesData } = useNodeTypes();

  const workflowId = useWorkflowStore((s) => s.workflowId);
  const name = useWorkflowStore((s) => s.name);
  const description = useWorkflowStore((s) => s.description);
  const nodes = useWorkflowStore((s) => s.nodes);
  const edges = useWorkflowStore((s) => s.edges);
  const dirty = useWorkflowStore((s) => s.dirty);
  const setWorkflowMeta = useWorkflowStore((s) => s.setWorkflowMeta);
  const loadGraph = useWorkflowStore((s) => s.loadGraph);
  const markClean = useWorkflowStore((s) => s.markClean);
  const setActiveRunId = useWorkflowStore((s) => s.setActiveRunId);
  const clearNodeStatuses = useWorkflowStore((s) => s.clearNodeStatuses);

  const createWorkflow = useCreateWorkflow();
  const saveWorkflow = useSaveWorkflow(workflowId ?? -1);
  const runWorkflow = useRunWorkflow();

  // Hydrate the canvas when a workflow is loaded from the server
  useEffect(() => {
    if (!loadedWorkflow || !nodeTypesData) return;
    const byType = new Map(nodeTypesData.node_types.map((nt) => [nt.type, nt]));
    const flowNodes: Node<WorkflowNodeData>[] = loadedWorkflow.definition.nodes.map((n, i) => {
      const def = byType.get(n.type);
      return {
        id: n.id,
        type: n.type,
        // Defend against nodes saved without a position (e.g. created via the
        // API directly, not the canvas) - lay them out in a row instead of
        // letting React Flow silently fail to render them.
        position: n.position ?? { x: 120 + i * 220, y: 120 },
        data: {
          label: def?.label ?? n.type,
          nodeType: n.type,
          category: (def?.category ?? "action") as WorkflowNodeData["category"],
          config: n.config,
          status: "IDLE",
        },
      };
    });
    const flowEdges = loadedWorkflow.definition.edges.map((e, i) => ({
      id: e.id ?? `e${i}_${e.source}_${e.target}`,
      source: e.source,
      target: e.target,
    }));
    setWorkflowMeta(loadedWorkflow.id, loadedWorkflow.name, loadedWorkflow.description ?? "");
    loadGraph(flowNodes, flowEdges);
    setActiveRunId(null);
  }, [loadedWorkflow, nodeTypesData, setWorkflowMeta, loadGraph, setActiveRunId]);

  const toDefinition = (): WorkflowDefinition => ({
    nodes: nodes.map((n) => ({
      id: n.id,
      type: n.data.nodeType,
      position: n.position,
      config: n.data.config,
    })),
    edges: edges.map((e) => ({ source: e.source, target: e.target })),
  });

  const handleNew = () => {
    setSelectedWorkflowId(null);
    setWorkflowMeta(null, "Untitled workflow", "");
    loadGraph([], []);
    setActiveRunId(null);
  };

  // Returns the workflow's id once persisted, so callers (e.g. Run) always
  // have a definitive id to act on regardless of whether this was a
  // create-then-save-again render cycle.
  const persistWorkflow = async (): Promise<number> => {
    const body = { name, description, definition: toDefinition() };
    let id = workflowId;
    if (id) {
      await saveWorkflow.mutateAsync(body);
    } else {
      const created = await createWorkflow.mutateAsync(body);
      id = created.id;
      setWorkflowMeta(created.id, created.name, created.description ?? "");
      setSelectedWorkflowId(created.id);
    }
    markClean();
    refetchWorkflows();
    return id;
  };

  const handleSave = () => persistWorkflow();

  const handleRun = async () => {
    // Run always executes the last SAVED definition (the backend has no way
    // to run unsaved in-browser state), so save first unconditionally -
    // otherwise a config edit you forgot to save silently re-runs the old
    // graph and any error from the stale version keeps recurring.
    const id = await persistWorkflow();
    clearNodeStatuses();
    const result = await runWorkflow.mutateAsync({ workflowId: id });
    setActiveRunId(result.run_id);
    setRightTab("runs");
  };

  return (
    <div className="app">
      <header className="topbar">
        <select
          value={selectedWorkflowId ?? ""}
          onChange={(e) => setSelectedWorkflowId(e.target.value ? Number(e.target.value) : null)}
        >
          <option value="">-- select a saved workflow --</option>
          {workflowsList?.workflows.map((w) => (
            <option key={w.id} value={w.id}>
              {w.name}
            </option>
          ))}
        </select>
        <button onClick={handleNew}>New</button>
        <input
          className="workflow-name-input"
          value={name}
          onChange={(e) => setWorkflowMeta(workflowId, e.target.value, description)}
        />
        <button onClick={handleSave} disabled={!dirty && !!workflowId}>
          {dirty ? "Save*" : "Save"}
        </button>
        <button onClick={handleRun} disabled={nodes.length === 0 || runWorkflow.isPending}>
          ▶ Run
        </button>
      </header>

      <div className="app-body">
        <NodePalette />
        <ReactFlowProvider>
          <Canvas />
        </ReactFlowProvider>
        <div className="right-sidebar">
          <div className="right-tabs">
            <button className={rightTab === "config" ? "active" : ""} onClick={() => setRightTab("config")}>
              Config
            </button>
            <button className={rightTab === "runs" ? "active" : ""} onClick={() => setRightTab("runs")}>
              Runs
            </button>
          </div>
          {rightTab === "config" ? <ConfigPanel /> : <RunHistoryPanel />}
        </div>
      </div>
    </div>
  );
}

export default App;
