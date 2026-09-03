import { create } from "zustand";
import {
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
  type Node,
  type Edge,
  type OnNodesChange,
  type OnEdgesChange,
  type OnConnect,
} from "@xyflow/react";
import type { NodeStatus, WorkflowNodeData } from "../types/workflow";

interface WorkflowState {
  workflowId: number | null;
  name: string;
  description: string;
  nodes: Node<WorkflowNodeData>[];
  edges: Edge[];
  selectedNodeId: string | null;
  dirty: boolean;
  activeRunId: number | null;

  setWorkflowMeta: (id: number | null, name: string, description: string) => void;
  loadGraph: (nodes: Node<WorkflowNodeData>[], edges: Edge[]) => void;
  addNode: (node: Node<WorkflowNodeData>) => void;
  updateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void;
  updateNodeStatuses: (statusByNodeId: Record<string, NodeStatus>) => void;
  clearNodeStatuses: () => void;
  setSelectedNode: (nodeId: string | null) => void;
  setActiveRunId: (runId: number | null) => void;
  onNodesChange: OnNodesChange<Node<WorkflowNodeData>>;
  onEdgesChange: OnEdgesChange;
  onConnect: OnConnect;
  markClean: () => void;
}

export const useWorkflowStore = create<WorkflowState>((set, get) => ({
  workflowId: null,
  name: "Untitled workflow",
  description: "",
  nodes: [],
  edges: [],
  selectedNodeId: null,
  dirty: false,
  activeRunId: null,

  setWorkflowMeta: (id, name, description) => set({ workflowId: id, name, description }),

  loadGraph: (nodes, edges) => set({ nodes, edges, dirty: false, selectedNodeId: null }),

  addNode: (node) => set((state) => ({ nodes: [...state.nodes, node], dirty: true })),

  updateNodeConfig: (nodeId, config) =>
    set((state) => ({
      nodes: state.nodes.map((n) =>
        n.id === nodeId ? { ...n, data: { ...n.data, config } } : n
      ),
      dirty: true,
    })),

  updateNodeStatuses: (statusByNodeId) =>
    set((state) => ({
      nodes: state.nodes.map((n) =>
        statusByNodeId[n.id]
          ? { ...n, data: { ...n.data, status: statusByNodeId[n.id] } }
          : n
      ),
    })),

  clearNodeStatuses: () =>
    set((state) => ({
      nodes: state.nodes.map((n) => ({ ...n, data: { ...n.data, status: "IDLE" as NodeStatus } })),
    })),

  setSelectedNode: (nodeId) => set({ selectedNodeId: nodeId }),

  setActiveRunId: (runId) => set({ activeRunId: runId }),

  onNodesChange: (changes) => set({ nodes: applyNodeChanges(changes, get().nodes), dirty: true }),

  onEdgesChange: (changes) => set({ edges: applyEdgeChanges(changes, get().edges), dirty: true }),

  onConnect: (connection) => set({ edges: addEdge(connection, get().edges), dirty: true }),

  markClean: () => set({ dirty: false }),
}));
