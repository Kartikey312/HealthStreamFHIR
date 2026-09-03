// Mirrors workflow-service/src/schemas.py and node_registry.py

export type NodeStatus = "IDLE" | "RUNNING" | "SUCCESS" | "FAILED" | "SKIPPED";

export interface ConfigField {
  key: string;
  label: string;
  type: "string" | "select" | "json" | "keyvalue";
  options?: string[];
  optionsFrom?: string;
  default?: unknown;
  placeholder?: string;
  showWhen?: Record<string, string>;
}

export interface NodeTypeDef {
  type: string;
  label: string;
  category: "trigger" | "action" | "utility";
  description: string;
  config_schema: ConfigField[];
}

export interface WorkflowNodeData {
  label: string;
  nodeType: string;
  category: "trigger" | "action" | "utility";
  config: Record<string, unknown>;
  status?: NodeStatus;
  [key: string]: unknown;
}

export interface WorkflowNode {
  id: string;
  type: string; // node type key (manual_trigger, http_request, ...)
  position: { x: number; y: number };
  config: Record<string, unknown>;
}

export interface WorkflowEdge {
  id?: string;
  source: string;
  target: string;
}

export interface WorkflowDefinition {
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
}

export interface WorkflowSummary {
  id: number;
  name: string;
  description: string | null;
  created_at: string;
  updated_at: string;
}

export interface WorkflowOut extends WorkflowSummary {
  definition: WorkflowDefinition;
}

export interface WorkflowRunStepOut {
  node_id: string;
  node_type: string;
  status: NodeStatus;
  input: Record<string, unknown> | null;
  output: Record<string, unknown> | null;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
}

export interface WorkflowRunOut {
  id: number;
  workflow_id: number;
  status: "RUNNING" | "SUCCESS" | "FAILED";
  trigger_input: Record<string, unknown> | null;
  error: string | null;
  started_at: string;
  finished_at: string | null;
  steps: WorkflowRunStepOut[];
}

export interface WorkflowRunSummary {
  id: number;
  status: "RUNNING" | "SUCCESS" | "FAILED";
  started_at: string;
  finished_at: string | null;
}

export interface RunAccepted {
  run_id: number;
  workflow_id: number;
  status: string;
  started_at: string;
}
