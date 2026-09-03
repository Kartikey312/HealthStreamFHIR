import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type {
  NodeTypeDef,
  WorkflowDefinition,
  WorkflowOut,
  WorkflowRunOut,
  WorkflowRunSummary,
  WorkflowSummary,
  RunAccepted,
} from "../types/workflow";

// Must be the HOST-mapped port - this code runs in the browser, not a container.
const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8002";

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(detail.detail || `Request failed: ${res.status}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

export function useNodeTypes() {
  return useQuery({
    queryKey: ["node-types"],
    queryFn: () => apiFetch<{ node_types: NodeTypeDef[] }>("/node-types"),
  });
}

export function useTopics() {
  return useQuery({
    queryKey: ["topics"],
    queryFn: () => apiFetch<{ topics: Record<string, string> }>("/topics"),
  });
}

export function useWorkflows() {
  return useQuery({
    queryKey: ["workflows"],
    queryFn: () => apiFetch<{ workflows: WorkflowSummary[] }>("/workflows"),
  });
}

export function useWorkflow(id: number | null) {
  return useQuery({
    queryKey: ["workflow", id],
    queryFn: () => apiFetch<WorkflowOut>(`/workflows/${id}`),
    enabled: id !== null,
  });
}

export function useCreateWorkflow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { name: string; description?: string; definition: WorkflowDefinition }) =>
      apiFetch<WorkflowOut>("/workflows", { method: "POST", body: JSON.stringify(body) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workflows"] }),
  });
}

export function useSaveWorkflow(id: number) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { name: string; description?: string; definition: WorkflowDefinition }) =>
      apiFetch<WorkflowOut>(`/workflows/${id}`, { method: "PUT", body: JSON.stringify(body) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["workflows"] });
      qc.invalidateQueries({ queryKey: ["workflow", id] });
    },
  });
}

export function useDeleteWorkflow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => apiFetch<void>(`/workflows/${id}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workflows"] }),
  });
}

export function useRunWorkflow(workflowId: number) {
  return useMutation({
    mutationFn: (triggerInput: Record<string, unknown> | undefined) =>
      apiFetch<RunAccepted>(`/workflows/${workflowId}/run`, {
        method: "POST",
        body: JSON.stringify({ trigger_input: triggerInput }),
      }),
  });
}

export function useRunPoll(runId: number | null) {
  return useQuery({
    queryKey: ["run", runId],
    queryFn: () => apiFetch<WorkflowRunOut>(`/runs/${runId}`),
    enabled: runId !== null,
    refetchInterval: (query) => (query.state.data?.status === "RUNNING" ? 1000 : false),
  });
}

export function useRunHistory(workflowId: number | null) {
  return useQuery({
    queryKey: ["runs", workflowId],
    queryFn: () => apiFetch<{ runs: WorkflowRunSummary[] }>(`/workflows/${workflowId}/runs`),
    enabled: workflowId !== null,
  });
}
