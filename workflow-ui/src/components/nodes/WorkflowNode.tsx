import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import type { WorkflowNodeData } from "../../types/workflow";
import { useWorkflowStore } from "../../store/workflowStore";

const STATUS_COLOR: Record<string, string> = {
  IDLE: "var(--node-idle-border)",
  RUNNING: "var(--warning)",
  SUCCESS: "var(--success)",
  FAILED: "var(--danger)",
  SKIPPED: "var(--skip)",
};

const CATEGORY_ICON: Record<string, string> = {
  trigger: "▶", // ▶
  action: "⚡", // ⚡
  utility: "\u{1F441}", // 👁
};

export function WorkflowNode({ id, data, selected }: NodeProps<Node<WorkflowNodeData>>) {
  const status = data.status || "IDLE";
  const borderColor = STATUS_COLOR[status] || STATUS_COLOR.IDLE;
  const removeNode = useWorkflowStore((s) => s.removeNode);

  return (
    <div
      className="workflow-node"
      style={{
        borderColor,
        boxShadow: selected ? `0 0 0 2px ${borderColor}` : undefined,
      }}
    >
      <button
        type="button"
        className="workflow-node-remove"
        title="Remove node"
        onClick={(e) => {
          e.stopPropagation();
          removeNode(id);
        }}
      >
        ✕
      </button>
      <Handle type="target" position={Position.Left} />
      <div className="workflow-node-header">
        <span className="workflow-node-icon">{CATEGORY_ICON[data.category] ?? "⚙"}</span>
        <span className="workflow-node-label">{data.label}</span>
      </div>
      <div className="workflow-node-status" style={{ color: borderColor }}>
        {status}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
