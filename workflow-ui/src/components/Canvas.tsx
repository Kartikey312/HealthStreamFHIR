import { useCallback, useRef } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useReactFlow,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useWorkflowStore } from "../store/workflowStore";
import { WorkflowNode } from "./nodes/WorkflowNode";
import { useNodeTypes } from "../api/client";
import type { WorkflowNodeData } from "../types/workflow";

const nodeTypesMap = {
  manual_trigger: WorkflowNode,
  http_request: WorkflowNode,
  kafka_publish: WorkflowNode,
  db_query: WorkflowNode,
  json_to_fhir: WorkflowNode,
  fhir_to_json: WorkflowNode,
  // Legacy key - no longer in the palette, kept so previously-saved workflows still render.
  fhir_transform: WorkflowNode,
  view: WorkflowNode,
};

let nodeIdCounter = 1;

export function Canvas() {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const { screenToFlowPosition } = useReactFlow();
  const { data: nodeTypesData } = useNodeTypes();

  const nodes = useWorkflowStore((s) => s.nodes);
  const edges = useWorkflowStore((s) => s.edges);
  const onNodesChange = useWorkflowStore((s) => s.onNodesChange);
  const onEdgesChange = useWorkflowStore((s) => s.onEdgesChange);
  const onConnect = useWorkflowStore((s) => s.onConnect);
  const addNode = useWorkflowStore((s) => s.addNode);
  const setSelectedNode = useWorkflowStore((s) => s.setSelectedNode);

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = "move";
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();
      const nodeType = event.dataTransfer.getData("application/workflow-node-type");
      if (!nodeType || !nodeTypesData) return;

      const nodeTypeDef = nodeTypesData.node_types.find((nt) => nt.type === nodeType);
      if (!nodeTypeDef) return;

      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
      const defaultConfig: Record<string, unknown> = {};
      for (const field of nodeTypeDef.config_schema) {
        if (field.default !== undefined) defaultConfig[field.key] = field.default;
      }

      const newNode: Node<WorkflowNodeData> = {
        id: `${nodeType}_${nodeIdCounter++}`,
        type: nodeType,
        position,
        data: {
          label: nodeTypeDef.label,
          nodeType,
          category: nodeTypeDef.category,
          config: defaultConfig,
          status: "IDLE",
        },
      };
      addNode(newNode);
    },
    [nodeTypesData, screenToFlowPosition, addNode]
  );

  return (
    <div className="canvas-wrapper" ref={wrapperRef}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onDrop={onDrop}
        onDragOver={onDragOver}
        onNodeClick={(_, node) => setSelectedNode(node.id)}
        onPaneClick={() => setSelectedNode(null)}
        nodeTypes={nodeTypesMap}
        colorMode="system"
        fitView
      >
        <Background />
        <Controls />
        <MiniMap />
      </ReactFlow>
    </div>
  );
}
