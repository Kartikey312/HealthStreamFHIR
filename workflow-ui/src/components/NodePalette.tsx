import { useNodeTypes } from "../api/client";

const CATEGORY_LABEL: Record<string, string> = {
  trigger: "Triggers",
  action: "Actions",
  utility: "Utility",
};

export function NodePalette() {
  const { data, isLoading, error } = useNodeTypes();

  if (isLoading || !data) return <div className="palette">Loading node types...</div>;
  if (error) return <div className="palette palette-error">Failed to load node types</div>;

  const byCategory = new Map<string, typeof data.node_types>();
  for (const nt of data.node_types) {
    const list = byCategory.get(nt.category) ?? [];
    list.push(nt);
    byCategory.set(nt.category, list);
  }

  const onDragStart = (event: React.DragEvent, nodeType: string) => {
    event.dataTransfer.setData("application/workflow-node-type", nodeType);
    event.dataTransfer.effectAllowed = "move";
  };

  return (
    <div className="palette">
      <h3>Nodes</h3>
      {[...byCategory.entries()].map(([category, nodeTypes]) => (
        <div key={category} className="palette-group">
          <div className="palette-group-label">{CATEGORY_LABEL[category] ?? category}</div>
          {nodeTypes.map((nt) => (
            <div
              key={nt.type}
              className="palette-item"
              draggable
              onDragStart={(e) => onDragStart(e, nt.type)}
              title={nt.description}
            >
              {nt.label}
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}
