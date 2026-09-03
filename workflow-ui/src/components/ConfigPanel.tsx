import { useState } from "react";
import { useWorkflowStore } from "../store/workflowStore";
import { useNodeTypes, useTopics } from "../api/client";
import type { ConfigField } from "../types/workflow";

function StringField({ value, onChange, placeholder }: { value: unknown; onChange: (v: string) => void; placeholder?: string }) {
  return (
    <input
      type="text"
      value={(value as string) ?? ""}
      placeholder={placeholder}
      onChange={(e) => onChange(e.target.value)}
    />
  );
}

function SelectField({
  value,
  onChange,
  options,
}: {
  value: unknown;
  onChange: (v: string) => void;
  options: string[];
}) {
  return (
    <select value={(value as string) ?? ""} onChange={(e) => onChange(e.target.value)}>
      <option value="">-- select --</option>
      {options.map((opt) => (
        <option key={opt} value={opt}>
          {opt}
        </option>
      ))}
    </select>
  );
}

function JsonField({ value, onChange }: { value: unknown; onChange: (v: unknown) => void }) {
  const [text, setText] = useState(() => JSON.stringify(value ?? {}, null, 2));
  const [error, setError] = useState<string | null>(null);

  const commit = (newText: string) => {
    setText(newText);
    try {
      const parsed = JSON.parse(newText || "{}");
      setError(null);
      onChange(parsed);
    } catch {
      setError("Invalid JSON");
    }
  };

  return (
    <div>
      <textarea
        rows={5}
        value={text}
        onChange={(e) => commit(e.target.value)}
        className={error ? "field-error" : ""}
      />
      {error && <div className="field-error-text">{error}</div>}
    </div>
  );
}

function KeyValueField({
  value,
  onChange,
  placeholder,
}: {
  value: unknown;
  onChange: (v: Record<string, string>) => void;
  placeholder?: string;
}) {
  const entries = Object.entries((value as Record<string, string>) ?? {});

  const update = (idx: number, key: string, val: string) => {
    const next = [...entries];
    next[idx] = [key, val];
    onChange(Object.fromEntries(next));
  };
  const remove = (idx: number) => {
    const next = entries.filter((_, i) => i !== idx);
    onChange(Object.fromEntries(next));
  };
  const add = () => onChange(Object.fromEntries([...entries, ["", ""]]));

  return (
    <div className="keyvalue-field">
      {entries.map(([k, v], idx) => (
        <div key={idx} className="keyvalue-row">
          <input placeholder="key" value={k} onChange={(e) => update(idx, e.target.value, v)} />
          <input placeholder={placeholder ?? "value"} value={v} onChange={(e) => update(idx, k, e.target.value)} />
          <button type="button" onClick={() => remove(idx)}>
            ✕
          </button>
        </div>
      ))}
      <button type="button" onClick={add} className="keyvalue-add">
        + add
      </button>
    </div>
  );
}

export function ConfigPanel() {
  const selectedNodeId = useWorkflowStore((s) => s.selectedNodeId);
  const nodes = useWorkflowStore((s) => s.nodes);
  const updateNodeConfig = useWorkflowStore((s) => s.updateNodeConfig);
  const { data: nodeTypesData } = useNodeTypes();
  const { data: topicsData } = useTopics();

  const selectedNode = nodes.find((n) => n.id === selectedNodeId);
  if (!selectedNode) {
    return <div className="config-panel config-panel-empty">Select a node to configure it</div>;
  }

  const nodeTypeDef = nodeTypesData?.node_types.find((nt) => nt.type === selectedNode.data.nodeType);
  if (!nodeTypeDef) return null;

  const config = selectedNode.data.config;
  const setField = (key: string, value: unknown) => {
    updateNodeConfig(selectedNode.id, { ...config, [key]: value });
  };

  const visibleFields = nodeTypeDef.config_schema.filter(
    (field) =>
      !field.showWhen ||
      Object.entries(field.showWhen).every(([k, v]) => config[k] === v)
  );

  return (
    <div className="config-panel">
      <h3>{nodeTypeDef.label}</h3>
      <p className="config-panel-desc">{nodeTypeDef.description}</p>
      <div className="config-field">
        <label>Node ID</label>
        <input value={selectedNode.id} disabled />
      </div>
      {visibleFields.map((field) => (
        <div key={field.key} className="config-field">
          <label>{field.label}</label>
          {renderField(field, config[field.key], (v) => setField(field.key, v), topicsData?.topics)}
        </div>
      ))}
    </div>
  );
}

function renderField(
  field: ConfigField,
  value: unknown,
  onChange: (v: unknown) => void,
  topics?: Record<string, string>
) {
  switch (field.type) {
    case "string":
      return <StringField value={value} onChange={onChange} placeholder={field.placeholder} />;
    case "select": {
      const options =
        field.optionsFrom === "/topics" && topics ? Object.values(topics) : field.options ?? [];
      return <SelectField value={value} onChange={onChange} options={options} />;
    }
    case "json":
      return <JsonField value={value} onChange={onChange} />;
    case "keyvalue":
      return (
        <KeyValueField
          value={value}
          onChange={onChange as (v: Record<string, string>) => void}
          placeholder={field.placeholder}
        />
      );
    default:
      return null;
  }
}
