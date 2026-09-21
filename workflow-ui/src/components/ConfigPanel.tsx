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
  const edges = useWorkflowStore((s) => s.edges);
  const activeRunId = useWorkflowStore((s) => s.activeRunId);
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

  // Claim-based Excel export (json_to_fhir / fhir_to_json nodes): looks up
  // the PreAuth audit log by Claim ID for the most recent chain execution -
  // request-only for JSON→FHIR, response-only for FHIR→JSON.
  let claimExcelExportPath: string | null = null;
  let claimExcelExportLabel = "";
  if (nodeTypeDef.type === "json_to_fhir" && config.function === "json_to_fhir_claim") {
    claimExcelExportPath = "current-trail/request/export/excel";
    claimExcelExportLabel = "Download Request Chain Excel (by Claim ID)";
  } else if (nodeTypeDef.type === "fhir_to_json" && config.function === "fhir_to_json_claim_response") {
    claimExcelExportPath = "current-trail/response/export/excel";
    claimExcelExportLabel = "Download Response Chain Excel (by Claim ID)";
  }

  // "To PreAuth tables" mapper: the stored rows it produces are what the
  // integration-api export copies out of the database, one sheet per table.
  const isPreauthTablesNode = nodeTypeDef.type === "fhir_to_json" && config.function === "to_preauth_tables";

  // Excel Export node: no ID, no DB lookup - just reads the PRECEDING
  // node's own input/output for the last Run from workflow_run_steps.
  const isExcelExportNode = nodeTypeDef.type === "excel_export";
  const hasUpstreamNode = edges.some((e) => e.target === selectedNode.id);

  return (
    <div className="config-panel">
      <h3>{nodeTypeDef.label}</h3>
      <p className="config-panel-desc">{nodeTypeDef.description}</p>
      {claimExcelExportPath && (
        (config.claimId as string) ? (
          <a
            className="download-button"
            href={`http://localhost:8000/preauth/${encodeURIComponent(config.claimId as string)}/${claimExcelExportPath}`}
            target="_blank"
            rel="noopener noreferrer"
          >
            ⬇ {claimExcelExportLabel}
          </a>
        ) : (
          <div className="download-button download-button-disabled">
            Enter a Claim ID below to download
          </div>
        )
      )}
      {isPreauthTablesNode && (
        <a
          className="download-button"
          href="http://localhost:8000/api/v1/preauth/export/excel"
          target="_blank"
          rel="noopener noreferrer"
        >
          ⬇ Download Stored PreAuth Tables (Excel)
        </a>
      )}
      {isExcelExportNode && (
        !hasUpstreamNode ? (
          <div className="download-button download-button-disabled">
            Wire this after a JSON → FHIR or FHIR → JSON node first
          </div>
        ) : !activeRunId ? (
          <div className="download-button download-button-disabled">
            Run the workflow, then download here
          </div>
        ) : (
          <a
            className="download-button"
            href={`http://localhost:8002/runs/${activeRunId}/nodes/${encodeURIComponent(selectedNode.id)}/export/excel`}
            target="_blank"
            rel="noopener noreferrer"
          >
            ⬇ Download This Chain's Excel
          </a>
        )
      )}
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
