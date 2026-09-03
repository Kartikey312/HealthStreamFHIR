import { JsonView, darkStyles } from "react-json-view-lite";
import "react-json-view-lite/dist/index.css";

export function JsonViewer({ data }: { data: unknown }) {
  if (data === null || data === undefined) {
    return <div className="json-empty">no data</div>;
  }
  if (typeof data !== "object") {
    return <pre className="json-scalar">{JSON.stringify(data)}</pre>;
  }
  return (
    <div className="json-viewer">
      <JsonView data={data as object} style={darkStyles} shouldExpandNode={(level) => level < 2} />
    </div>
  );
}
