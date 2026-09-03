import { JsonView, darkStyles, defaultStyles } from "react-json-view-lite";
import "react-json-view-lite/dist/index.css";
import { useSystemColorScheme } from "../hooks/useSystemColorScheme";

export function JsonViewer({ data }: { data: unknown }) {
  const scheme = useSystemColorScheme();

  if (data === null || data === undefined) {
    return <div className="json-empty">no data</div>;
  }
  if (typeof data !== "object") {
    return <pre className="json-scalar">{JSON.stringify(data)}</pre>;
  }
  return (
    <div className="json-viewer">
      <JsonView
        data={data as object}
        style={scheme === "dark" ? darkStyles : defaultStyles}
        shouldExpandNode={(level) => level < 2}
      />
    </div>
  );
}
