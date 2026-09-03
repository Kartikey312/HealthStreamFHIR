import { useEffect, useState } from "react";

/**
 * Tracks the OS-level prefers-color-scheme, live. Needed for the few things
 * that can't theme via our CSS variables (react-json-view-lite takes a style
 * object in JS, not CSS) - everything else should prefer the CSS variables
 * in index.css instead of reaching for this.
 */
export function useSystemColorScheme(): "light" | "dark" {
  const query = "(prefers-color-scheme: dark)";
  const [isDark, setIsDark] = useState(
    () => typeof window !== "undefined" && window.matchMedia(query).matches
  );

  useEffect(() => {
    const mql = window.matchMedia(query);
    const listener = (e: MediaQueryListEvent) => setIsDark(e.matches);
    mql.addEventListener("change", listener);
    return () => mql.removeEventListener("change", listener);
  }, []);

  return isDark ? "dark" : "light";
}
