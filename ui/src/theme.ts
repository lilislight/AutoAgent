import { useEffect, useState } from "react";

export type Theme = "light" | "dark";

export function useTheme() {
  const [theme, setTheme] = useState<Theme>(() =>
    document.documentElement.dataset.theme === "dark" ? "dark" : "light",
  );
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.querySelector('meta[name="theme-color"]')?.setAttribute("content", theme === "dark" ? "#141414" : "#e0e0e0");
    try { localStorage.setItem("autoagent:theme", theme); }
    catch { /* The theme still works when browser storage is unavailable. */ }
  }, [theme]);
  return [theme, () => setTheme(current => current === "light" ? "dark" : "light")] as const;
}
