import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

declare const process: { env: Record<string, string | undefined> };

const serverUrl = process.env.AUTOAGENT_TRACE_SERVER_URL ?? "http://127.0.0.1:8765";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: { "/api": serverUrl },
  },
  build: {
    outDir: "../autoagent/tracing/ui",
    emptyOutDir: true,
    sourcemap: false,
  },
});
