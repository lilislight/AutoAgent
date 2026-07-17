import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

declare const process: { env: Record<string, string | undefined> };

const autoAgentServerUrl = process.env.AUTOAGENT_SERVER_URL ?? "http://127.0.0.1:8765";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api": autoAgentServerUrl,
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
