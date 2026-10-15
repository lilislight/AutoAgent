import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  base: "./",
  server: {
    host: "127.0.0.1",
    proxy: { "/api": process.env.AUTOAGENT_SERVER_URL ?? "http://127.0.0.1:8765" },
  },
  build: {
    outDir: "../autoagent/server/static",
    emptyOutDir: true,
  },
});
