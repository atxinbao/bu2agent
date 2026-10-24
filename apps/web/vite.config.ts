import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Tauri dev serves this Vite server at http://localhost:5173
// (see apps/shell/src-tauri/tauri.conf.json devUrl). The build
// output (dist/) is what a packaged Tauri app loads instead.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
  },
  build: {
    outDir: "dist",
  },
});
