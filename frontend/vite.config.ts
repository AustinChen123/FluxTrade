import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    proxy: {
      "/api": "http://127.0.0.1:8080",
      "/evolution-epochs": "http://127.0.0.1:8080",
      "/genes": "http://127.0.0.1:8080",
      "/health": "http://127.0.0.1:8080"
    }
  }
});
