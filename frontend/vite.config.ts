import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            {
              name: "zrender",
              test: /node_modules[\\/]zrender[\\/]/
            }
          ]
        }
      }
    }
  },
  server: {
    host: "127.0.0.1",
    proxy: {
      "/api": "http://127.0.0.1:8080",
      "/evolution-epochs": "http://127.0.0.1:8080",
      "/genes": "http://127.0.0.1:8080",
      "/strategies": "http://127.0.0.1:8080",
      "/strategy-states": "http://127.0.0.1:8080",
      "/health": "http://127.0.0.1:8080"
    }
  }
});
