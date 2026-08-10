import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { fileURLToPath, URL } from 'node:url'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      // Same-origin development proxy keeps the Spring HttpSession cookie
      // valid for both localhost and 127.0.0.1 browser URLs.
      '/api': {
        target: 'http://localhost:8081',
        changeOrigin: true,
      },
      // Live TraCI vehicle stream (Control API on :9090) — HTTP + WebSocket.
      '/live': {
        target: 'http://127.0.0.1:9090',
        changeOrigin: true,
        ws: true,
      },
      '/ws': {
        target: 'http://127.0.0.1:9090',
        ws: true,
        changeOrigin: true,
      },
      // DQN agent status (Control API, read-only)
      '/rl': {
        target: 'http://127.0.0.1:9090',
        changeOrigin: true,
      },
    },
  },
})
