import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// `npm run dev` talks to a gateway on localhost:8080 (docker compose exposes it).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/ws': { target: 'ws://localhost:8080', ws: true },
    },
  },
})
