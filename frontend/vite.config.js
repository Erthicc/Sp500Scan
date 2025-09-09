import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  root: PathOrRoot(),
  build: {
    outDir: path.resolve(__dirname, '../public'),
    emptyOutDir: true,
    rollupOptions: {
      input: path.resolve(__dirname, 'index.html')
    }
  }
})

function PathOrRoot() { return '.' }
