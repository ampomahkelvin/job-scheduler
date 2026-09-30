import { defineConfig } from 'vitest/config'
import path from 'path'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: ['node_modules/', 'dist/', 'tests/', '*.config.*'],
    },
    setupFiles: ['tests/setup.ts'],
    testTimeout: 10000,
    projects: [
      {
        name: 'unit',
        test: {
          include: ['tests/unit/**/*.test.ts'],
          setupFiles: ['tests/setup.ts'],
        },
      },
      {
        name: 'integration',
        test: {
          include: ['tests/integration/**/*.test.ts'],
          setupFiles: ['tests/setup.integration.ts'],
          // These files share one Redis-backed queue singleton and each
          // obliterates it in beforeEach. Running them in parallel processes
          // causes cross-file races (one file's obliterate wiping another's
          // in-flight assertions). Sequential execution trades some speed
          // for correctness until the queue is namespaced per test file.
          fileParallelism: false,
        },
      },
    ],
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
})