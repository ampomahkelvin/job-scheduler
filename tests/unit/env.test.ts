import { describe, it, expect, vi, beforeEach } from 'vitest'

describe('config/env', () => {
  const originalEnv = process.env

  beforeEach(() => {
    vi.resetModules()
    process.env = { ...originalEnv }
  })

  it('should load valid env vars with defaults', async () => {
    process.env.REDIS_URL = 'redis://localhost:6379'
    const { loadEnv } = await import('@/config/env')
    const result = loadEnv()
    expect(result.PORT).toBe(3000)
    expect(result.LOG_LEVEL).toBe('info')
    expect(result.REDIS_URL).toBe('redis://localhost:6379')
    expect(result.BULL_BOARD_PATH).toBe('/admin/queues')
  })

  it('should parse custom PORT', async () => {
    process.env.PORT = '4000'
    process.env.REDIS_URL = 'redis://localhost:6379'
    const { loadEnv } = await import('@/config/env')
    const result = loadEnv()
    expect(result.PORT).toBe(4000)
  })

  it('should parse custom LOG_LEVEL', async () => {
    process.env.LOG_LEVEL = 'debug'
    process.env.REDIS_URL = 'redis://localhost:6379'
    const { loadEnv } = await import('@/config/env')
    const result = loadEnv()
    expect(result.LOG_LEVEL).toBe('debug')
  })

  it.skip('should throw on invalid REDIS_URL', async () => {
    process.env.REDIS_URL = 'invalid-url'
    const { loadEnv } = await import('@/config/env')
    expect(() => loadEnv()).toThrow(/Invalid environment variables/)
  })

  it.skip('should throw on invalid LOG_LEVEL', async () => {
    process.env.REDIS_URL = 'redis://localhost:6379'
    process.env.LOG_LEVEL = 'invalid'
    const { loadEnv } = await import('@/config/env')
    expect(() => loadEnv()).toThrow(/Invalid environment variables/)
  })
})