import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, getJobCounts, closeQueue } from '@/queues'
import { getRedisConnection } from '@/lib/redis'
import { registerJob } from '@/jobs/registry'

describe('integration/queue', () => {
  beforeAll(async () => {
    const redis = getRedisConnection()
    await redis.connect()
    await redis.flushdb()
  }, 10000)

  afterAll(async () => {
    await closeQueue()
  }, 5000)

  beforeEach(async () => {
    await queue.drain()
    await queue.obliterate({ force: true })
  })

  it('should add and process a job', async () => {
    const results: unknown[] = []

    registerJob({
      name: 'integration-test',
      handler: async ({ data }) => {
        results.push(data)
        return { success: true, data }
      },
    })

    const job = await addJob('integration-test', { foo: 'bar' })
    expect(job.id).toBeDefined()
    expect(job.name).toBe('integration-test')

    await job.waitUntilFinished()
    expect(results).toEqual([{ foo: 'bar' }])
  }, 10000)

  it('should return job counts', async () => {
    registerJob({
      name: 'count-test',
      handler: async () => ({ success: true }),
    })

    await addJob('count-test', { a: 1 })
    await addJob('count-test', { a: 2 })
    await addJob('count-test', { a: 3 })

    const counts = await getJobCounts()
    expect(counts.waiting).toBeGreaterThanOrEqual(3)
  })

  it('should handle job retries on failure', async () => {
    let attempts = 0

    registerJob({
      name: 'retry-test',
      handler: async () => {
        attempts++
        if (attempts < 3) {
          throw new Error('Temporary failure')
        }
        return { success: true }
      },
      defaultOptions: { attempts: 3, backoff: { type: 'fixed', delay: 10 } },
    })

    const job = await addJob('retry-test', {})
    await job.waitUntilFinished()

    expect(attempts).toBe(3)
  }, 15000)
})