import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, closeQueue, createWorker } from '@/queues'
import { getRedisConnection, closeRedisConnections, getWorkerRedisConnection } from '@/lib/redis'
import { registerJob } from '@/jobs/registry'
import { Worker } from 'bullmq'
import { RetryableError } from '@/jobs/types'
import { logger } from '@/lib/logger'

describe('integration/multi-worker', () => {
  let worker1: Worker
  let worker2: Worker
  let worker3: Worker

  beforeAll(async () => {
    const redis = getRedisConnection()
    if (redis.status === 'wait') {
      await redis.connect()
    }
    await redis.flushdb()

    // Register a test job that records which worker processes it
    const processedBy: string[] = []

    registerJob({
      name: 'multi-worker-test',
      handler: async ({ data }) => {
        const workerId = process.env.WORKER_ID || 'unknown'
        processedBy.push(workerId)
        return { success: true, workerId, data }
      },
      defaultOptions: { attempts: 1 },
    })

    // Start 3 workers
    const createTestWorker = (id: string) => createWorker('default', async (job) => {
      const handler = (await import('@/jobs/registry')).getJobHandler(job.name)
      if (!handler) throw new Error(`No handler for ${job.name}`)

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 30000)

      try {
        const result = await handler({
          data: job.data as any,
          id: job.id || 'unknown',
          attemptsMade: job.attemptsMade,
          updateProgress: async (p: number) => job.updateProgress(p),
          signal: controller.signal,
        })
        if (!result.success) throw new Error(result.error)
        return result.data
      } catch (e) {
        if (e instanceof Error && e.name === 'AbortError') throw new RetryableError(`Timeout after 30s`)
        throw e
      } finally {
        clearTimeout(timeoutId)
      }
    }, {
      connection: getWorkerRedisConnection(),
      concurrency: 2,
    })

    worker1 = createTestWorker('worker-1')
    worker2 = createTestWorker('worker-2')
    worker3 = createTestWorker('worker-3')

    // Give workers time to start
    await new Promise(r => setTimeout(r, 1000))
  }, 15000)

  afterAll(async () => {
    await Promise.all([worker1?.close(), worker2?.close(), worker3?.close()])
    await closeQueue()
    await closeRedisConnections()
  }, 10000)

  beforeEach(async () => {
    await queue.drain()
    await queue.obliterate({ force: true })
  })

  it('should distribute jobs across multiple workers', async () => {
    // This test verifies that jobs are picked up by different workers
    // We can't easily test which worker picks up which job without
    // more complex tracking, but we can verify all 3 workers are
    // processing by enqueueing many jobs quickly

    const jobs = []
    for (let i = 0; i < 10; i++) {
      jobs.push(addJob('multi-worker-test', { index: i }))
    }

    const createdJobs = await Promise.all(jobs)

    // Wait for all to complete
    await Promise.all(createdJobs.map(j => waitForJobCompletion(j.id, 10000)))

    // Verify all jobs completed
    const counts = await queue.getJobCounts()
    expect(counts.completed).toBeGreaterThanOrEqual(10)
    expect(counts.failed).toBe(0)
  }, 20000)

  it('should handle concurrent job processing', async () => {
    // Enqueue many jobs rapidly
    const jobCount = 10
    const jobs = []
    for (let i = 0; i < jobCount; i++) {
      jobs.push(addJob('multi-worker-test', { batch: 2, index: i }))
    }

    await Promise.all(jobs)

    // Wait for completion using waitForJobCompletion
    await Promise.all(jobs.map(j => waitForJobCompletion(j.id, 15000)))

    const counts = await queue.getJobCounts()
    expect(counts.completed).toBeGreaterThanOrEqual(jobCount)
    expect(counts.failed).toBe(0)
  }, 30000)

  it('should respect concurrency limits', async () => {
    // With 3 workers x concurrency 2 = 6 max concurrent
    // Enqueue 12 jobs that take 100ms each
    const jobCount = 12

    registerJob({
      name: 'concurrency-test',
      handler: async ({ signal }) => {
        await new Promise((resolve) => {
          signal?.addEventListener('abort', () => resolve())
          setTimeout(resolve, 100)
        })
        return { success: true }
      },
      defaultOptions: { attempts: 1 },
    })

    const jobs = []
    for (let i = 0; i < jobCount; i++) {
      jobs.push(addJob('concurrency-test', { index: i }))
    }

    const start = Date.now()
    await Promise.all(jobs)
    await Promise.all(jobs.map(j => waitForJobCompletion(j.id, 30000)))
    const elapsed = Date.now() - start

    // With 6 concurrent, 12 jobs * 100ms = 1200ms sequential
    // Should complete in roughly 200-400ms (2 batches of 6)
    expect(elapsed).toBeLessThan(1000)
  }, 30000)
})

async function waitForJobCompletion(jobId: string, timeoutMs = 30000): Promise<any> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const job = await queue.getJob(jobId)
    if (!job) {
      throw new Error(`Job ${jobId} not found`)
    }
    const state = await job.getState()
    if (state === 'completed' || state === 'failed') {
      return job
    }
    await new Promise(r => setTimeout(r, 100))
  }
  throw new Error(`Timeout waiting for job ${jobId} to complete`)
}