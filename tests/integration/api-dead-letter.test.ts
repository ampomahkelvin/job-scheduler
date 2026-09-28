import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, getJobCounts, closeQueue, createWorker } from '@/queues'
import { getRedisConnection, closeRedisConnections, getWorkerRedisConnection } from '@/lib/redis'
import { registerJob, getJobHandler } from '@/jobs/registry'
import { Queue, Worker } from 'bullmq'
import { RetryableError, UnrecoverableError } from '@/jobs/types'
import { buildServer } from '@/server'
import { dlqService } from '@/services/dead-letter'

// Register all test handlers BEFORE starting worker (module level)
registerJob({
  name: 'dlq-test-job',
  handler: async () => {
    throw new RetryableError('Network error')
  },
  defaultOptions: { attempts: 2, backoff: { type: 'fixed', delay: 50 } },
})

registerJob({
  name: 'unrecoverable-dlq-test',
  handler: async () => {
    throw new UnrecoverableError('Invalid input')
  },
  defaultOptions: { attempts: 1 },
})

registerJob({
  name: 'replay-dlq-test',
  handler: async ({ data }) => {
    if (data.shouldFail) throw new RetryableError('Fail once')
    return { success: true, data }
  },
  defaultOptions: { attempts: 1 },
})

registerJob({
  name: 'delete-dlq-test',
  handler: async () => {
    throw new RetryableError('Permanent failure')
  },
  defaultOptions: { attempts: 1 },
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

describe('integration/api-dead-letter', () => {
  let worker: Worker
  let app: any

  beforeAll(async () => {
    const redis = getRedisConnection()
    if (redis.status === 'wait') {
      await redis.connect()
    }
    await redis.flushdb()

    // Start worker with dynamic handler lookup (like reliability test)
    worker = createWorker('default', async (job) => {
      const handler = getJobHandler(job.name)
      if (!handler) {
        throw new UnrecoverableError(`No handler registered for job: ${job.name}`)
      }

      const timeout = job.opts?.timeout || 30000
      const timeoutRetryable = job.opts?.timeoutRetryable !== false

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), timeout)

      const updateProgress = async (progress: number): Promise<void> => {
        await job.updateProgress(progress)
      }

      try {
        const result = await handler({
          data: job.data as never,
          id: job.id || 'unknown',
          attemptsMade: job.attemptsMade,
          updateProgress,
          signal: controller.signal,
        })

        if (!result.success) {
          throw new RetryableError(result.error || 'Job failed')
        }
        return result.data
      } catch (error) {
        if (error instanceof Error && error.name === 'AbortError') {
          const message = `Job timeout after ${timeout}ms`
          throw timeoutRetryable ? new RetryableError(message) : new UnrecoverableError(message)
        }
        throw error
      } finally {
        clearTimeout(timeoutId)
      }
    }, {
      connection: getWorkerRedisConnection(),
      concurrency: 5,
    })

    // Build and start the Fastify app
    app = await buildServer()
    await app.ready()
  }, 15000)

  afterAll(async () => {
    if (worker) await worker.close()
    await closeQueue()
    await closeRedisConnections()
  }, 5000)

  beforeEach(async () => {
    await queue.drain()
    await queue.obliterate({ force: true })
  })

  it('should move failed job to dead-letter queue after retries exhausted', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'dlq-test-job', data: {} },
    })

    const job = JSON.parse(response.body)
    await waitForJobCompletion(job.id)

    // Check DLQ via API - wait longer for async DLQ move
    await new Promise(r => setTimeout(r, 1000))
    const dlqResponse = await app.inject({
      method: 'GET',
      url: '/dead-letter',
    })

    expect(dlqResponse.statusCode).toBe(200)
    const dlqJobs = JSON.parse(dlqResponse.body).jobs
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job.id)

    expect(dlqJob).toBeDefined()
    expect(dlqJob.data.originalJob.failedReason).toContain('Network error')
    expect(dlqJob.data.originalJob.attemptsMade).toBeGreaterThanOrEqual(1)
  }, 15000)

  it('should not move UnrecoverableError jobs to DLQ', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'unrecoverable-dlq-test', data: {} },
    })

    const job = JSON.parse(response.body)
    await waitForJobCompletion(job.id)

    await new Promise(r => setTimeout(r, 1000))
    const dlqResponse = await app.inject({
      method: 'GET',
      url: '/dead-letter',
    })

    expect(dlqResponse.statusCode).toBe(200)
    const dlqJobs = JSON.parse(dlqResponse.body).jobs
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job.id)

    expect(dlqJob).toBeUndefined()
  }, 15000)

  it('should replay job from DLQ', async () => {
    // First, fail and go to DLQ
    const response1 = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'replay-dlq-test', data: { shouldFail: true } },
    })

    const job1 = JSON.parse(response1.body)
    await waitForJobCompletion(job1.id)

    await new Promise(r => setTimeout(r, 1000))
    let dlqResponse = await app.inject({
      method: 'GET',
      url: '/dead-letter',
    })

    const dlqJobs = JSON.parse(dlqResponse.body).jobs
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job1.id)
    expect(dlqJob).toBeDefined()

    // Replay the job
    const replayResponse = await app.inject({
      method: 'POST',
      url: `/dead-letter/${dlqJob.id}/replay`,
    })

    expect(replayResponse.statusCode).toBe(200)
    const replayBody = JSON.parse(replayResponse.body)
    expect(replayBody.message).toBe('Job replayed successfully')
  }, 15000)

  it('should remove dead-letter job permanently', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'delete-dlq-test', data: {} },
    })

    const job = JSON.parse(response.body)
    await waitForJobCompletion(job.id)

    await new Promise(r => setTimeout(r, 1000))
    const dlqResponse = await app.inject({
      method: 'GET',
      url: '/dead-letter',
    })

    const dlqJobs = JSON.parse(dlqResponse.body).jobs
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job.id)

    expect(dlqJob).toBeDefined()

    // Remove permanently
    const deleteResponse = await app.inject({
      method: 'DELETE',
      url: `/dead-letter/${dlqJob.id}`,
    })

    expect(deleteResponse.statusCode).toBe(204)

    const afterRemoval = await app.inject({
      method: 'GET',
      url: '/dead-letter',
    })

    const afterJobs = JSON.parse(afterRemoval.body).jobs
    const found = afterJobs.find(j => j.id === dlqJob.id)
    expect(found).toBeUndefined()
  }, 15000)
})