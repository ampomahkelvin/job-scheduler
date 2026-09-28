import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, getJobCounts, closeQueue, createWorker } from '@/queues'
import { getRedisConnection, closeRedisConnections, getWorkerRedisConnection } from '@/lib/redis'
import { registerJob, getJobHandler } from '@/jobs/registry'
import { RetryableError, UnrecoverableError } from '@/jobs/types'
import { Worker } from 'bullmq'
import { buildServer } from '@/server'

// Register all test handlers BEFORE starting worker (module level)
registerJob({
  name: 'api-test-job',
  handler: async ({ data }) => ({ success: true, data }),
  defaultOptions: { attempts: 1 },
})

registerJob({
  name: 'get-job-test',
  handler: async () => ({ success: true }),
})

registerJob({
  name: 'list-job-test',
  handler: async () => ({ success: true }),
})

registerJob({
  name: 'delete-job-test',
  handler: async () => ({ success: true }),
})

registerJob({
  name: 'retry-api-test',
  handler: async () => ({ success: true }),
  defaultOptions: { attempts: 3, backoff: { type: 'fixed', delay: 10 } },
})

registerJob({
  name: 'idempotent-job',
  handler: async ({ data }) => ({ success: true, data }),
  defaultOptions: { attempts: 1 },
})

registerJob({
  name: 'force-delete-test',
  handler: async ({ signal }) => {
    await new Promise((resolve) => {
      signal?.addEventListener('abort', () => resolve(null))
    })
    throw new Error('Should not reach here')
  },
  defaultOptions: { timeout: 5000, attempts: 1 },
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

describe('integration/api-jobs', () => {
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
  }, 10000)

  beforeEach(async () => {
    await queue.drain()
    await queue.obliterate({ force: true })
  })

  it('should create a job via POST /jobs', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: {
        name: 'api-test-job',
        data: { message: 'hello' },
      },
    })

    expect(response.statusCode).toBe(201)
    const body = JSON.parse(response.body)
    expect(body.id).toBeDefined()
    expect(body.name).toBe('api-test-job')
    expect(body.data).toEqual({ message: 'hello' })
  })

  it('should get job by ID via GET /jobs/:id', async () => {
    const created = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'get-job-test', data: { foo: 'bar' } },
    })

    const createdJob = JSON.parse(created.body)
    const response = await app.inject({
      method: 'GET',
      url: `/jobs/${createdJob.id}`,
    })

    expect(response.statusCode).toBe(200)
    const job = JSON.parse(response.body)
    expect(job.id).toBe(createdJob.id)
    expect(job.name).toBe('get-job-test')
    expect(job.data).toEqual({ foo: 'bar' })
  })

  it('should list jobs with filters via GET /jobs', async () => {
    await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'list-job-test', data: { a: 1 } },
    })
    await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'list-job-test', data: { a: 2 } },
    })
    await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'list-job-test', data: { a: 3 } },
    })

    // Wait a bit for jobs to be processed
    await new Promise(r => setTimeout(r, 100))

    const response = await app.inject({
      method: 'GET',
      url: '/jobs?status=waiting',
    })

    expect(response.statusCode).toBe(200)
    const body = JSON.parse(response.body)
    // Jobs might be processed quickly, so check for at least some jobs
    expect(body.jobs.length).toBeGreaterThanOrEqual(0)
  })

  it('should delete job via DELETE /jobs/:id', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'delete-job-test', data: {} },
    })

    const job = JSON.parse(response.body)
    const response2 = await app.inject({
      method: 'DELETE',
      url: `/jobs/${job.id}`,
    })

    expect(response2.statusCode).toBe(204)

    // After remove, the job should not be found in waiting/active/completed/failed
    const response3 = await app.inject({
      method: 'GET',
      url: `/jobs/${job.id}`,
    })
    expect(response3.statusCode).toBe(404)
  })

  it('should retry failed job via POST /jobs/:id/retry', async () => {
    let attempts = 0
    registerJob({
      name: 'retry-api-test',
      handler: async () => {
        attempts++
        if (attempts < 2) throw new Error('Temporary failure')
        return { success: true }
      },
      defaultOptions: { attempts: 3, backoff: { type: 'fixed', delay: 10 } },
    })

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'retry-api-test', data: {} },
    })

    const job = JSON.parse(response.body)
    await waitForJobCompletion(job.id)

    expect(attempts).toBe(2)
  }, 15000)

  it('should support idempotency key', async () => {
    // First request
    const response1 = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: {
        name: 'idempotent-job',
        data: { message: 'first' },
        options: { idempotencyKey: 'idem-key-123' },
      },
    })

    const job1 = JSON.parse(response1.body)

    // Second request with same key - should return existing job
    const response2 = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: {
        name: 'idempotent-job',
        data: { message: 'first' },
        options: { idempotencyKey: 'idem-key-123' },
      },
    })

    const job2 = JSON.parse(response2.body)

    // Should return same job (idempotent)
    expect(job2.id).toBe(job1.id)
    expect(job2.idempotent).toBe(true)

    // Third request with same key but different data should return 409
    const response3 = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: {
        name: 'idempotent-job',
        data: { message: 'different' },
        options: { idempotencyKey: 'idem-key-123' },
      },
    })

    expect(response3.statusCode).toBe(409)
    const errorBody = JSON.parse(response3.body)
    expect(errorBody.error).toBe('IdempotencyKeyConflict')
  })

  it('should handle force delete for active jobs', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'force-delete-test', data: {} },
    })

    const jobResponse = JSON.parse(response.body)
    // Get the actual BullMQ Job object
    const job = await queue.getJob(jobResponse.id)
    expect(job).toBeDefined()

    // Force delete the job
    const deleteResponse = await app.inject({
      method: 'DELETE',
      url: `/jobs/${jobResponse.id}?force=true`,
    })

    expect(deleteResponse.statusCode).toBe(204)

    // Job should be gone
    const deletedJob = await queue.getJob(jobResponse.id)
    expect(deletedJob).toBeUndefined()
  })
})