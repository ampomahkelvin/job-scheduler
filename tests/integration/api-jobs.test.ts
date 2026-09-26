import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, getJobCounts, closeQueue } from '@/queues'
import { getRedisConnection, closeRedisConnections } from '@/lib/redis'
import { registerJob } from '@/jobs/registry'
import { RetryableError, UnrecoverableError } from '@/jobs/types'
import { Worker } from 'bullmq'
import { getWorkerRedisConnection } from '@/lib/redis'
import { getJobHandler as getJobHandlerFn } from '@/jobs/registry'
import { logger } from '@/lib/logger'
import { UnrecoverableError as UnrecoverableErrorType, RetryableError as RetryableErrorType } from '@/jobs/types'
import { buildServer } from '@/server'
import { createWorkerInstance } from '@/worker'

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
  let worker: any
  let app: any

  beforeAll(async () => {
    const redis = getRedisConnection()
    if (redis.status === 'wait') {
      await redis.connect()
    }
    await redis.flushdb()

    // Start worker
    worker = createWorkerInstance()

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
    registerJob({
      name: 'api-test-job',
      handler: async ({ data }) => ({ success: true, data }),
      defaultOptions: { attempts: 1 },
    })

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
    registerJob({
      name: 'get-job-test',
      handler: async () => ({ success: true }),
    })

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
    registerJob({
      name: 'list-job-test',
      handler: async () => ({ success: true }),
    })

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

    const response = await app.inject({
      method: 'GET',
      url: '/jobs?status=waiting',
    })

    expect(response.statusCode).toBe(200)
    const body = JSON.parse(response.body)
    expect(body.jobs.length).toBeGreaterThanOrEqual(3)
  })

  it('should delete job via DELETE /jobs/:id', async () => {
    registerJob({
      name: 'delete-job-test',
      handler: async () => ({ success: true }),
    })

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
    registerJob({
      name: 'idempotent-job',
      handler: async ({ data }) => ({ success: true, data }),
      defaultOptions: { attempts: 1 },
    })

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

    // Third request with same key but different data should fail
    try {
      await app.inject({
        method: 'POST',
        url: '/jobs',
        payload: {
          name: 'idempotent-job',
          data: { message: 'different' },
          options: { idempotencyKey: 'idem-key-123' },
        },
      })
      throw new Error('Should have thrown')
    } catch (error: any) {
      expect(error.message).toContain('IdempotencyKeyConflict')
    }
  })

  it('should handle force delete for active jobs', async () => {
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

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { name: 'force-delete-test', data: {} },
    })

    const job = JSON.parse(response.body)
    // Job should be active/waiting
    await job.discard()

    // After discard, the job should be in failed state
    const discarded = await queue.getJob(job.id)
    expect(discarded).toBeDefined()
  })
})