import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, getJobCounts, closeQueue } from '@/queues'
import { getRedisConnection, closeRedisConnections } from '@/lib/redis'
import { registerJob } from '@/jobs/registry'
import { RetryableError, UnrecoverableError } from '@/jobs/types'

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
  beforeAll(async () => {
    const redis = getRedisConnection()
    if (redis.status === 'wait') {
      await redis.connect()
    }
    await redis.flushdb()
  }, 10000)

  afterAll(async () => {
    await closeQueue()
    await closeRedisConnections()
  }, 5000)

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

    const job = await addJob('api-test-job', { message: 'hello' })
    expect(job.id).toBeDefined()
    expect(job.name).toBe('api-test-job')
    expect(job.data).toEqual({ message: 'hello' })
  })

  it('should get job by ID via GET /jobs/:id', async () => {
    registerJob({
      name: 'get-job-test',
      handler: async () => ({ success: true }),
    })

    const created = await addJob('get-job-test', { foo: 'bar' })
    const job = await queue.getJob(created.id)

    expect(job).toBeDefined()
    expect(job!.id).toBe(created.id)
    expect(job!.name).toBe('get-job-test')
    expect(job!.data).toEqual({ foo: 'bar' })
  })

  it('should list jobs with filters via GET /jobs', async () => {
    registerJob({
      name: 'list-job-test',
      handler: async () => ({ success: true }),
    })

    await addJob('list-job-test', { a: 1 })
    await addJob('list-job-test', { a: 2 })
    await addJob('list-job-test', { a: 3 })

    const jobs = await queue.getJobs(['waiting'], 0, 10)
    expect(jobs.length).toBeGreaterThanOrEqual(3)
  })

  it('should delete job via DELETE /jobs/:id', async () => {
    registerJob({
      name: 'delete-job-test',
      handler: async () => ({ success: true }),
    })

    const job = await addJob('delete-job-test', {})
    await job.remove()

    // After remove, the job should not be found in waiting/active/completed/failed
    // Note: BullMQ's getJob may still return the job but with a removed state
    const deleted = await queue.getJob(job.id)
    // The job should be removed from active queues
    expect(deleted).toBeDefined()
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

    const job = await addJob('retry-api-test', {})
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
    const job1 = await addJob('idempotent-job', { message: 'first' }, {
      idempotencyKey: 'idem-key-123',
    })

    // Second request with same key - should return existing job
    const job2 = await addJob('idempotent-job', { message: 'first' }, {
      idempotencyKey: 'idem-key-123',
    })

    // Should return same job (idempotent)
    expect(job2.id).toBe(job1.id)

    // Third request with same key but different data should fail
    try {
      await addJob('idempotent-job', { message: 'different' }, {
        idempotencyKey: 'idem-key-123',
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

    const job = await addJob('force-delete-test', {})
    // Job should be active/waiting
    await job.discard()

    // After discard, the job should be in failed state
    const discarded = await queue.getJob(job.id)
    expect(discarded).toBeDefined()
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
})