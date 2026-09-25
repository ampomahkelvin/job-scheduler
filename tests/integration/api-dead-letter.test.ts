import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, closeQueue } from '@/queues'
import { getRedisConnection, closeRedisConnections, getWorkerRedisConnection } from '@/lib/redis'
import { registerJob } from '@/jobs/registry'
import { Queue } from 'bullmq'
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

describe('integration/api-dead-letter', () => {
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

  it('should move failed job to dead-letter queue after retries exhausted', async () => {
    const dlqQueue = new Queue('dlq', {
      connection: getWorkerRedisConnection(),
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: 1000,
        removeOnFail: 1000,
      },
    })

    registerJob({
      name: 'dlq-test-job',
      handler: async () => {
        throw new RetryableError('Network error')
      },
      defaultOptions: { attempts: 2, backoff: { type: 'fixed', delay: 50 } },
    })

    const job = await addJob('dlq-test-job', {})
    await waitForJobCompletion(job.id)

    // Check DLQ
    await new Promise(r => setTimeout(r, 500))
    const dlqJobs = await dlqQueue.getJobs(['waiting', 'completed', 'failed'], 0, 10)
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job.id)

    expect(dlqJob).toBeDefined()
    expect(dlqJob!.data.originalJob.failedReason).toContain('Network error')
    expect(dlqJob!.data.originalJob.attemptsMade).toBeGreaterThanOrEqual(1)

    await dlqQueue.close()
  }, 15000)

  it('should not move UnrecoverableError jobs to DLQ', async () => {
    const dlqQueue = new Queue('dlq', {
      connection: getWorkerRedisConnection(),
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: 1000,
        removeOnFail: 1000,
      },
    })

    registerJob({
      name: 'unrecoverable-dlq-test',
      handler: async () => {
        throw new UnrecoverableError('Invalid input')
      },
      defaultOptions: { attempts: 1 },
    })

    const job = await addJob('unrecoverable-dlq-test', {})
    await waitForJobCompletion(job.id)

    await new Promise(r => setTimeout(r, 500))
    const dlqJobs = await dlqQueue.getJobs(['waiting', 'completed', 'failed'], 0, 10)
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job.id)

    expect(dlqJob).toBeUndefined()

    await dlqQueue.close()
  }, 15000)

  it('should replay job from DLQ', async () => {
    const dlqQueue = new Queue('dlq', {
      connection: getWorkerRedisConnection(),
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: 1000,
        removeOnFail: 1000,
      },
    })

    registerJob({
      name: 'replay-dlq-test',
      handler: async ({ data }) => {
        if (data.shouldFail) throw new RetryableError('Fail once')
        return { success: true, data }
      },
      defaultOptions: { attempts: 1 },
    })

    // First, fail and go to DLQ
    const job1 = await addJob('replay-dlq-test', { shouldFail: true })
    await waitForJobCompletion(job1.id)

    await new Promise(r => setTimeout(r, 500))
    let dlqJobs = await dlqQueue.getJobs(['waiting', 'completed', 'failed'], 0, 10)
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job1.id)
    expect(dlqJob).toBeDefined()

    // Replay the job
    const mainQueue = new Queue('default', { connection: getWorkerRedisConnection() })
    const originalJob = dlqJob!.data.originalJob
    const replayedJob = await mainQueue.add(originalJob.name, { ...originalJob.data, shouldFail: false })

    const finishedJob = await waitForJobCompletion(replayedJob.id)
    expect(finishedJob.returnvalue).toEqual({ success: true, data: { shouldFail: false } })

    await dlqQueue.close()
    await mainQueue.close()
  }, 15000)

  it('should remove dead-letter job permanently', async () => {
    const dlqQueue = new Queue('dlq', {
      connection: getWorkerRedisConnection(),
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: 1000,
        removeOnFail: 1000,
      },
    })

    registerJob({
      name: 'delete-dlq-test',
      handler: async () => {
        throw new RetryableError('Permanent failure')
      },
      defaultOptions: { attempts: 1 },
    })

    const job = await addJob('delete-dlq-test', {})
    await waitForJobCompletion(job.id)

    await new Promise(r => setTimeout(r, 500))
    const dlqJobs = await dlqQueue.getJobs(['waiting', 'completed', 'failed'], 0, 10)
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job.id)

    expect(dlqJob).toBeDefined()

    // Remove permanently
    await dlqJob!.remove()

    const afterRemoval = await dlqQueue.getJobs(['waiting', 'completed', 'failed'], 0, 10)
    const found = afterRemoval.find(j => j.id === dlqJob!.id)
    expect(found).toBeUndefined()

    await dlqQueue.close()
  }, 15000)
})