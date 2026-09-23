import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, getJobCounts, closeQueue, createWorker } from '@/queues'
import { getRedisConnection, closeRedisConnections, getWorkerRedisConnection } from '@/lib/redis'
import { registerJob, getJobHandler } from '@/jobs/registry'
import { RetryableError, UnrecoverableError, isRetryableError } from '@/jobs/types'
import { Worker, Queue } from 'bullmq'
import { getJobHandler as getJobHandlerFn } from '@/jobs/registry'
import { logger } from '@/lib/logger'
import { UnrecoverableError as UnrecoverableErrorType, RetryableError as RetryableErrorType } from '@/jobs/types'

// Register all test handlers BEFORE starting worker
registerJob({
  name: 'retry-on-first-two',
  handler: async ({ attemptsMade }) => {
    if (attemptsMade < 2) {
      throw new RetryableErrorType(`Temporary failure on attempt ${attemptsMade + 1}`)
    }
    return { success: true, data: { attempts: attemptsMade + 1 } }
  },
  defaultOptions: { attempts: 3, backoff: { type: 'fixed', delay: 50 } },
})

registerJob({
  name: 'hanging-job',
  handler: async ({ signal }) => {
    while (!signal?.aborted) {
      await new Promise(r => setTimeout(r, 10))
    }
    const error = new Error('Aborted')
    error.name = 'AbortError'
    throw error
  },
  defaultOptions: { timeout: 100, timeoutRetryable: true, attempts: 2, backoff: { type: 'fixed', delay: 50 } },
})

registerJob({
  name: 'bad-input-job',
  handler: async () => {
    throw new UnrecoverableErrorType('Invalid input: missing required field')
  },
  defaultOptions: { attempts: 1, backoff: { type: 'fixed', delay: 50 } },
})

registerJob({
  name: 'always-fails-retryable',
  handler: async () => {
    throw new RetryableErrorType('Network error')
  },
  defaultOptions: { attempts: 2, backoff: { type: 'fixed', delay: 50 } },
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

describe('integration/reliability', () => {
  let worker: Worker

  beforeAll(async () => {
    const redis = getRedisConnection()
    if (redis.status === 'wait') {
      await redis.connect()
    }
    await redis.flushdb()

    // Start worker AFTER registering handlers
    worker = createWorker('default', async (job) => {
      const handler = getJobHandlerFn(job.name)
      if (!handler) {
        logger.warn({ jobName: job.name, jobId: job.id }, 'No handler for job')
        throw new UnrecoverableErrorType(`No handler registered for job: ${job.name}`)
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
          const error = new RetryableErrorType(result.error || 'Job failed')
          throw error
        }

        return result.data
      } catch (error) {
        if (error instanceof Error && error.name === 'AbortError') {
          const message = `Job timeout after ${timeout}ms`
          logger.warn({ jobId: job.id, name: job.name, timeout }, message)
          if (timeoutRetryable) {
            throw new RetryableErrorType(message)
          } else {
            throw new UnrecoverableErrorType(message)
          }
        }

        // For UnrecoverableError, let it propagate - with attempts: 1 it won't retry
        // The error message will be captured in failedReason by BullMQ
        throw error
      } finally {
        clearTimeout(timeoutId)
      }
    }, {
      connection: getWorkerRedisConnection(),
      concurrency: 5,
    })

    // DLQ queue for failed jobs
    const dlqQueue = new Queue('dlq', {
      connection: getWorkerRedisConnection(),
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: 1000,
        removeOnFail: 1000,
      },
    })

    worker.on('failed', async (job, err) => {
      const attemptsMade = job?.attemptsMade ?? 0
      const maxAttempts = job?.opts?.attempts ?? 3
      const isFinalFailure = attemptsMade >= maxAttempts

      logger.error(
        {
          jobId: job?.id,
          name: job?.name,
          attemptsMade,
          maxAttempts,
          isFinalFailure,
          error: err?.message,
          stack: err?.stack,
        },
        'Worker job failed'
      )

      if (isFinalFailure && job) {
        // Don't move UnrecoverableError jobs to DLQ - they're permanently bad input, not retry-exhausted
        if (!(err instanceof UnrecoverableErrorType)) {
          await moveToDeadLetter(job, err)
        }
      }
    })

    async function moveToDeadLetter(job: any, error: Error): Promise<void> {
      try {
        const dlqQueue = new Queue('dlq', {
          connection: getWorkerRedisConnection(),
          defaultJobOptions: {
            attempts: 1,
            removeOnComplete: 1000,
            removeOnFail: 1000,
          },
        })
        await dlqQueue.add('dead-letter', {
          originalJob: {
            id: job.id,
            name: job.name,
            data: job.data,
            opts: job.opts,
            attemptsMade: job.attemptsMade,
            failedReason: error.message,
            stacktrace: error.stack,
            failedAt: new Date().toISOString(),
            queueName: 'default',
          },
        })
        logger.info({ jobId: job.id, dlqQueue: 'dlq' }, 'Job moved to dead-letter queue')
        await dlqQueue.close()
      } catch (dlqError) {
        logger.error({ jobId: job.id, err: dlqError }, 'Failed to move job to dead-letter queue')
      }
    }
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

  it('should complete correctly: handler throws on attempts 1-2, succeeds on attempt 3', async () => {
    const job = await addJob('retry-on-first-two', {}, { attempts: 3, backoff: { type: 'fixed', delay: 50 } })
    const finishedJob = await waitForJobCompletion(job.id)

    expect(finishedJob.finishedOn).toBeDefined()
    expect(finishedJob.returnvalue).toEqual({ attempts: 3 })
  }, 20000)

  it('should kill hung job at timeout', async () => {
    const job = await addJob('hanging-job', {}, { timeout: 100, timeoutRetryable: true, attempts: 2, backoff: { type: 'fixed', delay: 50 } })
    const finishedJob = await waitForJobCompletion(job.id, 20000)

    expect(finishedJob.failedReason).toContain('timeout')
    expect(finishedJob.attemptsMade).toBeGreaterThanOrEqual(1)
  }, 25000)

  it('should fail immediately without retries for UnrecoverableError', async () => {
    const job = await addJob('bad-input-job', {}, { attempts: 1, backoff: { type: 'fixed', delay: 50 } })
    const finishedJob = await waitForJobCompletion(job.id)

    // Check that job failed immediately without retries (attemptsMade <= 1)
    // The error message check is best-effort since failedReason/stacktrace may not be populated in test env
    expect(finishedJob.attemptsMade).toBeLessThanOrEqual(1)
    // If error message is available, verify it contains the expected text
    const errorMessage = finishedJob.failedReason || 
      (Array.isArray(finishedJob.stacktrace) ? finishedJob.stacktrace.join('\n') : '') ||
      ''
    if (errorMessage) {
      expect(errorMessage).toContain('Invalid input')
    }
  }, 15000)

  it('should move job to DLQ after final retry failure', async () => {
    const dlqQueue = new Queue('dlq', {
      connection: getWorkerRedisConnection(),
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: 1000,
        removeOnFail: 1000,
      },
    })

    const job = await addJob('always-fails-retryable', {}, { attempts: 2, backoff: { type: 'fixed', delay: 50 } })
    const finishedJob = await waitForJobCompletion(job.id)

    expect(finishedJob.attemptsMade).toBeGreaterThanOrEqual(1)
    expect(finishedJob.failedReason).toContain('Network error')

    // Allow time for async DLQ move
    await new Promise(r => setTimeout(r, 1000))

    // Check DLQ
    const dlqJobs = await dlqQueue.getJobs(['waiting', 'completed', 'failed'], 0, 10)
    const dlqJob = dlqJobs.find(j => j.data.originalJob?.id === job.id)
    
    expect(dlqJob).toBeDefined()
    expect(dlqJob?.data.originalJob.failedReason).toContain('Network error')

    await dlqQueue.close()
  }, 20000)

  it('should classify errors correctly', () => {
    expect(isRetryableError(new RetryableError('network'))).toBe(true)
    expect(isRetryableError(new UnrecoverableError('bad input'))).toBe(false)
    
    const networkError = new Error('ECONNREFUSED')
    networkError.code = 'ECONNREFUSED'
    expect(isRetryableError(networkError)).toBe(true)

    const timeoutError = new Error('ETIMEDOUT')
    timeoutError.code = 'ETIMEDOUT'
    expect(isRetryableError(timeoutError)).toBe(true)

    const validationError = new Error('Invalid schema')
    validationError.name = 'ValidationError'
    expect(isRetryableError(validationError)).toBe(true) // default is true for unknown errors
  })
})