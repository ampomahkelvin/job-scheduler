import type { Job } from 'bullmq'
import { queueService } from './services/queue'
import { getJobHandler } from './jobs/registry'
import { logger } from './lib/logger'
import { isRetryableError, UnrecoverableError, RetryableError } from './jobs/types'
import './jobs/handlers/echo'
import './jobs/handlers/webhook'
import './jobs/handlers/cleanup'
import './jobs/handlers/report'

const DLQ_NAME = 'dlq'
const DEFAULT_TIMEOUT = 30000

const dlqQueue = queueService.getQueue().constructor as any
const dlq = new dlqQueue('dlq', {
  connection: queueService.getRedisConnection(),
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: 1000,
    removeOnFail: 1000,
  },
})

interface JobWithTimeout extends Job {
  opts: Job['opts'] & {
    timeout?: number
    timeoutRetryable?: boolean
  }
}

const worker = queueService.createWorker('default', async (job: Job) => {
  const handler = getJobHandler(job.name)
  if (!handler) {
    logger.warn({ jobName: job.name, jobId: job.id }, 'No handler for job')
    throw new UnrecoverableError(`No handler registered for job: ${job.name}`)
  }

  const jobWithTimeout = job as JobWithTimeout
  const timeout = jobWithTimeout.opts?.timeout || DEFAULT_TIMEOUT
  const timeoutRetryable = jobWithTimeout.opts?.timeoutRetryable !== false

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
      const error = new RetryableError(result.error || 'Job failed')
      throw error
    }
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      const message = `Job timeout after ${timeout}ms`
      logger.warn({ jobId: job.id, name: job.name, timeout }, message)
      if (timeoutRetryable) {
        throw new RetryableError(message)
      } else {
        throw new UnrecoverableError(message)
      }
    }

    // For UnrecoverableError, let it propagate - with attempts: 1 it won't retry
    // The error message will be captured in failedReason by BullMQ
    throw error
  } finally {
    clearTimeout(timeoutId)
  }
}, {
  connection: queueService.getRedisConnection(),
  concurrency: 5,
  limiter: {
    max: 100,
    duration: 1000,
  },
})

worker.on('completed', (job: Job) => {
  logger.info({ jobId: job.id, name: job.name, attemptsMade: job.attemptsMade }, 'Worker completed job')
})

worker.on('failed', async (job: Job | undefined, err: Error) => {
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
    await moveToDeadLetter(job, err)
  }
})

worker.on('error', (err: Error) => {
  logger.error({ err }, 'Worker error')
})

worker.on('stalled', (jobId: string) => {
  logger.warn({ jobId }, 'Job stalled')
})

async function moveToDeadLetter(job: Job, error: Error): Promise<void> {
  try {
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
    logger.info({ jobId: job.id, dlqQueue: DLQ_NAME }, 'Job moved to dead-letter queue')
  } catch (dlqError) {
    logger.error({ jobId: job.id, err: dlqError }, 'Failed to move job to dead-letter queue')
  }
}

async function shutdown(): Promise<void> {
  logger.info('Shutting down worker...')
  await queueService.close()
  await (dlqQueue as any).close()
  logger.info('Worker and DLQ closed')
  process.exit(0)
}

const handleSignal = (): void => {
  void shutdown()
}

process.on('SIGTERM', handleSignal)
process.on('SIGINT', handleSignal)

logger.info('Worker started with dead-letter queue support')