import type { Job } from 'bullmq'
import { createWorker } from './queues'
import { getJobHandler } from './jobs/registry'
import { logger } from './lib/logger'
import { getWorkerRedisConnection } from './lib/redis'
import './jobs/handlers/echo'

const worker = createWorker('default', async (job: Job) => {
  const handler = getJobHandler(job.name)
  if (!handler) {
    logger.warn({ jobName: job.name, jobId: job.id }, 'No handler for job')
    throw new Error(`No handler registered for job: ${job.name}`)
  }

  const updateProgress = async (progress: number): Promise<void> => {
    await job.updateProgress(progress)
  }

  const result = await handler({
    data: job.data as never,
    id: job.id || 'unknown',
    attemptsMade: job.attemptsMade,
    updateProgress,
  })

  if (!result.success) {
    throw new Error(result.error || 'Job failed')
  }
}, {
  connection: getWorkerRedisConnection(),
  concurrency: 5,
  limiter: {
    max: 100,
    duration: 1000,
  },
})

worker.on('completed', (job) => {
  logger.info({ jobId: job.id, name: job.name }, 'Worker completed job')
})

worker.on('failed', (job, err) => {
  logger.error({ jobId: job?.id, name: job?.name, err }, 'Worker job failed')
})

worker.on('error', (err) => {
  logger.error({ err }, 'Worker error')
})

worker.on('stalled', (jobId) => {
  logger.warn({ jobId }, 'Job stalled')
})

async function shutdown(): Promise<void> {
  logger.info('Shutting down worker...')
  await worker.close()
  logger.info('Worker closed')
  process.exit(0)
}

const handleSignal = (): void => {
  void shutdown()
}

process.on('SIGTERM', handleSignal)
process.on('SIGINT', handleSignal)

logger.info('Worker started')