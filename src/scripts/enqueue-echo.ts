import { queueService } from '../services/queue'
import { logger } from '../lib/logger'

async function enqueueEcho(): Promise<void> {
  const message = process.argv[2] || 'Hello from job-scheduler!'
  const repeat = parseInt(process.argv[3] || '3', 10)
  const delay = parseInt(process.argv[4] || '100', 10)

  logger.info({ message, repeat, delay }, 'Enqueueing echo job')

  const job = await queueService.addJob('echo', { message, repeat, delay })

  logger.info({ jobId: job.id }, 'Job enqueued successfully')
  process.exit(0)
}

enqueueEcho().catch((err) => {
  logger.error({ err }, 'Failed to enqueue job')
  process.exit(1)
})