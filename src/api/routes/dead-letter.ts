import type { FastifyInstance } from 'fastify'
import { Queue } from 'bullmq'
import { logger } from '../../lib/logger'
import { AppError } from '../errorHandler'
import { getWorkerRedisConnection } from '../../lib/redis'
import type { JobQuery } from '../schemas'

const DLQ_NAME = 'dlq'

const dlqQueue = new Queue(DLQ_NAME, {
  connection: getWorkerRedisConnection(),
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: 1000,
    removeOnFail: 1000,
  },
})

async function deadLetterRoutes(app: FastifyInstance): Promise<void> {
  app.get<{ Querystring: JobQuery }>('/', async (req, reply) => {
    const { start = 0, count = 50 } = req.query
    const end = start + count - 1

    const jobs = await dlqQueue.getJobs(['waiting', 'active', 'completed', 'failed'], start, end)

    return reply.send({
      jobs: jobs.map((job) => ({
        id: job.id,
        name: job.name,
        data: job.data,
        opts: job.opts,
        progress: job.progress,
        attemptsMade: job.attemptsMade,
        timestamp: job.timestamp ? new Date(job.timestamp).toISOString() : undefined,
        processedOn: job.processedOn ? new Date(job.processedOn).toISOString() : undefined,
        finishedOn: job.finishedOn ? new Date(job.finishedOn).toISOString() : undefined,
        failedReason: job.failedReason,
        returnvalue: job.returnvalue,
        stacktrace: job.stacktrace,
      })),
    })
  })

  app.get<{ Params: { id: string } }>('/:id', async (req, reply) => {
    const { id } = req.params
    const job = await dlqQueue.getJob(id)

    if (!job) {
      throw new AppError(404, 'DeadLetterJobNotFound', `Dead letter job '${id}' not found`)
    }

    return reply.send({
      id: job.id,
      name: job.name,
      data: job.data,
      opts: job.opts,
      progress: job.progress,
      attemptsMade: job.attemptsMade,
      timestamp: job.timestamp,
      processedOn: job.processedOn,
      finishedOn: job.finishedOn,
      failedReason: job.failedReason,
      returnvalue: job.returnvalue,
      stacktrace: job.stacktrace,
    })
  })

  app.post<{ Params: { id: string } }>('/:id/replay', async (req, reply) => {
    const { id } = req.params
    const job = await dlqQueue.getJob(id)

    if (!job) {
      throw new AppError(404, 'DeadLetterJobNotFound', `Dead letter job '${id}' not found`)
    }

    const originalJob = job.data as {
      originalJob: {
        id: string
        name: string
        data: Record<string, unknown>
        opts: Record<string, unknown>
        attemptsMade: number
        failedReason: string
        stacktrace: string | undefined
        failedAt: string
        queueName: string
      }
    }

    const mainQueue = new Queue('default', {
      connection: getWorkerRedisConnection(),
    })

    const replayedJob = await mainQueue.add(originalJob.originalJob.name, originalJob.originalJob.data, {
      ...originalJob.originalJob.opts,
      attempts: 3,
    })

    logger.info({ originalJobId: id, replayedJobId: replayedJob.id, name: originalJob.originalJob.name }, 'Job replayed from dead-letter queue')

    return reply.send({
      id: replayedJob.id,
      name: replayedJob.name,
      message: 'Job replayed successfully',
    })
  })

  app.delete<{ Params: { id: string } }>('/:id', async (req, reply) => {
    const { id } = req.params
    const job = await dlqQueue.getJob(id)

    if (!job) {
      throw new AppError(404, 'DeadLetterJobNotFound', `Dead letter job '${id}' not found`)
    }

    await job.remove()
    logger.info({ deadLetterJobId: id }, 'Dead letter job permanently removed')
    return reply.status(204).send()
  })

  app.post('/pause', async (_req, reply) => {
    await dlqQueue.pause()
    return reply.send({ message: 'Dead-letter queue paused' })
  })

  app.post('/resume', async (_req, reply) => {
    await dlqQueue.resume()
    return reply.send({ message: 'Dead-letter queue resumed' })
  })
}

export default deadLetterRoutes