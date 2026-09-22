import type { FastifyInstance } from 'fastify'
import { queue, addJob, getJobCounts, pauseQueue, resumeQueue } from '../../queues'
import { getJob, listJobs } from '../../jobs/registry'
import { logger } from '../../lib/logger'
import { AppError } from '../errorHandler'
import type {
  CreateJobInput,
  JobIdParam,
  JobQuery,
} from '../schemas'

async function jobsRoutes(app: FastifyInstance): Promise<void> {
  app.post<{ Body: CreateJobInput }>('/', async (req, reply) => {
    const { name, data, options } = req.body

    const jobDef = getJob(name)
    if (!jobDef) {
      throw new AppError(422, 'JobTypeNotFound', `Job type '${name}' not found`)
    }

    if (jobDef.schema) {
      const result = jobDef.schema.safeParse(data)
      if (!result.success) {
        throw new AppError(400, 'ValidationError', 'Invalid job data', result.error.errors)
      }
    }

    const job = await addJob(name, data, {
      ...jobDef.defaultOptions,
      ...options,
    })

    logger.info({ jobId: job.id, name }, 'Job enqueued')
    return reply.status(201).send({
      id: job.id,
      name: job.name,
      data: job.data,
      opts: job.opts,
      timestamp: new Date().toISOString(),
    })
  })

  app.get<{ Querystring: JobQuery }>('/', async (req, reply) => {
    const { status, start = 0, count = 50 } = req.query
    const end = start + count - 1

    const counts = await getJobCounts()
    const jobs = await queue.getJobs(
      status ? [status] : ['waiting', 'active', 'completed', 'failed', 'delayed'],
      start,
      end
    )

    return reply.send({
      counts,
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
        failedAt: job.failedReason ? (job.finishedOn ? new Date(job.finishedOn).toISOString() : undefined) : undefined,
        returnvalue: job.returnvalue,
      })),
    })
  })

  app.get<{ Params: JobIdParam }>('/:id', async (req, reply) => {
    const { id } = req.params
    const job = await queue.getJob(id)

    if (!job) {
      throw new AppError(404, 'JobNotFound', `Job '${id}' not found`)
    }

    const finishedOn = job.finishedOn ? new Date(job.finishedOn).toISOString() : undefined
    const processedOn = job.processedOn ? new Date(job.processedOn).toISOString() : undefined
    const timestamp = job.timestamp ? new Date(job.timestamp).toISOString() : undefined

    return reply.send({
      id: job.id,
      name: job.name,
      data: job.data,
      opts: job.opts,
      progress: job.progress,
      attemptsMade: job.attemptsMade,
      timestamp,
      processedOn,
      finishedOn,
      failedReason: job.failedReason,
      failedAt: job.failedReason ? finishedOn : undefined,
      returnvalue: job.returnvalue,
      stacktrace: job.stacktrace,
    })
  })

  app.delete<{ Params: JobIdParam; Querystring: { force?: string } }>('/:id', async (req, reply) => {
    const { id } = req.params
    const force = req.query.force === 'true'
    const job = await queue.getJob(id)

    if (!job) {
      throw new AppError(404, 'JobNotFound', `Job '${id}' not found`)
    }

    if (force) {
      await job.discard()
      logger.info({ jobId: id, force: true }, 'Job discarded (forced)')
    } else {
      await job.remove()
      logger.info({ jobId: id, force: false }, 'Job removed')
    }

    return reply.status(204).send()
  })

  app.post<{ Params: JobIdParam }>('/:id/retry', async (req, reply) => {
    const { id } = req.params
    const job = await queue.getJob(id)

    if (!job) {
      throw new AppError(404, 'JobNotFound', `Job '${id}' not found`)
    }

    await job.retry()
    logger.info({ jobId: id }, 'Job retried')
    return reply.send({ id: job.id, message: 'Job requeued for retry' })
  })

  app.post('/pause', async (_req, reply) => {
    await pauseQueue()
    return reply.send({ message: 'Queue paused' })
  })

  app.post('/resume', async (_req, reply) => {
    await resumeQueue()
    return reply.send({ message: 'Queue resumed' })
  })

  app.get('/types', async (_req, reply) => {
    const jobs = listJobs()
    return reply.send(
      jobs.map((j) => ({
        name: j.name,
        schema: j.schema ? 'defined' : 'none',
        defaultOptions: j.defaultOptions,
      }))
    )
  })
}

export default jobsRoutes