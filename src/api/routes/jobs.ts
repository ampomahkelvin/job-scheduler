import type { FastifyInstance } from 'fastify'
import { queueService } from '../../services/queue'
import { getJob, listJobs } from '../../jobs/registry'
import { logger } from '../../lib/logger'
import { AppError } from '../errorHandler'
import { serializeJob, serializeJobs, serializeJobTypes, serializeJobForIdempotent } from '../serializers/job'
import { createHash } from 'crypto'
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

    // Handle idempotency key - check header first, then body options
    const idempotencyKey = req.headers['idempotency-key'] as string || options?.idempotencyKey
    let customJobId = options?.jobId

    // Generate jobId from idempotency key if provided
    if (idempotencyKey && !customJobId) {
      customJobId = createHash('sha256').update(`idem:${idempotencyKey}`).digest('hex').slice(0, 32)
    }

    // Check for existing job with same idempotency key
    if (customJobId) {
      const existingJob = await queueService.getJob(customJobId)
      if (existingJob) {
        // Verify payload matches for idempotency key
        if (idempotencyKey) {
          const existingData = existingJob.data as Record<string, unknown>
          if (JSON.stringify(existingData) !== JSON.stringify(data)) {
            throw new AppError(409, 'IdempotencyKeyConflict', 'Idempotency key already used with different payload')
          }
        }
        logger.info({ jobId: customJobId, name, idempotent: true }, 'Returning existing job (idempotent)')
        return reply.status(200).send(serializeJobForIdempotent(existingJob, true))
      }
    }

    // Check for existing job with same idempotency key
    if (customJobId) {
      const existingJob = await queueService.getJob(customJobId)
      if (existingJob) {
        // Verify payload matches for idempotency key
        if (idempotencyKey) {
          const existingData = existingJob.data as Record<string, unknown>
          if (JSON.stringify(existingData) !== JSON.stringify(data)) {
            throw new AppError(409, 'IdempotencyKeyConflict', 'Idempotency key already used with different payload')
          }
        }
        logger.info({ jobId: customJobId, name, idempotent: true }, 'Returning existing job (idempotent)')
        return reply.status(200).send(serializeJobForIdempotent(existingJob, true))
      }
    }

    let job
    try {
      job = await queueService.addJob(name, data, {
        ...jobDef.defaultOptions,
        ...options,
        jobId: customJobId,
      })
    } catch (error: any) {
      // Handle race condition: another request created the same job ID concurrently
      if (error.message?.includes('Job with this id already exists') || error.message?.includes('duplicate job')) {
        const existingJob = await queueService.getJob(customJobId!)
        if (existingJob) {
          logger.info({ jobId: customJobId, name, idempotent: true }, 'Race condition: returning existing job')
          return reply.status(200).send(serializeJobForIdempotent(existingJob, true))
        }
      }
      throw error
    }

    logger.info({ jobId: job.id, name, idempotent: !!idempotencyKey }, 'Job enqueued')
    return reply.status(201).send({
      ...serializeJob(job),
      idempotent: !!idempotencyKey,
      timestamp: new Date().toISOString(),
    })
  })

  app.get<{ Querystring: JobQuery }>('/', async (req, reply) => {
    const { status, start = 0, count = 50 } = req.query
    const end = start + count - 1

    const counts = await queueService.getJobCounts()
    const jobs = await queueService.getJobs(
      status ? [status] : ['waiting', 'active', 'completed', 'failed', 'delayed'],
      start,
      end
    )

    return reply.send(serializeJobs(jobs, counts))
  })

  app.get<{ Params: JobIdParam }>('/:id', async (req, reply) => {
    const { id } = req.params
    const job = await queueService.getJob(id)

    if (!job) {
      throw new AppError(404, 'JobNotFound', `Job '${id}' not found`)
    }

    return reply.send(serializeJob(job))
  })

  app.delete<{ Params: JobIdParam; Querystring: { force?: string } }>('/:id', async (req, reply) => {
    const { id } = req.params
    const force = req.query.force === 'true'
    const job = await queueService.getJob(id)

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
    const job = await queueService.getJob(id)

    if (!job) {
      throw new AppError(404, 'JobNotFound', `Job '${id}' not found`)
    }

    await job.retry()
    logger.info({ jobId: id }, 'Job retried')
    return reply.send({ id: job.id, message: 'Job requeued for retry' })
  })

  app.post('/pause', async (_req, reply) => {
    await queueService.pause()
    return reply.send({ message: 'Queue paused' })
  })

  app.post('/resume', async (_req, reply) => {
    await queueService.resume()
    return reply.send({ message: 'Queue resumed' })
  })

  app.get('/types', async (_req, reply) => {
    const jobs = listJobs()
    return reply.send(serializeJobTypes(jobs))
  })
}

export default jobsRoutes