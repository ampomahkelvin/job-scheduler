import type { FastifyInstance } from 'fastify'
import { queue } from '../../queues'
import { logger } from '../../lib/logger'
import { AppError } from '../errorHandler'
import type {
  ScheduleInput,
  ScheduleIdParam,
} from '../schemas'

interface RepeatableJob {
  id: string
  name: string
  data: Record<string, unknown>
  opts: Record<string, unknown>
  pattern?: string
  every?: number
  tz?: string
  limit?: number
  nextRun?: number
  key: string
}

async function schedulesRoutes(app: FastifyInstance): Promise<void> {
  app.post<{ Body: ScheduleInput }>('/', async (req, reply) => {
    const { name, data, pattern, every, tz, limit, options } = req.body

    if (!pattern && !every) {
      throw new AppError(400, 'ValidationError', 'Either pattern (cron) or every (ms) must be provided')
    }

    const repeat: Record<string, unknown> = {}
    if (pattern) repeat.pattern = pattern
    if (every) repeat.every = every
    if (tz) repeat.tz = tz
    if (limit) repeat.limit = limit

    const job = await queue.add(name, data, {
      repeat,
      ...options,
    })

    logger.info({ jobId: job.id, name, repeat }, 'Scheduled job created')
    return reply.status(201).send({
      id: job.id,
      name: job.name,
      data: job.data,
      opts: job.opts,
      repeat: job.opts?.repeat,
      timestamp: new Date().toISOString(),
    })
  })

  app.get('/', async (_req, reply) => {
    const repeatableJobs = (await queue.getRepeatableJobs()) as unknown as RepeatableJob[]

    return reply.send(
      repeatableJobs.map((job) => ({
        id: job.id,
        name: job.name,
        data: job.data,
        opts: job.opts,
        pattern: job.pattern,
        every: job.every,
        tz: job.tz,
        limit: job.limit,
        nextRun: job.nextRun,
      }))
    )
  })

  app.get<{ Params: ScheduleIdParam }>('/:id', async (req, reply) => {
    const { id } = req.params
    const repeatableJobs = (await queue.getRepeatableJobs()) as unknown as RepeatableJob[]
    const job = repeatableJobs.find((j) => j.id === id || j.key === id)

    if (!job) {
      throw new AppError(404, 'ScheduleNotFound', `Schedule '${id}' not found`)
    }

    return reply.send({
      id: job.id,
      name: job.name,
      data: job.data,
      opts: job.opts,
      pattern: job.pattern,
      every: job.every,
      tz: job.tz,
      limit: job.limit,
      nextRun: job.nextRun,
    })
  })

  app.delete<{ Params: ScheduleIdParam }>('/:id', async (req, reply) => {
    const { id } = req.params
    const repeatableJobs = (await queue.getRepeatableJobs()) as unknown as RepeatableJob[]
    const job = repeatableJobs.find((j) => j.id === id || j.key === id)

    if (!job) {
      throw new AppError(404, 'ScheduleNotFound', `Schedule '${id}' not found`)
    }

    await queue.removeRepeatableByKey(job.key)
    logger.info({ scheduleId: id }, 'Schedule removed')
    return reply.status(204).send()
  })
}

export default schedulesRoutes