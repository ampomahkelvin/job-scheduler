import type { FastifyInstance } from 'fastify'
import { queue, upsertJobScheduler } from '../../queues'
import { logger } from '../../lib/logger'
import { AppError } from '../errorHandler'
import type { RepeatOptions } from 'bullmq'
import type {
  ScheduleInput,
  ScheduleIdParam,
} from '../schemas'
import { createHash } from 'crypto'

interface RepeatableJob {
  id: string
  key: string
  name: string
  data: Record<string, unknown>
  opts: Record<string, unknown>
  pattern?: string
  every?: number
  tz?: string
  limit?: number
  nextRun?: number
  repeat?: RepeatOptions
}

interface UpsertedJob {
  id: string
  key: string
  name: string
  data: Record<string, unknown>
  opts: Record<string, unknown>
  repeat?: RepeatOptions
  nextRun?: number
}

function generateSchedulerId(name: string, repeat: Record<string, unknown>): string {
  const repeatStr = JSON.stringify(repeat, Object.keys(repeat).sort())
  const hash = createHash('sha256').update(`${name}:${repeatStr}`).digest('hex').slice(0, 16)
  return `${name}:${hash}`
}

async function schedulesRoutes(app: FastifyInstance): Promise<void> {
  app.post<{ Body: ScheduleInput }>('/', async (req, reply) => {
    const { name, data, pattern, every, tz, limit, options } = req.body

    const repeat: Record<string, unknown> = {}
    if (pattern) repeat.pattern = pattern
    if (every) repeat.every = every
    if (tz) repeat.tz = tz
    if (limit) repeat.limit = limit

    const schedulerId = generateSchedulerId(name, repeat)

    const { key: repeatKey, ...repeatOpts } = repeat as RepeatOptions & { key?: string }

    const repeatableJob = await upsertJobScheduler(schedulerId, repeatOpts, data, options) as UpsertedJob

    logger.info({ repeatableJobKey: repeatableJob.key, name, repeat }, 'Schedule upserted')
    return reply.status(201).send({
      id: repeatableJob.id,
      key: repeatableJob.key,
      name: repeatableJob.name,
      data: repeatableJob.data,
      opts: repeatableJob.opts,
      pattern: repeatableJob.repeat?.pattern,
      every: repeatableJob.repeat?.every,
      tz: repeatableJob.repeat?.tz,
      limit: repeatableJob.repeat?.limit,
      nextRun: repeatableJob.nextRun ? new Date(repeatableJob.nextRun).toISOString() : undefined,
      timestamp: new Date().toISOString(),
    })
  })

  app.get('/', async (_req, reply) => {
    const repeatableJobs = (await queue.getRepeatableJobs()) as unknown as RepeatableJob[]

    return reply.send(
      repeatableJobs.map((job) => ({
        id: job.id,
        key: job.key,
        name: job.name,
        data: job.data,
        opts: job.opts,
        pattern: job.pattern,
        every: job.every,
        tz: job.tz,
        limit: job.limit,
        nextRun: job.nextRun ? new Date(job.nextRun).toISOString() : undefined,
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
      key: job.key,
      name: job.name,
      data: job.data,
      opts: job.opts,
      pattern: job.pattern,
      every: job.every,
      tz: job.tz,
      limit: job.limit,
      nextRun: job.nextRun ? new Date(job.nextRun).toISOString() : undefined,
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