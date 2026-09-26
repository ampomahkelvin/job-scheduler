import type { FastifyInstance } from 'fastify'
import { queueService } from '../../services/queue'
import { logger } from '../../lib/logger'
import { AppError } from '../errorHandler'
import { serializeSchedule, serializeScheduleForUpsert } from '../serializers/schedule'
import { createHash } from 'crypto'
import type { RepeatOptions } from 'bullmq'
import type {
  ScheduleInput,
  ScheduleIdParam,
} from '../schemas'

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
  repeat?: { pattern?: string; every?: number; tz?: string; limit?: number }
}

interface UpsertedJob {
  id: string
  key: string
  name: string
  data: Record<string, unknown>
  opts: Record<string, unknown>
  repeat?: { pattern?: string; every?: number; tz?: string; limit?: number }
  nextRun?: number
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

    const repeatableJob = await queueService.upsertJobScheduler(schedulerId, repeatOpts, data, options) as UpsertedJob

    logger.info({ repeatableJobKey: repeatableJob.key, name, repeat }, 'Schedule upserted')
    return reply.status(201).send(serializeScheduleForUpsert(repeatableJob))
  })

  app.get('/', async (_req, reply) => {
    const repeatableJobs = (await queueService.getRepeatableJobs()) as RepeatableJob[]

    return reply.send(
      repeatableJobs.map((job) => serializeSchedule(job))
    )
  })

  app.get<{ Params: ScheduleIdParam }>('/:id', async (req, reply) => {
    const { id } = req.params
    const repeatableJobs = (await queueService.getRepeatableJobs()) as RepeatableJob[]
    const job = repeatableJobs.find((j) => j.id === id || j.key === id)

    if (!job) {
      throw new AppError(404, 'ScheduleNotFound', `Schedule '${id}' not found`)
    }

    return reply.send(serializeSchedule(job))
  })

  app.delete<{ Params: ScheduleIdParam }>('/:id', async (req, reply) => {
    const { id } = req.params
    const repeatableJobs = (await queueService.getRepeatableJobs()) as RepeatableJob[]
    const job = repeatableJobs.find((j) => j.id === id || j.key === id)

    if (!job) {
      throw new AppError(404, 'ScheduleNotFound', `Schedule '${id}' not found`)
    }

    await queueService.removeRepeatableByKey(job.key)
    logger.info({ scheduleId: id }, 'Schedule removed')
    return reply.status(204).send()
  })
}

function generateSchedulerId(name: string, repeat: Record<string, unknown>): string {
  const repeatStr = JSON.stringify(repeat, Object.keys(repeat).sort())
  const hash = createHash('sha256').update(`${name}:${repeatStr}`).digest('hex').slice(0, 16)
  return `${name}:${hash}`
}

export default schedulesRoutes