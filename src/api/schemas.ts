import { z } from 'zod'
import { CronExpressionParser } from 'cron-parser'

export const cronPatternSchema = z.string().refine(
  (val) => {
    try {
      CronExpressionParser.parse(val)
      return true
    } catch {
      return false
    }
  },
  { message: 'Invalid cron expression' }
)

export const createJobSchema = z.object({
  name: z.string().min(1),
  data: z.record(z.unknown()),
  options: z
    .object({
      priority: z.number().int().optional(),
      delay: z.number().int().nonnegative().optional(),
      attempts: z.number().int().positive().max(100).optional(),
      backoff: z
        .object({
          type: z.enum(['fixed', 'exponential']),
          delay: z.number().int().positive(),
        })
        .optional(),
      removeOnComplete: z.union([z.boolean(), z.number().int().positive()]).optional(),
      removeOnFail: z.union([z.boolean(), z.number().int().positive()]).optional(),
      repeat: z
        .object({
          pattern: z.string().optional(),
          every: z.number().int().positive().optional(),
          limit: z.number().int().positive().optional(),
          tz: z.string().optional(),
        })
        .optional(),
    })
    .optional(),
})

export const jobIdParamSchema = z.object({
  id: z.string().min(1),
})

export const jobQuerySchema = z.object({
  status: z.enum(['waiting', 'active', 'completed', 'failed', 'delayed', 'paused']).optional(),
  start: z.coerce.number().int().nonnegative().optional(),
  end: z.coerce.number().int().nonnegative().optional(),
  count: z.coerce.number().int().positive().max(1000).default(50),
})

export const scheduleSchema = z.object({
  name: z.string().min(1),
  data: z.record(z.unknown()),
  pattern: cronPatternSchema.optional(),
  every: z.number().int().positive().optional(),
  tz: z.string().optional(),
  limit: z.number().int().positive().optional(),
  options: z
    .object({
      priority: z.number().int().optional(),
      attempts: z.number().int().positive().max(100).optional(),
      backoff: z
        .object({
          type: z.enum(['fixed', 'exponential']),
          delay: z.number().int().positive(),
        })
        .optional(),
    })
    .optional(),
}).refine(
  (data) => data.pattern || data.every,
  { message: 'Either pattern (cron) or every (ms) must be provided', path: ['pattern'] }
)

export const scheduleIdParamSchema = z.object({
  id: z.string().min(1),
})

export const healthResponseSchema = z.object({
  status: z.enum(['ok', 'degraded', 'down']),
  timestamp: z.string().datetime(),
  uptime: z.number(),
  redis: z.object({
    connected: z.boolean(),
    latency: z.number().optional(),
  }),
  queue: z.object({
    waiting: z.number(),
    active: z.number(),
    completed: z.number(),
    failed: z.number(),
    delayed: z.number(),
  }).optional(),
})

export type CreateJobInput = z.infer<typeof createJobSchema>
export type JobIdParam = z.infer<typeof jobIdParamSchema>
export type JobQuery = z.infer<typeof jobQuerySchema>
export type ScheduleInput = z.infer<typeof scheduleSchema>
export type ScheduleIdParam = z.infer<typeof scheduleIdParamSchema>