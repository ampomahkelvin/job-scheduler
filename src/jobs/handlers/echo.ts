import { z } from 'zod'
import type { JobHandler, JobResult } from '../types'
import { registerJob } from '../registry'
import { logger } from '../../lib/logger'

export const echoJobSchema = z.object({
  message: z.string().min(1),
  repeat: z.number().int().positive().optional(),
  delay: z.number().int().nonnegative().optional(),
})

export type EchoJobData = z.infer<typeof echoJobSchema>

const echoHandler: JobHandler<EchoJobData> = async ({ data, id, attemptsMade, updateProgress }) => {
  const { message, repeat = 1, delay = 0 } = data

  logger.info({ jobId: id, message, attempt: attemptsMade + 1 }, 'Processing echo job')

  for (let i = 0; i < repeat; i++) {
    if (delay > 0) {
      await new Promise((resolve) => setTimeout(resolve, delay))
    }
    await updateProgress(Math.round(((i + 1) / repeat) * 100))
    logger.debug({ jobId: id, iteration: i + 1, total: repeat }, 'Echo iteration')
  }

  const result: JobResult = {
    success: true,
    data: { message, echoed: repeat, processedAt: new Date().toISOString() },
  }

  logger.info({ jobId: id, result }, 'Echo job completed')
  return result
}

registerJob({
  name: 'echo',
  handler: echoHandler,
  schema: echoJobSchema,
  defaultOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
    removeOnComplete: 100,
    removeOnFail: 50,
  },
})

export { echoHandler }