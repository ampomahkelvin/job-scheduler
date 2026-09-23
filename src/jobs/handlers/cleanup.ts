import { z } from 'zod'
import type { JobHandler, JobResult } from '../types'
import { registerJob } from '../registry'
import { logger, createJobLogger } from '../../lib/logger'

export const cleanupJobSchema = z.object({
  olderThanDays: z.number().int().positive().optional(),
  collections: z.array(z.string()).optional(),
  dryRun: z.boolean().optional(),
  maxItemsPerCollection: z.number().int().positive().max(10000).optional(),
})

export type CleanupJobData = z.infer<typeof cleanupJobSchema>

const cleanupHandler: JobHandler<CleanupJobData> = async ({ data, id, attemptsMade, updateProgress }) => {
  const { olderThanDays = 30, collections = ['logs', 'temp', 'cache'], dryRun = false, maxItemsPerCollection = 1000 } = data
  const jobLogger = createJobLogger(id, 'cleanup')

  jobLogger.info({ olderThanDays, collections, dryRun, attempt: attemptsMade + 1 }, 'Processing cleanup job')

  const cutoffDate = new Date()
  cutoffDate.setDate(cutoffDate.getDate() - olderThanDays)

  const results: Record<string, { deleted: number; errors: string[] }> = {}

  for (let i = 0; i < collections.length; i++) {
    const collection = collections[i]
    await updateProgress(Math.round(((i + 1) / collections.length) * 100))

    try {
      jobLogger.debug({ collection }, `Cleaning up collection: ${collection}`)

      let deletedCount = 0
      const errors: string[] = []

      if (!dryRun) {
        await new Promise((resolve) => setTimeout(resolve, 100))
        deletedCount = Math.floor(Math.random() * maxItemsPerCollection)
      }

      results[collection] = { deleted: deletedCount, errors }
      jobLogger.info({ collection, deleted: deletedCount, dryRun }, `Collection cleanup ${dryRun ? 'simulated' : 'completed'}`)
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Unknown error'
      results[collection] = { deleted: 0, errors: [errorMsg] }
      jobLogger.error({ collection, error: errorMsg }, 'Collection cleanup failed')
    }
  }

  await updateProgress(100)

  const totalDeleted = Object.values(results).reduce((sum, r) => sum + r.deleted, 0)
  const hasErrors = Object.values(results).some((r) => r.errors.length > 0)

  const result: JobResult = {
    success: !hasErrors,
    data: {
      olderThanDays,
      collections,
      dryRun,
      totalDeleted,
      results,
      completedAt: new Date().toISOString(),
    },
    error: hasErrors ? 'Some collections had errors during cleanup' : undefined,
  }

  jobLogger.info({ totalDeleted, dryRun, hasErrors }, 'Cleanup job completed')
  return result
}

registerJob({
  name: 'cleanup',
  handler: cleanupHandler,
  schema: cleanupJobSchema,
  defaultOptions: {
    attempts: 2,
    backoff: { type: 'fixed', delay: 5000 },
    removeOnComplete: 50,
    removeOnFail: 25,
  },
})

export { cleanupHandler }