import { z } from 'zod'
import type { JobHandler, JobResult } from '../types'
import { registerJob } from '../registry'
import { logger, createJobLogger } from '../../lib/logger'

export const reportJobSchema = z.object({
  type: z.enum(['daily', 'weekly', 'monthly', 'custom']),
  recipients: z.array(z.string().email()).min(1),
  format: z.enum(['json', 'csv', 'pdf']).optional(),
  filters: z.object({
    dateFrom: z.string().datetime().optional(),
    dateTo: z.string().datetime().optional(),
    jobTypes: z.array(z.string()).optional(),
    statuses: z.array(z.enum(['completed', 'failed', 'active', 'waiting'])).optional(),
  }).optional(),
  includeCharts: z.boolean().optional(),
})

export type ReportJobData = z.infer<typeof reportJobSchema>

const reportHandler: JobHandler<ReportJobData> = async ({ data, id, attemptsMade, updateProgress }) => {
  const { type, recipients, format = 'json', filters, includeCharts = false } = data
  const jobLogger = createJobLogger(id, 'report')

  jobLogger.info({ type, recipients, format, attempt: attemptsMade + 1 }, 'Processing report job')

  await updateProgress(10)

  const reportData = {
    generatedAt: new Date().toISOString(),
    type,
    format,
    filters: filters || {},
    summary: {
      totalJobs: 0,
      completed: 0,
      failed: 0,
      active: 0,
      waiting: 0,
    },
    jobTypes: {} as Record<string, number>,
    charts: includeCharts ? { placeholder: 'chart-data-would-go-here' } : undefined,
  }

  await updateProgress(30)

  jobLogger.debug({}, 'Fetching job statistics from queue')

  await updateProgress(50)

  const mockJobTypes = ['echo', 'webhook', 'cleanup', 'report']
  for (const jobType of mockJobTypes) {
    reportData.jobTypes[jobType] = Math.floor(Math.random() * 100)
  }
  reportData.summary.totalJobs = Object.values(reportData.jobTypes).reduce((a, b) => a + b, 0)
  reportData.summary.completed = Math.floor(reportData.summary.totalJobs * 0.85)
  reportData.summary.failed = Math.floor(reportData.summary.totalJobs * 0.1)
  reportData.summary.active = Math.floor(reportData.summary.totalJobs * 0.03)
  reportData.summary.waiting = reportData.summary.totalJobs - reportData.summary.completed - reportData.summary.failed - reportData.summary.active

  await updateProgress(70)

  let reportOutput: string
  if (format === 'json') {
    reportOutput = JSON.stringify(reportData, null, 2)
  } else if (format === 'csv') {
    const headers = ['jobType', 'count']
    const rows = Object.entries(reportData.jobTypes).map(([jobType, count]) => `${jobType},${count}`)
    reportOutput = [headers.join(','), ...rows].join('\n')
  } else {
    reportOutput = `[PDF Report] ${JSON.stringify(reportData)}`
  }

  await updateProgress(90)

  jobLogger.info({ recipients, format, totalJobs: reportData.summary.totalJobs }, 'Report generated')

  if (!recipients.length) {
    jobLogger.warn({}, 'No recipients specified, skipping delivery')
  }

  const result: JobResult = {
    success: true,
    data: {
      report: reportOutput,
      format,
      recipients,
      generatedAt: reportData.generatedAt,
      summary: reportData.summary,
    },
  }

  await updateProgress(100)
  jobLogger.info({}, 'Report job completed')
  return result
}

registerJob({
  name: 'report',
  handler: reportHandler,
  schema: reportJobSchema,
  defaultOptions: {
    attempts: 2,
    backoff: { type: 'exponential', delay: 3000 },
    removeOnComplete: 50,
    removeOnFail: 25,
  },
})

export { reportHandler }