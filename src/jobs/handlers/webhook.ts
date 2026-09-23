import { z } from 'zod'
import type { JobHandler, JobResult } from '../types'
import { registerJob } from '../registry'
import { logger, createJobLogger } from '../../lib/logger'

export const webhookJobSchema = z.object({
  url: z.string().url(),
  method: z.enum(['GET', 'POST', 'PUT', 'PATCH', 'DELETE']).optional(),
  headers: z.record(z.string()).optional(),
  body: z.unknown().optional(),
  timeout: z.number().int().positive().max(30000).optional(),
  retryOnFailure: z.boolean().optional(),
})

export type WebhookJobData = z.infer<typeof webhookJobSchema>

const webhookHandler: JobHandler<WebhookJobData> = async ({ data, id, attemptsMade, updateProgress, signal }) => {
  const { url, method = 'POST', headers = {}, body, timeout = 10000, retryOnFailure = false } = data

  const jobLogger = createJobLogger(id, 'webhook')
  jobLogger.info({ url, method, attempt: attemptsMade + 1 }, 'Processing webhook job')

  await updateProgress(10)

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeout)

  // Handle external abort signal (from worker timeout)
  const abortHandler = () => controller.abort()
  signal?.addEventListener('abort', abortHandler, { once: true })

  try {
    const response = await fetch(url, {
      method,
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'job-scheduler-webhook/1.0',
        ...headers,
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    })

    await updateProgress(50)

    const responseBody = await response.text().catch(() => '')

    await updateProgress(90)

    if (!response.ok && retryOnFailure) {
      const error = new Error(`Webhook failed with status ${response.status}: ${responseBody}`)
      jobLogger.warn({ status: response.status, url }, 'Webhook failed, will retry')
      throw error
    }

    const result: JobResult = {
      success: response.ok,
      data: {
        status: response.status,
        statusText: response.statusText,
        body: responseBody,
        url,
        method,
      },
      error: response.ok ? undefined : `HTTP ${response.status}: ${responseBody}`,
    }

    await updateProgress(100)
    jobLogger.info({ status: response.status, url }, 'Webhook job completed')
    return result
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      const result: JobResult = {
        success: false,
        error: `Request timeout after ${timeout}ms`,
      }
      jobLogger.error({ url, timeout }, 'Webhook request timeout')
      return result
    }
    throw error
  } finally {
    clearTimeout(timeoutId)
    signal?.removeEventListener('abort', abortHandler)
  }
}

registerJob({
  name: 'webhook',
  handler: webhookHandler,
  schema: webhookJobSchema,
  defaultOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: 100,
    removeOnFail: 50,
    rateLimit: {
      max: 10,
      duration: 1000,
    },
  },
})

export { webhookHandler }