import { z } from 'zod'
import type { JobHandler, JobResult } from '../types'
import { registerJob } from '../registry'
import { logger, createJobLogger } from '../../lib/logger'

// Private IP ranges to block for SSRF protection
const PRIVATE_IP_RANGES = [
  /^10\./,                    // 10.0.0.0/8
  /^172\.(1[6-9]|2[0-9]|3[0-1])\./,  // 172.16.0.0/12
  /^192\.168\./,              // 192.168.0.0/16
  /^127\./,                   // 127.0.0.0/8 (localhost)
  /^169\.254\./,              // 169.254.0.0/16 (link-local)
  /^::1$/,                    // IPv6 localhost
  /^fe80::/,                  // IPv6 link-local
  /^fc00:/,                   // IPv6 unique local
  /^fd00:/,                   // IPv6 unique local
]

function isPrivateIp(hostname: string): boolean {
  // Check if hostname is an IP address
  const ipv4Regex = /^(\d{1,3}\.){3}\d{1,3}$/
  const ipv6Regex = /^\[?([0-9a-fA-F:]+)\]?$/
  
  if (ipv4Regex.test(hostname)) {
    return PRIVATE_IP_RANGES.some(regex => regex.test(hostname))
  }
  
  if (ipv6Regex.test(hostname)) {
    // For IPv6, extract the address part (remove brackets if present)
    const addr = hostname.replace(/^\[|\]$/g, '')
    return PRIVATE_IP_RANGES.some(regex => regex.test(addr))
  }
  
  return false
}

async function resolveAndValidateUrl(url: string): Promise<void> {
  const parsed = new URL(url)
  const hostname = parsed.hostname
  
  // Check if it's an IP address
  if (isPrivateIp(hostname)) {
    throw new Error('SSRF protection: Access to private IP addresses is not allowed')
  }
  
  // Try to resolve the hostname to check for private IPs
  try {
    const { default: dns } = await import('dns/promises')
    const addresses = await dns.lookup(hostname, { all: true })
    for (const addr of addresses) {
      if (isPrivateIp(addr.address)) {
        throw new Error('SSRF protection: Hostname resolves to private IP address')
      }
    }
  } catch (error) {
    if (error instanceof Error && error.message.includes('SSRF')) {
      throw error
    }
    // If DNS lookup fails, we'll let the fetch handle it
    // but log a warning
    console.warn(`DNS lookup failed for ${hostname}:`, error)
  }
}

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

  // SSRF protection: validate URL before making request
  await resolveAndValidateUrl(url)

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