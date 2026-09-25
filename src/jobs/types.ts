import type { z } from 'zod'

export interface JobData {
  [key: string]: unknown
}

export interface JobResult {
  success: boolean
  data?: unknown
  error?: string
}

export interface JobMetadata {
  id: string
  name: string
  data: JobData
  opts?: JobOptions
  createdAt: Date
  processedAt?: Date
  finishedAt?: Date
  failedReason?: string
  attemptsMade: number
  progress?: number
}

export interface JobOptions {
  priority?: number
  delay?: number
  attempts?: number
  backoff?: {
    type: 'fixed' | 'exponential'
    delay: number
  }
  removeOnComplete?: boolean | number
  removeOnFail?: boolean | number
  repeat?: RepeatOptions
  timeout?: number
  timeoutRetryable?: boolean
  jobId?: string
  rateLimit?: {
    max: number
    duration: number
  }
}

export interface RepeatOptions {
  pattern?: string
  every?: number
  limit?: number
  tz?: string
}

export interface JobCounts {
  waiting: number
  active: number
  completed: number
  failed: number
  delayed: number
  paused: number
}

export type JobHandler<T extends JobData = JobData> = (
  job: { data: T; id: string; attemptsMade: number; updateProgress: (progress: number) => Promise<void>; signal?: AbortSignal }
) => Promise<JobResult>

export interface JobDefinition<T extends JobData = JobData> {
  name: string
  handler: JobHandler<T>
  schema?: z.ZodSchema<T>
  defaultOptions?: JobOptions
}

export class UnrecoverableError extends Error {
  public readonly retryable = false
  constructor(message: string, public readonly details?: unknown) {
    super(message)
    this.name = 'UnrecoverableError'
  }
}

export class RetryableError extends Error {
  public readonly retryable = true
  constructor(message: string, public readonly details?: unknown) {
    super(message)
    this.name = 'RetryableError'
  }
}

export function isRetryableError(error: unknown): boolean {
  if (error instanceof RetryableError) return true
  if (error instanceof UnrecoverableError) return false
  if (error instanceof Error) {
    const retryableNames = ['AbortError', 'TimeoutError', 'ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN']
    if (retryableNames.some(name => error.name.includes(name) || error.message.includes(name))) {
      return true
    }
    const retryableCodes = ['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN']
    if ('code' in error && typeof error.code === 'string' && retryableCodes.includes(error.code)) {
      return true
    }
  }
  return true
}