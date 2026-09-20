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
}

export interface RepeatOptions {
  pattern?: string
  every?: number
  limit?: number
  tz?: string
}

export type JobHandler<T extends JobData = JobData> = (
  job: { data: T; id: string; attemptsMade: number; updateProgress: (progress: number) => Promise<void> }
) => Promise<JobResult>

export interface JobDefinition<T extends JobData = JobData> {
  name: string
  handler: JobHandler<T>
  schema?: z.ZodSchema<T>
  defaultOptions?: JobOptions
}