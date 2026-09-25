import { Job } from 'bullmq'
import { JobCounts } from '../../jobs/types'

export interface JobResponse {
  id: string
  name: string
  data: Record<string, unknown>
  opts: Record<string, unknown>
  progress: number
  attemptsMade: number
  timestamp?: string
  processedOn?: string
  finishedOn?: string
  failedReason?: string
  failedAt?: string
  returnvalue?: unknown
  stacktrace?: string[] | null
}

export interface JobListResponse {
  counts: JobCounts
  jobs: JobResponse[]
}

export interface JobTypesResponse {
  name: string
  schema: string
  defaultOptions: Record<string, unknown> | undefined
}

export class JobSerializer {
  static serialize(job: Job): JobResponse {
    const finishedOn = job.finishedOn ? new Date(job.finishedOn).toISOString() : undefined
    const processedOn = job.processedOn ? new Date(job.processedOn).toISOString() : undefined
    const timestamp = job.timestamp ? new Date(job.timestamp).toISOString() : undefined
    const progress = typeof job.progress === 'number' ? job.progress : 0

    const response: JobResponse = {
      id: job.id as string,
      name: job.name as string,
      data: job.data as Record<string, unknown>,
      opts: job.opts as Record<string, unknown>,
      progress: typeof job.progress === 'number' ? job.progress : 0,
      attemptsMade: job.attemptsMade,
    }

    if (timestamp !== undefined) response.timestamp = timestamp
    if (processedOn !== undefined) response.processedOn = processedOn
    if (finishedOn !== undefined) response.finishedOn = finishedOn
    if (job.failedReason !== undefined) response.failedReason = job.failedReason
    if (finishedOn !== undefined && job.failedReason) response.failedAt = finishedOn
    if (job.returnvalue !== undefined) response.returnvalue = job.returnvalue
    if (job.stacktrace !== undefined && job.stacktrace !== null) response.stacktrace = job.stacktrace

    return response
  }

  static serializeList(jobs: Job[], counts: JobCounts): { counts: JobCounts; jobs: JobResponse[] } {
    return {
      counts,
      jobs: jobs.map(this.serialize),
    }
  }

  static serializeTypes(jobs: Array<{ name: string; schema?: unknown; defaultOptions?: unknown }>): { name: string; schema: string; defaultOptions: Record<string, unknown> | undefined }[] {
    return jobs.map((j) => ({
      name: j.name,
      schema: j.schema ? 'defined' : 'none',
      defaultOptions: j.defaultOptions as Record<string, unknown> | undefined,
    }))
  }

  static serializeForIdempotent(job: Job, idempotent: boolean): JobResponse & { idempotent: boolean } {
    const base = this.serialize(job)
    return { ...base, idempotent }
  }
}