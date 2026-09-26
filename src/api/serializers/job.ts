import { Job } from 'bullmq'
import { JobCounts, JobOptions } from '../../jobs/types'

export interface JobResponse {
  id: string
  name: string
  data: Record<string, unknown>
  opts: JobOptions
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

export function serializeJob(job: Job): JobResponse {
  const {
    id,
    name,
    data,
    opts,
    progress,
    attemptsMade,
    failedReason,
    returnvalue,
    stacktrace,
    timestamp: ts,
    processedOn: po,
    finishedOn: fo,
  } = job

  const timestamp = ts ? new Date(ts).toISOString() : undefined
  const processedOn = po ? new Date(po).toISOString() : undefined
  const finishedOn = fo ? new Date(fo).toISOString() : undefined

  return {
    id: id as string,
    name: name as string,
    data,
    opts: opts as JobOptions,
    progress: typeof progress === 'number' ? progress : 0,
    attemptsMade,
    timestamp,
    processedOn,
    finishedOn,
    failedReason,
    failedAt: failedReason ? finishedOn : undefined,
    returnvalue,
    stacktrace: stacktrace ?? undefined,
  }
}

export function serializeJobs(jobs: Job[], counts: JobCounts) {
  return {
    counts,
    jobs: jobs.map(serializeJob),
  }
}

export function serializeJobTypes(jobs: Array<{ name: string; schema?: unknown; defaultOptions?: unknown }>) {
  return jobs.map((j) => ({
    name: j.name,
    schema: j.schema ? 'defined' : 'none',
    defaultOptions: j.defaultOptions as Record<string, unknown> | undefined,
  }))
}

export function serializeJobForIdempotent(job: Job, idempotent: boolean) {
  const base = serializeJob(job)
  return { ...base, idempotent }
}