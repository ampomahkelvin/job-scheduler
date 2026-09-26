export interface ScheduleResponse {
  id: string
  key: string
  name: string
  data: Record<string, unknown>
  opts: Record<string, unknown>
  pattern?: string
  every?: number
  tz?: string
  limit?: number
  nextRun?: string
}

export interface ScheduleUpsertResponse extends ScheduleResponse {
  timestamp: string
}

export function serializeSchedule(job: Record<string, any>): ScheduleResponse {
  const nextRun = job.nextRun ? new Date(job.nextRun).toISOString() : undefined
  return {
    id: job.id,
    key: job.key,
    name: job.name,
    data: job.data as Record<string, unknown>,
    opts: job.opts as Record<string, unknown>,
    pattern: job.pattern,
    every: job.every,
    tz: job.tz,
    limit: job.limit,
    nextRun,
  }
}

export function serializeScheduleForUpsert(job: { 
  id: string
  key: string
  name: string
  data: Record<string, unknown>
  opts: Record<string, unknown>
  repeat?: { pattern?: string; every?: number; tz?: string; limit?: number }
  nextRun?: number
}): ScheduleResponse & { timestamp: string } {
  return {
    id: job.id,
    key: job.key,
    name: job.name,
    data: job.data,
    opts: job.opts,
    pattern: job.repeat?.pattern,
    every: job.repeat?.every,
    tz: job.repeat?.tz,
    limit: job.repeat?.limit,
    nextRun: job.nextRun ? new Date(job.nextRun).toISOString() : undefined,
    timestamp: new Date().toISOString(),
  }
}