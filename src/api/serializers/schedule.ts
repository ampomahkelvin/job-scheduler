export interface ScheduleResponse {
  id: string
  key: string
  name: string
  data: Record<string, unknown>
  opts: Record<string, unknown>
  pattern: string | undefined
  every: number | undefined
  tz: string | undefined
  limit: number | undefined
  nextRun: string | undefined
}

export class ScheduleSerializer {
  static serialize(job: Record<string, any>): ScheduleResponse {
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
      nextRun: job.nextRun ? new Date(job.nextRun).toISOString() : undefined,
    }
  }

  static serializeForUpsert(job: { 
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
}