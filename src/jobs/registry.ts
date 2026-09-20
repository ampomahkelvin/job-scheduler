import type { JobDefinition, JobHandler, JobData } from './types'
import { logger } from '../lib/logger'

const jobRegistry = new Map<string, JobDefinition<JobData>>()

export function registerJob<T extends JobData>(definition: JobDefinition<T>): void {
  if (jobRegistry.has(definition.name)) {
    logger.warn({ jobName: definition.name }, 'Job already registered, overwriting')
  }
  jobRegistry.set(definition.name, definition as unknown as JobDefinition<JobData>)
  logger.info({ jobName: definition.name }, 'Job registered')
}

export function getJob<T extends JobData>(name: string): JobDefinition<T> | undefined {
  return jobRegistry.get(name) as JobDefinition<T> | undefined
}

export function getJobHandler<T extends JobData>(name: string): JobHandler<T> | undefined {
  return jobRegistry.get(name)?.handler as JobHandler<T> | undefined
}

export function getJobSchema(name: string) {
  return jobRegistry.get(name)?.schema
}

export function listJobs(): JobDefinition<JobData>[] {
  return Array.from(jobRegistry.values())
}

export function hasJob(name: string): boolean {
  return jobRegistry.has(name)
}