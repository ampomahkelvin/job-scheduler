import { describe, it, expect, beforeEach, vi } from 'vitest'
import { registerJob, getJob, getJobHandler, listJobs, hasJob } from '@/jobs/registry'
import { JobHandler, JobData } from '@/jobs/types'

describe('jobs/registry', () => {
  beforeEach(() => {
    vi.resetModules()
  })

  it('should register and retrieve a job', async () => {
    const { registerJob: regJob, getJob: getJobFn } = await import('@/jobs/registry')

    const mockHandler: JobHandler = async () => ({ success: true })
    regJob({ name: 'test-job', handler: mockHandler })

    const job = getJobFn('test-job')
    expect(job).toBeDefined()
    expect(job?.name).toBe('test-job')
    expect(job?.handler).toBe(mockHandler)
  })

  it('should return undefined for unknown job', async () => {
    const { getJob: getJobFn } = await import('@/jobs/registry')
    const job = getJobFn('unknown-job')
    expect(job).toBeUndefined()
  })

  it('should list all registered jobs', async () => {
    const { registerJob: regJob, listJobs: listJobsFn } = await import('@/jobs/registry')

    const mockHandler: JobHandler = async () => ({ success: true })
    regJob({ name: 'job-1', handler: mockHandler })
    regJob({ name: 'job-2', handler: mockHandler })

    const jobs = listJobsFn()
    expect(jobs).toHaveLength(2)
    expect(jobs.map((j) => j.name)).toContain('job-1')
    expect(jobs.map((j) => j.name)).toContain('job-2')
  })

  it('should check if job exists', async () => {
    const { registerJob: regJob, hasJob: hasJobFn } = await import('@/jobs/registry')

    const mockHandler: JobHandler = async () => ({ success: true })
    regJob({ name: 'existing-job', handler: mockHandler })

    expect(hasJobFn('existing-job')).toBe(true)
    expect(hasJobFn('non-existing')).toBe(false)
  })
})