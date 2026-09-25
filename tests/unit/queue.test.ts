import { describe, it, expect, vi, beforeEach } from 'vitest'

// Mock BullMQ Queue
const mockQueue = {
  add: vi.fn(),
  addBulk: vi.fn(),
  upsertJobScheduler: vi.fn(),
  getJobCounts: vi.fn(),
  pause: vi.fn(),
  resume: vi.fn(),
  close: vi.fn(),
  on: vi.fn(),
}

vi.mock('bullmq', () => ({
  Queue: vi.fn().mockImplementation(() => mockQueue),
  Worker: vi.fn(),
}))

vi.mock('@/lib/redis', () => ({
  getRedisConnection: vi.fn(),
  getWorkerRedisConnection: vi.fn(),
}))

vi.mock('@/lib/logger', () => ({
  logger: {
    error: vi.fn(),
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}))

import { describe, it, expect, beforeEach } from 'vitest'

describe('queues/index', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.resetModules()
  })

  it('should export queue instance', async () => {
    const { queue } = await import('@/queues')
    expect(queue).toBeDefined()
  })

  it('should add job with options', async () => {
    const mockJob = { id: 'job-1', name: 'test', data: {}, opts: {} }
    mockQueue.add.mockResolvedValue(mockJob)

    const { addJob } = await import('@/queues')
    const job = await addJob('test', { foo: 'bar' }, { attempts: 3 })

    expect(mockQueue.add).toHaveBeenCalledWith('test', { foo: 'bar' }, { attempts: 3 })
    expect(job).toEqual(mockJob)
  })

  it('should add job with default options when none provided', async () => {
    const mockJob = { id: 'job-1', name: 'test', data: {}, opts: {} }
    mockQueue.add.mockResolvedValue(mockJob)

    const { addJob } = await import('@/queues')
    await addJob('test', { foo: 'bar' })

    expect(mockQueue.add).toHaveBeenCalledWith('test', { foo: 'bar' }, undefined)
  })

  it('should add bulk jobs', async () => {
    const mockJobs = [
      { id: 'job-1', name: 'test', data: { a: 1 }, opts: {} },
      { id: 'job-2', name: 'test', data: { a: 2 }, opts: {} },
    ]
    mockQueue.addBulk.mockResolvedValue(mockJobs)

    const { addJobBulk } = await import('@/queues')
    const jobs = await addJobBulk([
      { name: 'test', data: { a: 1 } },
      { name: 'test', data: { a: 2 } },
    ])

    expect(mockQueue.addBulk).toHaveBeenCalled()
    expect(jobs).toEqual(mockJobs)
  })

  it('should upsert job scheduler', async () => {
    const mockResult = { key: 'sched-1', name: 'test', data: {}, opts: {} }
    mockQueue.upsertJobScheduler.mockResolvedValue(mockResult)

    const { upsertJobScheduler } = await import('@/queues')
    const result = await upsertJobScheduler('test', { pattern: '* * * * *' }, { foo: 'bar' })

    expect(mockQueue.upsertJobScheduler).toHaveBeenCalledWith(
      'test',
      { pattern: '* * * * *' },
      { name: 'test', data: { foo: 'bar' }, opts: undefined }
    )
    expect(result).toEqual(mockResult)
  })

  it('should get job counts', async () => {
    mockQueue.getJobCounts.mockResolvedValue({ waiting: 5, active: 2, completed: 10 })

    const { getJobCounts } = await import('@/queues')
    const counts = await getJobCounts()

    expect(mockQueue.getJobCounts).toHaveBeenCalled()
    expect(counts).toEqual({ waiting: 5, active: 2, completed: 10 })
  })

  it('should pause queue', async () => {
    const { pauseQueue } = await import('@/queues')
    await pauseQueue()
    expect(mockQueue.pause).toHaveBeenCalled()
  })

  it('should resume queue', async () => {
    const { resumeQueue } = await import('@/queues')
    await resumeQueue()
    expect(mockQueue.resume).toHaveBeenCalled()
  })

  it('should close queue', async () => {
    const { closeQueue } = await import('@/queues')
    await closeQueue()
    expect(mockQueue.close).toHaveBeenCalled()
  })

  it('should create worker with custom options', async () => {
    const { Worker } = await import('bullmq')
    const { createWorker } = await import('@/queues')

    const mockWorker = { on: vi.fn(), close: vi.fn() }
    ;(Worker as any).mockImplementation(() => mockWorker)

    const processor = vi.fn()
    const worker = createWorker('test', processor, { concurrency: 10 })

    expect(Worker).toHaveBeenCalledWith('test', processor, expect.objectContaining({
      concurrency: 10,
    }))
    expect(worker).toBe(mockWorker)
  })
})