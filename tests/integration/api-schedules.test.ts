import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { queue, addJob, upsertJobScheduler, closeQueue } from '@/queues'
import { getRedisConnection, closeRedisConnections } from '@/lib/redis'
import { registerJob } from '@/jobs/registry'

describe('integration/api-schedules', () => {
  beforeAll(async () => {
    const redis = getRedisConnection()
    if (redis.status === 'wait') {
      await redis.connect()
    }
    await redis.flushdb()
  }, 10000)

  afterAll(async () => {
    await closeQueue()
    await closeRedisConnections()
  }, 5000)

  beforeEach(async () => {
    await queue.drain()
    await queue.obliterate({ force: true })
  })

  it('should create a schedule with cron pattern via upsertJobScheduler', async () => {
    registerJob({
      name: 'scheduled-cron-job',
      handler: async ({ data }) => ({ success: true, data }),
      defaultOptions: { attempts: 1 },
    })

    const repeatableJob = await upsertJobScheduler(
      'scheduled-cron-job',
      { pattern: '* * * * *' },
      { message: 'every minute' }
    )

    expect(repeatableJob).toBeDefined()
    expect(repeatableJob.key).toBeDefined()
    expect(repeatableJob.name).toBe('scheduled-cron-job')
  })

  it('should create a schedule with interval via upsertJobScheduler', async () => {
    registerJob({
      name: 'scheduled-interval-job',
      handler: async ({ data }) => ({ success: true, data }),
      defaultOptions: { attempts: 1 },
    )

    const repeatableJob = await upsertJobScheduler(
      'scheduled-interval-job',
      { every: 60000 },
      { message: 'every minute' }
    )

    expect(repeatableJob).toBeDefined()
    expect(repeatableJob.key).toBeDefined()
  })

  it('should be idempotent - upserting same schedule twice returns same job', async () => {
    registerJob({
      name: 'idempotent-schedule',
      handler: async ({ data }) => ({ success: true, data }),
      defaultOptions: { attempts: 1 },
    )

    const job1 = await upsertJobScheduler('idempotent-schedule', { pattern: '* * * * *' }, { foo: 'bar' })
    const job2 = await upsertJobScheduler('idempotent-schedule', { pattern: '* * * * *' }, { foo: 'bar' })

    expect(job2.key).toBe(job1.key)
    expect(job2.id).toBe(job1.id)
  })

  it('should update schedule data when upserting with different data', async () => {
    registerJob({
      name: 'update-schedule',
      handler: async ({ data }) => ({ success: true, data }),
      defaultOptions: { attempts: 1 },
    })

    const job1 = await upsertJobScheduler('update-schedule', { pattern: '* * * * *' }, { version: 1 })
    const job2 = await upsertJobScheduler('update-schedule', { pattern: '* * * * *' }, { version: 2 })

    expect(job2.key).toBe(job1.key)
    // Data should be updated
  })

  it('should list repeatable jobs', async () => {
    registerJob({
      name: 'list-schedule-job',
      handler: async () => ({ success: true }),
      defaultOptions: { attempts: 1 },
    })

    await upsertJobScheduler('list-schedule-job', { pattern: '* * * * *' }, { a: 1 })
    await upsertJobScheduler('list-schedule-job', { every: 30000 }, { b: 2 })

    const repeatableJobs = await queue.getRepeatableJobs()
    expect(repeatableJobs.length).toBeGreaterThanOrEqual(2)
  })

  it('should remove schedule via removeRepeatableByKey', async () => {
    registerJob({
      name: 'remove-schedule-job',
      handler: async () => ({ success: true }),
      defaultOptions: { attempts: 1 },
    })

    const repeatableJob = await upsertJobScheduler('remove-schedule-job', { pattern: '* * * * *' }, {})
    await queue.removeRepeatableByKey(repeatableJob.key)

    const repeatableJobs = await queue.getRepeatableJobs()
    const found = repeatableJobs.find(j => j.key === repeatableJob.key)
    expect(found).toBeUndefined()
  })

  it('should support timezone in schedule', async () => {
    registerJob({
      name: 'tz-schedule-job',
      handler: async () => ({ success: true }),
      defaultOptions: { attempts: 1 },
    })

    const repeatableJob = await upsertJobScheduler(
      'tz-schedule-job',
      { pattern: '0 9 * * *', tz: 'America/New_York' },
      { message: '9am EST' }
    )

    expect(repeatableJob).toBeDefined()
  })

  it('should support limit in schedule', async () => {
    registerJob({
      name: 'limited-schedule-job',
      handler: async () => ({ success: true }),
      defaultOptions: { attempts: 1 },
    )

    const repeatableJob = await upsertJobScheduler(
      'limited-schedule-job',
      { pattern: '* * * * *', limit: 5 },
      { message: 'run 5 times' }
    )

    expect(repeatableJob).toBeDefined()
  })
})