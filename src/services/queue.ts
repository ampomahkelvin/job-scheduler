import { Queue, Job, Worker } from 'bullmq'
import { getRedisConnection, getWorkerRedisConnection } from '../lib/redis'
import { logger } from '../lib/logger'
import { JobOptions, JobData } from '../jobs/types'
import { RepeatOptions } from 'bullmq'

export const QUEUE_NAME = 'default'

export interface JobCounts {
  waiting: number
  active: number
  completed: number
  failed: number
  delayed: number
  paused: number
}

export class QueueService {
  private queue: Queue
  private redisConnection: ReturnType<typeof getRedisConnection>

  constructor() {
    this.redisConnection = getRedisConnection()
    this.queue = new Queue(QUEUE_NAME, {
      connection: this.redisConnection,
      defaultJobOptions: {
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
        removeOnComplete: 100,
        removeOnFail: 50,
      },
    })

    this.queue.on('error', (err: Error) => {
      logger.error({ err }, 'Queue error')
    })

    this.queue.on('waiting', (job: Job) => {
      logger.debug({ jobId: job.id }, 'Job waiting')
    })
  }

  getQueue(): Queue {
    return this.queue
  }

  getRedisConnection() {
    return this.redisConnection
  }

  async addJob<T extends JobData>(name: string, data: T, options?: JobOptions): Promise<Job<T>> {
    return this.queue.add(name, data, options)
  }

  async addJobBulk<T extends JobData>(
    jobs: Array<{ name: string; data: T; options?: JobOptions }>
  ): Promise<Job<T>[]> {
    return this.queue.addBulk(jobs.map((j) => ({ name: j.name, data: j.data, opts: j.options })))
  }

  async upsertJobScheduler<T extends JobData>(
    schedulerId: string,
    repeat: Omit<RepeatOptions, 'key'>,
    data: T,
    options?: JobOptions
  ): Promise<unknown> {
    return this.queue.upsertJobScheduler(schedulerId, repeat, { name: schedulerId, data, opts: options })
  }

  async getJobCounts(): Promise<JobCounts> {
    const counts = await this.queue.getJobCounts()
    return {
      waiting: counts.waiting ?? 0,
      active: counts.active ?? 0,
      completed: counts.completed ?? 0,
      failed: counts.failed ?? 0,
      delayed: counts.delayed ?? 0,
      paused: counts.paused ?? 0,
    }
  }

  async pause(): Promise<void> {
    await this.queue.pause()
    logger.info('Queue paused')
  }

  async resume(): Promise<void> {
    await this.queue.resume()
    logger.info('Queue resumed')
  }

  async close(): Promise<void> {
    await this.queue.close()
    logger.info('Queue closed')
  }

  async getJob(jobId: string): Promise<Job | undefined> {
    return this.queue.getJob(jobId)
  }

  async getJobs(
    statuses: Array<'waiting' | 'active' | 'completed' | 'failed' | 'delayed' | 'paused'>,
    start: number,
    end: number
  ): Promise<Job[]> {
    return this.queue.getJobs(statuses, start, end)
  }

  async getRepeatableJobs(): Promise<unknown[]> {
    return this.queue.getRepeatableJobs()
  }

  async removeRepeatableByKey(key: string): Promise<void> {
    await this.queue.removeRepeatableByKey(key)
  }

  async obliterate(options?: { force?: boolean }): Promise<void> {
    await this.queue.pause()
    try {
      await this.queue.obliterate(options)
    } finally {
      await this.queue.resume()
    }
  }

  async drain(): Promise<void> {
    await this.queue.drain()
  }

  createWorker(
    name: string,
    processor: (job: Job) => Promise<void>,
    options?: Omit<Worker['opts'], 'connection'>
  ): Worker {
    // The worker-safe connection (maxRetriesPerRequest: null, required by
    // BullMQ's blocking commands) is always supplied here and cannot be
    // overridden by callers - hence `connection` is omitted from the
    // accepted options type rather than merely being spread-overridable.
    return new Worker(name, processor, {
      ...options,
      connection: getWorkerRedisConnection(),
    })
  }
}

export const queueService = new QueueService()