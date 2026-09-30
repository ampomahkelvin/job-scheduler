import { Queue } from 'bullmq'
import { getWorkerRedisConnection } from '../lib/redis'
import { logger } from '../lib/logger'

export const DLQ_NAME = 'dlq'

export interface DeadLetterEntry {
  id?: string
  name: string
  data: Record<string, unknown>
  opts?: unknown
  attemptsMade: number
  failedReason: string
  stacktrace?: string
  failedAt: string
  queueName: string
}

export class DeadLetterQueueService {
  private queue: Queue

  constructor() {
    this.queue = new Queue(DLQ_NAME, {
      connection: getWorkerRedisConnection(),
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: 1000,
        removeOnFail: 1000,
      },
    })

    this.queue.on('error', (err: Error) => {
      logger.error({ err }, 'Dead-letter queue error')
    })
  }

  getQueue() {
    return this.queue
  }

  async add(originalJob: {
    id?: string
    name: string
    data: Record<string, unknown>
    opts?: unknown
    attemptsMade: number
    failedReason: string
    stacktrace?: string
    failedAt: string
    queueName: string
  }) {
    return this.queue.add('dead-letter', { originalJob })
  }

  async getJobs(
    statuses: Array<'waiting' | 'active' | 'completed' | 'failed'>,
    start: number,
    end: number
  ) {
    return this.queue.getJobs(statuses, start, end)
  }

  async getJob(id: string) {
    return this.queue.getJob(id)
  }

  async pause(): Promise<void> {
    await this.queue.pause()
  }

  async resume(): Promise<void> {
    await this.queue.resume()
  }

  async close(): Promise<void> {
    await this.queue.close()
    logger.info('Dead-letter queue closed')
  }
}

export const dlqService = new DeadLetterQueueService()