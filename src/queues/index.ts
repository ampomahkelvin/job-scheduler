import type { QueueOptions, Job} from 'bullmq';
import { Queue, Worker } from 'bullmq'
import { getRedisConnection, getWorkerRedisConnection } from '../lib/redis'
import { logger } from '../lib/logger'
import type { JobOptions, JobData } from '../jobs/types'

export const QUEUE_NAME = 'default'

const defaultJobOptions: QueueOptions['defaultJobOptions'] = {
  attempts: 3,
  backoff: { type: 'exponential', delay: 1000 },
  removeOnComplete: 100,
  removeOnFail: 50,
}

export const queue = new Queue(QUEUE_NAME, {
  connection: getRedisConnection(),
  defaultJobOptions,
})

queue.on('error', (err: Error) => {
  logger.error({ err }, 'Queue error')
})

queue.on('waiting', (job: Job) => {
  logger.debug({ jobId: job.id }, 'Job waiting')
})

export async function addJob<T extends JobData>(
  name: string,
  data: T,
  options?: JobOptions
): Promise<Job<T>> {
  return queue.add(name, data, options)
}

export async function addJobBulk<T extends JobData>(
  jobs: Array<{ name: string; data: T; options?: JobOptions }>
): Promise<Job<T>[]> {
  return queue.addBulk(jobs.map((j) => ({ name: j.name, data: j.data, opts: j.options })))
}

export async function getJobCounts(): Promise<Record<string, number>> {
  return queue.getJobCounts()
}

export async function pauseQueue(): Promise<void> {
  await queue.pause()
  logger.info('Queue paused')
}

export async function resumeQueue(): Promise<void> {
  await queue.resume()
  logger.info('Queue resumed')
}

export async function closeQueue(): Promise<void> {
  await queue.close()
  logger.info('Queue closed')
}

export function createWorker(
  name: string,
  processor: (job: Job) => Promise<void>,
  options?: Worker['opts']
): Worker {
  return new Worker(name, processor, {
    connection: getWorkerRedisConnection(),
    ...options,
  })
}