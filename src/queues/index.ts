export { queueService, QueueService, QUEUE_NAME } from '../services/queue'
export { queueService as queue } from '../services/queue'
export type { JobCounts, JobOptions, JobData } from '../jobs/types'
export type { RepeatOptions } from 'bullmq'

// Re-export queue methods for backward compatibility
import { queueService } from '../services/queue'

export const addJob = queueService.addJob.bind(queueService)
export const addJobBulk = queueService.addJobBulk.bind(queueService)
export const upsertJobScheduler = queueService.upsertJobScheduler.bind(queueService)
export const getJobCounts = queueService.getJobCounts.bind(queueService)
export const pauseQueue = queueService.pause.bind(queueService)
export const resumeQueue = queueService.resume.bind(queueService)
export const closeQueue = queueService.close.bind(queueService)
export const getJob = queueService.getJob.bind(queueService)
export const getJobs = queueService.getJobs.bind(queueService)
export const getRepeatableJobs = queueService.getRepeatableJobs.bind(queueService)
export const removeRepeatableByKey = queueService.removeRepeatableByKey.bind(queueService)
export const obliterate = queueService.obliterate.bind(queueService)
export const drain = queueService.drain.bind(queueService)
export const createWorker = queueService.createWorker.bind(queueService)