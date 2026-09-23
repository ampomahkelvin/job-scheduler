import pino from 'pino'
import { env } from '../config/env'

const isProduction = process.env.NODE_ENV === 'production'

export const logger = pino({
  level: env.LOG_LEVEL,
  transport: isProduction
    ? undefined
    : {
        target: 'pino-pretty',
        options: {
          colorize: true,
          translateTime: 'SYS:standard',
          ignore: 'pid,hostname',
        },
      },
  base: {
    service: 'job-scheduler',
  },
  formatters: {
    level(label) {
      return { level: label }
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
})

export function createChildLogger(bindings: Record<string, unknown>) {
  return logger.child(bindings)
}

export function createJobLogger(jobId: string, jobName: string) {
  return logger.child({ jobId, jobName })
}