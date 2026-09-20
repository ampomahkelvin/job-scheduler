import Redis from 'ioredis'
import { env } from '../config/env'

let connection: Redis | null = null
let workerConnection: Redis | null = null

export function getRedisConnection(): Redis {
  if (connection) return connection

  connection = new Redis(env.REDIS_URL, {
    maxRetriesPerRequest: 3,
    retryStrategy(times) {
      const delay = Math.min(times * 50, 2000)
      return delay
    },
    enableReadyCheck: true,
    lazyConnect: true,
  })

  connection.on('error', (err) => {
    console.error('Redis connection error:', err)
  })

  return connection
}

export function getWorkerRedisConnection(): Redis {
  if (workerConnection) return workerConnection

  workerConnection = new Redis(env.REDIS_URL, {
    maxRetriesPerRequest: null,
    retryStrategy(times) {
      const delay = Math.min(times * 50, 2000)
      return delay
    },
    enableReadyCheck: true,
    lazyConnect: true,
  })

  workerConnection.on('error', (err) => {
    console.error('Redis worker connection error:', err)
  })

  return workerConnection
}

export async function closeRedisConnections(): Promise<void> {
  await Promise.all([
    connection?.quit(),
    workerConnection?.quit(),
  ])
  connection = null
  workerConnection = null
}