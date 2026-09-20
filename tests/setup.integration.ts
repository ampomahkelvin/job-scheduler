import { beforeAll, afterAll } from 'vitest'
import { getRedisConnection, closeRedisConnections } from '@/lib/redis'

beforeAll(async () => {
  const redis = getRedisConnection()
  await redis.connect()
  await redis.flushdb()
}, 10000)

afterAll(async () => {
  await closeRedisConnections()
}, 5000)