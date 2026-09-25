import type { FastifyInstance } from 'fastify'
import { queueService } from '../../services/queue'
import { getRedisConnection } from '../../lib/redis'

async function healthRoutes(app: FastifyInstance): Promise<void> {
  app.get('/', async (_req, reply) => {
    const start = Date.now()
    let redisConnected = false
    let redisLatency: number | undefined

    try {
      const redis = getRedisConnection()
      await redis.ping()
      redisConnected = true
      redisLatency = Date.now() - start
    } catch {
      redisConnected = false
    }

    const counts = await queueService.getJobCounts()

    const response = {
      status: redisConnected ? 'ok' : 'down',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      redis: {
        connected: redisConnected,
        latency: redisLatency,
      },
      queue: counts,
    }

    const statusCode = redisConnected ? 200 : 503
    return reply.status(statusCode).send(response)
  })
}

export default healthRoutes