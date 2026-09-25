import fastify from 'fastify'
import cors from '@fastify/cors'
import helmet from '@fastify/helmet'
import sensible from '@fastify/sensible'
import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter'
import { FastifyAdapter } from '@bull-board/fastify'
import { env } from './config/env'
import { logger } from './lib/logger'
import { queue } from './queues'
import { errorHandler, notFoundHandler } from './api/errorHandler'
import jobsRoutes from './api/routes/jobs'
import schedulesRoutes from './api/routes/schedules'
import healthRoutes from './api/routes/health'
import deadLetterRoutes from './api/routes/dead-letter'
import './jobs/handlers/echo'

const server = fastify({
  logger: false,
  ajv: {
    customOptions: {
      removeAdditional: 'all',
      coerceTypes: 'array',
    },
  },
})

async function buildServer(): Promise<typeof server> {
  await server.register(helmet)
  await server.register(cors, { origin: true })
  await server.register(sensible)

  server.setErrorHandler(errorHandler)
  server.setNotFoundHandler(notFoundHandler)

  // API Key authentication (skip for health and Bull Board)
  if (env.API_KEY) {
    const validKeys = new Set(env.API_KEY.split(',').map(k => k.trim()))
    server.addHook('onRequest', async (req, reply) => {
      const skipAuth = req.url.startsWith('/health')
      if (skipAuth) return

      const apiKey = req.headers['x-api-key'] as string
      if (!apiKey || !validKeys.has(apiKey)) {
        return reply.status(401).send({
          error: 'Unauthorized',
          message: 'Invalid or missing API key',
        })
      }
    })
  }

  server.addHook('onRequest', async (req) => {
    req.headers['x-request-id'] = req.headers['x-request-id'] || crypto.randomUUID()
  })

  server.addHook('onResponse', async (req, reply) => {
    logger.info(
      { req: { method: req.method, url: req.url }, res: { statusCode: reply.statusCode } },
      'Request completed'
    )
  })

  server.register(healthRoutes, { prefix: '/health' })
  server.register(jobsRoutes, { prefix: '/jobs' })
  server.register(schedulesRoutes, { prefix: '/schedules' })
  server.register(deadLetterRoutes, { prefix: '/dead-letter' })

  const serverAdapter = new FastifyAdapter()
  createBullBoard({
    queues: [new BullMQAdapter(queue)],
    serverAdapter,
  })

  await server.register(serverAdapter.registerPlugin(), { prefix: env.BULL_BOARD_PATH })

  return server
}

async function start(): Promise<void> {
  try {
    const app = await buildServer()
    await app.listen({ port: env.PORT, host: '0.0.0.0' })
    logger.info({ port: env.PORT }, 'Server started')
    logger.info({ path: env.BULL_BOARD_PATH }, 'Bull Board available')
  } catch (err) {
    logger.error({ err }, 'Failed to start server')
    process.exit(1)
  }
}

// Graceful shutdown
async function shutdown() {
  logger.info('Shutting down server...')
  await server.close()
  logger.info('Server closed')
  process.exit(0)
}

process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)

if (require.main === module) {
  void start()
}

export { buildServer, server }