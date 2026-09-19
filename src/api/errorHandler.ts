import type { FastifyError, FastifyReply, FastifyRequest } from 'fastify'
import { z } from 'zod'
import { logger } from '../lib/logger'

export class AppError extends Error {
  constructor(
    public statusCode: number,
    public code: string,
    message: string,
    public details?: unknown
  ) {
    super(message)
    this.name = 'AppError'
  }
}

export function errorHandler(
  err: FastifyError,
  req: FastifyRequest,
  reply: FastifyReply
): void {
  const requestId = req.headers['x-request-id'] as string | undefined

  if (err instanceof z.ZodError) {
    logger.warn({ err: err.errors, requestId }, 'Validation error')
    reply.status(400).send({
      error: 'ValidationError',
      message: 'Invalid request data',
      details: err.errors,
    })
    return
  }

  if (err instanceof AppError) {
    logger.warn({ err: err.message, code: err.code, requestId }, 'Application error')
    reply.status(err.statusCode).send({
      error: err.code,
      message: err.message,
      details: err.details,
    })
    return
  }

  if (err.validation) {
    logger.warn({ err: err.message, requestId }, 'Validation error')
    reply.status(400).send({
      error: 'ValidationError',
      message: err.message,
    })
    return
  }

  logger.error({ err, requestId }, 'Internal server error')
  reply.status(500).send({
    error: 'InternalServerError',
    message: 'An unexpected error occurred',
  })
}

export function notFoundHandler(req: FastifyRequest, reply: FastifyReply): void {
  reply.status(404).send({
    error: 'NotFound',
    message: `Route ${req.method} ${req.url} not found`,
  })
}