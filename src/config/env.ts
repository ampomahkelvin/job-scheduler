import { z } from 'zod'

const envSchema = z.object({
  PORT: z.coerce.number().default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  REDIS_URL: z.string().url().default('redis://localhost:6379'),
  BULL_BOARD_PATH: z.string().default('/admin/queues'),
  API_KEY: z.string().optional(),
  RATE_LIMIT_WEBHOOK: z.coerce.number().int().positive().default(10),
})

export type Env = z.infer<typeof envSchema>

let cachedEnv: Env | null = null

export function loadEnv(): Env {
  if (cachedEnv) return cachedEnv

  const result = envSchema.safeParse(process.env)
  if (!result.success) {
    const errors = result.error.flatten().fieldErrors
    const message = Object.entries(errors)
      .map(([key, val]) => `${key}: ${val?.join(', ')}`)
      .join('\n')
    throw new Error(`Invalid environment variables:\n${message}`)
  }

  cachedEnv = result.data
  return cachedEnv
}

export const env = loadEnv()