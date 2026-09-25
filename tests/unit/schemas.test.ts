import { describe, it, expect } from 'vitest'
import {
  createJobSchema,
  jobIdParamSchema,
  jobQuerySchema,
  scheduleSchema,
  scheduleIdParamSchema,
  cronPatternSchema,
} from '@/api/schemas'

describe('api/schemas', () => {
  describe('createJobSchema', () => {
    it('should validate valid job creation input', () => {
      const input = {
        name: 'echo',
        data: { message: 'hello' },
        options: { priority: 1, delay: 1000 },
      }
      const result = createJobSchema.safeParse(input)
      expect(result.success).toBe(true)
    })

    it('should require name field', () => {
      const input = { data: { message: 'hello' } }
      const result = createJobSchema.safeParse(input)
      expect(result.success).toBe(false)
    })

    it('should validate options.attempts max 100', () => {
      const input = { name: 'echo', data: {}, options: { attempts: 101 } }
      const result = createJobSchema.safeParse(input)
      expect(result.success).toBe(false)
    })

    it('should validate options.backoff type', () => {
      const input = { name: 'echo', data: {}, options: { backoff: { type: 'invalid', delay: 1000 } } }
      const result = createJobSchema.safeParse(input)
      expect(result.success).toBe(false)
    })

    it('should accept valid backoff types', () => {
      for (const type of ['fixed', 'exponential']) {
        const input = { name: 'echo', data: {}, options: { backoff: { type, delay: 1000 } } }
        const result = createJobSchema.safeParse(input)
        expect(result.success).toBe(true)
      }
    })

    it('should validate repeat pattern or every', () => {
      const input = { name: 'echo', data: {}, options: { repeat: { pattern: '* * * * *' } } }
      const result = createJobSchema.safeParse(input)
      expect(result.success).toBe(true)
    })

    it('should accept jobId and idempotencyKey in options', () => {
      const input = {
        name: 'echo',
        data: { message: 'hello' },
        options: { jobId: 'custom-id', idempotencyKey: 'idem-key-123' },
      }
      const result = createJobSchema.safeParse(input)
      expect(result.success).toBe(true)
    })
  })

  describe('jobIdParamSchema', () => {
    it('should validate job ID param', () => {
      const result = jobIdParamSchema.safeParse({ id: 'abc123' })
      expect(result.success).toBe(true)
    })

    it('should reject empty ID', () => {
      const result = jobIdParamSchema.safeParse({ id: '' })
      expect(result.success).toBe(false)
    })
  })

  describe('jobQuerySchema', () => {
    it('should validate default values', () => {
      const result = jobQuerySchema.safeParse({})
      expect(result.success).toBe(true)
      expect(result.data.count).toBe(50)
    })

    it('should validate status enum', () => {
      for (const status of ['waiting', 'active', 'completed', 'failed', 'delayed', 'paused']) {
        const result = jobQuerySchema.safeParse({ status })
        expect(result.success).toBe(true)
      }
    })

    it('should reject invalid status', () => {
      const result = jobQuerySchema.safeParse({ status: 'invalid' })
      expect(result.success).toBe(false)
    })

    it('should coerce start, end, count to numbers', () => {
      const result = jobQuerySchema.safeParse({ start: '0', end: '49', count: '25' })
      expect(result.success).toBe(true)
      expect(result.data.start).toBe(0)
      expect(result.data.end).toBe(49)
      expect(result.data.count).toBe(25)
    })

    it('should enforce count max 1000', () => {
      const result = jobQuerySchema.safeParse({ count: '1001' })
      expect(result.success).toBe(false)
    })
  })

  describe('scheduleSchema', () => {
    it('should require name and data', () => {
      const result = scheduleSchema.safeParse({ name: 'test', data: {}, pattern: '* * * * *' })
      expect(result.success).toBe(true)
    })

    it('should require either pattern or every', () => {
      const result = scheduleSchema.safeParse({ name: 'test', data: {} })
      expect(result.success).toBe(false)
    })

    it('should accept cron pattern', () => {
      const result = scheduleSchema.safeParse({ name: 'test', data: {}, pattern: '0 0 * * *' })
      expect(result.success).toBe(true)
    })

    it('should accept every interval', () => {
      const result = scheduleSchema.safeParse({ name: 'test', data: {}, every: 60000 })
      expect(result.success).toBe(true)
    })

    it('should accept timezone', () => {
      const result = scheduleSchema.safeParse({ name: 'test', data: {}, pattern: '0 0 * * *', tz: 'UTC' })
      expect(result.success).toBe(true)
    })

    it('should validate options if provided', () => {
      const result = scheduleSchema.safeParse({
        name: 'test',
        data: {},
        pattern: '* * * * *',
        options: { attempts: 5, backoff: { type: 'fixed', delay: 1000 } },
      })
      expect(result.success).toBe(true)
    })
  })

  describe('scheduleIdParamSchema', () => {
    it('should validate schedule ID param', () => {
      const result = scheduleIdParamSchema.safeParse({ id: 'sched-123' })
      expect(result.success).toBe(true)
    })

    it('should reject empty ID', () => {
      const result = scheduleIdParamSchema.safeParse({ id: '' })
      expect(result.success).toBe(false)
    })
  })

  describe('cronPatternSchema', () => {
    it('should accept valid cron patterns', () => {
      const patterns = [
        '* * * * *',
        '0 0 * * *',
        '0 12 * * 1-5',
        '*/5 * * * *',
        '0 0 1 1 *',
        '@yearly',
        '@monthly',
        '@weekly',
        '@daily',
        '@hourly',
      ]
      for (const pattern of patterns) {
        const result = cronPatternSchema.safeParse(pattern)
        expect(result.success, `pattern: ${pattern}`).toBe(true)
      }
    })

    it('should reject invalid cron patterns', () => {
      const patterns = [
        'invalid',
        '0 0 0', // too few fields
        '0 0 0 0 0 0 0', // too many fields
        '60 * * * *', // invalid minute
        '* * * * 8', // invalid day of week
      ]
      for (const pattern of patterns) {
        const result = cronPatternSchema.safeParse(pattern)
        expect(result.success, `pattern: ${pattern}`).toBe(false)
      }
    })
  })
})