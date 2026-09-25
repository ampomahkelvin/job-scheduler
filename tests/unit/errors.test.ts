import { describe, it, expect } from 'vitest'
import { UnrecoverableError, RetryableError, isRetryableError } from '@/jobs/types'

describe('jobs/types - error classification', () => {
  describe('UnrecoverableError', () => {
    it('should create error with retryable = false', () => {
      const error = new UnrecoverableError('Bad input')
      expect(error.message).toBe('Bad input')
      expect(error.retryable).toBe(false)
      expect(error.name).toBe('UnrecoverableError')
    })

    it('should include details when provided', () => {
      const error = new UnrecoverableError('Validation failed', { field: 'email' })
      expect(error.details).toEqual({ field: 'email' })
    })
  })

  describe('RetryableError', () => {
    it('should create error with retryable = true', () => {
      const error = new RetryableError('Network timeout')
      expect(error.message).toBe('Network timeout')
      expect(error.retryable).toBe(true)
      expect(error.name).toBe('RetryableError')
    })

    it('should include details when provided', () => {
      const error = new RetryableError('Connection failed', { host: 'api.example.com' })
      expect(error.details).toEqual({ host: 'api.example.com' })
    })
  })

  describe('isRetryableError', () => {
    it('should return true for RetryableError instances', () => {
      expect(isRetryableError(new RetryableError('test'))).toBe(true)
    })

    it('should return false for UnrecoverableError instances', () => {
      expect(isRetryableError(new UnrecoverableError('test'))).toBe(false)
    })

    it('should return true for network error codes', () => {
      const codes = ['ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'ECONNRESET', 'EPIPE']
      for (const code of codes) {
        const error = new Error('network error')
        error.code = code
        expect(isRetryableError(error), `code ${code}`).toBe(true)
      }
    })

    it('should return true for AbortError', () => {
      const error = new Error('aborted')
      error.name = 'AbortError'
      expect(isRetryableError(error)).toBe(true)
    })

    it('should return true for TimeoutError', () => {
      const error = new Error('timeout')
      error.name = 'TimeoutError'
      expect(isRetryableError(error)).toBe(true)
    })

    it('should return true for unknown errors (default to retryable)', () => {
      const error = new Error('some random error')
      expect(isRetryableError(error)).toBe(true)
    })

    it('should return true for validation-like errors by name', () => {
      const error = new Error('validation failed')
      error.name = 'ValidationError'
      expect(isRetryableError(error)).toBe(true)
    })
  })
})