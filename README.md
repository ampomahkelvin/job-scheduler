# Job Scheduler

A robust job scheduling system built with **BullMQ**, **Fastify**, **TypeScript**, and **Redis**.

## Features

- **BullMQ** - Reliable Redis-based queue with retries, backoff, and scheduling
- **Fastify** - High-performance API server with validation
- **TypeScript** - Full type safety with Zod runtime validation
- **Bull Board** - Web UI for monitoring queues at `/admin/queues`
- **Docker** - Ready for containerized deployment
- **Vitest** - Unit and integration testing

## Quick Start

### Prerequisites

- Node.js 20+
- Docker & Docker Compose

### Installation

```bash
# Install dependencies
npm install

# Start Redis
npm run db:up

# Development server
npm run dev

# Development worker (separate terminal)
npm run dev:worker
```

### Production Build

```bash
npm run build
npm start
npm run start:worker
```

## Configuration

Copy `.env.example` to `.env` and customize:

```env
PORT=3000
LOG_LEVEL=info
REDIS_URL=redis://localhost:6379
BULL_BOARD_PATH=/admin/queues
```

## API Endpoints

### Jobs

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/jobs` | Create a new job |
| GET | `/jobs` | List jobs with filters |
| GET | `/jobs/:id` | Get job details |
| DELETE | `/jobs/:id` | Cancel/remove a job |
| POST | `/jobs/:id/retry` | Retry a failed job |
| POST | `/jobs/pause` | Pause queue |
| POST | `/jobs/resume` | Resume queue |
| GET | `/jobs/types` | List registered job types |

### Schedules (Recurring Jobs)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/schedules` | Create recurring job |
| GET | `/schedules` | List all schedules |
| GET | `/schedules/:id` | Get schedule details |
| DELETE | `/schedules/:id` | Remove schedule |

### Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |

## Job Creation Example

```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "echo",
    "data": { "message": "Hello World", "repeat": 3, "delay": 100 },
    "options": { "attempts": 3, "priority": 1, "delay": 5000 }
  }'
```

### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Job type name (must be registered) |
| `data` | object | Yes | Job payload (validated against job's Zod schema) |
| `options` | object | No | Job options |

### Job Options

| Option | Type | Description |
|--------|------|-------------|
| `priority` | number | Higher priority jobs are processed first (default: 0) |
| `delay` | number | Delay in ms before job becomes available |
| `attempts` | number | Number of retry attempts (default: 3) |
| `backoff` | object | Retry backoff strategy: `{ type: 'fixed' \| 'exponential', delay: number }` |
| `removeOnComplete` | boolean/number | Keep completed jobs (count or true/false) |
| `removeOnFail` | boolean/number | Keep failed jobs (count or true/false) |
| `repeat` | object | Recurring job config (see Schedules) |

## Built-in Jobs

### echo

Echoes a message multiple times with optional delay.

```json
{
  "message": "string (required)",
  "repeat": "number (optional, default: 1)",
  "delay": "number in ms (optional, default: 0)"
}
```

## Job Cancellation Behavior

### DELETE /jobs/:id

Removes a job from the queue. Behavior depends on job state:

| Job State | Behavior (default) | Behavior (`?force=true`) |
|-----------|-------------------|--------------------------|
| `waiting` | Removed from queue, never executes | Same |
| `delayed` | Removed from queue, never executes | Same |
| `active` | **Soft cancel**: Job removed from Redis but worker continues current execution. Result won't be stored. No retry. | **Hard cancel**: Job marked as failed immediately with `JobDiscardedError`. Worker stops processing. |
| `completed` | Removed from Redis | Same |
| `failed` | Removed from Redis | Same |

**Recommendation:** Use default (soft cancel) for graceful cancellation. Use `?force=true` only when you need to immediately stop a long-running job and free up worker capacity.

```bash
# Soft cancel (default)
curl -X DELETE http://localhost:3000/jobs/abc123

# Hard cancel (force)
curl -X DELETE "http://localhost:3000/jobs/abc123?force=true"
```

## Enqueue Test Job

```bash
# Basic usage
npm run enqueue:echo

# With custom message
npm run enqueue:echo "Custom message"

# With repeat and delay
npm run enqueue:echo "Hello" 5 200
```

## Monitoring

Open Bull Board at: http://localhost:3000/admin/queues

## Project Structure

```
src/
├── config/env.ts          # Zod-validated environment config
├── lib/
│   ├── redis.ts           # ioredis connection factory
│   └── logger.ts          # Pino structured logging
├── queues/index.ts        # BullMQ queue setup
├── jobs/
│   ├── handlers/          # Job handlers (one per job type)
│   ├── registry.ts        # Job registry (name -> handler + schema)
│   └── types.ts           # TypeScript types
├── api/
│   ├── routes/            # Fastify route handlers
│   ├── schemas.ts         # Zod request/response schemas
│   └── errorHandler.ts    # Centralized error handling
├── server.ts              # API server + Bull Board
└── worker.ts              # BullMQ worker processes
```

## Testing

```bash
# Unit tests
npm test

# Watch mode
npm run test:watch

# Coverage report
npm run test:coverage
```

## Docker

```bash
# Build image
docker build -t job-scheduler .

# Run with docker-compose
docker compose up -d

# View logs
docker compose logs -f
```

## Error Format

All errors follow a consistent format:

```json
{
  "error": "ErrorCode",
  "message": "Human-readable description",
  "details": {}
}
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `ValidationError` | 400 | Invalid request payload |
| `JobTypeNotFound` | 422 | Unknown job type |
| `JobNotFound` | 404 | Job ID not found |
| `ScheduleNotFound` | 404 | Schedule ID not found |
| `InternalServerError` | 500 | Unexpected server error |

## License

MIT