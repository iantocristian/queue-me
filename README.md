# JobQueue for PostgreSQL

A simple, multi-instance job queue library built on top of PostgreSQL.

## All credit goes to ChatGPT 

This is an experiment. README and most code generated by ChatGPT and/or OpenAI API 

## Features

- JSON payload support for jobs.
- Cross-process, multi-instance capable.
- Producer and Consumer roles.
- Async job processing.
- Event-driven architecture.

## Installation

npm install --save queue-me

## API

### JobQueue

#### `new JobQueue(name: string, connectionString: string)`

Creates a new `JobQueue` instance.

#### `init()`

Initializes the job queue table in the database.

#### `enqueue(payload: object) -> Promise<number>`

Enqueues a new job and returns its ID.

#### `markCompleted(id: number, result: object)`

Marks a job as completed and stores the result.

#### `listenForCompletion()`

Listens for job completion events.

---

### JobConsumer

#### `new JobConsumer(queue: JobQueue, processFunc: Function)`

Creates a new `JobConsumer` linked to a `JobQueue`.

- `processFunc(payload: object) -> Promise<object>`: Function to process each job.

#### `start()`

Starts consuming jobs from the queue.

#### `stop()`

Stops consuming jobs.

---

### Events

#### 'completed'

Emitted when a job is completed.

- `result: object`: The result object of the completed job.

---

Example:

```javascript
const { JobQueue, JobConsumer } = require('./jobQueue');

const queue = new JobQueue('my_queue', 'postgresql://localhost/mydb');
await queue.init();

const consumer = new JobConsumer(queue, async (payload) => {
  return { success: true };
});
consumer.start();
