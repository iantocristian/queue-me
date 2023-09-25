const { Client } = require('pg');
const { JobQueue, JobConsumer } = require('..');

const connectionString = 'postgresql://queueme:nopass@localhost:5432/queueme';

describe('JobQueue', () => {
  let client;
  let queue;

  beforeAll(async () => {
    client = new Client(connectionString);
    await client.connect();
    queue = new JobQueue('test_queue', connectionString);
    await queue.init();
  });

  afterAll(async () => {
    await client.query('DROP TABLE IF EXISTS test_queue;');
    await client.end();
  });

  test('enqueue job and check status', async () => {
    const id = await queue.enqueue({ foo: 'bar' });
    const res = await client.query('SELECT * FROM test_queue WHERE id = $1;', [id]);
    expect(res.rows[0].status).toBe('pending');
  });

  test('markCompleted updates job status', async () => {
    const id = await queue.enqueue({ foo: 'bar' });
    await queue.markCompleted(id, { success: true });
    const res = await client.query('SELECT * FROM test_queue WHERE id = $1;', [id]);
    expect(res.rows[0].status).toBe('completed');
  });

  test('JobConsumer processes a job automatically', async () => {
    const id = await queue.enqueue({ value: 42 });
    const consumer = new JobConsumer(queue, async (payload) => {
      return { result: payload.value + 1 };
    });

    consumer.start();
    await new Promise(r => setTimeout(r, 1000));

    const res = await client.query('SELECT * FROM test_queue WHERE id = $1;', [id]);
    expect(res.rows[0].status).toBe('completed');
    expect(res.rows[0].result).toEqual({ result: 43 });
  });

  test('listenForCompletion gets notified', done => {
    const producer = new JobQueue('test_queue', connectionString);
    producer.init().then(() => {
      producer.listenForCompletion();
      producer.on('completed', result => {
        expect(result).toEqual({ id: expect.any(Number), result: { success: true } });
        producer.destroy();
        done();
      });

      queue.enqueue({ test: 'data' }).then(id => {
        queue.markCompleted(id, { success: true });
      });
    });
  });

  test('Each job is consumed by only one consumer', async () => {
    const jobs = [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }];
    const processed = new Set();
    const duplicates = new Set();

    // Create two consumers
    const consumer1 = new JobConsumer(queue, async (payload) => {
      if (processed.has(payload.x)) {
        duplicates.add(payload.x);
      }
      processed.add(payload.x);
      return { consumedBy: 'consumer1' };
    });

    const consumer2 = new JobConsumer(queue, async (payload) => {
      if (processed.has(payload.x)) {
        duplicates.add(payload.x);
      }
      processed.add(payload.x);
      return { consumedBy: 'consumer2' };
    });

    // Enqueue jobs
    for (const job of jobs) {
      await queue.enqueue(job);
    }

    // Start consumers and let them run for a bit
    consumer1.start();
    consumer2.start();

    await new Promise(r => setTimeout(r, 2000));

    // Stop consumers
    consumer1.stop();
    consumer2.stop();

    // Check no duplicates
    expect(duplicates.size).toBe(0);
  });
});
