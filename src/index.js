const { Pool } = require('pg');
const EventEmitter = require('events');

class JobQueue extends EventEmitter {
  constructor(name, connectionString) {
    super();
    this.name = name;
    this.pool = new Pool({ connectionString });
  }

  async init() {
    await this.pool.query(`CREATE TABLE IF NOT EXISTS ${this.name} (id serial PRIMARY KEY, payload JSONB, status TEXT, result JSONB);`);
  }

  async enqueue(payload) {
    const res = await this.pool.query(`INSERT INTO ${this.name} (payload, status) VALUES ($1, 'pending') RETURNING id;`, [payload]);
    return res.rows[0].id;
  }

  async listenForCompletion() {
    this.listener = await this.pool.connect();

    const query = `LISTEN ${this.name}_completed;`;
    await this.listener.query(query);

    this.listener.on('notification', (msg) => {
      if (msg.channel === `${this.name}_completed`) {
        this.emit('completed', JSON.parse(msg.payload));
      }
    });
  }

  async markCompleted(id, result) {
    await this.pool.query(`UPDATE ${this.name} SET status = 'completed', result = $1 WHERE id = $2;`, [result, id]);
    await this.pool.query(`NOTIFY ${this.name}_completed, '${JSON.stringify({ id, result })}';`);
  }

  destroy() {
    this.listener.removeAllListeners();
    this.listener.release(true);
    this.listener = null;
    this.pool = null;
  }
}

class JobConsumer {
  constructor(queue, processFunc = null, mode = 'auto') {
    this.queue = queue;
    this.processFunc = processFunc;
    this.mode = mode;
  }

  async start() {
    this.running = true;
    while (this.running) {
      const res = await this.queue.pool.query(`UPDATE ${this.queue.name} SET status = 'processing' WHERE id = (SELECT id FROM ${this.queue.name} WHERE status = 'pending' LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id, payload;`);
      const job = res.rows[0];
      if (job) {
        if (this.mode === 'auto') {
          await this.autoProcess(job);
        } else if (this.mode === 'manual') {
          this.emit('newJob', job);
        }
      } else {
        await new Promise(r => setTimeout(r, 1000)); // Wait when no jobs
      }
    }
  }

  stop() {
    this.running = false;
  }

  async autoProcess(job) {
    const { id, payload } = job;
    try {
      const result = await this.processFunc(payload);
      await this.queue.markCompleted(id, result);
    } catch (err) {
      await this.markError(id, err.message);
    }
  }

  async markCompleted(id, result) {
    await this.queue.markCompleted(id, result);
  }

  async markError(id, error) {
    await this.queue.pool.query(`UPDATE ${this.queue.name} SET status = 'completed', error = $1 WHERE id = $2;`, [error, id]);
  }
}

module.exports = { JobQueue, JobConsumer };
