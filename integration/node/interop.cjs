'use strict';

const { Queue, QueueEvents, Worker } = require('bullmq');
const IORedis = require('ioredis');

const queueName = process.env.QUEUE_NAME;
const redisMode = process.env.REDIS_MODE || 'standalone';
const redisAddresses = (process.env.REDIS_ADDRS || '127.0.0.1:6390')
  .split(',')
  .map((address) => address.trim())
  .filter(Boolean);

function emit(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

function parseAddress(address) {
  const [host, portText] = address.split(':');
  const port = Number(portText);
  if (!host || !Number.isInteger(port)) {
    throw new Error(`invalid Redis address: ${address}`);
  }
  return { host, port };
}

function newConnection() {
  if (redisMode === 'cluster') {
    return new IORedis.Cluster(redisAddresses.map(parseAddress), {
      redisOptions: {
        enableReadyCheck: true,
        maxRetriesPerRequest: null,
      },
    });
  }
  if (redisMode !== 'standalone') {
    throw new Error(`unsupported REDIS_MODE: ${redisMode}`);
  }
  const { host, port } = parseAddress(redisAddresses[0]);
  return new IORedis({
    host,
    port,
    enableReadyCheck: true,
    maxRetriesPerRequest: null,
  });
}

function validatePayload(data, source) {
  if (
    data === null ||
    typeof data !== 'object' ||
    data.source !== source ||
    data.value !== 41 ||
    data.nested === null ||
    data.nested.ok !== true
  ) {
    throw new Error(`unexpected ${source} payload: ${JSON.stringify(data)}`);
  }
}

async function closeQuietly(...resources) {
  for (const resource of resources) {
    if (!resource) continue;
    try {
      if (typeof resource.close === 'function') {
        await resource.close();
      } else if (typeof resource.quit === 'function') {
        await resource.quit();
      }
    } catch (error) {
      process.stderr.write(`cleanup failed: ${error.stack || error}\n`);
      if (typeof resource.disconnect === 'function') resource.disconnect();
    }
  }
}

async function runProducer() {
  const connection = newConnection();
  const queue = new Queue(queueName, { connection });
  const queueEventsConnection = newConnection();
  const queueEvents = new QueueEvents(queueName, {
    connection: queueEventsConnection,
    lastEventId: '0-0',
  });
  let progress;

  try {
    await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
    queueEvents.on('progress', ({ jobId, data }) => {
      progress = { jobId, data };
    });

    const job = await queue.add(
      'node-produced',
      { source: 'node', value: 41, nested: { ok: true } },
      {
        attempts: 2,
        backoff: { type: 'fixed', delay: 25 },
        removeOnComplete: false,
        removeOnFail: false,
      },
    );
    const result = await job.waitUntilFinished(queueEvents, 25_000);
    const state = await job.getState();

    if (state !== 'completed') throw new Error(`job state is ${state}, want completed`);
    if (!progress || progress.jobId !== job.id || progress.data.stage !== 'go') {
      throw new Error(`unexpected progress: ${JSON.stringify(progress)}`);
    }
    if (result.handledBy !== 'go' || result.value !== 42) {
      throw new Error(`unexpected result: ${JSON.stringify(result)}`);
    }

    emit({ event: 'result', direction: 'node-to-go', state, progress, result });
  } finally {
    await closeQuietly(queueEvents, queue, queueEventsConnection, connection);
  }
}

async function runWorker() {
  const connection = newConnection();
  let worker;
  let timeout;

  try {
    const completed = new Promise((resolve, reject) => {
      worker = new Worker(
        queueName,
        async (job) => {
          validatePayload(job.data, 'go');
          await job.updateProgress({ stage: 'node', value: job.data.value });
          return { handledBy: 'node', value: job.data.value + 1 };
        },
        { connection },
      );
      worker.once('completed', async (job, result) => {
        try {
          const state = await job.getState();
          if (state !== 'completed') throw new Error(`job state is ${state}, want completed`);
          if (result.handledBy !== 'node' || result.value !== 42) {
            throw new Error(`unexpected result: ${JSON.stringify(result)}`);
          }
          emit({ event: 'result', direction: 'go-to-node', state, jobId: job.id, result });
          resolve();
        } catch (error) {
          reject(error);
        }
      });
      worker.on('failed', (job, error) => {
        reject(new Error(`job ${job && job.id} failed: ${error.stack || error}`));
      });
      worker.on('error', reject);
    });

    await worker.waitUntilReady();
    emit({ event: 'ready' });
    timeout = setTimeout(() => {
      process.stderr.write('timed out waiting for a Go-produced job\n');
      process.exitCode = 1;
      void worker.close(true);
    }, 25_000);
    await completed;
  } finally {
    clearTimeout(timeout);
    await closeQuietly(worker, connection);
  }
}

async function main() {
  if (!queueName) throw new Error('QUEUE_NAME is required');
  const mode = process.argv[2];
  if (mode === 'producer') return runProducer();
  if (mode === 'worker') return runWorker();
  throw new Error(`usage: node interop.cjs <producer|worker>; got ${mode || 'no mode'}`);
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exitCode = 1;
});
