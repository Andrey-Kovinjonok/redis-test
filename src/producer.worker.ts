import { workerData, parentPort } from 'worker_threads';
import { Redis } from 'ioredis';
import { Config } from './config';

interface WorkerMessage {
  type: string;
  threadId?: number;
}

const {
  REDIS_HOST,
  REDIS_PORT,
  STREAM_KEY,
  MIN_NUMBER,
  MAX_NUMBER,
  COMPLETION_CHANNEL,
  LOG_INTERVAL,
  BLOCK_SIZE,
} = workerData as Config;

const redis = new Redis(REDIS_PORT, REDIS_HOST);
const pubSub = new Redis(REDIS_PORT, REDIS_HOST);
let threadId = -1;
let shouldStop = false;

parentPort?.on('message', (msg: WorkerMessage) => {
  if (msg.type === 'INIT_THREAD_ID' && msg.threadId) {
    threadId = msg.threadId;
    console.log(`🚀 Producer ${threadId} initialized`);
  }
});

pubSub.subscribe(COMPLETION_CHANNEL, (err) => {
  if (err) {
    console.error(`Error subscribing to ${COMPLETION_CHANNEL} in producer ${threadId}:`, err);
  } else {
    pubSub.on('message', () => {
      shouldStop = true;
      console.log(`🛑 Producer ${threadId} received STOP`);
      pubSub.unsubscribe(COMPLETION_CHANNEL).catch(console.error);
    });
  }
});

const PUBLISHER_BULK_SCRIPT = `
  local streamKey = KEYS[1]
  local count = #ARGV
  for i=1, count do
    redis.call('XADD', streamKey, '*', 'num', ARGV[i])
  end
  return count
`;

async function generateBatchLoop() {
  let batchesLogged = 0;
  let messagesSent = 0;

  const logTimer = setInterval(() => {
    console.log(
      `[${new Date().toISOString().slice(11, 19)}] ` +
      `PRODUCER ${threadId} | ` +
      `Batches: ${batchesLogged} | Rate: ${(messagesSent / (LOG_INTERVAL / 1000)).toFixed(1)}/sec`
    );
    batchesLogged = 0;
    messagesSent = 0;
  }, LOG_INTERVAL);

  const numberBuffer = new Array(BLOCK_SIZE);
  const RANGE = (MAX_NUMBER - MIN_NUMBER + 1);

  try {
    while (!shouldStop) {

      for (let i = 0; i < BLOCK_SIZE; i++) {
        numberBuffer[i] = Math.floor(Math.random() * RANGE) + MIN_NUMBER;
      }

      const count = await redis.eval(
        PUBLISHER_BULK_SCRIPT,
        1,
        STREAM_KEY,
        ...numberBuffer.map(String)
      );

      batchesLogged++;
      messagesSent += (BLOCK_SIZE);

    }
  } catch (err) {
    console.error(`🔥 Producer ${threadId} error in batch loop: `, err);
  } finally {
    clearInterval(logTimer);
    try {
      // redis.disconnect(true);
      await redis.quit();
      // await pubSub.disconnect(true);
      await pubSub.quit();
    } catch (e) {
      console.error('Error while closing redis connections:', e);
    }

    console.log(`💤 Producer ${threadId} exiting...`);
    setImmediate(() => process.exit(0));
  }
}

generateBatchLoop().catch(err => {
  console.error(`🔥 Producer ${threadId} fatal error: `, err);
  process.exit(1);
});