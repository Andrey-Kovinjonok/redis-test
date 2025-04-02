// producer.worker.ts
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

// redis.defineCommand('batchXADD', {
//   numberOfKeys: 1,
//   lua: `
//     local stream_key = KEYS[1]
//     local nums_added = 0
//     for i, num_str in ipairs(ARGV) do
//       redis.call('XADD', stream_key, '*', 'num', num_str)
//       nums_added = nums_added + 1
//     end
//     return nums_added
//   `,
// });

// declare module 'ioredis' {
//   interface RedisCommander<Context = unknown> {
//     batchXADD(key: string, ...args: (string | number)[]): Promise<number>;
//   }
// }

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
      const pipeline = redis.pipeline();

      for (let i = 0; i < BLOCK_SIZE; i++) {
        numberBuffer[i] = Math.floor(Math.random() * RANGE) + MIN_NUMBER;
      }

      // for (let i = 0; i < BLOCK_SIZE; i++) {
      //   pipeline.xadd(STREAM_KEY, '*', 'num', numberBuffer[i].toString());
      // }
      // const added = await redis.batchXADD(
      //   STREAM_KEY,
      //   ...numbers.map(String)
      // );
      const count = await redis.eval(
        PUBLISHER_BULK_SCRIPT,
        1,
        STREAM_KEY,
        ...numberBuffer.map(String)
      );
      await pipeline.exec();

      batchesLogged++;
      messagesSent += (BLOCK_SIZE);

    }
  } catch (err) {
    console.error(`🔥 Producer ${threadId} error in batch loop: `, err);
  } finally {
    clearInterval(logTimer);
    try {
      await redis.quit();
      await pubSub.quit();
    } catch (e) {
      console.error('Error while closing redis connections:', e);
    }

    console.log(`💤 Producer ${threadId} exiting...`);
    process.exit(0);
  }
}

generateBatchLoop().catch(err => {
  console.error(`🔥 Producer ${threadId} fatal error: `, err);
  process.exit(1);
});