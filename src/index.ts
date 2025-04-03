import { Worker } from 'worker_threads';
import Redis from 'ioredis';

import { config, printConfig } from './config';
import { runConsumer } from './consumer';

const sharedPubSub = new Redis(config.REDIS_PORT, config.REDIS_HOST);

const workers: Worker[] = [];
let stopInitiated = false;

async function gracefulShutdown(timeoutMs: number): Promise<void> {
  console.log('\nInitiating graceful shutdown...');

  const stopNotifier = new Redis(config.REDIS_PORT, config.REDIS_HOST);
  await stopNotifier.publish(config.COMPLETION_CHANNEL, 'STOP');
  await stopNotifier.quit();

  workers.forEach((worker) => {
    worker.postMessage({ type: 'STOP' });
  });

  const exitPromises = workers.map((worker, index) =>
    new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        console.error(`⏰ Worker ${index + 1} did not exit in time, attempting to terminate.`);
        worker.terminate().then(() => reject(new Error(`Worker ${index + 1} terminated due to timeout`)));
      }, timeoutMs);

      worker.once('exit', (code) => {
        clearTimeout(timer);
        if (code === 0) {
          console.log(`💤 Worker ${index + 1} exited gracefully.`);
          resolve();
        } else {
          reject(new Error(`Worker ${index + 1} exited with error code ${code}.`));
        }
      });

    })
  );

  try {
    await Promise.all(exitPromises);
    console.log('✅ All workers stopped.');
  } catch (err: any) {
    console.error('❌ Some workers did not stop gracefully:', err.message);
    // Можно выбрать, бросать ли ошибку дальше или нет
  } finally {
    try {
      await sharedPubSub.unsubscribe(config.COMPLETION_CHANNEL);
      console.log('✅ Unsubscribed from Redis channel');
      await sharedPubSub.quit();
      console.log('✅ PubSub Redis connection closed');
    } catch (subErr) {
      console.error('Error during PubSub cleanup:', subErr);
    }

    console.log('🏁 All clean, exiting main process');
    setTimeout(() => {
      process.exit(0);
    }, 1000); //
  }
}

async function main() {
  printConfig(config);
  console.log("🚀 Starting application...\n");

  await sharedPubSub.subscribe(config.COMPLETION_CHANNEL);
  sharedPubSub.on('message', (channel, message) => {
    if (channel === config.COMPLETION_CHANNEL && message === 'STOP' && !stopInitiated) {
      stopInitiated = true;
      console.log('🛑 Received STOP signal.');
      gracefulShutdown(5000).catch(err => {
        console.error("Error during graceful shutdown process:", err);
      });
    }
  });

  workers.push(
    ...Array.from({ length: config.PRODUCERS_COUNT }, (_, i) => {
      const worker = new Worker(
        './src/producer.worker.ts',
        {
          workerData: config,
        },
      );

      worker.on('online', () => {
        worker.postMessage({
          type: 'INIT_THREAD_ID',
          threadId: i + 1
        });
      });
      worker.on('message', (msg) => {
        if (msg === 'READY') {
          console.log(`⚡ Producer ${i + 1} ready`);
        }
      });
      return worker;
    })
  );

  await runConsumer(config);
}

main()
  .catch(async (err) => {
    console.error("🔥 Unhandled error in main:", err);
    if (!stopInitiated) {
      stopInitiated = true;
      const pub = new Redis(config.REDIS_PORT, config.REDIS_HOST);
      try {
        await pub.publish(config.COMPLETION_CHANNEL, 'STOP');
      } finally {
        await pub.quit();
        process.exit(1);
      }
    }
  });

['SIGINT', 'SIGTERM'].forEach(signal => {
  process.on(signal, async () => {
    console.info(`Received ${signal}, initiating shutdown...`);
    if (!stopInitiated) {
      stopInitiated = true;
      const pub = new Redis(config.REDIS_PORT, config.REDIS_HOST);
      try {
        await pub.publish(config.COMPLETION_CHANNEL, 'STOP');
      } finally {
        await pub.quit();
      }
    }
  });
});