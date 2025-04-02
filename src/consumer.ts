// src/consumer.ts
import { Redis } from 'ioredis';
import { Config } from './config';
import { writeFile } from 'fs/promises';
import { BloomFilter } from 'bloom-filters';

interface NumberMeta {
  num: number;
  generatedAt: number;
}

const PROCESS_NUMBERS_SCRIPT = `
  local stream_key = KEYS[1]
  local set_key = KEYS[2]
  local start_id = ARGV[1]
  local batch_size = tonumber(ARGV[2])
  
  -- Читаем пачку сообщений
  local messages = redis.call('XREAD', 'COUNT', batch_size, 'STREAMS', stream_key, start_id)
  if not messages or #messages == 0 then return nil end
  
  local new_numbers = {}
  local last_id = start_id
  
  -- Обрабатываем каждое сообщение
  for _, message in ipairs(messages[1][2]) do
    local num = nil
    -- Ищем поле 'num' в сообщении
    for i = 1, #message[2], 2 do
      if message[2][i] == 'num' then
        num = tonumber(message[2][i+1])
        break
      end
    end
    
    if num and not redis.call('SISMEMBER', set_key, num) then
      redis.call('SADD', set_key, num)
      table.insert(new_numbers, num)
    end
    last_id = message[1]
  end
  
  return {last_id, new_numbers}
`;

export async function runConsumer(config: Config): Promise<void> {
  const bloomFilter = BloomFilter.create(
    config.MAX_NUMBER - config.MIN_NUMBER + 1,
    0.01
  );

  const startTime = Date.now();
  const redis = new Redis(config.REDIS_PORT, config.REDIS_HOST);
  const uniqueNumbers = new Set<number>();
  const metaMap = new Map<number, NumberMeta>();

  let lastId = '$';
  let totalProcessed = 0;
  const totalRequired = config.MAX_NUMBER - config.MIN_NUMBER + 1;
  const UNIQUE_SET_KEY = `${config.STREAM_KEY}:unique`;

  const logTimer = setInterval(() => {
    console.log(
      `[${new Date().toISOString().slice(11, 19)}] CONSUMER | Unique: ${uniqueNumbers.size}/${totalRequired} | Processed: ${totalProcessed}`
    );
  }, config.LOG_INTERVAL);

  try {
    // Загружаем скрипт в Redis
    const processNumbersSha = await redis.script('LOAD', PROCESS_NUMBERS_SCRIPT) as string;

    while (uniqueNumbers.size < totalRequired) {
      const result = await redis.evalsha(
        processNumbersSha,
        2, // Количество ключей
        config.STREAM_KEY,
        UNIQUE_SET_KEY,
        lastId,
        config.BLOCK_SIZE
      );

      if (!result) {
        // Нет новых сообщений
        continue;
      }

      const [newLastId, newNumbers] = result as [string, number[]];
      lastId = newLastId;
      totalProcessed += newNumbers.length;

      // Обрабатываем новые уникальные числа
      for (const num of newNumbers) {
        if (!uniqueNumbers.has(num)) {
          uniqueNumbers.add(num);
          metaMap.set(num, {
            num,
            generatedAt: Date.now()
          });
        }
      }
    }

    // Сохраняем результаты
    const resultArray = Array.from(metaMap.values())
      .sort((a, b) => a.num - b.num);

    const resultJSON = {
      timeSpent: Date.now() - startTime,
      numbersGenerated: resultArray
    };

    await writeFile('result.json', JSON.stringify(resultJSON, null, 2));
    console.log(`✅ Consumer finished in ${resultJSON.timeSpent}ms. Data saved.`);

    await redis.publish(config.COMPLETION_CHANNEL, 'STOP');
  } catch (err) {
    console.error("🔥 Consumer error:", err);
    throw err;
  } finally {
    clearInterval(logTimer);
    await redis.quit();
  }
}