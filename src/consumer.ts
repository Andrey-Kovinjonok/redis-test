import { Redis } from 'ioredis';
import { Config } from './config';
import { writeFile } from 'fs/promises';

interface NumberMeta {
  num: number;
  generatedAt: number;
}

const PROCESS_NUMBERS_SCRIPT = `
  local stream_key = KEYS[1]
  local set_key = KEYS[2]
  local start_id = ARGV[1]
  local batch_size = tonumber(ARGV[2])

  if batch_size == nil or batch_size <= 0 then
      return redis.error_reply("Invalid batch size")
  end

  local messages = redis.call('XREAD', 'COUNT', batch_size, 'STREAMS', stream_key, start_id)
  if not messages or #messages == 0 then return nil end

  local nums = {}
  local last_id = start_id
  local result = messages[1][2]

  for _, message in ipairs(result) do
      last_id = message[1]
      local fields = message[2]
      for i = 1, #fields, 2 do
          if fields[i] == 'num' then
              local num = tonumber(fields[i+1])
              if num then
                  table.insert(nums, num)
              end
              break
          end
      end
  end

  if #nums == 0 then 
      return {last_id, {}} 
  end

  --redis.log(redis.LOG_NOTIE, "Extracted numbers: " .. table.concat(nums, ", "))

  local exists = redis.call('SMISMEMBER', set_key, unpack(nums))
  local new_numbers = {}

  for i, num in ipairs(nums) do
      if exists[i] == 0 then
          table.insert(new_numbers, num)
      end
  end

  -- redis.log(redis.LOG_NOTICE, "New unique numbers to add: " .. table.concat(new_numbers, ", "))

  if #new_numbers > 0 then
      redis.call('SADD', set_key, unpack(new_numbers))
  end

  return {last_id, new_numbers}
`;


export async function runConsumer(config: Config): Promise<void> {

  const startTime = Date.now();
  const redis = new Redis(config.REDIS_PORT, config.REDIS_HOST);
  const pubSub = new Redis(config.REDIS_PORT, config.REDIS_HOST);

  const uniqueNumbers = new Set<number>();
  const metaMap = new Map<number, NumberMeta>();

  let totalProcessed = 0;
  const totalRequired = config.MAX_NUMBER - config.MIN_NUMBER + 1;
  const UNIQUE_SET_KEY = `${config.STREAM_KEY}:unique`;

  const logTimer = setInterval(() => {
    console.log(
      `[${new Date().toISOString().slice(11, 19)}] CONSUMER | Unique: ${uniqueNumbers.size}/${totalRequired} | Processed: ${totalProcessed}`
    );
  }, config.LOG_INTERVAL);

  let shouldStop = false;
  await pubSub.subscribe(config.COMPLETION_CHANNEL);
  pubSub.on('message', (channel, message) => {
    if (channel === config.COMPLETION_CHANNEL && message === 'STOP') {
      console.log('\n🛑 Received STOP signal in consumer');
      shouldStop = true;
    }
  });

  let lastId = '0-0';

  try {
    await redis.del(config.STREAM_KEY);
    await redis.del(UNIQUE_SET_KEY);

    const processNumbersSha = await redis.script('LOAD', PROCESS_NUMBERS_SCRIPT) as string;

    while (uniqueNumbers.size < totalRequired) {
      const result = await redis.evalsha(
        processNumbersSha,
        2,
        config.STREAM_KEY,
        UNIQUE_SET_KEY,
        lastId,
        config.BLOCK_SIZE,
      );

      if (!result) {
        // console.log('No new messages, waiting...');
        await new Promise(resolve => setTimeout(resolve, 100));
        continue;
      }

      if (shouldStop) {
        console.log('\n⏹ Consumer stopped by request');
        process.exit(0);
        return;
      }

      const [newLastId, newNumbers] = result as [string, number[]];
      lastId = newLastId;
      totalProcessed += newNumbers.length;

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
