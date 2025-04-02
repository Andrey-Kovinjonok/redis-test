export interface Config {
  PRODUCERS_COUNT: number;
  MIN_NUMBER: number;
  MAX_NUMBER: number;
  REDIS_HOST: string;
  REDIS_PORT: number;
  STREAM_KEY: string;
  COMPLETION_CHANNEL: string;
  BLOCK_SIZE: number;
  LOG_INTERVAL: number;
}

export const config: Config = {
  PRODUCERS_COUNT: parseInt(process.env.PRODUCERS_COUNT || "4"),
  MIN_NUMBER: 0,
  MAX_NUMBER: parseInt(process.env.MAX_NUMBER || "100000"), // включительно: от 0 до MAX_NUMBER
  REDIS_HOST: process.env.REDIS_HOST || "localhost",
  REDIS_PORT: parseInt(process.env.REDIS_PORT || "6379"),
  STREAM_KEY: "numbers_stream",
  COMPLETION_CHANNEL: "completion_channel",
  BLOCK_SIZE: 5000,
  LOG_INTERVAL: 5000,
};

export function printConfig(cfg: Config) {
  console.log("Configuration:");
  console.log(`Producers count: ${cfg.PRODUCERS_COUNT}`);
  console.log(`Number range: [${cfg.MIN_NUMBER}-${cfg.MAX_NUMBER}]`);
  console.log(`Redis: ${cfg.REDIS_HOST}:${cfg.REDIS_PORT}`);
  console.log(`Stream key: ${cfg.STREAM_KEY}`);
  console.log(`Completion channel: ${cfg.COMPLETION_CHANNEL}`);
  console.log("-".repeat(50));
}