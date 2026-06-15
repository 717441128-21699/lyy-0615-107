import Redis, { Redis as RedisType, RedisOptions } from 'ioredis';
import { QuotaStore, ConsumeRequestResult, AcquireConnectionResult, ReleaseConnectionResult } from './types';

export interface RedisQuotaStoreOptions {
  redisOptions?: RedisOptions;
  redisClient?: RedisType;
  syncIntervalMs?: number;
  localCacheTtlMs?: number;
  keyPrefix?: string;
}

const KEYS = {
  concurrent: (prefix: string, clientId: string) => `${prefix}:conn:${clientId}`,
  tokens: (prefix: string, clientId: string) => `${prefix}:tokens:${clientId}`,
  tokensRefill: (prefix: string, clientId: string) => `${prefix}:tokens_refill:${clientId}`,
  trafficHour: (prefix: string, clientId: string) => `${prefix}:bw_h:${clientId}`,
  trafficDay: (prefix: string, clientId: string) => `${prefix}:bw_d:${clientId}`,
};

const TOKEN_BUCKET_SCRIPT = `
local tokens_key = KEYS[1]
local refill_key = KEYS[2]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local consume = tonumber(ARGV[4])

local last_refill = redis.call('GET', refill_key)
if last_refill == false then last_refill = now end
last_refill = tonumber(last_refill)

local current_tokens = redis.call('GET', tokens_key)
if current_tokens == false then current_tokens = capacity end
current_tokens = tonumber(current_tokens)

local elapsed = (now - last_refill) / 1000
if elapsed > 0 then
  local new_tokens = elapsed * refill_rate
  current_tokens = math.min(capacity, current_tokens + new_tokens)
end

local allowed = 0
local wait_ms = 0

if current_tokens >= consume then
  current_tokens = current_tokens - consume
  allowed = 1
  redis.call('SET', tokens_key, tostring(current_tokens))
  redis.call('SET', refill_key, tostring(now))
else
  local deficit = consume - current_tokens
  wait_ms = math.ceil((deficit / refill_rate) * 1000)
  redis.call('SET', tokens_key, tostring(current_tokens))
  redis.call('SET', refill_key, tostring(now))
end

local reset_ms = 0
local token_deficit = capacity - current_tokens
if token_deficit > 0 then
  reset_ms = math.ceil(token_deficit / refill_rate) * 1000
end

return {allowed, wait_ms, current_tokens, reset_ms}
`;

const TOKEN_BUCKET_PEEK_SCRIPT = `
local tokens_key = KEYS[1]
local refill_key = KEYS[2]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local last_refill = redis.call('GET', refill_key)
if last_refill == false then last_refill = now end
last_refill = tonumber(last_refill)

local current_tokens = redis.call('GET', tokens_key)
if current_tokens == false then current_tokens = capacity end
current_tokens = tonumber(current_tokens)

local elapsed = (now - last_refill) / 1000
if elapsed > 0 then
  local new_tokens = elapsed * refill_rate
  current_tokens = math.min(capacity, current_tokens + new_tokens)
end

local reset_ms = 0
local token_deficit = capacity - current_tokens
if token_deficit > 0 then
  reset_ms = math.ceil(token_deficit / refill_rate) * 1000
end

return {current_tokens, reset_ms}
`;

export class RedisQuotaStore implements QuotaStore {
  readonly mode = 'distributed' as const;
  private readonly client: RedisType;
  private readonly ownClient: boolean;
  private readonly syncIntervalMs: number;
  private readonly localCacheTtlMs: number;
  private readonly keyPrefix: string;
  private readonly localCache: Map<string, { timestamp: number; value: number }>;

  constructor(options: RedisQuotaStoreOptions = {}) {
    if (options.redisClient) {
      this.client = options.redisClient;
      this.ownClient = false;
    } else {
      this.client = new Redis(options.redisOptions ?? {});
      this.ownClient = true;
    }
    this.syncIntervalMs = options.syncIntervalMs ?? 100;
    this.localCacheTtlMs = options.localCacheTtlMs ?? 50;
    this.keyPrefix = options.keyPrefix ?? 'quota';
    this.localCache = new Map();
  }

  private getLocalCached(key: string): number | null {
    const entry = this.localCache.get(key);
    if (entry && Date.now() - entry.timestamp < this.localCacheTtlMs) {
      return entry.value;
    }
    return null;
  }

  private setLocalCached(key: string, value: number): void {
    this.localCache.set(key, { timestamp: Date.now(), value });
  }

  async acquireConnection(clientId: string, limit: number): Promise<AcquireConnectionResult> {
    const key = KEYS.concurrent(this.keyPrefix, clientId);

    try {
      const result = await this.client.eval(
        `
        local key = KEYS[1]
        local limit = tonumber(ARGV[1])
        local current = tonumber(redis.call('GET', key) or '0')
        if current >= limit then
          return {0, current}
        end
        redis.call('INCR', key)
        redis.call('EXPIRE', key, 300)
        return {1, current + 1}
        `,
        1,
        key,
        limit.toString(),
      ) as [number, number];

      const acquired = result[0] === 1;
      const current = result[1];

      if (acquired) {
        this.setLocalCached(key, current);
      }

      return { acquired, current, limit };
    } catch (e) {
      const cached = this.getLocalCached(key);
      const current = cached ?? 0;
      return {
        acquired: current < limit,
        current,
        limit,
      };
    }
  }

  async releaseConnection(clientId: string): Promise<ReleaseConnectionResult> {
    const key = KEYS.concurrent(this.keyPrefix, clientId);
    try {
      const result = await this.client.eval(
        `
        local key = KEYS[1]
        local current = tonumber(redis.call('GET', key) or '0')
        if current > 0 then
          current = current - 1
          redis.call('SET', key, tostring(current))
        end
        return current
        `,
        1,
        key,
      ) as number;
      this.setLocalCached(key, result);
      return { current: result };
    } catch (e) {
      return { current: 0 };
    }
  }

  private async sumTrafficBuckets(key: string, maxBuckets: number, currentBucket: number): Promise<number> {
    try {
      const data = await this.client.hgetall(key);
      const cutoff = currentBucket - maxBuckets;
      let total = 0;
      for (const [bucket, value] of Object.entries(data)) {
        const bucketNum = parseInt(bucket, 10);
        if (bucketNum >= cutoff) {
          total += parseInt(value, 10) || 0;
        }
      }
      return total;
    } catch (e) {
      return 0;
    }
  }

  private getRemainingMs(windowMs: number, currentBucket: number, bucketMs: number): number {
    const windowStartBucket = currentBucket - Math.floor(windowMs / bucketMs);
    const expiresAt = (windowStartBucket + 1) * bucketMs;
    return Math.max(0, expiresAt - Date.now());
  }

  async consumeRequest(
    clientId: string,
    rateLimit: number,
    rateBurst: number,
    bytesPerHour: number,
    bytesPerDay: number,
    payloadBytes: number = 0,
  ): Promise<ConsumeRequestResult> {
    const tokensKey = KEYS.tokens(this.keyPrefix, clientId);
    const refillKey = KEYS.tokensRefill(this.keyPrefix, clientId);
    const trafficHourKey = KEYS.trafficHour(this.keyPrefix, clientId);
    const trafficDayKey = KEYS.trafficDay(this.keyPrefix, clientId);
    const now = Date.now();
    const currentHourBucket = Math.floor(now / 10_000);
    const currentDayBucket = Math.floor(now / 60_000);

    try {
      const tokenResult = await this.client.eval(
        TOKEN_BUCKET_SCRIPT,
        2,
        tokensKey,
        refillKey,
        rateBurst.toString(),
        rateLimit.toString(),
        now.toString(),
        '1',
      ) as [number, number, number, number];

      const allowed = tokenResult[0] === 1;
      const waitMs = tokenResult[1];
      const remaining = tokenResult[2];
      const resetMs = tokenResult[3];

      if (!allowed) {
        const hourCount = await this.sumTrafficBuckets(trafficHourKey, 360, currentHourBucket);
        const dayCount = await this.sumTrafficBuckets(trafficDayKey, 1440, currentDayBucket);

        return {
          allowed: false,
          blockedDimension: 'requestsPerSecond',
          throttleDelayMs: waitMs,
          concurrentCurrent: 0,
          concurrentLimit: 0,
          rateRemaining: Math.floor(remaining),
          rateLimit,
          rateResetMs: resetMs,
          trafficHourRemaining: Math.max(0, bytesPerHour - hourCount),
          trafficHourLimit: bytesPerHour,
          trafficDayRemaining: Math.max(0, bytesPerDay - dayCount),
          trafficDayLimit: bytesPerDay,
          retryAfterSec: Math.ceil(waitMs / 1000),
          maxUsageRatio: 1,
        };
      }

      let hourCount = 0;
      let dayCount = 0;

      if (payloadBytes > 0) {
        const pipeline = this.client.pipeline();
        pipeline.hincrby(trafficHourKey, currentHourBucket.toString(), payloadBytes);
        pipeline.hincrby(trafficDayKey, currentDayBucket.toString(), payloadBytes);
        pipeline.expire(trafficHourKey, 3700);
        pipeline.expire(trafficDayKey, 86500);
        await pipeline.exec();
      }

      hourCount = await this.sumTrafficBuckets(trafficHourKey, 360, currentHourBucket);
      dayCount = await this.sumTrafficBuckets(trafficDayKey, 1440, currentDayBucket);

      if (hourCount >= bytesPerHour) {
        const remainingMs = this.getRemainingMs(3600000, currentHourBucket, 10000);
        return {
          allowed: false,
          blockedDimension: 'bytesPerHour',
          concurrentCurrent: 0,
          concurrentLimit: 0,
          rateRemaining: Math.floor(remaining),
          rateLimit,
          rateResetMs: resetMs,
          trafficHourRemaining: 0,
          trafficHourLimit: bytesPerHour,
          trafficDayRemaining: Math.max(0, bytesPerDay - dayCount),
          trafficDayLimit: bytesPerDay,
          retryAfterSec: Math.ceil(remainingMs / 1000),
          maxUsageRatio: 1,
        };
      }

      if (dayCount >= bytesPerDay) {
        const remainingMs = this.getRemainingMs(86400000, currentDayBucket, 60000);
        return {
          allowed: false,
          blockedDimension: 'bytesPerDay',
          concurrentCurrent: 0,
          concurrentLimit: 0,
          rateRemaining: Math.floor(remaining),
          rateLimit,
          rateResetMs: resetMs,
          trafficHourRemaining: Math.max(0, bytesPerHour - hourCount),
          trafficHourLimit: bytesPerHour,
          trafficDayRemaining: 0,
          trafficDayLimit: bytesPerDay,
          retryAfterSec: Math.ceil(remainingMs / 1000),
          maxUsageRatio: 1,
        };
      }

      const rateRatio = (rateBurst - remaining) / rateBurst;
      const hourRatio = hourCount / bytesPerHour;
      const dayRatio = dayCount / bytesPerDay;
      const maxUsageRatio = Math.max(rateRatio, hourRatio, dayRatio);

      return {
        allowed: true,
        concurrentCurrent: 0,
        concurrentLimit: 0,
        rateRemaining: Math.floor(remaining),
        rateLimit,
        rateResetMs: resetMs,
        trafficHourRemaining: Math.max(0, bytesPerHour - hourCount),
        trafficHourLimit: bytesPerHour,
        trafficDayRemaining: Math.max(0, bytesPerDay - dayCount),
        trafficDayLimit: bytesPerDay,
        maxUsageRatio,
      };
    } catch (e) {
      return {
        allowed: true,
        concurrentCurrent: 0,
        concurrentLimit: 0,
        rateRemaining: rateBurst,
        rateLimit,
        rateResetMs: 0,
        trafficHourRemaining: bytesPerHour,
        trafficHourLimit: bytesPerHour,
        trafficDayRemaining: bytesPerDay,
        trafficDayLimit: bytesPerDay,
        maxUsageRatio: 0,
      };
    }
  }

  async peekRequest(
    clientId: string,
    rateLimit: number,
    rateBurst: number,
    bytesPerHour: number,
    bytesPerDay: number,
  ): Promise<ConsumeRequestResult> {
    const tokensKey = KEYS.tokens(this.keyPrefix, clientId);
    const refillKey = KEYS.tokensRefill(this.keyPrefix, clientId);
    const trafficHourKey = KEYS.trafficHour(this.keyPrefix, clientId);
    const trafficDayKey = KEYS.trafficDay(this.keyPrefix, clientId);
    const now = Date.now();
    const currentHourBucket = Math.floor(now / 10_000);
    const currentDayBucket = Math.floor(now / 60_000);

    try {
      const tokenPeek = await this.client.eval(
        TOKEN_BUCKET_PEEK_SCRIPT,
        2,
        tokensKey,
        refillKey,
        rateBurst.toString(),
        rateLimit.toString(),
        now.toString(),
      ) as [number, number];

      const remaining = tokenPeek[0];
      const resetMs = tokenPeek[1];

      const hourCount = await this.sumTrafficBuckets(trafficHourKey, 360, currentHourBucket);
      const dayCount = await this.sumTrafficBuckets(trafficDayKey, 1440, currentDayBucket);

      const rateRatio = (rateBurst - remaining) / rateBurst;
      const hourRatio = hourCount / bytesPerHour;
      const dayRatio = dayCount / bytesPerDay;
      const maxUsageRatio = Math.max(rateRatio, hourRatio, dayRatio);

      return {
        allowed: true,
        concurrentCurrent: 0,
        concurrentLimit: 0,
        rateRemaining: Math.floor(remaining),
        rateLimit,
        rateResetMs: resetMs,
        trafficHourRemaining: Math.max(0, bytesPerHour - hourCount),
        trafficHourLimit: bytesPerHour,
        trafficDayRemaining: Math.max(0, bytesPerDay - dayCount),
        trafficDayLimit: bytesPerDay,
        maxUsageRatio,
      };
    } catch (e) {
      return {
        allowed: true,
        concurrentCurrent: 0,
        concurrentLimit: 0,
        rateRemaining: rateBurst,
        rateLimit,
        rateResetMs: 0,
        trafficHourRemaining: bytesPerHour,
        trafficHourLimit: bytesPerHour,
        trafficDayRemaining: bytesPerDay,
        trafficDayLimit: bytesPerDay,
        maxUsageRatio: 0,
      };
    }
  }

  async getCurrentUsage(clientId: string) {
    try {
      const concurrent = parseInt(await this.client.get(KEYS.concurrent(this.keyPrefix, clientId)) ?? '0', 10);
      const tokens = parseInt(await this.client.get(KEYS.tokens(this.keyPrefix, clientId)) ?? '0', 10);

      const hourKey = KEYS.trafficHour(this.keyPrefix, clientId);
      const dayKey = KEYS.trafficDay(this.keyPrefix, clientId);
      const now = Date.now();

      const hourCount = await this.sumTrafficBuckets(hourKey, 360, Math.floor(now / 10_000));
      const dayCount = await this.sumTrafficBuckets(dayKey, 1440, Math.floor(now / 60_000));

      return {
        concurrent,
        rateRemaining: tokens,
        trafficHourUsed: hourCount,
        trafficDayUsed: dayCount,
      };
    } catch (e) {
      return {
        concurrent: 0,
        rateRemaining: 0,
        trafficHourUsed: 0,
        trafficDayUsed: 0,
      };
    }
  }

  async resetClient(clientId: string): Promise<void> {
    const keys = [
      KEYS.concurrent(this.keyPrefix, clientId),
      KEYS.tokens(this.keyPrefix, clientId),
      KEYS.tokensRefill(this.keyPrefix, clientId),
      KEYS.trafficHour(this.keyPrefix, clientId),
      KEYS.trafficDay(this.keyPrefix, clientId),
    ];
    await this.client.del(...keys);
  }

  async cleanup(): Promise<void> {
    this.localCache.clear();
    if (this.ownClient) {
      await this.client.quit();
    }
  }
}
