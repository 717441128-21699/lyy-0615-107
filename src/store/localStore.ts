import { QuotaStore, ClientState, ConsumeRequestResult, AcquireConnectionResult, ReleaseConnectionResult } from './types';
import { TokenBucket } from '../tokenBucket';
import { SlidingWindowCounter } from '../slidingWindow';

export class LocalQuotaStore implements QuotaStore {
  readonly mode = 'local' as const;
  private clients: Map<string, ClientState> = new Map();
  private concurrentCounts: Map<string, number> = new Map();
  private readonly ttlMs: number;
  private cleanupTimer?: NodeJS.Timeout;

  constructor(ttlMs: number = 3600_000) {
    this.ttlMs = ttlMs;
    this.startCleanup();
  }

  private startCleanup(): void {
    this.cleanupTimer = setInterval(() => {
      const now = Date.now();
      for (const [clientId, state] of this.clients) {
        const concurrent = this.concurrentCounts.get(clientId) ?? 0;
        if (now - state.lastAccess > this.ttlMs && concurrent === 0) {
          this.clients.delete(clientId);
          this.concurrentCounts.delete(clientId);
        }
      }
    }, Math.max(60_000, this.ttlMs / 4));
    this.cleanupTimer.unref();
  }

  private getOrCreateState(
    clientId: string,
    rateLimit: number,
    rateBurst: number,
  ): ClientState {
    let state = this.clients.get(clientId);
    if (!state) {
      state = {
        clientId,
        concurrentConnections: 0,
        tokenBucket: new TokenBucket(rateBurst, rateLimit),
        trafficHour: new SlidingWindowCounter(3_600_000, 360),
        trafficDay: new SlidingWindowCounter(86_400_000, 1440),
        lastAccess: Date.now(),
      };
      this.clients.set(clientId, state);
    } else {
      state.lastAccess = Date.now();
    }
    return state;
  }

  async acquireConnection(clientId: string, limit: number): Promise<AcquireConnectionResult> {
    const current = this.concurrentCounts.get(clientId) ?? 0;
    if (current >= limit) {
      return { acquired: false, current, limit };
    }
    this.concurrentCounts.set(clientId, current + 1);
    return { acquired: true, current: current + 1, limit };
  }

  async releaseConnection(clientId: string): Promise<ReleaseConnectionResult> {
    const current = this.concurrentCounts.get(clientId) ?? 0;
    const newCount = Math.max(0, current - 1);
    this.concurrentCounts.set(clientId, newCount);
    return { current: newCount };
  }

  async consumeRequest(
    clientId: string,
    rateLimit: number,
    rateBurst: number,
    bytesPerHour: number,
    bytesPerDay: number,
    payloadBytes: number = 0,
  ): Promise<ConsumeRequestResult> {
    const state = this.getOrCreateState(clientId, rateLimit, rateBurst);
    const concurrentCurrent = this.concurrentCounts.get(clientId) ?? 0;

    const rateResult = state.tokenBucket.tryConsume(1);
    if (!rateResult.allowed) {
      const { count: hourCount } = state.trafficHour.getApproximateCount();
      const { count: dayCount } = state.trafficDay.getApproximateCount();
      return {
        allowed: false,
        blockedDimension: 'requestsPerSecond',
        throttleDelayMs: rateResult.waitTimeMs,
        concurrentCurrent,
        concurrentLimit: 0,
        rateRemaining: Math.floor(rateResult.remaining),
        rateLimit: rateLimit,
        rateResetMs: state.tokenBucket.getResetTimeMs(),
        trafficHourRemaining: Math.max(0, bytesPerHour - hourCount),
        trafficHourLimit: bytesPerHour,
        trafficDayRemaining: Math.max(0, bytesPerDay - dayCount),
        trafficDayLimit: bytesPerDay,
        retryAfterSec: Math.ceil(rateResult.waitTimeMs / 1000),
        maxUsageRatio: 1,
      };
    }

    if (payloadBytes > 0) {
      state.trafficHour.increment(payloadBytes);
      state.trafficDay.increment(payloadBytes);
    }

    const { count: hourCount } = state.trafficHour.getApproximateCount();
    const { count: dayCount } = state.trafficDay.getApproximateCount();

    if (hourCount >= bytesPerHour) {
      return {
        allowed: false,
        blockedDimension: 'bytesPerHour',
        concurrentCurrent,
        concurrentLimit: 0,
        rateRemaining: Math.floor(rateResult.remaining),
        rateLimit: rateLimit,
        rateResetMs: state.tokenBucket.getResetTimeMs(),
        trafficHourRemaining: 0,
        trafficHourLimit: bytesPerHour,
        trafficDayRemaining: Math.max(0, bytesPerDay - dayCount),
        trafficDayLimit: bytesPerDay,
        retryAfterSec: Math.ceil(state.trafficHour.getRemainingMs() / 1000),
        maxUsageRatio: 1,
      };
    }

    if (dayCount >= bytesPerDay) {
      return {
        allowed: false,
        blockedDimension: 'bytesPerDay',
        concurrentCurrent,
        concurrentLimit: 0,
        rateRemaining: Math.floor(rateResult.remaining),
        rateLimit: rateLimit,
        rateResetMs: state.tokenBucket.getResetTimeMs(),
        trafficHourRemaining: Math.max(0, bytesPerHour - hourCount),
        trafficHourLimit: bytesPerHour,
        trafficDayRemaining: 0,
        trafficDayLimit: bytesPerDay,
        retryAfterSec: Math.ceil(state.trafficDay.getRemainingMs() / 1000),
        maxUsageRatio: 1,
      };
    }

    const rateRatio = (rateBurst - rateResult.remaining) / rateBurst;
    const hourRatio = hourCount / bytesPerHour;
    const dayRatio = dayCount / bytesPerDay;
    const maxUsageRatio = Math.max(rateRatio, hourRatio, dayRatio);

    return {
      allowed: true,
      concurrentCurrent,
      concurrentLimit: 0,
      rateRemaining: Math.floor(rateResult.remaining),
      rateLimit: rateLimit,
      rateResetMs: state.tokenBucket.getResetTimeMs(),
      trafficHourRemaining: Math.max(0, bytesPerHour - hourCount),
      trafficHourLimit: bytesPerHour,
      trafficDayRemaining: Math.max(0, bytesPerDay - dayCount),
      trafficDayLimit: bytesPerDay,
      maxUsageRatio,
    };
  }

  async getCurrentUsage(clientId: string) {
    const state = this.clients.get(clientId);
    if (!state) {
      return {
        concurrent: this.concurrentCounts.get(clientId) ?? 0,
        rateRemaining: 0,
        trafficHourRemaining: 0,
        trafficDayRemaining: 0,
      };
    }
    const { count: hourCount } = state.trafficHour.getApproximateCount();
    const { count: dayCount } = state.trafficDay.getApproximateCount();
    return {
      concurrent: this.concurrentCounts.get(clientId) ?? 0,
      rateRemaining: Math.floor(state.tokenBucket.peekRemaining()),
      trafficHourRemaining: Math.max(0, hourCount),
      trafficDayRemaining: Math.max(0, dayCount),
    };
  }

  async resetClient(clientId: string): Promise<void> {
    this.clients.delete(clientId);
    this.concurrentCounts.delete(clientId);
  }

  async cleanup(): Promise<void> {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = undefined;
    }
    this.clients.clear();
    this.concurrentCounts.clear();
  }
}
