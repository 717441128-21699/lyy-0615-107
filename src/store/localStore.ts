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
      return state;
    }

    state.lastAccess = Date.now();

    const currentBurst = state.tokenBucket.getCapacity();
    const currentRate = state.tokenBucket.getRefillRate();
    if (currentBurst !== rateBurst || currentRate !== rateLimit) {
      const remaining = state.tokenBucket.peekRemaining();
      state.tokenBucket = new TokenBucket(rateBurst, rateLimit, Math.min(remaining, rateBurst));
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

  private buildResultFromState(
    clientId: string,
    rateLimit: number,
    rateBurst: number,
    bytesPerHour: number,
    bytesPerDay: number,
    allowed: boolean,
    blockedDimension?: string,
    throttleDelayMs?: number,
    retryAfterSec?: number,
  ): ConsumeRequestResult {
    const state = this.getOrCreateState(clientId, rateLimit, rateBurst);
    const concurrentCurrent = this.concurrentCounts.get(clientId) ?? 0;
    const { count: hourCount } = state.trafficHour.getApproximateCount();
    const { count: dayCount } = state.trafficDay.getApproximateCount();
    const rateRemaining = Math.floor(state.tokenBucket.peekRemaining());
    const rateResetMs = state.tokenBucket.getResetTimeMs();

    const rateRatio = (rateBurst - rateRemaining) / rateBurst;
    const hourRatio = hourCount / bytesPerHour;
    const dayRatio = dayCount / bytesPerDay;
    const maxUsageRatio = Math.max(rateRatio, hourRatio, dayRatio);

    return {
      allowed,
      blockedDimension,
      throttleDelayMs,
      concurrentCurrent,
      concurrentLimit: 0,
      rateRemaining,
      rateLimit,
      rateResetMs,
      trafficHourRemaining: Math.max(0, bytesPerHour - hourCount),
      trafficHourLimit: bytesPerHour,
      trafficDayRemaining: Math.max(0, bytesPerDay - dayCount),
      trafficDayLimit: bytesPerDay,
      retryAfterSec,
      maxUsageRatio,
    };
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

    const rateResult = state.tokenBucket.tryConsume(1);
    if (!rateResult.allowed) {
      return this.buildResultFromState(
        clientId, rateLimit, rateBurst, bytesPerHour, bytesPerDay,
        false, 'requestsPerSecond', rateResult.waitTimeMs,
        Math.ceil(rateResult.waitTimeMs / 1000),
      );
    }

    if (payloadBytes > 0) {
      state.trafficHour.increment(payloadBytes);
      state.trafficDay.increment(payloadBytes);
    }

    const { count: hourCount } = state.trafficHour.getApproximateCount();
    const { count: dayCount } = state.trafficDay.getApproximateCount();

    if (hourCount >= bytesPerHour) {
      return this.buildResultFromState(
        clientId, rateLimit, rateBurst, bytesPerHour, bytesPerDay,
        false, 'bytesPerHour', undefined,
        Math.ceil(state.trafficHour.getRemainingMs() / 1000),
      );
    }

    if (dayCount >= bytesPerDay) {
      return this.buildResultFromState(
        clientId, rateLimit, rateBurst, bytesPerHour, bytesPerDay,
        false, 'bytesPerDay', undefined,
        Math.ceil(state.trafficDay.getRemainingMs() / 1000),
      );
    }

    return this.buildResultFromState(
      clientId, rateLimit, rateBurst, bytesPerHour, bytesPerDay,
      true,
    );
  }

  async peekRequest(
    clientId: string,
    rateLimit: number,
    rateBurst: number,
    bytesPerHour: number,
    bytesPerDay: number,
  ): Promise<ConsumeRequestResult> {
    return this.buildResultFromState(
      clientId, rateLimit, rateBurst, bytesPerHour, bytesPerDay,
      true,
    );
  }

  async addTraffic(
    clientId: string,
    bytesPerHour: number,
    bytesPerDay: number,
    bytes: number,
  ): Promise<{ hourUsed: number; dayUsed: number; hourExceeded: boolean; dayExceeded: boolean }> {
    if (bytes <= 0) {
      const state = this.clients.get(clientId);
      if (!state) {
        return { hourUsed: 0, dayUsed: 0, hourExceeded: false, dayExceeded: false };
      }
      const { count: hourUsed } = state.trafficHour.getApproximateCount();
      const { count: dayUsed } = state.trafficDay.getApproximateCount();
      return {
        hourUsed,
        dayUsed,
        hourExceeded: hourUsed >= bytesPerHour,
        dayExceeded: dayUsed >= bytesPerDay,
      };
    }

    const state = this.getOrCreateState(clientId, 1, 1);
    state.trafficHour.increment(bytes);
    state.trafficDay.increment(bytes);

    const { count: hourUsed } = state.trafficHour.getApproximateCount();
    const { count: dayUsed } = state.trafficDay.getApproximateCount();

    return {
      hourUsed,
      dayUsed,
      hourExceeded: hourUsed >= bytesPerHour,
      dayExceeded: dayUsed >= bytesPerDay,
    };
  }

  async getCurrentUsage(clientId: string) {
    const state = this.clients.get(clientId);
    if (!state) {
      return {
        concurrent: this.concurrentCounts.get(clientId) ?? 0,
        rateRemaining: 0,
        trafficHourUsed: 0,
        trafficDayUsed: 0,
      };
    }
    const { count: hourCount } = state.trafficHour.getApproximateCount();
    const { count: dayCount } = state.trafficDay.getApproximateCount();
    return {
      concurrent: this.concurrentCounts.get(clientId) ?? 0,
      rateRemaining: Math.floor(state.tokenBucket.peekRemaining()),
      trafficHourUsed: hourCount,
      trafficDayUsed: dayCount,
    };
  }

  async getActiveClientIds(): Promise<string[]> {
    const ids = new Set<string>();
    for (const id of this.clients.keys()) ids.add(id);
    for (const id of this.concurrentCounts.keys()) ids.add(id);
    return Array.from(ids);
  }

  async resetClient(clientId: string): Promise<void> {
    this.concurrentCounts.delete(clientId);
    const state = this.clients.get(clientId);
    if (state) {
      state.tokenBucket.resetToFull();
      state.trafficHour.reset();
      state.trafficDay.reset();
      state.lastAccess = Date.now();
    }
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
