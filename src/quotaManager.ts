import {
  ClientQuotaConfig,
  QuotaCheckResult,
  QuotaDimension,
  QuotaStrategyConfig,
  QuotaAction,
  DEFAULT_STRATEGY_CONFIG,
  DEFAULT_QUOTA_CONFIG,
  QuotaResponseHeaders,
} from './types';
import { QuotaStore, ConsumeRequestResult, AcquireConnectionResult } from './store/types';
import { LocalQuotaStore } from './store/localStore';

export interface QuotaManagerOptions {
  store?: QuotaStore;
  strategyConfig?: Partial<QuotaStrategyConfig>;
  getRequestPayloadBytes?: (req: any) => number;
  onQuotaExceeded?: (clientId: string, dimension: QuotaDimension, action: QuotaAction) => void;
  onDegrade?: (clientId: string, usageRatio: number, delayMs: number) => void;
}

export type UpdateQuotaPatch = Partial<Omit<ClientQuotaConfig, 'clientId'>>;

export class QuotaManager {
  private readonly store: QuotaStore;
  private readonly strategy: QuotaStrategyConfig;
  private readonly clientConfigs: Map<string, ClientQuotaConfig> = new Map();
  private readonly defaultConfig: Omit<ClientQuotaConfig, 'clientId'>;
  private readonly getRequestPayloadBytes: (req: any) => number;
  private readonly onQuotaExceeded?: (clientId: string, dimension: QuotaDimension, action: QuotaAction) => void;
  private readonly onDegrade?: (clientId: string, usageRatio: number, delayMs: number) => void;

  constructor(options: QuotaManagerOptions = {}) {
    this.store = options.store ?? new LocalQuotaStore();
    this.strategy = { ...DEFAULT_STRATEGY_CONFIG, ...options.strategyConfig };
    this.defaultConfig = { ...DEFAULT_QUOTA_CONFIG };
    this.getRequestPayloadBytes = options.getRequestPayloadBytes ?? (() => 0);
    this.onQuotaExceeded = options.onQuotaExceeded;
    this.onDegrade = options.onDegrade;
  }

  registerClient(config: Partial<ClientQuotaConfig> & { clientId: string }): void {
    this.clientConfigs.set(config.clientId, {
      ...this.defaultConfig,
      ...config,
    });
  }

  updateClientConfig(clientId: string, patch: UpdateQuotaPatch): ClientQuotaConfig {
    const existing = this.getClientConfig(clientId);
    const updated = { ...existing, ...patch };
    this.clientConfigs.set(clientId, updated);
    return updated;
  }

  getClientConfig(clientId: string): ClientQuotaConfig {
    return this.clientConfigs.get(clientId) ?? {
      ...this.defaultConfig,
      clientId,
    };
  }

  private buildHeaders(
    clientId: string,
    config: ClientQuotaConfig,
    result: ConsumeRequestResult,
    concurrentResult?: AcquireConnectionResult,
  ): QuotaResponseHeaders {
    const resetSeconds = Math.ceil(result.rateResetMs / 1000);
    return {
      'X-Quota-Client-Id': clientId,
      'X-Quota-Concurrent-Current': concurrentResult?.current ?? result.concurrentCurrent,
      'X-Quota-Concurrent-Limit': concurrentResult?.limit ?? config.maxConcurrentConnections,
      'X-Quota-Rate-Remaining': result.rateRemaining,
      'X-Quota-Rate-Limit': result.rateLimit,
      'X-Quota-Rate-Reset': resetSeconds,
      'X-Quota-Traffic-Hour-Remaining': result.trafficHourRemaining,
      'X-Quota-Traffic-Hour-Limit': result.trafficHourLimit,
      'X-Quota-Traffic-Day-Remaining': result.trafficDayRemaining,
      'X-Quota-Traffic-Day-Limit': result.trafficDayLimit,
      'Retry-After': result.retryAfterSec,
    };
  }

  private determineAction(
    usageRatio: number,
    consumeResult: ConsumeRequestResult,
  ): { action: QuotaAction; delayMs: number } {
    if (!consumeResult.allowed) {
      return { action: 'reject', delayMs: 0 };
    }

    if (this.strategy.enableDegradation && usageRatio >= this.strategy.throttleThreshold) {
      const overshoot = (usageRatio - this.strategy.throttleThreshold) / (this.strategy.rejectThreshold - this.strategy.throttleThreshold);
      const delayMs = Math.min(this.strategy.throttleMaxDelayMs, Math.floor(overshoot * this.strategy.throttleMaxDelayMs));
      return { action: 'throttle', delayMs };
    }

    return { action: 'allow', delayMs: 0 };
  }

  async checkAndConsume(
    clientId: string,
    payloadBytes: number = 0,
  ): Promise<QuotaCheckResult & { action: QuotaAction; delayMs: number }> {
    const config = this.getClientConfig(clientId);

    const concurrentResult = await this.store.acquireConnection(
      clientId,
      config.maxConcurrentConnections,
    );

    if (!concurrentResult.acquired) {
      if (this.onQuotaExceeded) {
        this.onQuotaExceeded(clientId, 'concurrentConnections', 'reject');
      }
      return {
        allowed: false,
        blockedDimension: 'concurrentConnections',
        usageRatio: 1,
        retryAfterSec: 1,
        headers: {
          'X-Quota-Client-Id': clientId,
          'X-Quota-Concurrent-Current': concurrentResult.current,
          'X-Quota-Concurrent-Limit': config.maxConcurrentConnections,
          'X-Quota-Rate-Remaining': 0,
          'X-Quota-Rate-Limit': config.requestsPerSecond,
          'X-Quota-Rate-Reset': 1,
          'X-Quota-Traffic-Hour-Remaining': 0,
          'X-Quota-Traffic-Hour-Limit': config.bytesPerHour,
          'X-Quota-Traffic-Day-Remaining': 0,
          'X-Quota-Traffic-Day-Limit': config.bytesPerDay,
          'Retry-After': 1,
        },
        action: 'reject',
        delayMs: 0,
      };
    }

    const consumeResult = await this.store.consumeRequest(
      clientId,
      config.requestsPerSecond,
      config.requestsBurst,
      config.bytesPerHour,
      config.bytesPerDay,
      payloadBytes,
    );

    const usageRatio = Math.max(
      consumeResult.maxUsageRatio,
      concurrentResult.current / config.maxConcurrentConnections,
    );

    const { action, delayMs } = this.determineAction(usageRatio, consumeResult);

    if (action === 'throttle' && this.onDegrade) {
      this.onDegrade(clientId, usageRatio, delayMs);
    }

    if (action === 'reject' && consumeResult.blockedDimension && this.onQuotaExceeded) {
      this.onQuotaExceeded(
        clientId,
        consumeResult.blockedDimension as QuotaDimension,
        'reject',
      );
    }

    if (action === 'reject') {
      await this.store.releaseConnection(clientId);
    }

    const headers = this.buildHeaders(clientId, config, consumeResult, concurrentResult);

    return {
      allowed: action !== 'reject',
      blockedDimension: consumeResult.blockedDimension as QuotaDimension | undefined,
      throttleDelayMs: delayMs,
      usageRatio,
      retryAfterSec: consumeResult.retryAfterSec,
      headers,
      action,
      delayMs,
    };
  }

  async peekUsage(
    clientId: string,
  ): Promise<{
    allowed: boolean;
    usageRatio: number;
    headers: QuotaResponseHeaders;
  }> {
    const config = this.getClientConfig(clientId);

    const peekResult = await this.store.peekRequest(
      clientId,
      config.requestsPerSecond,
      config.requestsBurst,
      config.bytesPerHour,
      config.bytesPerDay,
    );

    const currentConcurrent = (await this.store.getCurrentUsage(clientId)).concurrent;
    const concurrentRatio = config.maxConcurrentConnections > 0
      ? currentConcurrent / config.maxConcurrentConnections
      : 0;

    const usageRatio = Math.max(peekResult.maxUsageRatio, concurrentRatio);

    const headers = this.buildHeaders(clientId, config, {
      ...peekResult,
      concurrentCurrent: currentConcurrent,
    }, {
      acquired: currentConcurrent < config.maxConcurrentConnections,
      current: currentConcurrent,
      limit: config.maxConcurrentConnections,
    });

    return {
      allowed: peekResult.allowed && currentConcurrent < config.maxConcurrentConnections,
      usageRatio,
      headers,
    };
  }

  async releaseConnection(clientId: string): Promise<void> {
    await this.store.releaseConnection(clientId);
  }

  getStoreMode(): 'local' | 'distributed' {
    return this.store.mode;
  }

  async getUsage(clientId: string) {
    const config = this.getClientConfig(clientId);
    const usage = await this.store.getCurrentUsage(clientId);
    return {
      config,
      usage,
      remaining: {
        concurrent: Math.max(0, config.maxConcurrentConnections - usage.concurrent),
        rate: usage.rateRemaining,
        trafficHour: Math.max(0, config.bytesPerHour - usage.trafficHourUsed),
        trafficDay: Math.max(0, config.bytesPerDay - usage.trafficDayUsed),
      },
      ratios: {
        concurrent: config.maxConcurrentConnections > 0
          ? usage.concurrent / config.maxConcurrentConnections
          : 0,
        rate: config.requestsBurst > 0
          ? (config.requestsBurst - usage.rateRemaining) / config.requestsBurst
          : 0,
        trafficHour: config.bytesPerHour > 0
          ? usage.trafficHourUsed / config.bytesPerHour
          : 0,
        trafficDay: config.bytesPerDay > 0
          ? usage.trafficDayUsed / config.bytesPerDay
          : 0,
      },
    };
  }

  async cleanup(): Promise<void> {
    await this.store.cleanup();
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
