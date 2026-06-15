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

export type QuotaTemplate = 'free' | 'vip' | 'default';

export interface LastExceededInfo {
  dimension: QuotaDimension;
  action: QuotaAction;
  at: number;
  retryAfterSec?: number;
}

export type AuditEventType = 'config_update' | 'template_apply' | 'usage_reset' | 'quota_exceeded' | 'client_register';

export interface AuditEntry {
  id: number;
  at: number;
  atIso: string;
  event: AuditEventType;
  clientId: string;
  detail: Record<string, any>;
}

export const TEMPLATES: Record<QuotaTemplate, Omit<ClientQuotaConfig, 'clientId'>> = {
  free: {
    maxConcurrentConnections: 10,
    requestsPerSecond: 10,
    requestsBurst: 30,
    bytesPerHour: 10 * 1024 * 1024,
    bytesPerDay: 100 * 1024 * 1024,
  },
  vip: {
    maxConcurrentConnections: 200,
    requestsPerSecond: 200,
    requestsBurst: 500,
    bytesPerHour: 500 * 1024 * 1024,
    bytesPerDay: 10 * 1024 * 1024 * 1024,
  },
  default: { ...DEFAULT_QUOTA_CONFIG },
};

const MAX_AUDIT_ENTRIES = 1000;

export class QuotaManager {
  private readonly store: QuotaStore;
  private readonly strategy: QuotaStrategyConfig;
  private readonly clientConfigs: Map<string, ClientQuotaConfig> = new Map();
  private readonly defaultConfig: Omit<ClientQuotaConfig, 'clientId'>;
  private readonly getRequestPayloadBytes: (req: any) => number;
  private readonly onQuotaExceeded?: (clientId: string, dimension: QuotaDimension, action: QuotaAction) => void;
  private readonly onDegrade?: (clientId: string, usageRatio: number, delayMs: number) => void;
  private readonly lastExceeded: Map<string, LastExceededInfo> = new Map();
  private readonly auditLog: AuditEntry[] = [];
  private auditCounter = 0;

  constructor(options: QuotaManagerOptions = {}) {
    this.store = options.store ?? new LocalQuotaStore();
    this.strategy = { ...DEFAULT_STRATEGY_CONFIG, ...options.strategyConfig };
    this.defaultConfig = { ...DEFAULT_QUOTA_CONFIG };
    this.getRequestPayloadBytes = options.getRequestPayloadBytes ?? (() => 0);
    this.onQuotaExceeded = options.onQuotaExceeded;
    this.onDegrade = options.onDegrade;
  }

  private addAudit(event: AuditEventType, clientId: string, detail: Record<string, any>): void {
    const now = Date.now();
    const entry: AuditEntry = {
      id: ++this.auditCounter,
      at: now,
      atIso: new Date(now).toISOString(),
      event,
      clientId,
      detail,
    };
    this.auditLog.push(entry);
    if (this.auditLog.length > MAX_AUDIT_ENTRIES) {
      this.auditLog.splice(0, this.auditLog.length - MAX_AUDIT_ENTRIES);
    }
  }

  getAuditLog(options?: { clientId?: string; event?: AuditEventType; limit?: number; offset?: number }): AuditEntry[] {
    let entries = this.auditLog;
    if (options?.clientId) {
      entries = entries.filter(e => e.clientId === options.clientId);
    }
    if (options?.event) {
      entries = entries.filter(e => e.event === options.event);
    }
    entries = entries.slice().reverse();
    const offset = options?.offset ?? 0;
    const limit = options?.limit ?? 50;
    return entries.slice(offset, offset + limit);
  }

  registerClient(config: Partial<ClientQuotaConfig> & { clientId: string }): void {
    this.clientConfigs.set(config.clientId, {
      ...this.defaultConfig,
      ...config,
    });
    this.addAudit('client_register', config.clientId, { config: this.clientConfigs.get(config.clientId) });
  }

  applyTemplate(clientId: string, template: QuotaTemplate | 'custom', customConfig?: UpdateQuotaPatch): ClientQuotaConfig {
    let base: Omit<ClientQuotaConfig, 'clientId'>;
    if (template === 'custom') {
      base = { ...this.getClientConfig(clientId) };
    } else {
      base = { ...TEMPLATES[template] };
    }
    const merged: ClientQuotaConfig = {
      ...base,
      ...(customConfig ?? {}),
      clientId,
    };
    this.clientConfigs.set(clientId, merged);
    this.addAudit('template_apply', clientId, { template, customConfig, resultConfig: merged });
    return merged;
  }

  updateClientConfig(clientId: string, patch: UpdateQuotaPatch): ClientQuotaConfig {
    const existing = this.getClientConfig(clientId);
    const updated = { ...existing, ...patch };
    this.clientConfigs.set(clientId, updated);
    this.addAudit('config_update', clientId, { patch, previousConfig: existing, newConfig: updated });
    return updated;
  }

  getClientConfig(clientId: string): ClientQuotaConfig {
    return this.clientConfigs.get(clientId) ?? {
      ...this.defaultConfig,
      clientId,
    };
  }

  isClientRegistered(clientId: string): boolean {
    return this.clientConfigs.has(clientId);
  }

  getAllClientIds(): string[] {
    return Array.from(this.clientConfigs.keys());
  }

  ensureClientRegistered(clientId: string): void {
    if (!this.clientConfigs.has(clientId)) {
      this.clientConfigs.set(clientId, { ...this.defaultConfig, clientId });
      this.addAudit('client_register', clientId, { source: 'auto', config: this.defaultConfig });
    }
  }

  private trackExceeded(clientId: string, dimension: QuotaDimension, action: QuotaAction, retryAfterSec?: number): void {
    this.lastExceeded.set(clientId, { dimension, action, at: Date.now(), retryAfterSec });
    this.addAudit('quota_exceeded', clientId, { dimension, action, retryAfterSec });
  }

  getLastExceeded(clientId: string): LastExceededInfo | undefined {
    return this.lastExceeded.get(clientId);
  }

  async resetUsage(clientId: string): Promise<void> {
    await this.store.resetClient(clientId);
    this.addAudit('usage_reset', clientId, {});
  }

  async addResponseTraffic(clientId: string, bytes: number): Promise<void> {
    if (bytes <= 0) return;
    const config = this.getClientConfig(clientId);
    const result = await this.store.addTraffic(
      clientId,
      config.bytesPerHour,
      config.bytesPerDay,
      bytes,
    );
    if (result.hourExceeded) {
      this.trackExceeded(clientId, 'bytesPerHour', 'reject');
    } else if (result.dayExceeded) {
      this.trackExceeded(clientId, 'bytesPerDay', 'reject');
    }
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
    this.ensureClientRegistered(clientId);
    const config = this.getClientConfig(clientId);

    const concurrentResult = await this.store.acquireConnection(
      clientId,
      config.maxConcurrentConnections,
    );

    if (!concurrentResult.acquired) {
      this.trackExceeded(clientId, 'concurrentConnections', 'reject', 1);
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

    if (action === 'reject' && consumeResult.blockedDimension) {
      const dim = consumeResult.blockedDimension as QuotaDimension;
      this.trackExceeded(clientId, dim, 'reject', consumeResult.retryAfterSec);
      if (this.onQuotaExceeded) {
        this.onQuotaExceeded(clientId, dim, 'reject');
      }
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
    const peek = await this.store.peekRequest(
      clientId,
      config.requestsPerSecond,
      config.requestsBurst,
      config.bytesPerHour,
      config.bytesPerDay,
    );

    const safeRateRemaining = Math.min(
      Math.max(peek.rateRemaining, 0),
      config.requestsBurst,
    );
    const rateCurrent = config.requestsBurst - safeRateRemaining;

    const hourUsed = Math.min(usage.trafficHourUsed, config.bytesPerHour);
    const dayUsed = Math.min(usage.trafficDayUsed, config.bytesPerDay);
    const currentConcurrent = Math.min(usage.concurrent, config.maxConcurrentConnections);

    const concurrentRemaining = Math.max(0, config.maxConcurrentConnections - currentConcurrent);
    const trafficHourRemaining = Math.max(0, config.bytesPerHour - hourUsed);
    const trafficDayRemaining = Math.max(0, config.bytesPerDay - dayUsed);

    const concurrentRatio = config.maxConcurrentConnections > 0
      ? currentConcurrent / config.maxConcurrentConnections
      : 0;
    const rateRatio = config.requestsBurst > 0
      ? rateCurrent / config.requestsBurst
      : 0;
    const trafficHourRatio = config.bytesPerHour > 0
      ? hourUsed / config.bytesPerHour
      : 0;
    const trafficDayRatio = config.bytesPerDay > 0
      ? dayUsed / config.bytesPerDay
      : 0;

    const exceeded = this.getLastExceeded(clientId);

    return {
      clientId,
      config,
      usage: {
        concurrent: currentConcurrent,
        rateRemaining: safeRateRemaining,
        rateCurrent,
        trafficHourUsed: hourUsed,
        trafficDayUsed: dayUsed,
      },
      remaining: {
        concurrent: concurrentRemaining,
        rate: safeRateRemaining,
        trafficHour: trafficHourRemaining,
        trafficDay: trafficDayRemaining,
      },
      ratios: {
        concurrent: concurrentRatio,
        rate: rateRatio,
        trafficHour: trafficHourRatio,
        trafficDay: trafficDayRatio,
        max: Math.max(concurrentRatio, rateRatio, trafficHourRatio, trafficDayRatio),
      },
      lastExceeded: exceeded
        ? {
            dimension: exceeded.dimension,
            action: exceeded.action,
            at: exceeded.at,
            atIso: new Date(exceeded.at).toISOString(),
            retryAfterSec: exceeded.retryAfterSec,
          }
        : null,
    };
  }

  async listAllUsage() {
    const configuredIds = new Set(this.clientConfigs.keys());
    const activeIds = await this.store.getActiveClientIds();
    const allIds = new Set([...configuredIds, ...activeIds]);

    const result = [];
    for (const id of allIds) {
      result.push(await this.getUsage(id));
    }
    return result;
  }

  async cleanup(): Promise<void> {
    await this.store.cleanup();
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
