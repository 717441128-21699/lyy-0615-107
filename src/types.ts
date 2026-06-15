export interface ClientQuotaConfig {
  clientId: string;
  maxConcurrentConnections: number;
  requestsPerSecond: number;
  requestsBurst: number;
  bytesPerHour: number;
  bytesPerDay: number;
}

export interface QuotaUsage {
  concurrentConnections: number;
  requestsLastMinute: number;
  bytesLastHour: number;
  bytesLastDay: number;
}

export type QuotaDimension =
  | 'concurrentConnections'
  | 'requestsPerSecond'
  | 'bytesPerHour'
  | 'bytesPerDay';

export interface QuotaCheckResult {
  allowed: boolean;
  blockedDimension?: QuotaDimension;
  throttleDelayMs?: number;
  usageRatio: number;
  retryAfterSec?: number;
  headers: QuotaResponseHeaders;
}

export interface QuotaResponseHeaders {
  'X-Quota-Client-Id': string;
  'X-Quota-Concurrent-Current': number;
  'X-Quota-Concurrent-Limit': number;
  'X-Quota-Rate-Remaining': number;
  'X-Quota-Rate-Limit': number;
  'X-Quota-Rate-Reset': number;
  'X-Quota-Traffic-Hour-Remaining': number;
  'X-Quota-Traffic-Day-Remaining': number;
  'Retry-After'?: number;
}

export type QuotaAction = 'allow' | 'throttle' | 'reject';

export interface QuotaStrategyConfig {
  throttleThreshold: number;
  rejectThreshold: number;
  throttleMaxDelayMs: number;
  enableDegradation: boolean;
}

export const DEFAULT_STRATEGY_CONFIG: QuotaStrategyConfig = {
  throttleThreshold: 0.8,
  rejectThreshold: 1.0,
  throttleMaxDelayMs: 2000,
  enableDegradation: true,
};

export const DEFAULT_QUOTA_CONFIG: Omit<ClientQuotaConfig, 'clientId'> = {
  maxConcurrentConnections: 100,
  requestsPerSecond: 50,
  requestsBurst: 100,
  bytesPerHour: 100 * 1024 * 1024,
  bytesPerDay: 1024 * 1024 * 1024,
};
