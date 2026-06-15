import { TokenBucket } from '../tokenBucket';
import { SlidingWindowCounter } from '../slidingWindow';

export interface ClientState {
  clientId: string;
  concurrentConnections: number;
  tokenBucket: TokenBucket;
  trafficHour: SlidingWindowCounter;
  trafficDay: SlidingWindowCounter;
  lastAccess: number;
}

export interface ConsumeRequestResult {
  allowed: boolean;
  blockedDimension?: string;
  throttleDelayMs?: number;
  concurrentCurrent: number;
  concurrentLimit: number;
  rateRemaining: number;
  rateLimit: number;
  rateResetMs: number;
  trafficHourRemaining: number;
  trafficHourLimit: number;
  trafficDayRemaining: number;
  trafficDayLimit: number;
  retryAfterSec?: number;
  maxUsageRatio: number;
}

export interface AcquireConnectionResult {
  acquired: boolean;
  current: number;
  limit: number;
}

export interface ReleaseConnectionResult {
  current: number;
}

export interface QuotaStore {
  readonly mode: 'local' | 'distributed';
  acquireConnection(clientId: string, limit: number): Promise<AcquireConnectionResult>;
  releaseConnection(clientId: string): Promise<ReleaseConnectionResult>;
  consumeRequest(
    clientId: string,
    rateLimit: number,
    rateBurst: number,
    bytesPerHour: number,
    bytesPerDay: number,
    payloadBytes?: number,
  ): Promise<ConsumeRequestResult>;
  peekRequest(
    clientId: string,
    rateLimit: number,
    rateBurst: number,
    bytesPerHour: number,
    bytesPerDay: number,
  ): Promise<ConsumeRequestResult>;
  getCurrentUsage(clientId: string): Promise<{
    concurrent: number;
    rateRemaining: number;
    trafficHourUsed: number;
    trafficDayUsed: number;
  }>;
  resetClient(clientId: string): Promise<void>;
  cleanup(): Promise<void>;
}
