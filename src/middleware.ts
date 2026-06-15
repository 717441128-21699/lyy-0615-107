import { Request, Response, NextFunction } from 'express';
import { QuotaManager, sleep } from './quotaManager';
import { QuotaDimension } from './types';

export interface QuotaMiddlewareOptions {
  quotaManager: QuotaManager;
  clientIdExtractor?: (req: Request) => string;
  payloadBytesExtractor?: (req: Request) => number;
  onBlocked?: (req: Request, res: Response, dimension: QuotaDimension, retryAfterSec?: number) => void;
  onDegrade?: (req: Request, delayMs: number) => void;
  enableRetryAfterHeader?: boolean;
}

const BLOCK_MESSAGES: Record<QuotaDimension, string> = {
  concurrentConnections: '并发连接数已达上限，请稍后再试',
  requestsPerSecond: '请求频率过高，请稍后再试',
  bytesPerHour: '每小时流量已用尽，请稍后再试',
  bytesPerDay: '每日流量已用尽，请稍后再试',
};

function defaultClientIdExtractor(req: Request): string {
  const headerId = req.headers['x-client-id'] as string | undefined;
  if (headerId) return headerId;

  const apiKey = req.headers['x-api-key'] as string | undefined;
  if (apiKey) return `api:${apiKey}`;

  const ip = (req.headers['x-forwarded-for'] as string | undefined)?.split(',')[0].trim()
    || req.ip
    || (req.socket?.remoteAddress as string)
    || 'unknown';
  return `ip:${ip}`;
}

function defaultPayloadBytesExtractor(req: Request): number {
  const contentLength = req.headers['content-length'];
  if (contentLength) {
    return parseInt(contentLength, 10) || 0;
  }
  if (typeof req.body === 'string') {
    return Buffer.byteLength(req.body, 'utf-8');
  }
  return 0;
}

export interface QuotaStatusDetail {
  current: number;
  limit: number;
  remaining: number;
  ratio: number;
  resetSec?: number;
}

export interface QuotaStatus {
  concurrent: QuotaStatusDetail;
  rate: QuotaStatusDetail;
  trafficHour: QuotaStatusDetail;
  trafficDay: QuotaStatusDetail;
}

export interface QuotaErrorResponse {
  error: string;
  code: 'QUOTA_EXCEEDED';
  dimension: QuotaDimension;
  message: string;
  retryAfterSec: number;
  usageRatio: number;
  status: QuotaStatus;
  suggestion: string[];
  nextResetAt?: string;
}

export function buildQuotaStatusFromHeaders(
  headers: Record<string, string | number | undefined>,
): QuotaStatus {
  const concurrentCurrent = Number(headers['X-Quota-Concurrent-Current'] ?? 0);
  const concurrentLimit = Number(headers['X-Quota-Concurrent-Limit'] ?? 0);
  const rateRemaining = Number(headers['X-Quota-Rate-Remaining'] ?? 0);
  const rateLimit = Number(headers['X-Quota-Rate-Limit'] ?? 0);
  const rateReset = Number(headers['X-Quota-Rate-Reset'] ?? 0);
  const trafficHourRemaining = Number(headers['X-Quota-Traffic-Hour-Remaining'] ?? 0);
  const trafficHourLimit = Number(headers['X-Quota-Traffic-Hour-Limit'] ?? 0);
  const trafficDayRemaining = Number(headers['X-Quota-Traffic-Day-Remaining'] ?? 0);
  const trafficDayLimit = Number(headers['X-Quota-Traffic-Day-Limit'] ?? 0);

  const trafficHourCurrent = Math.max(0, trafficHourLimit - trafficHourRemaining);
  const trafficDayCurrent = Math.max(0, trafficDayLimit - trafficDayRemaining);

  return {
    concurrent: {
      current: concurrentCurrent,
      limit: concurrentLimit,
      remaining: Math.max(0, concurrentLimit - concurrentCurrent),
      ratio: concurrentLimit > 0 ? concurrentCurrent / concurrentLimit : 0,
    },
    rate: {
      current: rateLimit - rateRemaining,
      limit: rateLimit,
      remaining: rateRemaining,
      ratio: rateLimit > 0 ? (rateLimit - rateRemaining) / rateLimit : 0,
      resetSec: rateReset,
    },
    trafficHour: {
      current: trafficHourCurrent,
      limit: trafficHourLimit,
      remaining: trafficHourRemaining,
      ratio: trafficHourLimit > 0 ? trafficHourCurrent / trafficHourLimit : 0,
    },
    trafficDay: {
      current: trafficDayCurrent,
      limit: trafficDayLimit,
      remaining: trafficDayRemaining,
      ratio: trafficDayLimit > 0 ? trafficDayCurrent / trafficDayLimit : 0,
    },
  };
}

export function createQuotaMiddleware(options: QuotaMiddlewareOptions) {
  const {
    quotaManager,
    clientIdExtractor = defaultClientIdExtractor,
    payloadBytesExtractor = defaultPayloadBytesExtractor,
    onBlocked,
    onDegrade,
    enableRetryAfterHeader = true,
  } = options;

  return async function quotaMiddleware(
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> {
    const clientId = clientIdExtractor(req);
    const payloadBytes = payloadBytesExtractor(req);

    try {
      const result = await quotaManager.checkAndConsume(clientId, payloadBytes);

      const headers = result.headers as unknown as Record<string, string | number | undefined>;
      for (const [key, value] of Object.entries(headers)) {
        if (value !== undefined && value !== null) {
          res.setHeader(key, String(value));
        }
      }

      if (result.action === 'reject') {
        if (enableRetryAfterHeader && result.retryAfterSec) {
          res.setHeader('Retry-After', String(result.retryAfterSec));
        }

        if (onBlocked) {
          onBlocked(req, res, result.blockedDimension!, result.retryAfterSec);
          return;
        }

        const message = result.blockedDimension
          ? BLOCK_MESSAGES[result.blockedDimension]
          : '请求被拒绝，请稍后再试';

        const status = buildQuotaStatusFromHeaders(headers);
        const suggestion = buildSuggestion(result.blockedDimension, headers);
        const retryAfterSec = result.retryAfterSec ?? 1;

        const response: QuotaErrorResponse = {
          error: 'Too Many Requests',
          code: 'QUOTA_EXCEEDED',
          dimension: result.blockedDimension!,
          message,
          retryAfterSec,
          usageRatio: result.usageRatio,
          status,
          suggestion,
          nextResetAt: new Date(Date.now() + retryAfterSec * 1000).toISOString(),
        };

        res.status(429).json(response);
        return;
      }

      if (result.action === 'throttle' && result.throttleDelayMs && result.throttleDelayMs > 0) {
        if (onDegrade) {
          onDegrade(req, result.throttleDelayMs);
        }
        res.setHeader('X-Quota-Throttled', String(result.throttleDelayMs));
        await sleep(result.throttleDelayMs);
      }

      const originalEnd = res.end.bind(res) as any;
      let responseSize = 0;
      const originalWrite = res.write.bind(res) as any;

      (res as any).write = function (chunk: any, encoding?: any, cb?: any) {
        if (chunk) {
          responseSize += typeof chunk === 'string'
            ? Buffer.byteLength(chunk, typeof encoding === 'string' ? encoding as BufferEncoding : 'utf-8')
            : (Buffer.isBuffer(chunk) ? chunk.length : 0);
        }
        return originalWrite(chunk, encoding, cb);
      };

      (res as any).end = function (chunk: any, encoding?: any, cb?: any) {
        if (chunk) {
          responseSize += typeof chunk === 'string'
            ? Buffer.byteLength(chunk, typeof encoding === 'string' ? encoding as BufferEncoding : 'utf-8')
            : (Buffer.isBuffer(chunk) ? chunk.length : 0);
        }
        res.setHeader('X-Quota-Response-Bytes', String(responseSize));
        quotaManager.releaseConnection(clientId).catch(() => {});
        return originalEnd(chunk, encoding, cb);
      };

      next();
    } catch (error) {
      quotaManager.releaseConnection(clientId).catch(() => {});
      next(error);
    }
  };
}

function buildSuggestion(
  dimension: QuotaDimension | undefined,
  headers: Record<string, string | number | undefined>,
): string[] {
  const suggestions: string[] = [];

  switch (dimension) {
    case 'concurrentConnections':
      suggestions.push('减少并发连接数，使用连接池复用连接');
      suggestions.push('考虑使用批量接口合并多个请求');
      break;
    case 'requestsPerSecond':
      suggestions.push(`请降低请求频率，限制为每秒 ${headers['X-Quota-Rate-Limit']} 次`);
      suggestions.push(`在 ${headers['X-Quota-Rate-Reset']} 秒后速率限制将重置`);
      suggestions.push('考虑使用指数退避算法重试');
      break;
    case 'bytesPerHour':
      suggestions.push('减少请求/响应的数据量，启用压缩');
      suggestions.push('等待下一个小时周期重置后再发起请求');
      break;
    case 'bytesPerDay':
      suggestions.push('减少大流量请求，联系管理员提升配额');
      suggestions.push('等待次日流量配额重置');
      break;
    default:
      suggestions.push('请稍后重试');
  }

  return suggestions;
}
