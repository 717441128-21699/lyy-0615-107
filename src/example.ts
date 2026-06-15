import express from 'express';
import { QuotaManager, UpdateQuotaPatch } from './quotaManager';
import { LocalQuotaStore } from './store/localStore';
import { RedisQuotaStore } from './store/redisStore';
import { createQuotaMiddleware, QuotaErrorResponse } from './middleware';
import { QuotaStore } from './store/types';

function createStore(): QuotaStore {
  const mode = (process.env.QUOTA_STORE_MODE ?? 'local').toLowerCase();

  if (mode === 'redis' || mode === 'distributed') {
    const redisUrl = process.env.REDIS_URL;
    const keyPrefix = process.env.REDIS_KEY_PREFIX ?? 'quota';

    console.log(`[Quota Store] 使用 Redis 分布式模式`);
    if (redisUrl) {
      console.log(`[Quota Store] Redis URL: ${redisUrl}`);
    }
    console.log(`[Quota Store] Key Prefix: ${keyPrefix}`);

    return new RedisQuotaStore({
      redisOptions: redisUrl ? undefined : {
        host: process.env.REDIS_HOST ?? 'localhost',
        port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
        password: process.env.REDIS_PASSWORD,
        db: parseInt(process.env.REDIS_DB ?? '0', 10),
      },
      redisClient: redisUrl ? undefined : undefined,
      keyPrefix,
      localCacheTtlMs: parseInt(process.env.REDIS_CACHE_TTL_MS ?? '50', 10),
    });
  }

  console.log(`[Quota Store] 使用本地内存模式`);
  return new LocalQuotaStore();
}

async function main() {
  const app = express();
  app.use(express.json());

  const store = createStore();

  const quotaManager = new QuotaManager({
    store,
    strategyConfig: {
      throttleThreshold: 0.7,
      rejectThreshold: 1.0,
      throttleMaxDelayMs: 1500,
      enableDegradation: true,
    },
    onQuotaExceeded: (clientId, dimension, action) => {
      console.log(`[Quota Exceeded] client=${clientId} dimension=${dimension} action=${action}`);
    },
    onDegrade: (clientId, usageRatio, delayMs) => {
      console.log(`[Quota Degrade] client=${clientId} ratio=${usageRatio.toFixed(2)} delay=${delayMs}ms`);
    },
  });

  quotaManager.registerClient({
    clientId: 'vip-client',
    maxConcurrentConnections: 200,
    requestsPerSecond: 200,
    requestsBurst: 500,
    bytesPerHour: 500 * 1024 * 1024,
    bytesPerDay: 10 * 1024 * 1024 * 1024,
  });

  quotaManager.registerClient({
    clientId: 'free-client',
    maxConcurrentConnections: 10,
    requestsPerSecond: 10,
    requestsBurst: 30,
    bytesPerHour: 10 * 1024 * 1024,
    bytesPerDay: 100 * 1024 * 1024,
  });

  const quotaMiddleware = createQuotaMiddleware({
    quotaManager,
    clientIdExtractor: (req) => {
      const tier = req.headers['x-client-tier'] as string | undefined;
      const id = req.headers['x-client-id'] as string | undefined;
      if (tier === 'vip') return 'vip-client';
      if (tier === 'free') return 'free-client';
      return id ?? 'default';
    },
  });

  function extractClientId(req: express.Request): string {
    const tier = req.headers['x-client-tier'] as string | undefined;
    const id = req.headers['x-client-id'] as string | undefined;
    if (tier === 'vip') return 'vip-client';
    if (tier === 'free') return 'free-client';
    return id ?? 'default';
  }

  async function setQuotaHeaders(res: express.Response, clientId: string): Promise<void> {
    try {
      const result = await quotaManager.peekUsage(clientId);
      const headers = result.headers as unknown as Record<string, string | number | undefined>;
      for (const [key, value] of Object.entries(headers)) {
        if (value !== undefined && value !== null && key !== 'Retry-After') {
          res.setHeader(key, String(value));
        }
      }
    } catch {
    }
  }

  app.get('/health', async (req, res) => {
    const clientId = extractClientId(req);
    await setQuotaHeaders(res, clientId);
    res.json({
      status: 'ok',
      mode: quotaManager.getStoreMode(),
      clientId,
    });
  });

  app.get('/api/usage', async (req, res) => {
    const clientId = extractClientId(req);
    const info = await quotaManager.getUsage(clientId);
    res.json(info);
  });

  app.patch('/admin/quota/:clientId', async (req, res) => {
    const clientId = req.params.clientId;
    const patch = req.body as UpdateQuotaPatch;

    const validKeys: (keyof UpdateQuotaPatch)[] = [
      'maxConcurrentConnections',
      'requestsPerSecond',
      'requestsBurst',
      'bytesPerHour',
      'bytesPerDay',
    ];

    const cleanedPatch: UpdateQuotaPatch = {};
    for (const key of validKeys) {
      if (patch[key] !== undefined && typeof patch[key] === 'number' && patch[key]! >= 0) {
        (cleanedPatch as any)[key] = patch[key];
      }
    }

    if (Object.keys(cleanedPatch).length === 0) {
      res.status(400).json({
        error: 'Bad Request',
        message: '无效的配额配置项，支持的字段：maxConcurrentConnections, requestsPerSecond, requestsBurst, bytesPerHour, bytesPerDay',
      });
      return;
    }

    const updatedConfig = quotaManager.updateClientConfig(clientId, cleanedPatch);
    const usage = await quotaManager.getUsage(clientId);

    res.json({
      message: '配额配置已更新',
      clientId,
      config: updatedConfig,
      currentUsage: usage,
    });
  });

  app.get('/admin/quota/:clientId', async (req, res) => {
    const clientId = req.params.clientId;
    const config = quotaManager.getClientConfig(clientId);
    const usage = await quotaManager.getUsage(clientId);
    res.json({
      clientId,
      config,
      usage,
    });
  });

  app.use('/api', quotaMiddleware);

  app.get('/api/data', (_req, res) => {
    const data = Array.from({ length: 100 }, (_, i) => ({
      id: i,
      value: Math.random(),
      timestamp: Date.now(),
    }));
    res.json({ data, count: data.length });
  });

  app.post('/api/upload', (req, res) => {
    const size = JSON.stringify(req.body).length;
    res.json({
      status: 'received',
      receivedBytes: size,
      message: `已接收 ${size} 字节数据`,
    });
  });

  const PORT = process.env.PORT ?? 3000;
  app.listen(PORT, () => {
    console.log(`\n=== 配额管理系统示例服务已启动 ===`);
    console.log(`服务地址: http://localhost:${PORT}`);
    console.log(`存储模式: ${quotaManager.getStoreMode()}`);
    console.log(`\n测试接口:`);
    console.log(`  GET    http://localhost:${PORT}/health              - 健康检查（带配额状态，不消耗配额）`);
    console.log(`  GET    http://localhost:${PORT}/api/usage           - 查看配额使用情况`);
    console.log(`  GET    http://localhost:${PORT}/api/data            - 获取数据（受配额限制）`);
    console.log(`  POST   http://localhost:${PORT}/api/upload          - 上传数据（计入流量）`);
    console.log(`  GET    http://localhost:${PORT}/admin/quota/:id     - 查看客户端配额配置`);
    console.log(`  PATCH  http://localhost:${PORT}/admin/quota/:id     - 调整客户端配额配置`);
    console.log(`\n环境变量:`);
    console.log(`  QUOTA_STORE_MODE=local|redis    - 存储模式（默认 local）`);
    console.log(`  REDIS_URL=redis://...           - Redis 连接 URL`);
    console.log(`  REDIS_HOST/REDIS_PORT/...       - Redis 单独配置`);
    console.log(`  PORT=3000                       - 服务端口`);
    console.log(`\n测试示例:`);
    console.log(`  # 健康检查（不消耗配额，可看到当前配额状态）`);
    console.log(`  curl -i -H "X-Client-Id: free-client" -H "X-Client-Tier: free" http://localhost:${PORT}/health`);
    console.log(`  `);
    console.log(`  # 正常请求（消耗配额）`);
    console.log(`  curl -i -H "X-Client-Id: free-client" -H "X-Client-Tier: free" http://localhost:${PORT}/api/data`);
    console.log(`  `);
    console.log(`  # 调整 free-client 的 QPS 为 2`);
    console.log(`  curl -X PATCH -H "Content-Type: application/json" -d '{"requestsPerSecond":2,"requestsBurst":5}' http://localhost:${PORT}/admin/quota/free-client`);
    console.log(`  `);
    console.log(`  # 压测触发 429，查看完整超限信息`);
    console.log(`  for i in {1..20}; do curl -s -o /dev/null -w "%{http_code} " -H "X-Client-Id: free-client" -H "X-Client-Tier: free" http://localhost:${PORT}/api/data; done; echo`);
    console.log(``);
  });
}

main().catch(console.error);
