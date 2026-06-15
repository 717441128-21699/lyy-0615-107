import express from 'express';
import { QuotaManager, UpdateQuotaPatch, QuotaTemplate, TEMPLATES, AuditEventType } from './quotaManager';
import { LocalQuotaStore } from './store/localStore';
import { RedisQuotaStore } from './store/redisStore';
import { createQuotaMiddleware } from './middleware';
import { QuotaStore } from './store/types';

function createStore(): QuotaStore {
  const mode = (process.env.QUOTA_STORE_MODE ?? 'local').toLowerCase();

  if (mode === 'redis' || mode === 'distributed') {
    const redisUrl = process.env.REDIS_URL;
    const keyPrefix = process.env.REDIS_KEY_PREFIX ?? 'quota';

    console.log(`[Quota Store] 使用 Redis 分布式模式`);
    if (redisUrl) {
      console.log(`[Quota Store] Redis URL: ${redisUrl}`);
    } else {
      console.log(`[Quota Store] Redis Host: ${process.env.REDIS_HOST ?? 'localhost'}:${process.env.REDIS_PORT ?? '6379'}`);
    }
    console.log(`[Quota Store] Key Prefix: ${keyPrefix}`);

    return new RedisQuotaStore({
      redisUrl,
      redisOptions: redisUrl ? undefined : {
        host: process.env.REDIS_HOST ?? 'localhost',
        port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
        password: process.env.REDIS_PASSWORD,
        db: parseInt(process.env.REDIS_DB ?? '0', 10),
      },
      keyPrefix,
      localCacheTtlMs: parseInt(process.env.REDIS_CACHE_TTL_MS ?? '50', 10),
    });
  }

  console.log(`[Quota Store] 使用本地内存模式`);
  return new LocalQuotaStore();
}

function getInstanceId(): string {
  return process.env.INSTANCE_ID ?? `instance-${process.pid}`;
}

async function main() {
  const app = express();
  app.use(express.json());

  const store = createStore();
  const instanceId = getInstanceId();

  const quotaManager = new QuotaManager({
    store,
    strategyConfig: {
      throttleThreshold: 0.7,
      rejectThreshold: 1.0,
      throttleMaxDelayMs: 1500,
      enableDegradation: true,
    },
    onQuotaExceeded: (clientId, dimension, action) => {
      console.log(`[${instanceId}] [Quota Exceeded] client=${clientId} dimension=${dimension} action=${action}`);
    },
    onDegrade: (clientId, usageRatio, delayMs) => {
      console.log(`[${instanceId}] [Quota Degrade] client=${clientId} ratio=${usageRatio.toFixed(2)} delay=${delayMs}ms`);
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

  app.use((req, res, next) => {
    res.setHeader('X-Instance-Id', instanceId);
    res.setHeader('X-Quota-Store-Mode', quotaManager.getStoreMode());
    next();
  });

  app.get('/health', async (req, res) => {
    const clientId = extractClientId(req);
    await setQuotaHeaders(res, clientId);
    res.json({
      status: 'ok',
      mode: quotaManager.getStoreMode(),
      instanceId,
      clientId,
    });
  });

  app.get('/api/usage', async (req, res) => {
    const clientId = extractClientId(req);
    const info = await quotaManager.getUsage(clientId);
    res.json({ ...info, instanceId });
  });

  app.get('/admin/quota', async (req, res) => {
    const all = await quotaManager.listAllUsage();
    res.json({
      instanceId,
      mode: quotaManager.getStoreMode(),
      total: all.length,
      clients: all,
    });
  });

  app.get('/admin/quota/:clientId', async (req, res) => {
    const clientId = req.params.clientId;
    const info = await quotaManager.getUsage(clientId);
    res.json({ ...info, instanceId });
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
      instanceId,
      clientId,
      config: updatedConfig,
      currentUsage: usage,
    });
  });

  app.post('/admin/quota/:clientId/apply-template', async (req, res) => {
    const clientId = req.params.clientId;
    const { template, custom } = req.body as { template?: QuotaTemplate | 'custom'; custom?: UpdateQuotaPatch };

    const validTemplates: (QuotaTemplate | 'custom')[] = ['free', 'vip', 'default', 'custom'];
    if (!template || !validTemplates.includes(template)) {
      res.status(400).json({
        error: 'Bad Request',
        message: `无效的模板类型，支持：${validTemplates.join(', ')}`,
        availableTemplates: {
          free: TEMPLATES.free,
          vip: TEMPLATES.vip,
          default: TEMPLATES.default,
          custom: '基于当前配置叠加 custom 字段',
        },
      });
      return;
    }

    const updatedConfig = quotaManager.applyTemplate(clientId, template, custom);
    const usage = await quotaManager.getUsage(clientId);

    res.json({
      message: `已套用模板: ${template}`,
      instanceId,
      clientId,
      appliedTemplate: template,
      config: updatedConfig,
      currentUsage: usage,
    });
  });

  app.post('/admin/quota/:clientId/reset', async (req, res) => {
    const clientId = req.params.clientId;
    await quotaManager.resetUsage(clientId);
    const usage = await quotaManager.getUsage(clientId);
    res.json({
      message: '配额使用量已重置',
      instanceId,
      clientId,
      currentUsage: usage,
    });
  });

  app.get('/admin/templates', (_req, res) => {
    res.json({
      instanceId,
      templates: TEMPLATES,
    });
  });

  app.get('/admin/audit', (req, res) => {
    const clientId = req.query.clientId as string | undefined;
    const event = req.query.event as AuditEventType | undefined;
    const limit = Math.min(parseInt(req.query.limit as string ?? '50', 10), 200);
    const offset = parseInt(req.query.offset as string ?? '0', 10);

    const entries = quotaManager.getAuditLog({ clientId, event, limit, offset });

    res.json({
      instanceId,
      total: entries.length,
      offset,
      limit,
      entries,
    });
  });

  app.post('/admin/batch/apply-template', async (req, res) => {
    const { clientIds, template, custom } = req.body as {
      clientIds?: string[];
      template?: QuotaTemplate | 'custom';
      custom?: UpdateQuotaPatch;
    };

    if (!Array.isArray(clientIds) || clientIds.length === 0) {
      res.status(400).json({
        error: 'Bad Request',
        message: 'clientIds 必须是非空字符串数组',
      });
      return;
    }

    const validTemplates: (QuotaTemplate | 'custom')[] = ['free', 'vip', 'default', 'custom'];
    if (!template || !validTemplates.includes(template)) {
      res.status(400).json({
        error: 'Bad Request',
        message: `无效的模板类型，支持：${validTemplates.join(', ')}`,
      });
      return;
    }

    const results = [];
    for (const clientId of clientIds) {
      try {
        const updatedConfig = quotaManager.applyTemplate(clientId, template, custom);
        const usage = await quotaManager.getUsage(clientId);
        results.push({
          clientId,
          success: true,
          config: updatedConfig,
          currentUsage: usage,
        });
      } catch (e: any) {
        results.push({
          clientId,
          success: false,
          error: e.message ?? String(e),
        });
      }
    }

    res.json({
      message: '批量套用模板完成',
      instanceId,
      template,
      total: clientIds.length,
      succeeded: results.filter(r => r.success).length,
      failed: results.filter(r => !r.success).length,
      results,
    });
  });

  app.post('/admin/batch/reset', async (req, res) => {
    const { clientIds } = req.body as { clientIds?: string[] };

    if (!Array.isArray(clientIds) || clientIds.length === 0) {
      res.status(400).json({
        error: 'Bad Request',
        message: 'clientIds 必须是非空字符串数组',
      });
      return;
    }

    const results = [];
    for (const clientId of clientIds) {
      try {
        await quotaManager.resetUsage(clientId);
        const usage = await quotaManager.getUsage(clientId);
        results.push({
          clientId,
          success: true,
          currentUsage: usage,
        });
      } catch (e: any) {
        results.push({
          clientId,
          success: false,
          error: e.message ?? String(e),
        });
      }
    }

    res.json({
      message: '批量重置完成',
      instanceId,
      total: clientIds.length,
      succeeded: results.filter(r => r.success).length,
      failed: results.filter(r => !r.success).length,
      results,
    });
  });

  app.get('/admin/export', async (req, res) => {
    const format = (req.query.format as string ?? 'json').toLowerCase();
    const all = await quotaManager.listAllUsage();

    const exportData = all.map(c => ({
      instanceId,
      storeMode: quotaManager.getStoreMode(),
      clientId: c.clientId,
      maxConcurrentConnections: c.config.maxConcurrentConnections,
      requestsPerSecond: c.config.requestsPerSecond,
      requestsBurst: c.config.requestsBurst,
      bytesPerHour: c.config.bytesPerHour,
      bytesPerDay: c.config.bytesPerDay,
      concurrentUsed: c.usage.concurrent,
      concurrentRemaining: c.remaining.concurrent,
      rateRemaining: c.remaining.rate,
      trafficHourUsed: c.usage.trafficHourUsed,
      trafficHourRemaining: c.remaining.trafficHour,
      trafficDayUsed: c.usage.trafficDayUsed,
      trafficDayRemaining: c.remaining.trafficDay,
      maxRatio: c.ratios.max,
      lastExceededDimension: c.lastExceeded?.dimension ?? '',
      lastExceededAt: c.lastExceeded?.atIso ?? '',
      lastExceededRetryAfter: c.lastExceeded?.retryAfterSec ?? '',
    }));

    if (format === 'csv') {
      if (exportData.length === 0) {
        res.setHeader('Content-Type', 'text/csv');
        res.send('');
        return;
      }
      const headers = Object.keys(exportData[0]);
      const csvRows = [
        headers.join(','),
        ...exportData.map(row =>
          headers.map(h => {
            const val = (row as any)[h];
            const str = String(val ?? '');
            return str.includes(',') || str.includes('"') || str.includes('\n')
              ? `"${str.replace(/"/g, '""')}"`
              : str;
          }).join(','),
        ),
      ];
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename="quota-export.csv"');
      res.send('\uFEFF' + csvRows.join('\n'));
      return;
    }

    res.setHeader('Content-Disposition', 'attachment; filename="quota-export.json"');
    res.json({
      exportedAt: new Date().toISOString(),
      instanceId,
      storeMode: quotaManager.getStoreMode(),
      totalClients: exportData.length,
      clients: exportData,
    });
  });

  app.use('/api', quotaMiddleware);

  app.get('/api/data', (_req, res) => {
    const data = Array.from({ length: 100 }, (_, i) => ({
      id: i,
      value: Math.random(),
      timestamp: Date.now(),
    }));
    res.json({ data, count: data.length, instanceId });
  });

  app.post('/api/upload', (req, res) => {
    const size = JSON.stringify(req.body).length;
    res.json({
      status: 'received',
      receivedBytes: size,
      message: `已接收 ${size} 字节数据`,
      instanceId,
    });
  });

  const PORT = process.env.PORT ?? 3000;
  app.listen(PORT, () => {
    console.log(`\n=== 配额管理系统示例服务已启动 ===`);
    console.log(`实例 ID: ${instanceId}`);
    console.log(`服务地址: http://localhost:${PORT}`);
    console.log(`存储模式: ${quotaManager.getStoreMode()}`);
    console.log(`\n公共接口:`);
    console.log(`  GET    http://localhost:${PORT}/health                          - 健康检查（带配额状态，不消耗配额）`);
    console.log(`  GET    http://localhost:${PORT}/api/usage                       - 查看当前客户端配额使用情况`);
    console.log(`  GET    http://localhost:${PORT}/api/data                        - 获取数据（消耗配额+流量含响应体）`);
    console.log(`  POST   http://localhost:${PORT}/api/upload                      - 上传数据（计入流量）`);
    console.log(`\n管理接口:`);
    console.log(`  GET    http://localhost:${PORT}/admin/quota                     - 所有客户端配额汇总（含自动发现的客户端）`);
    console.log(`  GET    http://localhost:${PORT}/admin/quota/:id                 - 查看单个客户端配额详情`);
    console.log(`  PATCH  http://localhost:${PORT}/admin/quota/:id                 - 调整配额（并发/速率/流量）`);
    console.log(`  POST   http://localhost:${PORT}/admin/quota/:id/apply-template  - 套用 free/vip/default/custom 模板`);
    console.log(`  POST   http://localhost:${PORT}/admin/quota/:id/reset           - 重置客户端使用量`);
    console.log(`  GET    http://localhost:${PORT}/admin/templates                 - 查看预设配额模板`);
    console.log(`  GET    http://localhost:${PORT}/admin/audit                     - 审计流水（?clientId=&event=&limit=&offset=）`);
    console.log(`  POST   http://localhost:${PORT}/admin/batch/apply-template      - 批量套用模板`);
    console.log(`  POST   http://localhost:${PORT}/admin/batch/reset               - 批量重置用量`);
    console.log(`  GET    http://localhost:${PORT}/admin/export?format=json|csv    - 导出配额状态`);
    console.log(`\n环境变量:`);
    console.log(`  QUOTA_STORE_MODE=local|redis    - 存储模式（默认 local）`);
    console.log(`  REDIS_URL=redis://...           - Redis 连接 URL（优先）`);
    console.log(`  REDIS_HOST/REDIS_PORT/...       - Redis 单独配置`);
    console.log(`  REDIS_KEY_PREFIX=quota          - Redis key 前缀（多实例共用时需相同）`);
    console.log(`  INSTANCE_ID=server-1            - 实例标识（默认 instance-<pid>）`);
    console.log(`  PORT=3000                       - 服务端口`);
    console.log(`\nRedis 多实例演示:`);
    console.log(`  # 终端1: REDIS_URL=redis://localhost:6379 REDIS_KEY_PREFIX=demo INSTANCE_ID=server-a PORT=3001 node dist/example.js`);
    console.log(`  # 终端2: REDIS_URL=redis://localhost:6379 REDIS_KEY_PREFIX=demo INSTANCE_ID=server-b PORT=3002 node dist/example.js`);
    console.log(`  # 交替访问 → 配额一起消耗:`);
    console.log(`  curl -s http://localhost:3001/api/data -H "X-Client-Id: free-client" -H "X-Client-Tier: free"`);
    console.log(`  curl -s http://localhost:3002/api/usage -H "X-Client-Id: free-client" -H "X-Client-Tier: free"`);
    console.log(``);
  });
}

main().catch(console.error);
