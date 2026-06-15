import express from 'express';
import { QuotaManager } from './quotaManager';
import { LocalQuotaStore } from './store/localStore';
import { createQuotaMiddleware } from './middleware';

async function main() {
  const app = express();
  app.use(express.json());

  const store = new LocalQuotaStore();

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

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', mode: quotaManager.getStoreMode() });
  });

  app.get('/api/usage', async (req, res) => {
    const clientId = (req.headers['x-client-id'] as string) ?? 'default';
    const info = await quotaManager.getUsage(clientId);
    res.json(info);
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
    console.log(`  GET  http://localhost:${PORT}/health         - 健康检查`);
    console.log(`  GET  http://localhost:${PORT}/api/usage      - 查看配额使用情况`);
    console.log(`  GET  http://localhost:${PORT}/api/data       - 获取数据（受配额限制）`);
    console.log(`  POST http://localhost:${PORT}/api/upload     - 上传数据（计入流量）`);
    console.log(`\n测试示例:`);
    console.log(`  curl -H "X-Client-Id: free-client" -H "X-Client-Tier: free" http://localhost:${PORT}/api/data`);
    console.log(`  curl -H "X-Client-Id: vip-client"  -H "X-Client-Tier: vip"  http://localhost:${PORT}/api/data`);
    console.log(``);
  });
}

main().catch(console.error);
