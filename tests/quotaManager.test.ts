import { QuotaManager } from '../src/quotaManager';
import { LocalQuotaStore } from '../src/store/localStore';
import { QuotaAction } from '../src/types';

describe('QuotaManager', () => {
  let manager: QuotaManager;
  let store: LocalQuotaStore;

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(0);
    store = new LocalQuotaStore();
    manager = new QuotaManager({
      store,
      strategyConfig: {
        throttleThreshold: 0.7,
        rejectThreshold: 1.0,
        throttleMaxDelayMs: 1000,
        enableDegradation: true,
      },
    });
  });

  afterEach(async () => {
    jest.useRealTimers();
    await manager.cleanup();
  });

  describe('多维度配额验证', () => {
    test('并发连接数限制', async () => {
      manager.registerClient({
        clientId: 'test-client',
        maxConcurrentConnections: 2,
        requestsPerSecond: 100,
        requestsBurst: 100,
        bytesPerHour: 10_000_000,
        bytesPerDay: 100_000_000,
      });

      await manager.checkAndConsume('test-client');
      await manager.checkAndConsume('test-client');

      const result = await manager.checkAndConsume('test-client');
      expect(result.allowed).toBe(false);
      expect(result.blockedDimension).toBe('concurrentConnections');
      expect(result.action).toBe('reject');
    });

    test('请求速率限制', async () => {
      manager.registerClient({
        clientId: 'test-client',
        maxConcurrentConnections: 100,
        requestsPerSecond: 5,
        requestsBurst: 5,
        bytesPerHour: 10_000_000,
        bytesPerDay: 100_000_000,
      });

      for (let i = 0; i < 5; i++) {
        const r = await manager.checkAndConsume('test-client');
        expect(r.allowed).toBe(true);
        await manager.releaseConnection('test-client');
      }

      const result = await manager.checkAndConsume('test-client');
      expect(result.allowed).toBe(false);
      expect(result.blockedDimension).toBe('requestsPerSecond');
    });

    test('累计小时流量限制', async () => {
      manager.registerClient({
        clientId: 'test-client',
        maxConcurrentConnections: 100,
        requestsPerSecond: 100,
        requestsBurst: 100,
        bytesPerHour: 1000,
        bytesPerDay: 100_000_000,
      });

      let r = await manager.checkAndConsume('test-client', 600);
      expect(r.allowed).toBe(true);
      await manager.releaseConnection('test-client');

      r = await manager.checkAndConsume('test-client', 500);
      expect(r.allowed).toBe(false);
      expect(r.blockedDimension).toBe('bytesPerHour');
    });

    test('累计日流量限制', async () => {
      manager.registerClient({
        clientId: 'test-client',
        maxConcurrentConnections: 100,
        requestsPerSecond: 100,
        requestsBurst: 100,
        bytesPerHour: 100_000_000,
        bytesPerDay: 1000,
      });

      let r = await manager.checkAndConsume('test-client', 600);
      expect(r.allowed).toBe(true);
      await manager.releaseConnection('test-client');

      r = await manager.checkAndConsume('test-client', 500);
      expect(r.allowed).toBe(false);
      expect(r.blockedDimension).toBe('bytesPerDay');
    });
  });

  describe('降级策略', () => {
    test('使用比例超过阈值后触发降级延迟', async () => {
      manager.registerClient({
        clientId: 'test-client',
        maxConcurrentConnections: 10,
        requestsPerSecond: 100,
        requestsBurst: 10,
        bytesPerHour: 10_000_000,
        bytesPerDay: 100_000_000,
      });

      for (let i = 0; i < 8; i++) {
        const r = await manager.checkAndConsume('test-client');
        await manager.releaseConnection('test-client');
      }

      const result = await manager.checkAndConsume('test-client');
      expect(result.allowed).toBe(true);
      expect(result.action).toBe('throttle');
      expect(result.throttleDelayMs).toBeGreaterThan(0);
    });

    test('禁用降级后直接拒绝', async () => {
      const strictManager = new QuotaManager({
        store: new LocalQuotaStore(),
        strategyConfig: {
          throttleThreshold: 1.0,
          rejectThreshold: 1.0,
          throttleMaxDelayMs: 0,
          enableDegradation: false,
        },
      });

      strictManager.registerClient({
        clientId: 'test',
        maxConcurrentConnections: 5,
        requestsPerSecond: 5,
        requestsBurst: 5,
        bytesPerHour: 1_000_000,
        bytesPerDay: 10_000_000,
      });

      for (let i = 0; i < 5; i++) {
        const r = await strictManager.checkAndConsume('test');
        await strictManager.releaseConnection('test');
      }

      const result = await strictManager.checkAndConsume('test');
      expect(result.allowed).toBe(false);
      expect(result.action).toBe('reject');
      await strictManager.cleanup();
    });
  });

  describe('响应头', () => {
    test('响应头包含配额信息', async () => {
      manager.registerClient({
        clientId: 'test-client',
        maxConcurrentConnections: 10,
        requestsPerSecond: 50,
        requestsBurst: 100,
        bytesPerHour: 1_000_000,
        bytesPerDay: 10_000_000,
      });

      const result = await manager.checkAndConsume('test-client', 1000);
      expect(result.headers['X-Quota-Client-Id']).toBe('test-client');
      expect(result.headers['X-Quota-Concurrent-Current']).toBe(1);
      expect(result.headers['X-Quota-Concurrent-Limit']).toBe(10);
      expect(result.headers['X-Quota-Rate-Remaining']).toBeLessThan(100);
      expect(result.headers['X-Quota-Rate-Limit']).toBe(50);
      expect(typeof result.headers['X-Quota-Traffic-Hour-Remaining']).toBe('number');
      expect(typeof result.headers['X-Quota-Traffic-Day-Remaining']).toBe('number');
    });

    test('拒绝时包含 Retry-After', async () => {
      manager.registerClient({
        clientId: 'test',
        maxConcurrentConnections: 1,
        requestsPerSecond: 1,
        requestsBurst: 1,
        bytesPerHour: 1,
        bytesPerDay: 10,
      });

      const r = await manager.checkAndConsume('test');
      await manager.releaseConnection('test');
      const result = await manager.checkAndConsume('test', 2);
      expect(result.retryAfterSec).toBeDefined();
      expect(result.headers['Retry-After']).toBeDefined();
    });
  });

  describe('回调事件', () => {
    test('超限触发 onQuotaExceeded 回调', async () => {
      let triggered = false;
      let triggeredDimension: string | undefined;
      let triggeredAction: QuotaAction | undefined;

      const m = new QuotaManager({
        store: new LocalQuotaStore(),
        onQuotaExceeded: (clientId, dimension, action) => {
          triggered = true;
          triggeredDimension = dimension;
          triggeredAction = action;
        },
      });

      m.registerClient({
        clientId: 'cb-test',
        maxConcurrentConnections: 1,
        requestsPerSecond: 100,
        requestsBurst: 100,
        bytesPerHour: 1_000,
        bytesPerDay: 10_000,
      });

      await m.checkAndConsume('cb-test');
      await m.checkAndConsume('cb-test');

      expect(triggered).toBe(true);
      expect(triggeredDimension).toBe('concurrentConnections');
      expect(triggeredAction).toBe('reject');
      await m.cleanup();
    });
  });

  test('使用默认配置处理未注册客户端', async () => {
    const result = await manager.checkAndConsume('unknown-client');
    expect(result.allowed).toBe(true);
    expect(result.headers['X-Quota-Client-Id']).toBe('unknown-client');
  });

  test('getStoreMode 返回正确模式', () => {
    expect(manager.getStoreMode()).toBe('local');
  });
});
