import { LocalQuotaStore } from '../src/store/localStore';

describe('LocalQuotaStore', () => {
  let store: LocalQuotaStore;

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(0);
    store = new LocalQuotaStore();
  });

  afterEach(async () => {
    jest.useRealTimers();
    await store.cleanup();
  });

  describe('连接管理', () => {
    test('成功获取连接', async () => {
      const result = await store.acquireConnection('client-1', 5);
      expect(result.acquired).toBe(true);
      expect(result.current).toBe(1);
      expect(result.limit).toBe(5);
    });

    test('连接数达到上限后拒绝', async () => {
      for (let i = 0; i < 3; i++) {
        await store.acquireConnection('client-1', 3);
      }
      const result = await store.acquireConnection('client-1', 3);
      expect(result.acquired).toBe(false);
      expect(result.current).toBe(3);
    });

    test('释放连接后可以重新获取', async () => {
      await store.acquireConnection('client-1', 1);
      await store.releaseConnection('client-1');
      const result = await store.acquireConnection('client-1', 1);
      expect(result.acquired).toBe(true);
    });

    test('不同客户端隔离', async () => {
      await store.acquireConnection('client-a', 1);
      const result = await store.acquireConnection('client-b', 1);
      expect(result.acquired).toBe(true);
    });
  });

  describe('请求速率限制', () => {
    test('正常请求通过', async () => {
      const result = await store.consumeRequest('client-1', 100, 200, 1_000_000, 10_000_000);
      expect(result.allowed).toBe(true);
      expect(result.rateRemaining).toBe(199);
    });

    test('超过突发限制后被拒绝', async () => {
      for (let i = 0; i < 10; i++) {
        await store.consumeRequest('client-1', 5, 10, 1_000_000, 10_000_000);
      }
      const result = await store.consumeRequest('client-1', 5, 10, 1_000_000, 10_000_000);
      expect(result.allowed).toBe(false);
      expect(result.blockedDimension).toBe('requestsPerSecond');
      expect(result.retryAfterSec).toBeGreaterThan(0);
    });

    test('时间推移后令牌补充', async () => {
      for (let i = 0; i < 10; i++) {
        await store.consumeRequest('client-1', 10, 10, 1_000_000, 10_000_000);
      }

      jest.advanceTimersByTime(1000);

      const result = await store.consumeRequest('client-1', 10, 10, 1_000_000, 10_000_000);
      expect(result.allowed).toBe(true);
    });
  });

  describe('流量限制', () => {
    test('小时流量超限后被拒绝', async () => {
      await store.consumeRequest('client-1', 100, 200, 500, 10_000, 500);
      const result = await store.consumeRequest('client-1', 100, 200, 500, 10_000, 100);
      expect(result.allowed).toBe(false);
      expect(result.blockedDimension).toBe('bytesPerHour');
    });

    test('日流量超限后被拒绝', async () => {
      await store.consumeRequest('client-1', 100, 200, 10_000, 500, 500);
      const result = await store.consumeRequest('client-1', 100, 200, 10_000, 500, 100);
      expect(result.allowed).toBe(false);
      expect(result.blockedDimension).toBe('bytesPerDay');
    });

    test('使用比例正确计算', async () => {
      const result = await store.consumeRequest('client-1', 100, 200, 1_000_000, 10_000_000, 500_000);
      expect(result.maxUsageRatio).toBeGreaterThanOrEqual(0.5);
    });
  });

  describe('重置', () => {
    test('重置客户端状态', async () => {
      await store.acquireConnection('client-1', 5);
      await store.consumeRequest('client-1', 100, 200, 1_000_000, 10_000_000, 500_000);

      await store.resetClient('client-1');

      const usage = await store.getCurrentUsage('client-1');
      expect(usage.concurrent).toBe(0);
      expect(usage.trafficHourRemaining).toBe(0);
    });
  });

  test('mode 为 local', () => {
    expect(store.mode).toBe('local');
  });
});
