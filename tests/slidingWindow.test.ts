import { SlidingWindowCounter } from '../src/slidingWindow';

describe('SlidingWindowCounter', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(0);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('初始计数为0', () => {
    const counter = new SlidingWindowCounter(60000, 60);
    expect(counter.getCount()).toBe(0);
  });

  test('计数累加正确', () => {
    const counter = new SlidingWindowCounter(60000, 60);
    counter.increment(100);
    counter.increment(200);
    expect(counter.getCount()).toBe(300);
  });

  test('过期数据被清理', () => {
    const counter = new SlidingWindowCounter(60000, 60);
    counter.increment(500);
    expect(counter.getCount()).toBe(500);

    jest.advanceTimersByTime(60001);
    expect(counter.getCount()).toBe(0);
  });

  test('部分窗口内数据保留', () => {
    const counter = new SlidingWindowCounter(60000, 60);
    counter.increment(100);

    jest.advanceTimersByTime(30000);
    counter.increment(200);

    const { count, isPartial } = counter.getApproximateCount();
    expect(count).toBe(300);
    expect(isPartial).toBe(true);
  });

  test('近似计数精度可接受', () => {
    const windowMs = 60000;
    const bucketCount = 60;
    const counter = new SlidingWindowCounter(windowMs, bucketCount);

    counter.increment(100);
    jest.advanceTimersByTime(50000);
    counter.increment(200);

    const result = counter.getApproximateCount();
    expect(result.count).toBeGreaterThanOrEqual(200);
    expect(result.count).toBeLessThanOrEqual(300);
  });

  test('重置后计数为0', () => {
    const counter = new SlidingWindowCounter(60000, 60);
    counter.increment(1000);
    counter.reset();
    expect(counter.getCount()).toBe(0);
  });

  test('计算剩余到期时间', () => {
    const counter = new SlidingWindowCounter(60000, 60);
    counter.increment(100);

    jest.advanceTimersByTime(30000);
    const remaining = counter.getRemainingMs();
    expect(remaining).toBeGreaterThan(0);
    expect(remaining).toBeLessThanOrEqual(30000);
  });

  test('长时间无访问后所有数据过期', () => {
    const counter = new SlidingWindowCounter(1000, 10);
    counter.increment(100);
    jest.advanceTimersByTime(500);
    counter.increment(200);

    jest.advanceTimersByTime(2000);
    expect(counter.getCount()).toBe(0);
  });
});
