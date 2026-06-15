import { TokenBucket } from '../src/tokenBucket';

describe('TokenBucket', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(0);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('初始时令牌桶装满', () => {
    const bucket = new TokenBucket(100, 10);
    expect(bucket.peekRemaining()).toBe(100);
  });

  test('正常消费令牌', () => {
    const bucket = new TokenBucket(100, 10);
    const result = bucket.tryConsume(50);
    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(50);
    expect(result.waitTimeMs).toBe(0);
  });

  test('令牌不足时消费失败并返回等待时间', () => {
    const bucket = new TokenBucket(10, 5);
    bucket.tryConsume(8);
    const result = bucket.tryConsume(5);
    expect(result.allowed).toBe(false);
    expect(result.waitTimeMs).toBeGreaterThan(0);
    expect(result.remaining).toBe(2);
  });

  test('按时间正确补充令牌', () => {
    const bucket = new TokenBucket(100, 10);
    bucket.tryConsume(100);
    expect(bucket.peekRemaining()).toBe(0);

    jest.advanceTimersByTime(5000);
    expect(bucket.peekRemaining()).toBe(50);

    jest.advanceTimersByTime(5000);
    expect(bucket.peekRemaining()).toBe(100);
  });

  test('令牌数量不会超过容量', () => {
    const bucket = new TokenBucket(50, 100);
    jest.advanceTimersByTime(100000);
    expect(bucket.peekRemaining()).toBe(50);
  });

  test('计算重置时间', () => {
    const bucket = new TokenBucket(100, 10);
    bucket.tryConsume(100);
    const resetMs = bucket.getResetTimeMs();
    expect(resetMs).toBe(10000);
  });

  test('支持自定义初始令牌数', () => {
    const bucket = new TokenBucket(100, 10, 30);
    expect(bucket.peekRemaining()).toBe(30);
  });
});
