interface WindowBucket {
  startTs: number;
  count: number;
}

export class SlidingWindowCounter {
  private readonly windowMs: number;
  private readonly bucketMs: number;
  private buckets: Map<number, WindowBucket>;
  private currentCount: number;

  constructor(windowMs: number, bucketCount: number = 60) {
    this.windowMs = windowMs;
    this.bucketMs = Math.max(1, Math.floor(windowMs / bucketCount));
    this.buckets = new Map();
    this.currentCount = 0;
  }

  increment(amount: number = 1): void {
    const now = Date.now();
    const bucketKey = Math.floor(now / this.bucketMs);
    const bucketStart = bucketKey * this.bucketMs;

    const existing = this.buckets.get(bucketKey);
    if (existing) {
      existing.count += amount;
    } else {
      this.buckets.set(bucketKey, { startTs: bucketStart, count: amount });
    }

    this.currentCount += amount;
    this.evictExpired();
  }

  getCount(): number {
    this.evictExpired();
    return this.currentCount;
  }

  getApproximateCount(): { count: number; isPartial: boolean; partialRatio: number } {
    const now = Date.now();
    this.evictExpired();

    const windowStart = now - this.windowMs;
    let totalCount = 0;
    let oldestBucketKey: number | null = null;
    let newestBucketKey: number | null = null;

    for (const [key, bucket] of this.buckets) {
      totalCount += bucket.count;
      if (oldestBucketKey === null || key < oldestBucketKey) {
        oldestBucketKey = key;
      }
      if (newestBucketKey === null || key > newestBucketKey) {
        newestBucketKey = key;
      }
    }

    if (this.buckets.size === 0) {
      return { count: 0, isPartial: false, partialRatio: 1 };
    }

    const oldestBucket = this.buckets.get(oldestBucketKey!);
    const overlapStart = Math.max(oldestBucket!.startTs, windowStart);
    const bucketSpan = this.bucketMs;
    const overlapDuration = (oldestBucket!.startTs + bucketSpan) - overlapStart;
    const oldestRatio = Math.min(1, Math.max(0, overlapDuration / bucketSpan));

    const adjustedCount = totalCount - oldestBucket!.count + oldestBucket!.count * oldestRatio;
    const coveredDuration = (newestBucketKey! + 1) * this.bucketMs - oldestBucketKey! * this.bucketMs;
    const isPartial = coveredDuration < this.windowMs;
    const partialRatio = Math.min(1, coveredDuration / this.windowMs);

    return {
      count: Math.round(adjustedCount),
      isPartial,
      partialRatio,
    };
  }

  reset(): void {
    this.buckets.clear();
    this.currentCount = 0;
  }

  private evictExpired(): void {
    const now = Date.now();
    const cutoffKey = Math.floor((now - this.windowMs) / this.bucketMs);
    let evicted = 0;

    for (const [key, bucket] of this.buckets) {
      if (key < cutoffKey) {
        evicted += bucket.count;
        this.buckets.delete(key);
      }
    }

    this.currentCount -= evicted;
  }

  getWindowMs(): number {
    return this.windowMs;
  }

  getRemainingMs(): number {
    this.evictExpired();
    if (this.buckets.size === 0) return 0;
    let oldestStart = Infinity;
    for (const bucket of this.buckets.values()) {
      if (bucket.startTs < oldestStart) oldestStart = bucket.startTs;
    }
    const expiresAt = oldestStart + this.windowMs;
    return Math.max(0, expiresAt - Date.now());
  }
}
