export interface TokenBucketState {
  tokens: number;
  lastRefillTimestamp: number;
}

export class TokenBucket {
  private state: TokenBucketState;
  private readonly capacity: number;
  private readonly refillRatePerSecond: number;

  constructor(capacity: number, refillRatePerSecond: number, initialTokens?: number) {
    this.capacity = capacity;
    this.refillRatePerSecond = refillRatePerSecond;
    this.state = {
      tokens: initialTokens ?? capacity,
      lastRefillTimestamp: Date.now(),
    };
  }

  tryConsume(tokens: number = 1): { allowed: boolean; waitTimeMs: number; remaining: number } {
    this.refill();
    const now = Date.now();
    if (this.state.tokens >= tokens) {
      this.state.tokens -= tokens;
      return {
        allowed: true,
        waitTimeMs: 0,
        remaining: this.state.tokens,
      };
    }

    const deficit = tokens - this.state.tokens;
    const waitTimeMs = Math.ceil((deficit / this.refillRatePerSecond) * 1000);
    return {
      allowed: false,
      waitTimeMs,
      remaining: this.state.tokens,
    };
  }

  peekRemaining(): number {
    this.refill();
    return this.state.tokens;
  }

  private refill(): void {
    const now = Date.now();
    const elapsedSeconds = (now - this.state.lastRefillTimestamp) / 1000;
    if (elapsedSeconds <= 0) return;

    const newTokens = elapsedSeconds * this.refillRatePerSecond;
    this.state.tokens = Math.min(this.capacity, this.state.tokens + newTokens);
    this.state.lastRefillTimestamp = now;
  }

  getCapacity(): number {
    return this.capacity;
  }

  getRefillRate(): number {
    return this.refillRatePerSecond;
  }

  getResetTimeMs(): number {
    this.refill();
    const deficit = this.capacity - this.state.tokens;
    if (deficit <= 0) return 0;
    return Math.ceil(deficit / this.refillRatePerSecond) * 1000;
  }

  resetToFull(): void {
    this.state.tokens = this.capacity;
    this.state.lastRefillTimestamp = Date.now();
  }
}
