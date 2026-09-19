package com.brixdata.durableflow.model;

/**
 * 重试策略：最大尝试次数 + 指数退避。
 *
 * @param maxAttempts  最大尝试次数（含首次执行），1 表示不重试
 * @param backoffMillis 首次失败后的退避基数（毫秒）
 * @param multiplier   每次失败后退避时间的倍增因子
 */
public record RetryPolicy(int maxAttempts, long backoffMillis, double multiplier) {

    /** 单次退避上限，避免退避时间无限增长。 */
    public static final long MAX_DELAY_MILLIS = 5 * 60 * 1000L;

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (backoffMillis < 0) {
            throw new IllegalArgumentException("backoffMillis must be >= 0");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, 1000, 2.0);
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, 0, 1.0);
    }

    /**
     * 第 failedAttempt 次失败（1 起计）后需要等待的毫秒数。
     */
    public long delayMillis(int failedAttempt) {
        double delay = backoffMillis * Math.pow(multiplier, Math.max(0, failedAttempt - 1));
        return Math.min((long) delay, MAX_DELAY_MILLIS);
    }
}
