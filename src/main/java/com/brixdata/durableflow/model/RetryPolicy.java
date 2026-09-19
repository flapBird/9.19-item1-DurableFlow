package com.brixdata.durableflow.model;

import com.brixdata.durableflow.json.Jsons;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 步骤失败后的自动重试策略。
 *
 * @param maxAttempts      最大尝试次数（含首次执行），1 表示不重试
 * @param initialBackoffMs 首次重试前的退避时间（毫秒）
 * @param multiplier       退避时间倍增因子
 * @param maxBackoffMs     退避时间上界（毫秒）
 */
public record RetryPolicy(int maxAttempts, long initialBackoffMs, double multiplier, long maxBackoffMs) {

    /** 默认策略：不重试。 */
    public static final RetryPolicy NONE = new RetryPolicy(1, 0, 1.0, 0);

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts 必须 >= 1: " + maxAttempts);
        }
        if (initialBackoffMs < 0 || maxBackoffMs < 0) {
            throw new IllegalArgumentException("退避时间不能为负");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier 必须 >= 1.0: " + multiplier);
        }
    }

    /**
     * 计算第 {@code failedAttempt} 次（1 起计）执行失败后、下一次重试前的退避毫秒数。
     */
    public long backoffAfter(int failedAttempt) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("failedAttempt 必须 >= 1");
        }
        double backoff = initialBackoffMs * Math.pow(multiplier, failedAttempt - 1.0);
        return Math.min(maxBackoffMs, (long) backoff);
    }

    public ObjectNode toJson() {
        ObjectNode node = Jsons.obj();
        node.put("maxAttempts", maxAttempts);
        node.put("initialBackoffMs", initialBackoffMs);
        node.put("multiplier", multiplier);
        node.put("maxBackoffMs", maxBackoffMs);
        return node;
    }

    public static RetryPolicy fromJson(JsonNode node) {
        if (node == null || node.isNull()) {
            return NONE;
        }
        return new RetryPolicy(
                node.path("maxAttempts").asInt(1),
                node.path("initialBackoffMs").asLong(0),
                node.path("multiplier").asDouble(1.0),
                node.path("maxBackoffMs").asLong(0));
    }
}
