package com.brixdata.durableflow.engine;

import com.brixdata.durableflow.persistence.IdempotencyStore;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;

/** 步骤执行上下文。 */
public class StepContext {

    private final String instanceId;
    private final String stepId;
    private final int attempt;
    private final Map<String, String> args;
    private final Map<String, Object> input;
    private final IdempotencyStore idempotency;
    private final Path workDir;

    public StepContext(String instanceId, String stepId, int attempt,
                       Map<String, String> args, Map<String, Object> input,
                       IdempotencyStore idempotency, Path workDir) {
        this.instanceId = instanceId;
        this.stepId = stepId;
        this.attempt = attempt;
        this.args = args;
        this.input = input;
        this.idempotency = idempotency;
        this.workDir = workDir;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getStepId() {
        return stepId;
    }

    /** 当前尝试次数（1 起计），重试时递增。 */
    public int getAttempt() {
        return attempt;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public Path getWorkDir() {
        return workDir;
    }

    public String arg(String key, String fallback) {
        return args.getOrDefault(key, fallback);
    }

    public int intArg(String key, int fallback) {
        String v = args.get(key);
        return v == null ? fallback : Integer.parseInt(v.trim());
    }

    public long longArg(String key, long fallback) {
        String v = args.get(key);
        return v == null ? fallback : Long.parseLong(v.trim());
    }

    /**
     * 当前步骤的幂等键。同一实例的同一步骤在重试与崩溃恢复间保持不变，
     * 使“已执行成功但状态未落盘”的副作用可以被去重。
     */
    public String idempotencyKey() {
        return instanceId + ":" + stepId;
    }

    /**
     * 幂等执行副作用操作：若该键已有记录则直接返回缓存结果，
     * 不重复执行 action；否则执行并先落盘再返回。
     */
    public String executeIdempotent(String suffix, Supplier<String> action) {
        return idempotency.computeIfAbsent(idempotencyKey() + ":" + suffix, action);
    }
}
