package com.brixdata.durableflow.engine;

import com.brixdata.durableflow.persistence.DurableEffectStore;

import java.util.Map;
import java.util.function.Supplier;

/**
 * 步骤执行上下文，由引擎在每次尝试时构建。
 */
public final class StepContext {

    private final String runId;
    private final String stepId;
    private final int attempt;
    private final String idempotencyKey;
    private final Map<String, Object> input;
    private final Map<String, Object> config;
    private final Map<String, Object> stepResults;
    private final DurableEffectStore effects;

    public StepContext(String runId, String stepId, int attempt, String idempotencyKey,
                       Map<String, Object> input, Map<String, Object> config,
                       Map<String, Object> stepResults, DurableEffectStore effects) {
        this.runId = runId;
        this.stepId = stepId;
        this.attempt = attempt;
        this.idempotencyKey = idempotencyKey;
        this.input = input;
        this.config = config;
        this.stepResults = stepResults;
        this.effects = effects;
    }

    /** 工作流运行 ID。 */
    public String runId() {
        return runId;
    }

    /** 当前步骤 ID。 */
    public String stepId() {
        return stepId;
    }

    /** 当前是第几次尝试（1 起计）。 */
    public int attempt() {
        return attempt;
    }

    /**
     * 本次尝试的幂等键。崩溃恢复后同一尝试会以相同的键重新执行，
     * 因此可以用它去重已经发生的副作用。
     */
    public String idempotencyKey() {
        return idempotencyKey;
    }

    /** 工作流输入。 */
    public Map<String, Object> input() {
        return input;
    }

    /** 步骤配置。 */
    public Map<String, Object> config() {
        return config;
    }

    /** 已完成步骤的结果（步骤 ID → 结果），只读快照。 */
    public Map<String, Object> stepResults() {
        return stepResults;
    }

    /**
     * 以幂等方式执行副作用：副作用的结果先持久化再返回。
     * 若进程在"副作用已发生、步骤完成事件未写入"之间崩溃，恢复后重新执行
     * 本步骤时直接返回已记录的结果，副作用不会重复发生。
     */
    public Object executeIdempotent(Supplier<Object> effect) {
        return effects.executeIdempotent(idempotencyKey, effect);
    }

    /**
     * 同 {@link #executeIdempotent(Supplier)}，但用于一步内的多个副作用，
     * 以 suffix 区分键。
     */
    public Object executeIdempotent(String suffix, Supplier<Object> effect) {
        return effects.executeIdempotent(idempotencyKey + "/" + suffix, effect);
    }
}
