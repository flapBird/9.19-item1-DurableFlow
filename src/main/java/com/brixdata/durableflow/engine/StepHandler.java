package com.brixdata.durableflow.engine;

/**
 * 步骤处理器：承载步骤的实际业务逻辑。
 * 实现必须是确定性的——同一工作流在相同输入下重放应得到相同结果。
 */
@FunctionalInterface
public interface StepHandler {

    /**
     * 执行步骤逻辑。
     *
     * @param context 步骤执行上下文（输入、配置、幂等工具等）
     * @return 步骤结果（必须可 JSON 序列化），会持久化并对后续步骤可见
     * @throws Exception 执行失败时抛出，引擎将按重试策略处理
     */
    Object execute(StepContext context) throws Exception;
}
