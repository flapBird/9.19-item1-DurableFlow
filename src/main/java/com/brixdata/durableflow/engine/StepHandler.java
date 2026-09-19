package com.brixdata.durableflow.engine;

/** 步骤处理器：业务逻辑的执行单元。 */
@FunctionalInterface
public interface StepHandler {

    /**
     * 执行步骤逻辑。
     *
     * @param ctx 执行上下文（参数、输入、尝试次数、幂等工具）
     * @return 步骤结果（将被持久化，供条件表达式与状态查询使用）
     * @throws Exception 任何异常都视为本次尝试失败，触发重试或终止
     */
    String execute(StepContext ctx) throws Exception;
}
