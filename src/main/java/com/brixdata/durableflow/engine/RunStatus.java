package com.brixdata.durableflow.engine;

/** 工作流运行状态。 */
public enum RunStatus {
    /** 正在执行。 */
    RUNNING,
    /** 正在执行补偿流程。 */
    COMPENSATING,
    /** 全部步骤完成。 */
    COMPLETED,
    /** 最终失败（补偿流程已按定义执行完毕）。 */
    FAILED
}
