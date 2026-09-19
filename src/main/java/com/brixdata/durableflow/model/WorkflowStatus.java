package com.brixdata.durableflow.model;

/** 工作流实例状态。 */
public enum WorkflowStatus {
    RUNNING,
    SUCCEEDED,
    /** 存在步骤最终失败，且无可补偿步骤。 */
    FAILED,
    /** 正在执行补偿。 */
    COMPENSATING,
    /** 补偿全部完成。 */
    COMPENSATED,
    /** 补偿重试耗尽。 */
    COMPENSATION_FAILED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == COMPENSATED || this == COMPENSATION_FAILED;
    }
}
