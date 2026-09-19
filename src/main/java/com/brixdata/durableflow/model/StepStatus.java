package com.brixdata.durableflow.model;

/** 步骤执行状态。 */
public enum StepStatus {
    /** 等待依赖满足。 */
    PENDING,
    /** 依赖已满足，等待线程池调度。 */
    READY,
    /** 定时等待中（WAIT 步骤计时或失败重试退避），resumeAt 为绝对唤醒时间。 */
    WAITING,
    /** 正在执行。 */
    RUNNING,
    /** 执行成功。 */
    SUCCEEDED,
    /** 重试次数耗尽，最终失败。 */
    DEAD,
    /** 条件不满足被跳过。 */
    SKIPPED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD || this == SKIPPED;
    }
}
