package com.brixdata.durableflow.engine;

/** 步骤运行时状态。 */
public enum StepStatus {
    /** 等待依赖满足。 */
    PENDING,
    /** 等待定时唤醒（定时等待或重试退避）。 */
    WAITING,
    /** 正在执行。 */
    RUNNING,
    /** 已成功完成。 */
    COMPLETED,
    /** 因条件不满足或依赖被跳过而跳过。 */
    SKIPPED,
    /** 重试耗尽，最终失败。 */
    FAILED
}
