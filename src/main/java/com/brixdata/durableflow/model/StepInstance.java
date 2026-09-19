package com.brixdata.durableflow.model;

/** 步骤运行时状态（由事件日志重建，引擎运行期更新）。 */
public class StepInstance {

    private final String stepId;
    private StepStatus status = StepStatus.PENDING;
    private int attempts;
    private long resumeAt;
    private String result;
    private String error;
    private long startedAt;
    private long finishedAt;
    private int compensationAttempts;
    private boolean compensated;

    public StepInstance(String stepId) {
        this.stepId = stepId;
    }

    public String getStepId() {
        return stepId;
    }

    public StepStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    /** 定时等待/重试退避的绝对唤醒时间（epoch millis）。 */
    public long getResumeAt() {
        return resumeAt;
    }

    public String getResult() {
        return result;
    }

    public String getError() {
        return error;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public long getFinishedAt() {
        return finishedAt;
    }

    public int getCompensationAttempts() {
        return compensationAttempts;
    }

    public boolean isCompensated() {
        return compensated;
    }

    public void markReady() {
        this.status = StepStatus.READY;
    }

    public void markWaiting(long resumeAt) {
        this.status = StepStatus.WAITING;
        this.resumeAt = resumeAt;
    }

    public void markRunning(int attempt) {
        this.status = StepStatus.RUNNING;
        this.attempts = Math.max(this.attempts, attempt);
        this.startedAt = System.currentTimeMillis();
    }

    public void markSucceeded(String result) {
        this.status = StepStatus.SUCCEEDED;
        this.result = result;
        this.finishedAt = System.currentTimeMillis();
    }

    public void markFailed(String error, long retryAt) {
        this.error = error;
        if (retryAt > 0) {
            markWaiting(retryAt);
        } else {
            this.status = StepStatus.DEAD;
            this.finishedAt = System.currentTimeMillis();
        }
    }

    public void markSkipped() {
        this.status = StepStatus.SKIPPED;
        this.finishedAt = System.currentTimeMillis();
    }

    public void markCompensationRunning(int attempt) {
        this.compensationAttempts = Math.max(this.compensationAttempts, attempt);
    }

    public void markCompensated() {
        this.compensated = true;
    }

    /** 事件回放专用：直接恢复字段，不改写时间戳语义。 */
    public void restore(StepStatus status, int attempts, long resumeAt, String result,
                        String error, long startedAt, long finishedAt,
                        int compensationAttempts, boolean compensated) {
        this.status = status;
        this.attempts = attempts;
        this.resumeAt = resumeAt;
        this.result = result;
        this.error = error;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.compensationAttempts = compensationAttempts;
        this.compensated = compensated;
    }
}
