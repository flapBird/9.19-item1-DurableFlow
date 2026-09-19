package com.brixdata.durableflow.engine;

import java.util.List;
import java.util.Map;

/**
 * 工作流运行的只读快照，用于状态查询。
 */
public record WorkflowSnapshot(
        String runId,
        String workflowName,
        RunStatus status,
        List<StepSnapshot> steps,
        Map<String, Object> result,
        String error,
        List<String> compensatedSteps,
        List<String> failedCompensations) {

    /** 单个步骤的状态快照。 */
    public record StepSnapshot(String stepId, StepStatus status, int attempts, Object result) {
    }
}
