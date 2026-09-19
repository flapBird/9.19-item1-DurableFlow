package com.brixdata.durableflow.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/** 工作流实例运行时状态。 */
public class WorkflowInstance {

    private final String id;
    private final WorkflowDefinition definition;
    private final Map<String, Object> input;
    private final Map<String, StepInstance> steps = new LinkedHashMap<>();
    private volatile WorkflowStatus status = WorkflowStatus.RUNNING;
    private final long createdAt;
    private volatile long finishedAt;

    public WorkflowInstance(String id, WorkflowDefinition definition, Map<String, Object> input) {
        this.id = id;
        this.definition = definition;
        this.input = input;
        this.createdAt = System.currentTimeMillis();
        for (StepDefinition s : definition.getSteps()) {
            steps.put(s.getId(), new StepInstance(s.getId()));
        }
    }

    public String getId() {
        return id;
    }

    public WorkflowDefinition getDefinition() {
        return definition;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public WorkflowStatus getStatus() {
        return status;
    }

    public void setStatus(WorkflowStatus status) {
        this.status = status;
        if (status.isTerminal()) {
            this.finishedAt = System.currentTimeMillis();
        }
    }

    public Collection<StepInstance> getSteps() {
        return steps.values();
    }

    public StepInstance step(String stepId) {
        return steps.get(stepId);
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getFinishedAt() {
        return finishedAt;
    }

    /** 所有步骤均已到达终态（成功或跳过）。 */
    public boolean allStepsFinished() {
        return steps.values().stream().allMatch(s -> s.getStatus().isTerminal());
    }
}
