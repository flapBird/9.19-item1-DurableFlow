package com.brixdata.durableflow.persistence;

import com.brixdata.durableflow.model.StepInstance;
import com.brixdata.durableflow.model.StepStatus;
import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.model.WorkflowInstance;
import com.brixdata.durableflow.model.WorkflowStatus;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

/** 事件溯源：顺序回放事件日志，重建工作流实例的内存状态。 */
public final class StateRebuilder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StateRebuilder() {
    }

    public static WorkflowInstance rebuild(String instanceId, List<WorkflowEvent> events) {
        WorkflowInstance instance = null;
        for (WorkflowEvent event : events) {
            switch (event.getType()) {
                case WorkflowEvent.Types.WORKFLOW_SUBMITTED -> {
                    WorkflowDefinition def = MAPPER.convertValue(event.getData().get("definition"),
                            WorkflowDefinition.class);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> input = event.getData().get("input") instanceof Map<?, ?> m
                            ? (Map<String, Object>) m : Map.of();
                    instance = new WorkflowInstance(instanceId, def, input);
                }
                case WorkflowEvent.Types.STEP_READY -> step(instance, event).markReady();
                case WorkflowEvent.Types.STEP_WAITING ->
                        step(instance, event).markWaiting(event.longData("resumeAt", 0));
                case WorkflowEvent.Types.STEP_STARTED ->
                        step(instance, event).markRunning((int) event.longData("attempt", 1));
                case WorkflowEvent.Types.STEP_SUCCEEDED ->
                        step(instance, event).markSucceeded(event.stringData("result"));
                case WorkflowEvent.Types.STEP_FAILED ->
                        step(instance, event).markFailed(event.stringData("error"),
                                event.longData("retryAt", -1));
                case WorkflowEvent.Types.STEP_SKIPPED -> step(instance, event).markSkipped();
                case WorkflowEvent.Types.WORKFLOW_SUCCEEDED ->
                        instance.setStatus(WorkflowStatus.SUCCEEDED);
                case WorkflowEvent.Types.WORKFLOW_FAILED ->
                        instance.setStatus(WorkflowStatus.FAILED);
                case WorkflowEvent.Types.COMPENSATION_STARTED ->
                        instance.setStatus(WorkflowStatus.COMPENSATING);
                case WorkflowEvent.Types.COMPENSATION_STEP_STARTED ->
                        step(instance, event).markCompensationRunning((int) event.longData("attempt", 1));
                case WorkflowEvent.Types.COMPENSATION_STEP_SUCCEEDED ->
                        step(instance, event).markCompensated();
                case WorkflowEvent.Types.COMPENSATION_STEP_FAILED ->
                        step(instance, event).setCompensationRetryAt(event.longData("retryAt", 0));
                case WorkflowEvent.Types.WORKFLOW_COMPENSATED ->
                        instance.setStatus(WorkflowStatus.COMPENSATED);
                case WorkflowEvent.Types.WORKFLOW_COMPENSATION_FAILED ->
                        instance.setStatus(WorkflowStatus.COMPENSATION_FAILED);
                default -> {
                    // 未知事件类型（如 COMPENSATION_STEP_FAILED 仅影响重试计时）：忽略
                }
            }
        }
        if (instance == null) {
            throw new IllegalStateException("journal without WORKFLOW_SUBMITTED: " + instanceId);
        }
        return instance;
    }

    private static StepInstance step(WorkflowInstance instance, WorkflowEvent event) {
        StepInstance step = instance.step(event.getStepId());
        if (step == null) {
            throw new IllegalStateException("event references unknown step: " + event.getStepId());
        }
        return step;
    }
}
