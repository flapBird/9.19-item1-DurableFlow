package com.brixdata.durableflow.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 工作流定义：一组带依赖关系的步骤构成的 DAG。 */
public class WorkflowDefinition {

    private String id;
    private List<StepDefinition> steps = new ArrayList<>();

    public WorkflowDefinition() {
    }

    public WorkflowDefinition(String id, List<StepDefinition> steps) {
        this.id = id;
        this.steps = steps;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<StepDefinition> getSteps() {
        return steps;
    }

    public void setSteps(List<StepDefinition> steps) {
        this.steps = steps == null ? new ArrayList<>() : steps;
    }

    public StepDefinition step(String stepId) {
        for (StepDefinition s : steps) {
            if (s.getId().equals(stepId)) {
                return s;
            }
        }
        throw new IllegalArgumentException("unknown step: " + stepId);
    }

    /** 校验：id 非空唯一、依赖存在、无环、TASK 必须有 handler。 */
    public void validate() {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("workflow id must not be blank");
        }
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("workflow must contain at least one step");
        }
        Set<String> ids = new HashSet<>();
        for (StepDefinition s : steps) {
            if (s.getId() == null || s.getId().isBlank()) {
                throw new IllegalArgumentException("step id must not be blank");
            }
            if (!ids.add(s.getId())) {
                throw new IllegalArgumentException("duplicate step id: " + s.getId());
            }
            if (s.getType() == StepType.TASK && (s.getHandler() == null || s.getHandler().isBlank())) {
                throw new IllegalArgumentException("TASK step must declare a handler: " + s.getId());
            }
            if (s.getType() == StepType.WAIT && s.getWaitMillis() < 0) {
                throw new IllegalArgumentException("waitMillis must be >= 0: " + s.getId());
            }
        }
        for (StepDefinition s : steps) {
            for (String dep : s.getDependsOn()) {
                if (!ids.contains(dep)) {
                    throw new IllegalArgumentException("step " + s.getId() + " depends on unknown step: " + dep);
                }
            }
        }
        // 拓扑排序检测环
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> outgoing = new HashMap<>();
        for (StepDefinition s : steps) {
            indegree.putIfAbsent(s.getId(), 0);
            for (String dep : s.getDependsOn()) {
                indegree.merge(s.getId(), 1, Integer::sum);
                outgoing.computeIfAbsent(dep, k -> new ArrayList<>()).add(s.getId());
            }
        }
        List<String> queue = new ArrayList<>();
        for (var e : indegree.entrySet()) {
            if (e.getValue() == 0) {
                queue.add(e.getKey());
            }
        }
        int visited = 0;
        while (!queue.isEmpty()) {
            String cur = queue.remove(queue.size() - 1);
            visited++;
            for (String next : outgoing.getOrDefault(cur, List.of())) {
                if (indegree.merge(next, -1, Integer::sum) == 0) {
                    queue.add(next);
                }
            }
        }
        if (visited != steps.size()) {
            throw new IllegalArgumentException("workflow definition contains a dependency cycle");
        }
    }
}
