package com.brixdata.durableflow.model;

import com.brixdata.durableflow.json.Jsons;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 工作流定义：一组带依赖关系（DAG）的步骤。串行流程通过依赖链表达，
 * 并行流程通过多个步骤共享同一依赖表达，条件分支通过步骤上的条件表达式表达。
 *
 * @param name  工作流名
 * @param steps 步骤列表
 */
public record WorkflowDefinition(String name, List<StepDefinition> steps) {

    public WorkflowDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("工作流名不能为空");
        }
        steps = List.copyOf(steps);
        validate(steps);
    }

    public StepDefinition step(String id) {
        return steps.stream().filter(s -> s.id().equals(id)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("未知步骤: " + id));
    }

    private static void validate(List<StepDefinition> steps) {
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("工作流至少包含一个步骤");
        }
        Map<String, StepDefinition> byId = new LinkedHashMap<>();
        for (StepDefinition step : steps) {
            if (byId.putIfAbsent(step.id(), step) != null) {
                throw new IllegalArgumentException("步骤 ID 重复: " + step.id());
            }
        }
        for (StepDefinition step : steps) {
            for (String dep : step.dependsOn()) {
                if (!byId.containsKey(dep)) {
                    throw new IllegalArgumentException(
                            "步骤 " + step.id() + " 依赖了不存在的步骤: " + dep);
                }
                if (dep.equals(step.id())) {
                    throw new IllegalArgumentException("步骤不能依赖自身: " + dep);
                }
            }
        }
        // 拓扑排序检测环
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> outgoing = new HashMap<>();
        for (StepDefinition step : steps) {
            indegree.put(step.id(), step.dependsOn().size());
            for (String dep : step.dependsOn()) {
                outgoing.computeIfAbsent(dep, k -> new ArrayList<>()).add(step.id());
            }
        }
        Deque<String> ready = new ArrayDeque<>();
        indegree.forEach((id, d) -> {
            if (d == 0) {
                ready.add(id);
            }
        });
        Set<String> visited = new HashSet<>();
        while (!ready.isEmpty()) {
            String id = ready.poll();
            visited.add(id);
            for (String next : outgoing.getOrDefault(id, List.of())) {
                if (indegree.merge(next, -1, Integer::sum) == 0) {
                    ready.add(next);
                }
            }
        }
        if (visited.size() != steps.size()) {
            throw new IllegalArgumentException("工作流依赖存在环");
        }
    }

    public ObjectNode toJson() {
        ObjectNode node = Jsons.obj();
        node.put("name", name);
        ArrayNode array = node.putArray("steps");
        for (StepDefinition step : steps) {
            array.add(step.toJson());
        }
        return node;
    }

    public static WorkflowDefinition fromJson(JsonNode node) {
        List<StepDefinition> steps = new ArrayList<>();
        node.path("steps").forEach(s -> steps.add(StepDefinition.fromJson(s)));
        return new WorkflowDefinition(node.path("name").asText(), steps);
    }
}
