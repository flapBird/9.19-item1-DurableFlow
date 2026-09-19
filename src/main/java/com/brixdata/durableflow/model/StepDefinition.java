package com.brixdata.durableflow.model;

import com.brixdata.durableflow.json.Jsons;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 工作流中的一个步骤定义。
 *
 * @param id           步骤 ID（同一工作流内唯一，仅限字母数字、下划线、连字符）
 * @param type         步骤类型：TASK 执行处理器，WAIT 定时等待
 * @param handler      处理器名（TASK 必填）
 * @param dependsOn    依赖的步骤 ID 列表，全部完成后本步骤才可执行
 * @param config       传递给处理器的配置
 * @param retry        失败重试策略
 * @param condition    条件表达式（可为空），不满足时步骤被跳过
 * @param compensation 补偿定义（可为空）
 * @param waitMs       WAIT 类型的等待毫秒数
 */
public record StepDefinition(String id, StepType type, String handler, List<String> dependsOn,
                             Map<String, Object> config, RetryPolicy retry, String condition,
                             CompensationDef compensation, long waitMs) {

    /** 步骤类型。 */
    public enum StepType {
        TASK, WAIT
    }

    public StepDefinition {
        if (id == null || !id.matches("[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException("非法步骤 ID: " + id);
        }
        type = type == null ? StepType.TASK : type;
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
        config = config == null ? Map.of() : Map.copyOf(config);
        retry = retry == null ? RetryPolicy.NONE : retry;
        if (type == StepType.TASK && (handler == null || handler.isBlank())) {
            throw new IllegalArgumentException("TASK 步骤必须指定 handler: " + id);
        }
        if (type == StepType.WAIT && waitMs < 0) {
            throw new IllegalArgumentException("waitMs 不能为负: " + id);
        }
    }

    public static Builder task(String id, String handler) {
        return new Builder(id, StepType.TASK).handler(handler);
    }

    public static Builder waitStep(String id, long waitMs) {
        return new Builder(id, StepType.WAIT).waitMs(waitMs);
    }

    public ObjectNode toJson() {
        ObjectNode node = Jsons.obj();
        node.put("id", id);
        node.put("type", type.name());
        if (handler != null) {
            node.put("handler", handler);
        }
        if (!dependsOn.isEmpty()) {
            node.set("dependsOn", Jsons.toNode(dependsOn));
        }
        if (!config.isEmpty()) {
            node.set("config", Jsons.toNode(config));
        }
        node.set("retry", retry.toJson());
        if (condition != null) {
            node.put("condition", condition);
        }
        if (compensation != null) {
            node.set("compensation", compensation.toJson());
        }
        if (type == StepType.WAIT) {
            node.put("waitMs", waitMs);
        }
        return node;
    }

    public static StepDefinition fromJson(JsonNode node) {
        List<String> deps = new ArrayList<>();
        node.path("dependsOn").forEach(d -> deps.add(d.asText()));
        String condition = node.hasNonNull("condition") ? node.get("condition").asText() : null;
        String handler = node.hasNonNull("handler") ? node.get("handler").asText() : null;
        return new StepDefinition(
                node.path("id").asText(),
                StepType.valueOf(node.path("type").asText("TASK")),
                handler,
                deps,
                Jsons.toMap(node.path("config")),
                RetryPolicy.fromJson(node.path("retry")),
                condition,
                CompensationDef.fromJson(node.get("compensation")),
                node.path("waitMs").asLong(0));
    }

    /** 步骤定义构建器。 */
    public static final class Builder {
        private final String id;
        private final StepType type;
        private String handler;
        private List<String> dependsOn = List.of();
        private Map<String, Object> config = Map.of();
        private RetryPolicy retry = RetryPolicy.NONE;
        private String condition;
        private CompensationDef compensation;
        private long waitMs;

        private Builder(String id, StepType type) {
            this.id = id;
            this.type = type;
        }

        public Builder handler(String handler) {
            this.handler = handler;
            return this;
        }

        public Builder dependsOn(String... ids) {
            this.dependsOn = List.of(ids);
            return this;
        }

        public Builder config(Map<String, Object> config) {
            this.config = config;
            return this;
        }

        public Builder retry(RetryPolicy retry) {
            this.retry = retry;
            return this;
        }

        public Builder condition(String condition) {
            this.condition = condition;
            return this;
        }

        public Builder compensation(CompensationDef compensation) {
            this.compensation = compensation;
            return this;
        }

        public Builder waitMs(long waitMs) {
            this.waitMs = waitMs;
            return this;
        }

        public StepDefinition build() {
            return new StepDefinition(id, type, handler, dependsOn, config, retry, condition,
                    compensation, waitMs);
        }
    }
}
