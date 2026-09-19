package com.brixdata.durableflow.model;

import com.brixdata.durableflow.json.Jsons;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

/**
 * 步骤的补偿（saga 回滚）定义。步骤成功完成后，若后续步骤最终失败，
 * 引擎将按完成顺序的逆序执行各步骤的补偿操作。
 *
 * @param handler 补偿处理器名
 * @param config  补偿处理器配置
 * @param retry   补偿失败时的重试策略
 */
public record CompensationDef(String handler, Map<String, Object> config, RetryPolicy retry) {

    public CompensationDef {
        if (handler == null || handler.isBlank()) {
            throw new IllegalArgumentException("补偿 handler 不能为空");
        }
        config = config == null ? Map.of() : Map.copyOf(config);
        retry = retry == null ? RetryPolicy.NONE : retry;
    }

    public ObjectNode toJson() {
        ObjectNode node = Jsons.obj();
        node.put("handler", handler);
        node.set("config", Jsons.toNode(config));
        node.set("retry", retry.toJson());
        return node;
    }

    public static CompensationDef fromJson(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return new CompensationDef(
                node.path("handler").asText(),
                Jsons.toMap(node.path("config")),
                RetryPolicy.fromJson(node.path("retry")));
    }
}
