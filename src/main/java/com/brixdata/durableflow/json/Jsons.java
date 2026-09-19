package com.brixdata.durableflow.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

/**
 * 全局共享的 JSON 工具，封装 Jackson 的常用操作。
 */
public final class Jsons {

    /** 共享 ObjectMapper（线程安全）。 */
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private Jsons() {
    }

    public static ObjectNode obj() {
        return MAPPER.createObjectNode();
    }

    public static String write(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("JSON 序列化失败", e);
        }
    }

    public static JsonNode read(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON 解析失败: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(JsonNode node) {
        if (node == null || node.isNull()) {
            return Map.of();
        }
        return MAPPER.convertValue(node, Map.class);
    }

    public static JsonNode toNode(Object value) {
        return MAPPER.valueToTree(value);
    }

    public static Object toJava(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return MAPPER.convertValue(node, Object.class);
    }
}
