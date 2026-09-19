package com.brixdata.durableflow.engine;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** JSON 工具：规范化序列化（键排序）与实例 id 派生。 */
public final class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtil() {
    }

    /** 递归排序 Map 键后序列化，保证相同输入产生相同字节。 */
    public static String canonicalJson(Object value) {
        try {
            return MAPPER.writeValueAsString(sortDeep(value));
        } catch (Exception e) {
            throw new IllegalStateException("cannot serialize to canonical json", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object sortDeep(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            map.forEach((k, v) -> sorted.put(String.valueOf(k), sortDeep(v)));
            return sorted;
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            list.forEach(v -> out.add(sortDeep(v)));
            return out;
        }
        return value;
    }

    /**
     * 由工作流定义 id + 规范化输入派生确定性实例 id：
     * 同一工作流在相同输入下得到相同实例，重复提交返回已有实例，结果保持稳定。
     */
    public static String deriveInstanceId(String definitionId, Map<String, Object> input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(
                    (definitionId + "\n" + canonicalJson(input)).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return "wf-" + HexFormat.of().formatHex(hash, 0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
