package com.brixdata.durableflow.config;

import com.brixdata.durableflow.model.RetryPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 引擎配置。字段与 {@code config/durableflow.example.yaml} 一一对应；
 * 配置文件不存在时使用默认值。
 */
public class EngineConfig {

    private String nodeId = "default-node";
    private Path dataDir = Path.of("data");
    private int maxConcurrency = 4;
    private RetryPolicy defaultRetry = RetryPolicy.defaults();

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Path getDataDir() {
        return dataDir;
    }

    public void setDataDir(Path dataDir) {
        this.dataDir = dataDir;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1");
        }
        this.maxConcurrency = maxConcurrency;
    }

    public RetryPolicy getDefaultRetry() {
        return defaultRetry;
    }

    public void setDefaultRetry(RetryPolicy defaultRetry) {
        this.defaultRetry = defaultRetry;
    }

    /** 从 YAML 文件加载；文件不存在时返回默认配置。 */
    public static EngineConfig load(Path yamlPath) {
        EngineConfig config = new EngineConfig();
        if (yamlPath == null || !Files.exists(yamlPath)) {
            return config;
        }
        try {
            ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
            JsonNode root = yaml.readTree(Files.newInputStream(yamlPath));
            JsonNode df = root.path("durableflow");
            if (df.hasNonNull("node-id")) {
                config.setNodeId(df.get("node-id").asText());
            }
            JsonNode storage = df.path("storage");
            if (storage.hasNonNull("data-dir")) {
                config.setDataDir(Path.of(storage.get("data-dir").asText()));
            }
            JsonNode execution = df.path("execution");
            if (execution.hasNonNull("max-concurrency")) {
                config.setMaxConcurrency(execution.get("max-concurrency").asInt());
            }
            JsonNode retry = execution.path("default-retry");
            if (!retry.isMissingNode()) {
                config.setDefaultRetry(new RetryPolicy(
                        retry.path("max-attempts").asInt(3),
                        retry.path("backoff-millis").asLong(1000),
                        retry.path("multiplier").asDouble(2.0)));
            }
            return config;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load config: " + yamlPath, e);
        }
    }
}
