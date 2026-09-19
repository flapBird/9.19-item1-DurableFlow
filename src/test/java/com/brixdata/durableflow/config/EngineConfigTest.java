package com.brixdata.durableflow.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EngineConfigTest {

    @TempDir
    Path dir;

    @Test
    void missingFileYieldsDefaults() {
        EngineConfig config = EngineConfig.load(dir.resolve("nonexistent.yaml"));
        assertEquals("default-node", config.getNodeId());
        assertEquals(Path.of("data"), config.getDataDir());
        assertEquals(4, config.getMaxConcurrency());
        assertEquals(3, config.getDefaultRetry().maxAttempts());
    }

    @Test
    void loadsYamlValues() throws Exception {
        Path yaml = dir.resolve("durableflow.yaml");
        Files.writeString(yaml, """
                durableflow:
                  node-id: node-7
                  storage:
                    data-dir: /tmp/df-data
                  execution:
                    max-concurrency: 8
                    default-retry:
                      max-attempts: 5
                      backoff-millis: 250
                      multiplier: 1.5
                """);
        EngineConfig config = EngineConfig.load(yaml);
        assertEquals("node-7", config.getNodeId());
        assertEquals(Path.of("/tmp/df-data"), config.getDataDir());
        assertEquals(8, config.getMaxConcurrency());
        assertEquals(5, config.getDefaultRetry().maxAttempts());
        assertEquals(250, config.getDefaultRetry().backoffMillis());
        assertEquals(1.5, config.getDefaultRetry().multiplier());
    }

    @Test
    void partialYamlKeepsDefaultsForMissingKeys() throws Exception {
        Path yaml = dir.resolve("partial.yaml");
        Files.writeString(yaml, """
                durableflow:
                  execution:
                    max-concurrency: 2
                """);
        EngineConfig config = EngineConfig.load(yaml);
        assertEquals(2, config.getMaxConcurrency());
        assertEquals("default-node", config.getNodeId());
        assertEquals(3, config.getDefaultRetry().maxAttempts());
    }
}
