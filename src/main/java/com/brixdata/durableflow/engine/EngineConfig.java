package com.brixdata.durableflow.engine;

import java.nio.file.Path;
import java.util.Objects;

/**
 * 引擎配置。
 *
 * @param dataDir        本地持久化数据目录（事件日志、效果存储均存于此）
 * @param maxConcurrency 同时运行的步骤数上限
 */
public record EngineConfig(Path dataDir, int maxConcurrency) {

    public EngineConfig {
        Objects.requireNonNull(dataDir, "dataDir");
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency 必须 >= 1: " + maxConcurrency);
        }
    }

    public static EngineConfig defaults(Path dataDir) {
        return new EngineConfig(dataDir, 4);
    }
}
