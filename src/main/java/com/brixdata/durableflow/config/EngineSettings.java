package com.brixdata.durableflow.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * 引擎运行配置，对应 config/durableflow.yaml。
 *
 * @param dataDir        本地持久化数据目录
 * @param maxConcurrency 同时运行的步骤数上限
 */
public record EngineSettings(Path dataDir, int maxConcurrency) {

    /** 默认数据目录。 */
    public static final Path DEFAULT_DATA_DIR = Path.of("./data");
    /** 默认并发上限。 */
    public static final int DEFAULT_MAX_CONCURRENCY = 4;

    /**
     * 加载配置：配置文件（若存在）提供默认值，显式参数优先。
     *
     * @param configFile          配置文件路径，不存在时全部使用默认值
     * @param dataDirOverride     命令行覆盖的数据目录（可为 null）
     * @param concurrencyOverride 命令行覆盖的并发上限（可为 null）
     */
    public static EngineSettings load(Path configFile, Path dataDirOverride,
                                      Integer concurrencyOverride) {
        Path dataDir = DEFAULT_DATA_DIR;
        int maxConcurrency = DEFAULT_MAX_CONCURRENCY;
        if (configFile != null && Files.exists(configFile)) {
            Map<String, Object> root = loadYaml(configFile);
            Map<String, Object> durableflow = subMap(root, "durableflow");
            Map<String, Object> storage = subMap(durableflow, "storage");
            Map<String, Object> execution = subMap(durableflow, "execution");
            if (storage.get("data-dir") != null) {
                dataDir = Path.of(storage.get("data-dir").toString());
            }
            if (execution.get("max-concurrency") != null) {
                maxConcurrency = Integer.parseInt(execution.get("max-concurrency").toString());
            }
        }
        if (dataDirOverride != null) {
            dataDir = dataDirOverride;
        }
        if (concurrencyOverride != null) {
            maxConcurrency = concurrencyOverride;
        }
        return new EngineSettings(dataDir, maxConcurrency);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadYaml(Path file) {
        try (Reader reader = Files.newBufferedReader(file)) {
            Map<String, Object> root = new Yaml().load(reader);
            return root != null ? root : Map.of();
        } catch (IOException e) {
            throw new UncheckedIOException("读取配置文件失败: " + file, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> subMap(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        return value instanceof Map ? (Map<String, Object>) value : Map.of();
    }
}
