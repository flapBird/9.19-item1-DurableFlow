package com.brixdata.durableflow.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 幂等结果存储：key → 已完成副作用的结果。
 *
 * <p>处理器通过 {@code StepContext.executeIdempotent} 把有副作用的操作包进来：
 * 若同一 key 已有记录，直接返回缓存结果而不重复执行，从而保证
 * “实际执行成功但完成状态尚未落盘”的任务在恢复后不会产生重复副作用。</p>
 *
 * <p>存储为 {@code <dataDir>/idempotency.journal}，JSON 行追加写并 force 落盘。</p>
 */
public class IdempotencyStore implements AutoCloseable {

    private final Path file;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, String> results = new LinkedHashMap<>();
    private volatile boolean closed;

    public IdempotencyStore(Path dataDir) {
        this.file = dataDir.resolve("idempotency.journal");
        try {
            Files.createDirectories(dataDir);
            if (Files.exists(file)) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, String> entry = mapper.readValue(line, Map.class);
                        results.put(entry.get("key"), entry.get("result"));
                    } catch (IOException parseFailure) {
                        // 忽略崩溃截断行
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load idempotency store: " + file, e);
        }
    }

    public Optional<String> lookup(String key) {
        return Optional.ofNullable(results.get(key));
    }

    /**
     * 若 key 已存在则返回缓存结果；否则执行 action、先落盘再返回。
     * 注意：action 的副作用与结果落盘之间仍存在极小的崩溃窗口，
     * 因此 action 本身也应以 key 为幂等键保证外部系统侧可去重。
     */
    public synchronized String computeIfAbsent(String key, Supplier<String> action) {
        String existing = results.get(key);
        if (existing != null) {
            return existing;
        }
        if (closed) {
            throw new IllegalStateException("idempotency store is closed");
        }
        String result = action.get();
        Map<String, String> entry = new LinkedHashMap<>();
        entry.put("key", key);
        entry.put("result", result);
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap((mapper.writeValueAsString(entry) + "\n")
                    .getBytes(StandardCharsets.UTF_8)));
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist idempotent result for " + key, e);
        }
        results.put(key, result);
        return result;
    }

    @Override
    public synchronized void close() {
        closed = true;
    }
}
