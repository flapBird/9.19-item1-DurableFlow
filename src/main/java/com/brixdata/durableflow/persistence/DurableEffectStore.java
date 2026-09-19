package com.brixdata.durableflow.persistence;

import com.brixdata.durableflow.json.Jsons;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
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
 * 幂等效果存储：记录"步骤的副作用已经实际发生"这一事实及其结果。
 *
 * <p>处理器通过 {@link #executeIdempotent(String, Supplier)} 执行副作用：
 * 副作用的结果先以幂等键写入本存储并 fsync，之后引擎才记录步骤完成事件。
 * 若进程在"副作用已发生、完成事件尚未写入"之间崩溃，恢复后步骤会以相同的
 * 幂等键重新执行，此时直接从本存储返回已记录的结果，副作用不会重复发生。</p>
 */
public final class DurableEffectStore implements AutoCloseable {

    private final Path path;
    private final FileChannel channel;
    private final Map<String, Object> effects = new LinkedHashMap<>();
    private boolean closed;

    private DurableEffectStore(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
    }

    /**
     * 打开（不存在则创建）效果存储文件，并加载已有记录。
     */
    public static DurableEffectStore open(Path path) {
        try {
            Files.createDirectories(path.getParent());
            FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            DurableEffectStore store = new DurableEffectStore(path, channel);
            store.load();
            return store;
        } catch (IOException e) {
            throw new UncheckedIOException("无法打开效果存储: " + path, e);
        }
    }

    private void load() {
        if (!Files.exists(path)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode node;
                try {
                    node = Jsons.read(line);
                } catch (IllegalArgumentException e) {
                    break; // 崩溃残留的半行记录
                }
                effects.put(node.path("key").asText(), Jsons.toJava(node.path("result")));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取效果存储失败: " + path, e);
        }
    }

    /**
     * 查询幂等键对应的效果是否已发生。
     */
    public synchronized Optional<Object> find(String key) {
        return Optional.ofNullable(effects.get(key));
    }

    /**
     * 以幂等方式执行副作用：若键已存在则直接返回已记录的结果，
     * 否则执行 supplier、将结果落盘后再返回。
     */
    public Object executeIdempotent(String key, Supplier<Object> effect) {
        Optional<Object> existing = find(key);
        if (existing.isPresent()) {
            return existing.get();
        }
        Object result = effect.get();
        record(key, result);
        return result;
    }

    /**
     * 记录一条效果并落盘。
     */
    public synchronized void record(String key, Object result) {
        if (closed) {
            throw new IllegalStateException("效果存储已关闭: " + path);
        }
        ObjectNode node = Jsons.obj();
        node.put("key", key);
        node.set("result", Jsons.toNode(result));
        try {
            channel.write(ByteBuffer.wrap((Jsons.write(node) + "\n").getBytes(StandardCharsets.UTF_8)));
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("写入效果存储失败: " + path, e);
        }
        effects.put(key, result);
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException("关闭效果存储失败: " + path, e);
            }
        }
    }
}
