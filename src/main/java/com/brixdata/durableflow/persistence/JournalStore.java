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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * 基于本地文件的事件日志存储。
 *
 * <p>每个工作流实例一个 {@code <dataDir>/workflows/<instanceId>.journal} 文件，
 * 追加写入后立即 {@code force} 落盘。加载时容忍最后一行被截断
 * （进程在状态写入过程中被终止的场景），截断行直接丢弃。</p>
 */
public class JournalStore implements AutoCloseable {

    private final Path workflowsDir;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile boolean closed;

    public JournalStore(Path dataDir) {
        this.workflowsDir = dataDir.resolve("workflows");
        try {
            Files.createDirectories(workflowsDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create workflows dir: " + workflowsDir, e);
        }
    }

    /** 追加一个事件并强制落盘。 */
    public void append(String instanceId, WorkflowEvent event) {
        writeLock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("journal is closed");
            }
            Path file = fileOf(instanceId);
            byte[] line = (mapper.writeValueAsString(event) + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileChannel channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                channel.write(ByteBuffer.wrap(line));
                channel.force(false);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append event for " + instanceId, e);
        } finally {
            writeLock.unlock();
        }
    }

    public boolean exists(String instanceId) {
        return Files.exists(fileOf(instanceId));
    }

    /** 读取单个实例的全部事件，无法解析的行（崩溃截断）被跳过。 */
    public List<WorkflowEvent> load(String instanceId) {
        Path file = fileOf(instanceId);
        if (!Files.exists(file)) {
            return List.of();
        }
        List<WorkflowEvent> events = new ArrayList<>();
        try {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    events.add(mapper.readValue(line, WorkflowEvent.class));
                } catch (IOException parseFailure) {
                    // 崩溃导致的截断行：丢弃，之前的完整事件仍然有效
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read journal: " + file, e);
        }
        return events;
    }

    /** 加载数据目录下所有实例的事件，按实例分组。 */
    public Map<String, List<WorkflowEvent>> loadAll() {
        Map<String, List<WorkflowEvent>> all = new LinkedHashMap<>();
        for (String id : instanceIds()) {
            all.put(id, load(id));
        }
        return all;
    }

    /** 列出全部已知实例 id（按字典序）。 */
    public Set<String> instanceIds() {
        Set<String> ids = new TreeSet<>();
        try (Stream<Path> files = Files.list(workflowsDir)) {
            files.filter(p -> p.getFileName().toString().endsWith(".journal"))
                    .forEach(p -> {
                        String name = p.getFileName().toString();
                        ids.add(name.substring(0, name.length() - ".journal".length()));
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list journals in " + workflowsDir, e);
        }
        return ids;
    }

    private Path fileOf(String instanceId) {
        return workflowsDir.resolve(instanceId + ".journal");
    }

    @Override
    public void close() {
        writeLock.lock();
        try {
            closed = true;
        } finally {
            writeLock.unlock();
        }
    }
}
