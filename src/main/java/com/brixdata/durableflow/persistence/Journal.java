package com.brixdata.durableflow.persistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 单个工作流实例的追加式事件日志。每条事件是一行 JSON，
 * 追加后立即 fsync，保证已返回的事件在进程崩溃后仍然可读。
 * 崩溃可能留下半行残缺的尾部记录，重放时忽略无法解析的最后一行。
 */
public final class Journal implements AutoCloseable {

    private final Path path;
    private final FileChannel channel;
    private boolean closed;

    private Journal(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
    }

    /**
     * 打开（不存在则创建）指定路径的日志文件，写入指针定位到文件末尾。
     */
    public static Journal open(Path path) {
        try {
            Files.createDirectories(path.getParent());
            FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            return new Journal(path, channel);
        } catch (IOException e) {
            throw new UncheckedIOException("无法打开日志文件: " + path, e);
        }
    }

    /**
     * 追加一条事件并落盘。
     */
    public synchronized void append(Event event) {
        if (closed) {
            throw new IllegalStateException("日志已关闭: " + path);
        }
        try {
            byte[] bytes = (event.toLine() + "\n").getBytes(StandardCharsets.UTF_8);
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("写入日志失败: " + path, e);
        }
    }

    /**
     * 重放日志中的全部事件。尾部的残缺行（崩溃时写了一半）被忽略。
     */
    public static List<Event> replay(Path path) {
        List<Event> events = new ArrayList<>();
        if (!Files.exists(path)) {
            return events;
        }
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    events.add(Event.parse(line));
                } catch (IllegalArgumentException e) {
                    // 崩溃残留的半行记录，只可能出现在文件尾部，忽略
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取日志失败: " + path, e);
        }
        return events;
    }

    public Path path() {
        return path;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException("关闭日志失败: " + path, e);
            }
        }
    }
}
