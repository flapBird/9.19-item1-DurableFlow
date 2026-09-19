package com.brixdata.durableflow.builtin;

import com.brixdata.durableflow.engine.StepHandler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * 引擎内置的步骤处理器，供 CLI 提交的工作流定义直接引用。
 *
 * <ul>
 *   <li>{@code echo}：返回 config.message（缺省返回 "echo"）；</li>
 *   <li>{@code fail}：总是失败，用于演示重试与补偿；</li>
 *   <li>{@code flaky}：前 config.failTimes（默认 2）次尝试失败，之后成功；</li>
 *   <li>{@code sleep}：休眠 config.ms 毫秒；</li>
 *   <li>{@code counter}：将 data-dir/counters/&lt;name&gt;.txt 中的计数器加一并返回新值，
 *       通过幂等效果存储保证崩溃恢复后不会重复计数。</li>
 * </ul>
 */
public final class BuiltinHandlers {

    private BuiltinHandlers() {
    }

    /**
     * 创建内置处理器注册表。
     *
     * @param dataDir 引擎数据目录（counter 处理器的计数文件存于其下）
     */
    public static Map<String, StepHandler> create(Path dataDir) {
        Map<String, StepHandler> handlers = new HashMap<>();

        handlers.put("echo", ctx -> ctx.config().getOrDefault("message", "echo"));

        handlers.put("fail", ctx -> {
            throw new RuntimeException(String.valueOf(
                    ctx.config().getOrDefault("message", "fail handler 总是失败")));
        });

        handlers.put("flaky", ctx -> {
            int failTimes = ((Number) ctx.config().getOrDefault("failTimes", 2)).intValue();
            if (ctx.attempt() <= failTimes) {
                throw new RuntimeException("flaky 第 " + ctx.attempt() + " 次尝试失败");
            }
            return "ok-after-" + ctx.attempt();
        });

        handlers.put("sleep", ctx -> {
            long ms = ((Number) ctx.config().getOrDefault("ms", 1000)).longValue();
            Thread.sleep(ms);
            return "slept-" + ms;
        });

        handlers.put("counter", ctx -> {
            String name = String.valueOf(ctx.config().getOrDefault("name", ctx.stepId()));
            Path counterFile = dataDir.resolve("counters").resolve(name + ".txt");
            return ctx.executeIdempotent(() -> incrementCounter(counterFile));
        });

        return handlers;
    }

    private static Object incrementCounter(Path file) {
        try {
            Files.createDirectories(file.getParent());
            long current = 0;
            if (Files.exists(file)) {
                String content = Files.readString(file, StandardCharsets.UTF_8).trim();
                if (!content.isEmpty()) {
                    current = Long.parseLong(content);
                }
            }
            long next = current + 1;
            Files.writeString(file, Long.toString(next), StandardCharsets.UTF_8);
            return next;
        } catch (IOException e) {
            throw new UncheckedIOException("更新计数器失败: " + file, e);
        }
    }
}
