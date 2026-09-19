package com.brixdata.durableflow.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 内置处理器，供 CLI 与演示工作流直接使用。
 *
 * <ul>
 *   <li>{@code echo} — 返回 args.message（默认 "echo"）</li>
 *   <li>{@code sleep} — 休眠 args.millis 毫秒</li>
 *   <li>{@code fail} — 前 args.times 次尝试抛异常（缺省永远失败），用于演练重试</li>
 *   <li>{@code log} — 幂等地把 args.message 追加到 {@code <dataDir>/handler.log}</li>
 * </ul>
 */
public final class BuiltinHandlers {

    private BuiltinHandlers() {
    }

    public static HandlerRegistry create() {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register("echo", ctx -> ctx.arg("message", "echo"));
        registry.register("sleep", ctx -> {
            Thread.sleep(ctx.longArg("millis", 100));
            return "slept";
        });
        registry.register("fail", ctx -> {
            int times = ctx.intArg("times", Integer.MAX_VALUE);
            if (ctx.getAttempt() <= times) {
                throw new IllegalStateException("simulated failure, attempt " + ctx.getAttempt());
            }
            return "recovered after " + (ctx.getAttempt() - 1) + " failures";
        });
        registry.register("log", ctx -> ctx.executeIdempotent("log", () -> {
            String message = ctx.arg("message", ctx.getStepId());
            Path logFile = ctx.getWorkDir().resolve("handler.log");
            try {
                Files.writeString(logFile, message + System.lineSeparator(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return "logged:" + message;
        }));
        return registry;
    }
}
