package com.brixdata.durableflow.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.brixdata.durableflow.engine.StepContext;
import com.brixdata.durableflow.engine.StepHandler;
import com.brixdata.durableflow.persistence.DurableEffectStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BuiltinHandlersTest {

    @TempDir
    Path dataDir;

    private StepContext context(String key, Map<String, Object> config, DurableEffectStore effects) {
        int attempt = 1;
        return new StepContext("run-1", "step-1", attempt, key,
                Map.of(), config, Map.of(), effects);
    }

    @Test
    void flakyFailsConfiguredTimesThenSucceeds() throws Exception {
        StepHandler flaky = BuiltinHandlers.create(dataDir).get("flaky");
        try (DurableEffectStore effects = DurableEffectStore.open(dataDir.resolve("e.effects"))) {
            StepContext ctx = context("k", Map.of("failTimes", 2), effects);
            // 前两次尝试失败
            assertThrows(RuntimeException.class, () -> flaky.execute(ctx));
            StepContext attempt2 = new StepContext("run-1", "step-1", 2, "k2",
                    Map.of(), Map.of("failTimes", 2), Map.of(), effects);
            assertThrows(RuntimeException.class, () -> flaky.execute(attempt2));
            StepContext attempt3 = new StepContext("run-1", "step-1", 3, "k3",
                    Map.of(), Map.of("failTimes", 2), Map.of(), effects);
            assertEquals("ok-after-3", flaky.execute(attempt3));
        }
    }

    @Test
    void counterIncrementsPersistentlyAndIdempotently() throws Exception {
        StepHandler counter = BuiltinHandlers.create(dataDir).get("counter");
        Path counterFile = dataDir.resolve("counters").resolve("orders.txt");

        try (DurableEffectStore effects = DurableEffectStore.open(dataDir.resolve("e.effects"))) {
            assertEquals(1L, counter.execute(context("run-1:step:1", Map.of("name", "orders"), effects)));
            // 相同幂等键重试：不重复计数
            assertEquals(1L, counter.execute(context("run-1:step:1", Map.of("name", "orders"), effects)));
            // 新的幂等键：正常计数
            assertEquals(2L, counter.execute(context("run-1:step:2", Map.of("name", "orders"), effects)));
        }
        // 计数器值持久化在文件中
        assertEquals("2", Files.readString(counterFile).trim());
    }

    @Test
    void echoReturnsConfiguredMessage() throws Exception {
        StepHandler echo = BuiltinHandlers.create(dataDir).get("echo");
        try (DurableEffectStore effects = DurableEffectStore.open(dataDir.resolve("e.effects"))) {
            assertEquals("你好", echo.execute(context("k", Map.of("message", "你好"), effects)));
            assertEquals("echo", echo.execute(context("k2", Map.of(), effects)));
        }
    }
}
