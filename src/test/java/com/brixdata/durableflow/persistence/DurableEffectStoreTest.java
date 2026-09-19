package com.brixdata.durableflow.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableEffectStoreTest {

    @TempDir
    Path dir;

    @Test
    void effectIsExecutedOnlyOnceAcrossReopens() {
        Path file = dir.resolve("run.effects");
        AtomicInteger executions = new AtomicInteger();

        try (DurableEffectStore store = DurableEffectStore.open(file)) {
            Object result = store.executeIdempotent("step-a", () -> {
                executions.incrementAndGet();
                return "done-1";
            });
            assertEquals("done-1", result);
        }
        // 模拟重启后恢复：相同幂等键不再执行副作用
        try (DurableEffectStore store = DurableEffectStore.open(file)) {
            Object result = store.executeIdempotent("step-a", () -> {
                executions.incrementAndGet();
                return "done-2";
            });
            assertEquals("done-1", result);
        }
        assertEquals(1, executions.get());
    }

    @Test
    void unknownKeyExecutesEffect() {
        try (DurableEffectStore store = DurableEffectStore.open(dir.resolve("run.effects"))) {
            assertTrue(store.find("missing").isEmpty());
            store.executeIdempotent("k", () -> 42);
            assertEquals(42, store.find("k").orElseThrow());
        }
    }
}
