package com.brixdata.durableflow.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.brixdata.durableflow.model.RetryPolicy;
import com.brixdata.durableflow.model.StepDefinition;
import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.persistence.Event;
import com.brixdata.durableflow.persistence.Journal;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkflowEngineTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @TempDir
    Path dataDir;

    private WorkflowEngine engine(Map<String, StepHandler> handlers, int maxConcurrency) {
        return new WorkflowEngine(new EngineConfig(dataDir, maxConcurrency), handlers);
    }

    private static StepHandler returning(Object value) {
        return ctx -> value;
    }

    // ---------------------------------------------------------------- 基本流程

    @Test
    void serialStepsExecuteInDependencyOrder() {
        List<String> order = new CopyOnWriteArrayList<>();
        Map<String, StepHandler> handlers = new HashMap<>();
        for (String id : List.of("a", "b", "c")) {
            handlers.put(id, ctx -> {
                order.add(ctx.stepId());
                return ctx.stepId() + "-done";
            });
        }
        WorkflowDefinition def = new WorkflowDefinition("serial", List.of(
                StepDefinition.task("a", "a").build(),
                StepDefinition.task("b", "b").dependsOn("a").build(),
                StepDefinition.task("c", "c").dependsOn("b").build()));

        try (WorkflowEngine engine = engine(handlers, 4)) {
            String runId = engine.submit(def, Map.of());
            assertTrue(engine.await(runId, TIMEOUT));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            assertEquals(RunStatus.COMPLETED, snapshot.status());
            assertEquals(List.of("a", "b", "c"), order);
            assertEquals(Map.of("a", "a-done", "b", "b-done", "c", "c-done"), snapshot.result());
        }
    }

    @Test
    void parallelBranchesRespectConcurrencyLimit() {
        AtomicInteger current = new AtomicInteger();
        AtomicInteger maxSeen = new AtomicInteger();
        Map<String, StepHandler> handlers = new HashMap<>();
        List<StepDefinition> steps = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String id = "p" + i;
            steps.add(StepDefinition.task(id, "parallel").build());
        }
        handlers.put("parallel", ctx -> {
            int now = current.incrementAndGet();
            maxSeen.accumulateAndGet(now, Math::max);
            Thread.sleep(150);
            current.decrementAndGet();
            return ctx.stepId();
        });
        WorkflowDefinition def = new WorkflowDefinition("parallel", steps);

        try (WorkflowEngine engine = engine(handlers, 2)) {
            String runId = engine.submit(def, Map.of());
            assertTrue(engine.await(runId, TIMEOUT));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            assertEquals(RunStatus.COMPLETED, snapshot.status());
            assertEquals(6, snapshot.result().size());
            assertTrue(maxSeen.get() <= 2, "并发峰值 " + maxSeen.get() + " 超过限制 2");
            assertTrue(maxSeen.get() >= 1);
        }
    }

    @Test
    void conditionalBranchesAreSkippedWhenConditionFalse() {
        Map<String, StepHandler> handlers = Map.of("echo", returning("ok"));
        WorkflowDefinition def = new WorkflowDefinition("cond", List.of(
                StepDefinition.task("a", "echo").build(),
                StepDefinition.task("b", "echo").dependsOn("a")
                        .condition("input.vip == true").build(),
                StepDefinition.task("c", "echo").dependsOn("b").build(),
                StepDefinition.task("d", "echo").dependsOn("a")
                        .condition("input.level >= 5").build()));

        try (WorkflowEngine engine = engine(handlers, 4)) {
            String runId = engine.submit(def, Map.of("vip", false, "level", 9));
            assertTrue(engine.await(runId, TIMEOUT));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            assertEquals(RunStatus.COMPLETED, snapshot.status());
            Map<String, StepStatus> statuses = statuses(snapshot);
            assertEquals(StepStatus.COMPLETED, statuses.get("a"));
            assertEquals(StepStatus.SKIPPED, statuses.get("b"));
            // 依赖全部被跳过 -> 级联跳过
            assertEquals(StepStatus.SKIPPED, statuses.get("c"));
            assertEquals(StepStatus.COMPLETED, statuses.get("d"));
            assertEquals(Map.of("a", "ok", "d", "ok"), snapshot.result());
        }
    }

    // ---------------------------------------------------------------- 重试

    @Test
    void failedStepRetriesWithBackoffThenSucceeds() {
        AtomicInteger attempts = new AtomicInteger();
        List<Long> attemptTimes = new CopyOnWriteArrayList<>();
        Map<String, StepHandler> handlers = Map.of("flaky", ctx -> {
            attemptTimes.add(System.currentTimeMillis());
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("临时失败 " + ctx.attempt());
            }
            return "recovered";
        });
        WorkflowDefinition def = new WorkflowDefinition("retry", List.of(
                StepDefinition.task("a", "flaky")
                        .retry(new RetryPolicy(3, 200, 2.0, 5000)).build()));

        try (WorkflowEngine engine = engine(handlers, 2)) {
            String runId = engine.submit(def, Map.of());
            assertTrue(engine.await(runId, TIMEOUT));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            assertEquals(RunStatus.COMPLETED, snapshot.status());
            assertEquals(3, attempts.get());
            assertEquals(Map.of("a", "recovered"), snapshot.result());
            // 退避 200ms、400ms（调度 tick 会向上取整，容差取下界）
            long gap1 = attemptTimes.get(1) - attemptTimes.get(0);
            long gap2 = attemptTimes.get(2) - attemptTimes.get(1);
            assertTrue(gap1 >= 180, "首次退避过短: " + gap1);
            assertTrue(gap2 >= 380, "第二次退避过短: " + gap2);
        }
    }

    @Test
    void retryExhaustionFailsWorkflow() {
        AtomicInteger attempts = new AtomicInteger();
        Map<String, StepHandler> handlers = Map.of("alwaysFail", ctx -> {
            attempts.incrementAndGet();
            throw new RuntimeException("永久失败");
        });
        WorkflowDefinition def = new WorkflowDefinition("exhaust", List.of(
                StepDefinition.task("a", "alwaysFail")
                        .retry(new RetryPolicy(2, 50, 1.0, 1000)).build(),
                StepDefinition.task("b", "alwaysFail").dependsOn("a").build()));

        try (WorkflowEngine engine = engine(handlers, 2)) {
            String runId = engine.submit(def, Map.of());
            assertTrue(engine.await(runId, TIMEOUT));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            assertEquals(RunStatus.FAILED, snapshot.status());
            assertEquals(2, attempts.get());
            assertEquals(StepStatus.FAILED, statuses(snapshot).get("a"));
            // 依赖未满足，b 从未执行
            assertEquals(StepStatus.PENDING, statuses(snapshot).get("b"));
        }
    }

    @Test
    void retryScheduleSurvivesRestartWithoutReTiming() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<Long> attemptTimes = new CopyOnWriteArrayList<>();
        Map<String, StepHandler> handlers = Map.of("flaky", ctx -> {
            attemptTimes.add(System.currentTimeMillis());
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("首次失败");
            }
            return "ok";
        });
        WorkflowDefinition def = new WorkflowDefinition("retry-restart", List.of(
                StepDefinition.task("a", "flaky")
                        .retry(new RetryPolicy(2, 1500, 1.0, 5000)).build()));

        WorkflowEngine first = engine(handlers, 2);
        String runId = first.submit(def, Map.of());
        // 等待首次失败及重试排期（dueAt）落盘
        waitFor(() -> attemptTimes.size() == 1 && journalEvents(runId).stream()
                .anyMatch(e -> e.type() == Event.EventType.STEP_FAILED), TIMEOUT);
        long failedAt = System.currentTimeMillis();
        first.crash();

        Thread.sleep(1000); // 崩溃期间流逝 1 秒
        long recoveredAt = System.currentTimeMillis();
        WorkflowEngine second = engine(handlers, 2);
        assertTrue(second.await(runId, TIMEOUT));
        assertEquals(RunStatus.COMPLETED, second.snapshot(runId).status());
        second.crash();

        assertEquals(2, attempts.get());
        long retryAt = attemptTimes.get(1);
        // 原计划：失败后 1500ms 重试。恢复发生在失败 1 秒后，
        // 因此恢复后约 500ms 即应重试；若重启重新计时则需再等 1500ms。
        assertTrue(retryAt - recoveredAt < 1200,
                "重试疑似被重新计时: 恢复后 " + (retryAt - recoveredAt) + "ms 才执行");
        assertTrue(retryAt - failedAt >= 1300,
                "重试早于原计划: 距失败仅 " + (retryAt - failedAt) + "ms");
    }

    // ---------------------------------------------------------------- 定时等待

    @Test
    void waitStepResumesAtPersistedTimeAfterRestart() throws Exception {
        Map<String, StepHandler> handlers = Map.of("echo", returning("done"));
        WorkflowDefinition def = new WorkflowDefinition("wait", List.of(
                StepDefinition.waitStep("w", 2000).build(),
                StepDefinition.task("after", "echo").dependsOn("w").build()));

        long startedAt = System.currentTimeMillis();
        WorkflowEngine first = engine(handlers, 2);
        String runId = first.submit(def, Map.of());
        // 等待 WAIT_SCHEDULED（含 resumeAt）落盘
        waitFor(() -> journalEvents(runId).stream()
                .anyMatch(e -> e.type() == Event.EventType.WAIT_SCHEDULED), TIMEOUT);
        first.crash();

        Thread.sleep(1000); // 崩溃期间流逝 1 秒
        WorkflowEngine second = engine(handlers, 2);
        assertTrue(second.await(runId, TIMEOUT));
        long completedAt = System.currentTimeMillis();
        assertEquals(RunStatus.COMPLETED, second.snapshot(runId).status());
        second.crash();

        long total = completedAt - startedAt;
        // 原计划 2000ms 后唤醒：总耗时应约 2000ms，而非恢复后重新计时 2000ms（约 3000ms+）
        assertTrue(total >= 1800, "定时等待提前完成: " + total + "ms");
        assertTrue(total < 2900, "定时等待疑似被重新计时: " + total + "ms");
        // 恢复后不应产生第二条 WAIT_SCHEDULED
        long waitScheduledCount = journalEvents(runId).stream()
                .filter(e -> e.type() == Event.EventType.WAIT_SCHEDULED).count();
        assertEquals(1, waitScheduledCount);
    }

    // ---------------------------------------------------------------- 崩溃恢复

    @Test
    void crashBeforeQueuedStepStartsRecoversWithoutReRunningIt() {
        AtomicInteger aExecutions = new AtomicInteger();
        AtomicInteger bExecutions = new AtomicInteger();
        CountDownLatch aStarted = new CountDownLatch(1);
        Map<String, StepHandler> handlers = Map.of(
                "blocker", ctx -> {
                    if (aExecutions.incrementAndGet() == 1) {
                        aStarted.countDown();
                        Thread.sleep(60_000); // 占住唯一的工作线程，直到崩溃中断
                    }
                    return "a-done";
                },
                "queued", ctx -> {
                    bExecutions.incrementAndGet();
                    return "b-done";
                });
        WorkflowDefinition def = new WorkflowDefinition("crash-before-start", List.of(
                StepDefinition.task("a", "blocker").build(),
                StepDefinition.task("b", "queued").build()));

        // 单线程执行池：a 占住线程，b 的 STEP_STARTED 已落盘但任务仍在队列中
        WorkflowEngine first = engine(handlers, 1);
        String runId = first.submit(def, Map.of());
        waitFor(() -> {
            try {
                return aStarted.getCount() == 0
                        && journalEvents(runId).stream()
                        .filter(e -> e.type() == Event.EventType.STEP_STARTED)
                        .count() == 2;
            } catch (Exception e) {
                return false;
            }
        }, TIMEOUT);
        first.crash();

        WorkflowEngine second = engine(handlers, 2);
        assertTrue(second.await(runId, TIMEOUT));
        WorkflowSnapshot snapshot = second.snapshot(runId);
        assertEquals(RunStatus.COMPLETED, snapshot.status());
        assertEquals(Map.of("a", "a-done", "b", "b-done"), snapshot.result());
        second.crash();

        assertEquals(2, aExecutions.get(), "a 应在恢复后重新执行一次");
        assertEquals(1, bExecutions.get(), "b 在崩溃前未开始，恢复后只应执行一次");
    }

    @Test
    void crashDuringExecutionDoesNotDuplicateSideEffects() {
        AtomicInteger sideEffects = new AtomicInteger();
        CountDownLatch effectRecorded = new CountDownLatch(1);
        StepHandler chargeHandler = ctx -> {
            boolean[] freshExecution = {false};
            Object result = ctx.executeIdempotent(() -> {
                freshExecution[0] = true;
                sideEffects.incrementAndGet(); // 模拟对外副作用（如扣款）
                return "receipt-1";
            });
            if (freshExecution[0]) {
                // 副作用刚发生、完成事件尚未写入：挂起等待测试触发崩溃。
                // 恢复后重试时 executeIdempotent 直接命中已记录结果，不会走到这里。
                effectRecorded.countDown();
                Thread.sleep(60_000);
            }
            return result;
        };
        WorkflowDefinition def = new WorkflowDefinition("payment", List.of(
                StepDefinition.task("charge", "charge").build(),
                StepDefinition.task("notify", "notify").dependsOn("charge").build()));

        WorkflowEngine first = engine(Map.of(
                "charge", chargeHandler,
                "notify", returning("notified")), 2);
        String runId = first.submit(def, Map.of());
        // 副作用已发生并落盘，但 STEP_COMPLETED 尚未写入时崩溃
        waitFor(() -> effectRecorded.getCount() == 0, TIMEOUT);
        first.crash();

        // 恢复后：charge 以相同幂等键重试，直接取回已记录结果，副作用不重复
        WorkflowEngine second = engine(Map.of(
                "charge", chargeHandler,
                "notify", returning("notified")), 2);
        assertTrue(second.await(runId, TIMEOUT));
        WorkflowSnapshot snapshot = second.snapshot(runId);
        assertEquals(RunStatus.COMPLETED, snapshot.status());
        assertEquals(1, sideEffects.get(), "副作用被执行了 " + sideEffects.get() + " 次");
        assertEquals("receipt-1", snapshot.result().get("charge"));
        second.crash();
    }

    @Test
    void finalResultIsStableAcrossCrashAndRecovery() {
        // 第一次：干净执行
        Map<String, Object> cleanResult;
        try (WorkflowEngine engine = engine(deterministicHandlers(), 2)) {
            String runId = engine.submit(deterministicWorkflow(), Map.of("seed", 7));
            assertTrue(engine.await(runId, TIMEOUT));
            cleanResult = engine.snapshot(runId).result();
        }
        // 第二次：执行中崩溃后恢复
        Path crashDir = dataDir.resolve("run2");
        CountDownLatch bStarted = new CountDownLatch(1);
        Map<String, StepHandler> handlers = new HashMap<>(deterministicHandlers());
        handlers.put("b", ctx -> {
            bStarted.countDown();
            Thread.sleep(60_000);
            return deterministicHandlers().get("b").execute(ctx);
        });
        WorkflowEngine first = new WorkflowEngine(new EngineConfig(crashDir, 2), handlers);
        String runId = first.submit(deterministicWorkflow(), Map.of("seed", 7));
        waitFor(() -> bStarted.getCount() == 0, TIMEOUT);
        first.crash();

        WorkflowEngine second = new WorkflowEngine(new EngineConfig(crashDir, 2), deterministicHandlers());
        assertTrue(second.await(runId, TIMEOUT));
        Map<String, Object> recoveredResult = second.snapshot(runId).result();
        second.crash();

        assertEquals(cleanResult, recoveredResult, "相同输入下崩溃恢复后的最终结果应保持一致");
    }

    private static Map<String, StepHandler> deterministicHandlers() {
        Map<String, StepHandler> handlers = new HashMap<>();
        handlers.put("a", ctx -> Map.of("value", ((Number) ctx.input().get("seed")).intValue() * 2));
        handlers.put("b", ctx -> {
            Number v = (Number) ((Map<?, ?>) ctx.stepResults().get("a")).get("value");
            return Map.of("value", v.intValue() + 1);
        });
        handlers.put("c", ctx -> {
            Number v = (Number) ((Map<?, ?>) ctx.stepResults().get("a")).get("value");
            return Map.of("value", v.intValue() + 2);
        });
        handlers.put("d", ctx -> {
            Number b = (Number) ((Map<?, ?>) ctx.stepResults().get("b")).get("value");
            Number c = (Number) ((Map<?, ?>) ctx.stepResults().get("c")).get("value");
            return Map.of("total", b.intValue() + c.intValue());
        });
        return handlers;
    }

    private static WorkflowDefinition deterministicWorkflow() {
        return new WorkflowDefinition("deterministic", List.of(
                StepDefinition.task("a", "a").build(),
                StepDefinition.task("b", "b").dependsOn("a").build(),
                StepDefinition.task("c", "c").dependsOn("a").build(),
                StepDefinition.task("d", "d").dependsOn("b", "c").build()));
    }

    // ---------------------------------------------------------------- 工具

    private static Map<String, StepStatus> statuses(WorkflowSnapshot snapshot) {
        Map<String, StepStatus> map = new ConcurrentHashMap<>();
        snapshot.steps().forEach(s -> map.put(s.stepId(), s.status()));
        return map;
    }

    private List<Event> journalEvents(String runId) {
        Path file = dataDir.resolve("workflows").resolve(runId + ".journal");
        return Journal.replay(file);
    }

    private static void waitFor(Check check, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                if (check.passed()) {
                    return;
                }
            } catch (Exception ignored) {
                // 继续等待
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("等待被中断");
            }
        }
        throw new AssertionError("等待条件超时");
    }

    @FunctionalInterface
    private interface Check {
        boolean passed() throws Exception;
    }
}
