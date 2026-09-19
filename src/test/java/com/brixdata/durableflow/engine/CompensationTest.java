package com.brixdata.durableflow.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.brixdata.durableflow.model.CompensationDef;
import com.brixdata.durableflow.model.RetryPolicy;
import com.brixdata.durableflow.model.StepDefinition;
import com.brixdata.durableflow.model.WorkflowDefinition;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 补偿（saga 回滚）流程测试。
 */
class CompensationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @TempDir
    Path dataDir;

    private final List<String> compensated = new CopyOnWriteArrayList<>();
    private final Map<String, AtomicInteger> compensationAttempts = new ConcurrentHashMap<>();

    private StepHandler recordingCompensation(String name) {
        return ctx -> {
            compensationAttempts.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            compensated.add(name);
            return null;
        };
    }

    private WorkflowDefinition sagaWorkflow(RetryPolicy stepRetry, RetryPolicy compRetry) {
        return new WorkflowDefinition("saga", List.of(
                StepDefinition.task("reserve", "ok")
                        .compensation(new CompensationDef("undo-reserve", Map.of(), compRetry))
                        .build(),
                StepDefinition.task("pay", "ok").dependsOn("reserve")
                        .compensation(new CompensationDef("undo-pay", Map.of(), compRetry))
                        .build(),
                StepDefinition.task("ship", "broken").dependsOn("pay")
                        .retry(stepRetry)
                        .compensation(new CompensationDef("undo-ship", Map.of(), compRetry))
                        .build()));
    }

    private Map<String, StepHandler> baseHandlers() {
        Map<String, StepHandler> handlers = new HashMap<>();
        handlers.put("ok", ctx -> ctx.stepId() + "-done");
        handlers.put("broken", ctx -> {
            throw new RuntimeException("发货失败");
        });
        handlers.put("undo-reserve", recordingCompensation("undo-reserve"));
        handlers.put("undo-pay", recordingCompensation("undo-pay"));
        handlers.put("undo-ship", recordingCompensation("undo-ship"));
        return handlers;
    }

    @Test
    void completedStepsAreCompensatedInReverseCompletionOrder() {
        WorkflowDefinition def = sagaWorkflow(new RetryPolicy(2, 50, 1.0, 500), RetryPolicy.NONE);

        try (WorkflowEngine engine = new WorkflowEngine(
                new EngineConfig(dataDir, 2), baseHandlers())) {
            String runId = engine.submit(def, Map.of());
            assertTrue(engine.await(runId, TIMEOUT));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            assertEquals(RunStatus.FAILED, snapshot.status());
            // ship 未成功完成，不参与补偿；已完成的 pay、reserve 按完成逆序补偿
            assertEquals(List.of("undo-pay", "undo-reserve"), compensated);
            assertEquals(List.of("pay", "reserve"), snapshot.compensatedSteps());
            assertTrue(snapshot.failedCompensations().isEmpty());
        }
    }

    @Test
    void failedCompensationIsRetriedUntilSuccess() {
        AtomicInteger undoPayCalls = new AtomicInteger();
        Map<String, StepHandler> handlers = baseHandlers();
        handlers.put("undo-pay", ctx -> {
            if (undoPayCalls.incrementAndGet() == 1) {
                throw new RuntimeException("退款暂时失败");
            }
            compensated.add("undo-pay");
            return null;
        });
        RetryPolicy compRetry = new RetryPolicy(3, 100, 1.0, 1000);
        WorkflowDefinition def = sagaWorkflow(RetryPolicy.NONE, compRetry);

        try (WorkflowEngine engine = new WorkflowEngine(new EngineConfig(dataDir, 2), handlers)) {
            String runId = engine.submit(def, Map.of());
            assertTrue(engine.await(runId, TIMEOUT));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            assertEquals(RunStatus.FAILED, snapshot.status());
            assertEquals(2, undoPayCalls.get(), "补偿失败后应重试");
            assertEquals(List.of("pay", "reserve"), snapshot.compensatedSteps());
            assertTrue(snapshot.failedCompensations().isEmpty());
        }
    }

    @Test
    void exhaustedCompensationIsRecordedAndRemainingCompensationsContinue() {
        Map<String, StepHandler> handlers = baseHandlers();
        handlers.put("undo-pay", ctx -> {
            throw new RuntimeException("退款永久失败");
        });
        RetryPolicy compRetry = new RetryPolicy(2, 50, 1.0, 500);
        WorkflowDefinition def = sagaWorkflow(RetryPolicy.NONE, compRetry);

        try (WorkflowEngine engine = new WorkflowEngine(new EngineConfig(dataDir, 2), handlers)) {
            String runId = engine.submit(def, Map.of());
            assertTrue(engine.await(runId, TIMEOUT));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            assertEquals(RunStatus.FAILED, snapshot.status());
            // pay 的补偿重试耗尽被记录，reserve 的补偿仍继续执行
            assertEquals(List.of("pay"), snapshot.failedCompensations());
            assertEquals(List.of("reserve"), snapshot.compensatedSteps());
        }
    }

    @Test
    void compensationResumesAfterCrash() {
        CountDownLatch undoPayStarted = new CountDownLatch(1);
        Map<String, StepHandler> handlers = baseHandlers();
        handlers.put("undo-pay", ctx -> {
            undoPayStarted.countDown();
            Thread.sleep(60_000); // 占住直到崩溃；恢复后由新引擎的 handler 完成
            compensated.add("undo-pay");
            return null;
        });
        WorkflowDefinition def = sagaWorkflow(RetryPolicy.NONE, new RetryPolicy(2, 100, 1.0, 500));

        WorkflowEngine first = new WorkflowEngine(new EngineConfig(dataDir, 2), handlers);
        String runId = first.submit(def, Map.of());
        // 等待补偿开始（COMPENSATION_STARTED 已落盘）后崩溃
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        while (undoPayStarted.getCount() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        first.crash();

        // 恢复后补偿流程从断点继续：undo-pay 重新执行，随后 undo-reserve
        WorkflowEngine second = new WorkflowEngine(new EngineConfig(dataDir, 2), baseHandlers());
        assertTrue(second.await(runId, TIMEOUT));
        WorkflowSnapshot snapshot = second.snapshot(runId);
        assertEquals(RunStatus.FAILED, snapshot.status());
        assertEquals(List.of("undo-pay", "undo-reserve"), compensated);
        assertEquals(List.of("pay", "reserve"), snapshot.compensatedSteps());
        second.crash();
    }
}
