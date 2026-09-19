package com.brixdata.durableflow.engine;

import com.brixdata.durableflow.config.EngineConfig;
import com.brixdata.durableflow.model.RetryPolicy;
import com.brixdata.durableflow.model.StepDefinition;
import com.brixdata.durableflow.model.StepStatus;
import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.model.WorkflowInstance;
import com.brixdata.durableflow.model.WorkflowStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowEngineTest {

    @TempDir
    Path dataDir;

    private EngineConfig config() {
        EngineConfig config = new EngineConfig();
        config.setDataDir(dataDir);
        config.setMaxConcurrency(4);
        config.setDefaultRetry(new RetryPolicy(3, 50, 2.0));
        return config;
    }

    private static WorkflowDefinition def(String id, StepDefinition... steps) {
        return new WorkflowDefinition(id, List.of(steps));
    }

    // ------------------------------------------------------------------
    // 正常执行
    // ------------------------------------------------------------------

    @Test
    void serialStepsRunInOrder() {
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        HandlerRegistry handlers = new HandlerRegistry()
                .register("record", ctx -> {
                    order.add(ctx.getStepId());
                    return "ok";
                });
        try (WorkflowEngine engine = new WorkflowEngine(config(), handlers)) {
            String id = engine.submit(def("serial",
                    StepDefinition.task("a", "record"),
                    StepDefinition.task("b", "record").dependsOn("a"),
                    StepDefinition.task("c", "record").dependsOn("b")), Map.of());
            assertEquals(WorkflowStatus.SUCCEEDED, engine.awaitTerminal(id, Duration.ofSeconds(10)));
            assertEquals(List.of("a", "b", "c"), order);
        }
    }

    @Test
    void parallelBranchesRespectConcurrencyLimit() {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        HandlerRegistry handlers = new HandlerRegistry().register("slow", ctx -> {
            int current = running.incrementAndGet();
            maxRunning.accumulateAndGet(current, Math::max);
            Thread.sleep(80);
            running.decrementAndGet();
            return "done";
        });
        EngineConfig config = config();
        config.setMaxConcurrency(2);
        try (WorkflowEngine engine = new WorkflowEngine(config, handlers)) {
            String id = engine.submit(def("parallel",
                    StepDefinition.task("p1", "slow"),
                    StepDefinition.task("p2", "slow"),
                    StepDefinition.task("p3", "slow"),
                    StepDefinition.task("p4", "slow"),
                    StepDefinition.task("join", "slow")
                            .dependsOn("p1", "p2", "p3", "p4")), Map.of());
            assertEquals(WorkflowStatus.SUCCEEDED, engine.awaitTerminal(id, Duration.ofSeconds(10)));
            assertTrue(maxRunning.get() <= 2, "concurrency exceeded limit: " + maxRunning.get());
            assertTrue(maxRunning.get() >= 2, "branches did not run in parallel");
        }
    }

    @Test
    void conditionalBranchSkipsUnmatchedSteps() {
        HandlerRegistry handlers = HandlerRegistry.withBuiltins();
        try (WorkflowEngine engine = new WorkflowEngine(config(), handlers)) {
            WorkflowDefinition definition = def("conditional",
                    StepDefinition.task("check", "echo")
                            .args(Map.of("message", "fast")),
                    StepDefinition.task("express", "echo").dependsOn("check")
                            .condition("input.express == true"),
                    StepDefinition.task("standard", "echo").dependsOn("check")
                            .condition("input.express != true"),
                    StepDefinition.task("fastLane", "echo").dependsOn("check")
                            .condition("steps.check.result == 'fast'"),
                    StepDefinition.task("done", "echo")
                            .dependsOn("express", "standard", "fastLane"));

            String id1 = engine.submit(definition, Map.of("express", true));
            assertEquals(WorkflowStatus.SUCCEEDED, engine.awaitTerminal(id1, Duration.ofSeconds(10)));
            WorkflowInstance inst1 = engine.instance(id1);
            assertEquals(StepStatus.SUCCEEDED, inst1.step("express").getStatus());
            assertEquals(StepStatus.SKIPPED, inst1.step("standard").getStatus());
            assertEquals(StepStatus.SUCCEEDED, inst1.step("fastLane").getStatus());
            assertEquals(StepStatus.SUCCEEDED, inst1.step("done").getStatus());

            String id2 = engine.submit(definition, Map.of("express", false));
            assertEquals(WorkflowStatus.SUCCEEDED, engine.awaitTerminal(id2, Duration.ofSeconds(10)));
            WorkflowInstance inst2 = engine.instance(id2);
            assertEquals(StepStatus.SKIPPED, inst2.step("express").getStatus());
            assertEquals(StepStatus.SUCCEEDED, inst2.step("standard").getStatus());
        }
    }

    // ------------------------------------------------------------------
    // 重试
    // ------------------------------------------------------------------

    @Test
    void failedStepIsRetriedWithBackoffUntilSuccess() {
        AtomicInteger attempts = new AtomicInteger();
        ConcurrentLinkedQueue<Long> attemptTimes = new ConcurrentLinkedQueue<>();
        HandlerRegistry handlers = new HandlerRegistry().register("flaky", ctx -> {
            attemptTimes.add(System.currentTimeMillis());
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("boom " + ctx.getAttempt());
            }
            return "ok";
        });
        EngineConfig config = config();
        config.setDefaultRetry(new RetryPolicy(3, 100, 2.0));
        try (WorkflowEngine engine = new WorkflowEngine(config, handlers)) {
            String id = engine.submit(def("retry",
                    StepDefinition.task("task", "flaky")), Map.of());
            assertEquals(WorkflowStatus.SUCCEEDED, engine.awaitTerminal(id, Duration.ofSeconds(10)));
            assertEquals(3, attempts.get());
            assertEquals(3, engine.instance(id).step("task").getAttempts());
            // 指数退避：第 1、2 次失败分别等待 100ms、200ms
            List<Long> times = List.copyOf(attemptTimes);
            assertTrue(times.get(1) - times.get(0) >= 80, "first backoff too short");
            assertTrue(times.get(2) - times.get(1) >= 150, "second backoff too short");
        }
    }

    @Test
    void stepFailingBeyondMaxAttemptsFailsWorkflow() {
        HandlerRegistry handlers = new HandlerRegistry().register("alwaysFail", ctx -> {
            throw new IllegalStateException("permanent");
        });
        try (WorkflowEngine engine = new WorkflowEngine(config(), handlers)) {
            String id = engine.submit(def("exhausted",
                    StepDefinition.task("bad", "alwaysFail").retry(new RetryPolicy(2, 20, 1.0)),
                    StepDefinition.task("never", "alwaysFail").dependsOn("bad")), Map.of());
            assertEquals(WorkflowStatus.FAILED, engine.awaitTerminal(id, Duration.ofSeconds(10)));
            WorkflowInstance instance = engine.instance(id);
            assertEquals(StepStatus.DEAD, instance.step("bad").getStatus());
            assertEquals(2, instance.step("bad").getAttempts());
            assertEquals(StepStatus.PENDING, instance.step("never").getStatus());
        }
    }

    // ------------------------------------------------------------------
    // 定时等待与恢复
    // ------------------------------------------------------------------

    @Test
    void waitStepResumesOnOriginalScheduleAfterRestart() throws Exception {
        WorkflowDefinition definition = def("waiter", StepDefinition.wait("nap", 400));
        long resumeAt;
        try (WorkflowEngine engine1 = new WorkflowEngine(config(), HandlerRegistry.withBuiltins())) {
            String id = engine1.submit(definition, Map.of());
            waitFor(() -> engine1.instance(id).step("nap").getStatus() == StepStatus.WAITING);
            resumeAt = engine1.instance(id).step("nap").getResumeAt();
        } // 在等待期间关闭引擎（模拟进程终止）

        long remaining = resumeAt - System.currentTimeMillis();
        if (remaining > 0) {
            Thread.sleep(remaining + 50); // 等到原计划时间之后
        }
        try (WorkflowEngine engine2 = new WorkflowEngine(config(), HandlerRegistry.withBuiltins())) {
            assertEquals(1, engine2.recover());
            long start = System.currentTimeMillis();
            String id = JsonUtil.deriveInstanceId("waiter", Map.of());
            assertEquals(WorkflowStatus.SUCCEEDED, engine2.awaitTerminal(id, Duration.ofSeconds(5)));
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed < 300,
                    "wait step re-timed after restart, elapsed=" + elapsed + "ms");
        }
    }

    @Test
    void retryScheduleSurvivesRestart() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicLong secondAttemptAt = new AtomicLong();
        HandlerRegistry handlers = new HandlerRegistry().register("flaky", ctx -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("first attempt fails");
            }
            secondAttemptAt.set(System.currentTimeMillis());
            return "ok";
        });
        EngineConfig config = config();
        config.setDefaultRetry(new RetryPolicy(3, 400, 1.0));
        String id = JsonUtil.deriveInstanceId("retry-restart", Map.of());
        long retryAt;
        try (WorkflowEngine engine1 = new WorkflowEngine(config, handlers)) {
            engine1.submit(def("retry-restart", StepDefinition.task("task", "flaky")), Map.of());
            waitFor(() -> engine1.instance(id).step("task").getStatus() == StepStatus.WAITING);
            retryAt = engine1.instance(id).step("task").getResumeAt();
        } // 在退避等待期间关闭

        try (WorkflowEngine engine2 = new WorkflowEngine(config, handlers)) {
            assertEquals(1, engine2.recover());
            assertEquals(WorkflowStatus.SUCCEEDED, engine2.awaitTerminal(id, Duration.ofSeconds(5)));
            assertEquals(2, attempts.get());
            // 按原计划时间重试，而非重启后重新计时
            assertTrue(secondAttemptAt.get() >= retryAt - 100,
                    "retry fired too early: scheduled=" + retryAt + " actual=" + secondAttemptAt.get());
        }
    }

    @Test
    void crashedEngineRecoversAndCompletes() {
        HandlerRegistry handlers = new HandlerRegistry()
                .register("echo2", ctx -> "ok-" + ctx.getStepId());
        String id;
        try (WorkflowEngine engine1 = new WorkflowEngine(config(), handlers)) {
            id = engine1.submit(def("crash",
                    StepDefinition.task("a", "echo2"),
                    StepDefinition.task("b", "echo2").dependsOn("a")), Map.of());
        } // 提交后立即关闭：步骤可能尚未开始或正在执行
        try (WorkflowEngine engine2 = new WorkflowEngine(config(), handlers)) {
            engine2.recover();
            assertEquals(WorkflowStatus.SUCCEEDED, engine2.awaitTerminal(id, Duration.ofSeconds(10)));
            assertEquals("ok-a", engine2.instance(id).step("a").getResult());
            assertEquals("ok-b", engine2.instance(id).step("b").getResult());
        }
    }

    // ------------------------------------------------------------------
    // 崩溃窗口：执行成功但完成状态未落盘
    // ------------------------------------------------------------------

    @Test
    void succeededButUnrecordedStepIsNotReexecutedAfterRecovery() throws Exception {
        AtomicInteger sideEffects = new AtomicInteger();
        HandlerRegistry handlers = new HandlerRegistry().register("once", ctx ->
                ctx.executeIdempotent("side-effect", () -> {
                    sideEffects.incrementAndGet();
                    return "done";
                }));
        AtomicReference<WorkflowEngine> ref = new AtomicReference<>();
        String id;
        try (WorkflowEngine engine1 = new WorkflowEngine(config(), handlers)) {
            ref.set(engine1);
            // 模拟崩溃窗口：处理器成功后、完成事件写入前进程终止
            engine1.setAfterHandlerSuccessHook(() -> ref.get().close());
            id = engine1.submit(def("crash-window",
                    StepDefinition.task("task", "once")), Map.of());
            assertTrue(engine1.awaitTermination(Duration.ofSeconds(5)));
        }
        assertEquals(1, sideEffects.get());

        try (WorkflowEngine engine2 = new WorkflowEngine(config(), handlers)) {
            engine2.recover();
            assertEquals(WorkflowStatus.SUCCEEDED, engine2.awaitTerminal(id, Duration.ofSeconds(10)));
            assertEquals("done", engine2.instance(id).step("task").getResult());
        }
        // 恢复后重新派发，但幂等存储命中缓存，副作用没有重复发生
        assertEquals(1, sideEffects.get());
    }

    @Test
    void duplicateSubmissionReturnsSameInstance() {
        AtomicInteger executions = new AtomicInteger();
        HandlerRegistry handlers = new HandlerRegistry()
                .register("count", ctx -> "n" + executions.incrementAndGet());
        WorkflowDefinition definition = def("dedup", StepDefinition.task("task", "count"));
        try (WorkflowEngine engine = new WorkflowEngine(config(), handlers)) {
            String id1 = engine.submit(definition, Map.of("k", "v"));
            String id2 = engine.submit(definition, Map.of("k", "v"));
            assertEquals(id1, id2);
            assertEquals(WorkflowStatus.SUCCEEDED, engine.awaitTerminal(id1, Duration.ofSeconds(10)));
            assertEquals(1, executions.get());
        }
        // 引擎重启后重复提交相同输入：返回已有实例，不重新执行
        try (WorkflowEngine engine2 = new WorkflowEngine(config(), handlers)) {
            engine2.recover();
            String id3 = engine2.submit(definition, Map.of("k", "v"));
            assertEquals(WorkflowStatus.SUCCEEDED, engine2.awaitTerminal(id3, Duration.ofSeconds(10)));
            assertEquals(1, executions.get());
            assertNotEquals(null, engine2.instance(id3));
        }
    }

    // ------------------------------------------------------------------
    // 补偿
    // ------------------------------------------------------------------

    @Test
    void failedWorkflowCompensatesSucceededStepsInReverseOrder() {
        List<String> compensated = Collections.synchronizedList(new ArrayList<>());
        HandlerRegistry handlers = new HandlerRegistry()
                .register("ok", ctx -> "ok")
                .register("comp", ctx -> {
                    compensated.add(ctx.getStepId());
                    return "compensated";
                })
                .register("bad", ctx -> {
                    throw new IllegalStateException("final failure");
                });
        try (WorkflowEngine engine = new WorkflowEngine(config(), handlers)) {
            String id = engine.submit(def("saga",
                    StepDefinition.task("a", "ok").compensation("comp", Map.of()),
                    StepDefinition.task("b", "ok").dependsOn("a").compensation("comp", Map.of()),
                    StepDefinition.task("c", "bad").dependsOn("b").retry(RetryPolicy.noRetry())),
                    Map.of());
            assertEquals(WorkflowStatus.COMPENSATED, engine.awaitTerminal(id, Duration.ofSeconds(10)));
            assertEquals(List.of("b", "a"), compensated);
            assertTrue(engine.instance(id).step("a").isCompensated());
            assertTrue(engine.instance(id).step("b").isCompensated());
        }
    }

    @Test
    void failingCompensationIsRetriedAndEventuallySucceeds() {
        AtomicInteger compAttempts = new AtomicInteger();
        HandlerRegistry handlers = new HandlerRegistry()
                .register("ok", ctx -> "ok")
                .register("flakyComp", ctx -> {
                    if (compAttempts.incrementAndGet() < 2) {
                        throw new IllegalStateException("comp failure");
                    }
                    return "compensated";
                })
                .register("bad", ctx -> {
                    throw new IllegalStateException("boom");
                });
        try (WorkflowEngine engine = new WorkflowEngine(config(), handlers)) {
            String id = engine.submit(def("saga-retry",
                    StepDefinition.task("a", "ok")
                            .retry(new RetryPolicy(3, 20, 1.0))
                            .compensation("flakyComp", Map.of()),
                    StepDefinition.task("c", "bad").dependsOn("a").retry(RetryPolicy.noRetry())),
                    Map.of());
            assertEquals(WorkflowStatus.COMPENSATED, engine.awaitTerminal(id, Duration.ofSeconds(10)));
            assertEquals(2, compAttempts.get());
            assertEquals(2, engine.instance(id).step("a").getCompensationAttempts());
        }
    }

    @Test
    void exhaustedCompensationMarksWorkflowCompensationFailed() {
        HandlerRegistry handlers = new HandlerRegistry()
                .register("ok", ctx -> "ok")
                .register("badComp", ctx -> {
                    throw new IllegalStateException("comp always fails");
                })
                .register("bad", ctx -> {
                    throw new IllegalStateException("boom");
                });
        try (WorkflowEngine engine = new WorkflowEngine(config(), handlers)) {
            String id = engine.submit(def("saga-dead",
                    StepDefinition.task("a", "ok")
                            .retry(new RetryPolicy(2, 20, 1.0))
                            .compensation("badComp", Map.of()),
                    StepDefinition.task("c", "bad").dependsOn("a").retry(RetryPolicy.noRetry())),
                    Map.of());
            assertEquals(WorkflowStatus.COMPENSATION_FAILED,
                    engine.awaitTerminal(id, Duration.ofSeconds(10)));
        }
    }

    @Test
    void compensationContinuesAfterCrashWithIdempotentHandlers() throws Exception {
        List<String> compLog = Collections.synchronizedList(new ArrayList<>());
        HandlerRegistry handlers = new HandlerRegistry()
                .register("ok", ctx -> "ok")
                .register("comp", ctx -> ctx.executeIdempotent("comp", () -> {
                    compLog.add(ctx.getStepId());
                    return "compensated";
                }))
                .register("bad", ctx -> {
                    throw new IllegalStateException("boom");
                });
        AtomicInteger handlerSuccesses = new AtomicInteger();
        AtomicReference<WorkflowEngine> ref = new AtomicReference<>();
        String id;
        try (WorkflowEngine engine1 = new WorkflowEngine(config(), handlers)) {
            ref.set(engine1);
            // a、b 成功后，第一个补偿（comp-b）处理器成功但事件未写入即崩溃
            engine1.setAfterHandlerSuccessHook(() -> {
                if (handlerSuccesses.incrementAndGet() == 3) {
                    ref.get().close();
                }
            });
            id = engine1.submit(def("saga-crash",
                    StepDefinition.task("a", "ok").compensation("comp", Map.of()),
                    StepDefinition.task("b", "ok").dependsOn("a").compensation("comp", Map.of()),
                    StepDefinition.task("c", "bad").dependsOn("b").retry(RetryPolicy.noRetry())),
                    Map.of());
            assertTrue(engine1.awaitTermination(Duration.ofSeconds(5)));
        }
        try (WorkflowEngine engine2 = new WorkflowEngine(config(), handlers)) {
            engine2.recover();
            assertEquals(WorkflowStatus.COMPENSATED, engine2.awaitTerminal(id, Duration.ofSeconds(10)));
        }
        // comp-b 的完成事件虽丢失，但幂等键去重，副作用各只发生一次
        assertEquals(List.of("b", "a"), compLog);
    }

    // ------------------------------------------------------------------

    private static void waitFor(Check check) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("condition not met within timeout");
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }
}
