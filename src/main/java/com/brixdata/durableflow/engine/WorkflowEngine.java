package com.brixdata.durableflow.engine;

import com.brixdata.durableflow.engine.RunState.StepRuntime;
import com.brixdata.durableflow.json.Jsons;
import com.brixdata.durableflow.model.CompensationDef;
import com.brixdata.durableflow.model.Condition;
import com.brixdata.durableflow.model.RetryPolicy;
import com.brixdata.durableflow.model.StepDefinition;
import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.persistence.DurableEffectStore;
import com.brixdata.durableflow.persistence.Event;
import com.brixdata.durableflow.persistence.Event.EventType;
import com.brixdata.durableflow.persistence.Journal;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * 持久化工作流引擎。
 *
 * <p>所有状态变迁先以事件形式追加到该运行的日志文件并 fsync，再更新内存状态，
 * 因此进程在任意时刻被终止，重启后都能通过重放日志恢复到一致状态继续执行：</p>
 * <ul>
 *   <li>步骤开始前崩溃：STEP_STARTED 已写入，恢复后按同一尝试号与幂等键重新执行；</li>
 *   <li>步骤执行中崩溃：恢复后重新执行该尝试，副作用通过幂等效果存储去重；</li>
 *   <li>步骤完成后崩溃：STEP_COMPLETED 已写入，恢复后直接推进后续步骤；</li>
 *   <li>等待重试或定时唤醒期间崩溃：重试/唤醒的绝对时间已持久化，恢复后按原计划触发。</li>
 * </ul>
 */
public final class WorkflowEngine implements AutoCloseable {

    private final EngineConfig config;
    private final Map<String, StepHandler> handlers;
    private final ExecutorService workers;
    private final Map<String, RunState> runs = new LinkedHashMap<>();
    private final Object lock = new Object();
    private final Thread dispatcher;
    private volatile boolean running = true;

    public WorkflowEngine(EngineConfig config, Map<String, StepHandler> handlers) {
        this.config = config;
        this.handlers = new HashMap<>(handlers);
        this.workers = Executors.newFixedThreadPool(config.maxConcurrency(), r -> {
            Thread t = new Thread(r, "durableflow-worker");
            t.setDaemon(true);
            return t;
        });
        recover();
        this.dispatcher = new Thread(this::dispatchLoop, "durableflow-dispatcher");
        this.dispatcher.setDaemon(true);
        this.dispatcher.start();
    }

    // ---------------------------------------------------------------- 提交与查询

    /**
     * 提交一个工作流，返回运行 ID。提交事件落盘后调度即刻开始。
     */
    public String submit(WorkflowDefinition definition, Map<String, Object> input) {
        String runId = UUID.randomUUID().toString();
        synchronized (lock) {
            ensureRunning();
            RunState state = new RunState(runId);
            state.journal = Journal.open(journalPath(runId));
            state.effects = DurableEffectStore.open(effectsPath(runId));
            ObjectNode data = Jsons.obj();
            data.set("definition", definition.toJson());
            data.set("input", Jsons.toNode(input == null ? Map.of() : input));
            Event event = Event.of(EventType.WORKFLOW_SUBMITTED, data);
            state.journal.append(event);
            applyEvent(state, event);
            runs.put(runId, state);
            lock.notifyAll();
            return runId;
        }
    }

    /**
     * 等待指定运行进入终态。
     *
     * @return true 表示已进入终态，false 表示超时
     */
    public boolean await(String runId, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        synchronized (lock) {
            RunState state = runs.get(runId);
            if (state == null) {
                throw new IllegalArgumentException("未知运行: " + runId);
            }
            while (!state.isTerminal()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    lock.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * 等待所有已注册运行进入终态。
     */
    public boolean awaitAll(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        synchronized (lock) {
            while (runs.values().stream().anyMatch(r -> !r.isTerminal())) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    lock.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * 查询运行状态快照。
     */
    public WorkflowSnapshot snapshot(String runId) {
        synchronized (lock) {
            RunState state = runs.get(runId);
            if (state == null) {
                throw new IllegalArgumentException("未知运行: " + runId);
            }
            return toSnapshot(state);
        }
    }

    /**
     * 列出本引擎管理的全部运行快照。
     */
    public List<WorkflowSnapshot> listSnapshots() {
        synchronized (lock) {
            return runs.values().stream().map(WorkflowEngine::toSnapshot).toList();
        }
    }

    /**
     * 直接重放日志文件生成快照（不启动引擎），供 CLI 查询历史运行。
     */
    public static WorkflowSnapshot readSnapshot(Path journalFile) {
        RunState state = new RunState(journalFile.getFileName().toString().replace(".journal", ""));
        for (Event event : Journal.replay(journalFile)) {
            applyEvent(state, event);
        }
        return toSnapshot(state);
    }

    /**
     * 列出数据目录下全部运行日志文件。
     */
    public static List<Path> listJournalFiles(Path dataDir) {
        Path dir = workflowsDir(dataDir);
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(".journal"))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("列举运行日志失败: " + dir, e);
        }
    }

    // ---------------------------------------------------------------- 恢复

    private void recover() {
        for (Path file : listJournalFiles(config.dataDir())) {
            String runId = file.getFileName().toString().replace(".journal", "");
            RunState state = new RunState(runId);
            for (Event event : Journal.replay(file)) {
                applyEvent(state, event);
            }
            if (state.isTerminal()) {
                continue;
            }
            state.journal = Journal.open(file);
            state.effects = DurableEffectStore.open(effectsPath(runId));
            runs.put(runId, state);
        }
    }

    private static void applyEvent(RunState state, Event event) {
        ObjectNode data = event.data();
        String stepId = data.path("stepId").asText(null);
        switch (event.type()) {
            case WORKFLOW_SUBMITTED -> {
                state.definition = WorkflowDefinition.fromJson(data.get("definition"));
                state.input = Jsons.toMap(data.get("input"));
                for (StepDefinition step : state.definition.steps()) {
                    state.steps.put(step.id(), new StepRuntime());
                }
            }
            case STEP_STARTED -> {
                StepRuntime st = state.steps.get(stepId);
                st.status = StepStatus.RUNNING;
                st.attempt = data.path("attempt").asInt();
                st.inFlight = false; // 重放时不存在在执行的任务，恢复后由调度器重新提交
            }
            case STEP_COMPLETED -> {
                StepRuntime st = state.steps.get(stepId);
                st.status = StepStatus.COMPLETED;
                st.result = Jsons.toJava(data.get("result"));
                st.completedSeq = data.path("seq").asLong();
                state.completionSeq = Math.max(state.completionSeq, st.completedSeq);
            }
            case STEP_FAILED -> {
                StepRuntime st = state.steps.get(stepId);
                if (data.path("final").asBoolean()) {
                    st.status = StepStatus.FAILED;
                    state.error = data.path("error").asText();
                } else {
                    st.status = StepStatus.WAITING;
                    st.dueAt = data.path("dueAt").asLong();
                }
            }
            case WAIT_SCHEDULED -> {
                StepRuntime st = state.steps.get(stepId);
                st.status = StepStatus.WAITING;
                st.dueAt = data.path("resumeAt").asLong();
            }
            case STEP_SKIPPED -> state.steps.get(stepId).status = StepStatus.SKIPPED;
            case COMPENSATION_STARTED -> {
                StepRuntime st = state.steps.get(stepId);
                st.compStatus = RunState.CompStatus.RUNNING;
                st.compAttempt = data.path("attempt").asInt();
                st.compInFlight = false;
            }
            case COMPENSATION_COMPLETED -> {
                StepRuntime st = state.steps.get(stepId);
                st.compStatus = RunState.CompStatus.DONE;
                if (!state.compensatedSteps.contains(stepId)) {
                    state.compensatedSteps.add(stepId);
                }
            }
            case COMPENSATION_FAILED -> {
                StepRuntime st = state.steps.get(stepId);
                if (data.path("final").asBoolean()) {
                    st.compStatus = RunState.CompStatus.FAILED;
                    if (!state.failedCompensations.contains(stepId)) {
                        state.failedCompensations.add(stepId);
                    }
                } else {
                    st.compStatus = RunState.CompStatus.WAITING;
                    st.compDueAt = data.path("dueAt").asLong();
                }
            }
            case WORKFLOW_COMPLETED -> {
                state.status = RunStatus.COMPLETED;
                @SuppressWarnings("unchecked")
                Map<String, Object> result = Jsons.toMap(data.get("result"));
                state.result = result;
            }
            case WORKFLOW_FAILED -> {
                state.status = RunStatus.FAILED;
                state.error = data.path("error").asText(null);
            }
        }
    }

    // ---------------------------------------------------------------- 调度

    private void dispatchLoop() {
        while (running) {
            synchronized (lock) {
                try {
                    scheduleAll();
                } catch (Throwable t) {
                    // 调度异常不应杀死调度线程
                    t.printStackTrace();
                }
                try {
                    lock.wait(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void scheduleAll() {
        for (RunState state : runs.values()) {
            if (!state.isTerminal()) {
                scheduleRun(state);
            }
        }
    }

    private void scheduleRun(RunState state) {
        long now = System.currentTimeMillis();
        if (state.status == RunStatus.COMPENSATING) {
            scheduleCompensation(state, now);
            return;
        }
        // 有步骤最终失败 -> 待其余在执行/等待唤醒的步骤收尾后，进入补偿/失败流程
        boolean anyFailed = state.steps.values().stream().anyMatch(st -> st.status == StepStatus.FAILED);
        if (anyFailed) {
            boolean anyActive = state.steps.values().stream().anyMatch(
                    st -> st.status == StepStatus.RUNNING || st.status == StepStatus.WAITING);
            if (!anyActive) {
                beginCompensation(state);
            }
            return;
        }
        // 全部步骤已解决 -> 完成
        boolean allResolved = state.steps.values().stream()
                .allMatch(st -> st.status == StepStatus.COMPLETED || st.status == StepStatus.SKIPPED);
        if (allResolved) {
            completeRun(state);
            return;
        }
        for (StepDefinition step : state.definition.steps()) {
            StepRuntime st = state.steps.get(step.id());
            switch (st.status) {
                case PENDING -> {
                    if (!depsResolved(state, step)) {
                        break;
                    }
                    if (allDepsSkipped(state, step)) {
                        skipStep(state, step, "依赖步骤均被跳过");
                    } else if (step.condition() != null
                            && !Condition.parse(step.condition()).evaluate(state.input, resultsOf(state))) {
                        skipStep(state, step, "条件不满足: " + step.condition());
                    } else if (step.type() == StepDefinition.StepType.WAIT) {
                        ObjectNode data = Jsons.obj();
                        data.put("stepId", step.id());
                        data.put("resumeAt", now + step.waitMs());
                        append(state, EventType.WAIT_SCHEDULED, data);
                        st.status = StepStatus.WAITING;
                        st.dueAt = now + step.waitMs();
                    } else {
                        startAttempt(state, step, st, st.attempt + 1);
                    }
                }
                case WAITING -> {
                    if (st.dueAt <= now) {
                        if (step.type() == StepDefinition.StepType.WAIT) {
                            completeStep(state, step, st, null);
                        } else {
                            startAttempt(state, step, st, st.attempt + 1);
                        }
                    }
                }
                case RUNNING -> {
                    if (!st.inFlight) {
                        submitStepTask(state, step, st);
                    }
                }
                default -> {
                    // COMPLETED / SKIPPED / FAILED 无需调度
                }
            }
        }
    }

    private boolean depsResolved(RunState state, StepDefinition step) {
        return step.dependsOn().stream().allMatch(dep -> {
            StepStatus s = state.steps.get(dep).status;
            return s == StepStatus.COMPLETED || s == StepStatus.SKIPPED;
        });
    }

    private boolean allDepsSkipped(RunState state, StepDefinition step) {
        return !step.dependsOn().isEmpty() && step.dependsOn().stream()
                .allMatch(dep -> state.steps.get(dep).status == StepStatus.SKIPPED);
    }

    private void startAttempt(RunState state, StepDefinition step, StepRuntime st, int attempt) {
        st.attempt = attempt;
        st.status = StepStatus.RUNNING;
        ObjectNode data = Jsons.obj();
        data.put("stepId", step.id());
        data.put("attempt", attempt);
        data.put("idempotencyKey", idempotencyKey(state.runId, step.id(), attempt));
        append(state, EventType.STEP_STARTED, data);
        submitStepTask(state, step, st);
    }

    private void submitStepTask(RunState state, StepDefinition step, StepRuntime st) {
        st.inFlight = true;
        workers.submit(() -> executeStep(state.runId, step.id()));
    }

    private void executeStep(String runId, String stepId) {
        RunState state;
        StepDefinition step;
        StepContext context;
        StepHandler handler;
        synchronized (lock) {
            state = runs.get(runId);
            if (state == null || state.isTerminal()) {
                return;
            }
            step = state.definition.step(stepId);
            handler = handlers.get(step.handler());
            if (handler == null) {
                failStep(state, step, state.steps.get(stepId),
                        new IllegalStateException("未注册的 handler: " + step.handler()));
                lock.notifyAll();
                return;
            }
            StepRuntime st = state.steps.get(stepId);
            context = new StepContext(runId, stepId, st.attempt,
                    idempotencyKey(runId, stepId, st.attempt),
                    state.input, step.config(), resultsOf(state), state.effects);
        }
        Object result = null;
        Throwable error = null;
        try {
            result = handler.execute(context);
        } catch (Throwable t) {
            error = t;
        }
        synchronized (lock) {
            RunState current = runs.get(runId);
            if (current == null || current.isTerminal()) {
                return;
            }
            StepRuntime st = current.steps.get(stepId);
            st.inFlight = false;
            if (error == null) {
                completeStep(current, current.definition.step(stepId), st, result);
            } else {
                failStep(current, current.definition.step(stepId), st, error);
            }
            lock.notifyAll();
        }
    }

    private void completeStep(RunState state, StepDefinition step, StepRuntime st, Object result) {
        long seq = ++state.completionSeq;
        ObjectNode data = Jsons.obj();
        data.put("stepId", step.id());
        data.put("attempt", st.attempt);
        data.set("result", Jsons.toNode(result));
        data.put("seq", seq);
        append(state, EventType.STEP_COMPLETED, data);
        st.status = StepStatus.COMPLETED;
        st.result = result;
        st.completedSeq = seq;
    }

    private void failStep(RunState state, StepDefinition step, StepRuntime st, Throwable error) {
        RetryPolicy retry = step.retry();
        boolean exhausted = st.attempt >= retry.maxAttempts();
        ObjectNode data = Jsons.obj();
        data.put("stepId", step.id());
        data.put("attempt", st.attempt);
        data.put("error", String.valueOf(error.getMessage() != null ? error.getMessage() : error));
        data.put("final", exhausted);
        if (!exhausted) {
            long dueAt = System.currentTimeMillis() + retry.backoffAfter(st.attempt);
            data.put("dueAt", dueAt);
            st.status = StepStatus.WAITING;
            st.dueAt = dueAt;
        } else {
            st.status = StepStatus.FAILED;
            state.error = step.id() + ": " + data.get("error").asText();
        }
        append(state, EventType.STEP_FAILED, data);
    }

    private void skipStep(RunState state, StepDefinition step, String reason) {
        ObjectNode data = Jsons.obj();
        data.put("stepId", step.id());
        data.put("reason", reason);
        append(state, EventType.STEP_SKIPPED, data);
        state.steps.get(step.id()).status = StepStatus.SKIPPED;
    }

    private void completeRun(RunState state) {
        Map<String, Object> result = resultsOf(state);
        ObjectNode data = Jsons.obj();
        data.set("result", Jsons.toNode(result));
        append(state, EventType.WORKFLOW_COMPLETED, data);
        state.status = RunStatus.COMPLETED;
        state.result = result;
        lock.notifyAll();
    }

    // ---------------------------------------------------------------- 补偿

    private void beginCompensation(RunState state) {
        // 已成功完成且定义了补偿的步骤，按完成顺序的逆序补偿
        List<StepDefinition> toCompensate = new ArrayList<>();
        for (StepDefinition step : state.definition.steps()) {
            StepRuntime st = state.steps.get(step.id());
            if (st.status == StepStatus.COMPLETED && step.compensation() != null) {
                toCompensate.add(step);
            }
        }
        toCompensate.sort(Comparator.comparingLong(
                (StepDefinition s) -> state.steps.get(s.id()).completedSeq).reversed());
        toCompensate.forEach(s -> state.compensationOrder.add(s.id()));
        state.compensationIndex = 0;
        state.status = RunStatus.COMPENSATING;
        scheduleCompensation(state, System.currentTimeMillis());
    }

    private void scheduleCompensation(RunState state, long now) {
        // 跳过已完成的补偿
        while (state.compensationIndex < state.compensationOrder.size()) {
            String stepId = state.compensationOrder.get(state.compensationIndex);
            RunState.CompStatus cs = state.steps.get(stepId).compStatus;
            if (cs == RunState.CompStatus.DONE || cs == RunState.CompStatus.FAILED) {
                state.compensationIndex++;
            } else {
                break;
            }
        }
        if (state.compensationIndex >= state.compensationOrder.size()) {
            failRun(state);
            return;
        }
        String stepId = state.compensationOrder.get(state.compensationIndex);
        StepRuntime st = state.steps.get(stepId);
        switch (st.compStatus) {
            case NONE -> startCompensationAttempt(state, stepId, st, st.compAttempt + 1);
            case WAITING -> {
                if (st.compDueAt <= now) {
                    startCompensationAttempt(state, stepId, st, st.compAttempt + 1);
                }
            }
            case RUNNING -> {
                if (!st.compInFlight) {
                    submitCompensationTask(state, stepId, st);
                }
            }
            default -> {
                // DONE / FAILED 已在上方跳过
            }
        }
    }

    private void startCompensationAttempt(RunState state, String stepId, StepRuntime st, int attempt) {
        st.compAttempt = attempt;
        st.compStatus = RunState.CompStatus.RUNNING;
        ObjectNode data = Jsons.obj();
        data.put("stepId", stepId);
        data.put("attempt", attempt);
        append(state, EventType.COMPENSATION_STARTED, data);
        submitCompensationTask(state, stepId, st);
    }

    private void submitCompensationTask(RunState state, String stepId, StepRuntime st) {
        st.compInFlight = true;
        workers.submit(() -> executeCompensation(state.runId, stepId));
    }

    private void executeCompensation(String runId, String stepId) {
        StepContext context;
        StepHandler handler;
        synchronized (lock) {
            RunState state = runs.get(runId);
            if (state == null || state.isTerminal()) {
                return;
            }
            StepDefinition step = state.definition.step(stepId);
            CompensationDef compensation = step.compensation();
            handler = handlers.get(compensation.handler());
            StepRuntime st = state.steps.get(stepId);
            if (handler == null) {
                failCompensation(state, stepId, st,
                        new IllegalStateException("未注册的补偿 handler: " + compensation.handler()));
                lock.notifyAll();
                return;
            }
            context = new StepContext(runId, stepId, st.compAttempt,
                    idempotencyKey(runId, stepId, "compensation-" + st.compAttempt),
                    state.input, compensation.config(), resultsOf(state), state.effects);
        }
        Throwable error = null;
        try {
            handler.execute(context);
        } catch (Throwable t) {
            error = t;
        }
        synchronized (lock) {
            RunState state = runs.get(runId);
            if (state == null || state.isTerminal()) {
                return;
            }
            StepRuntime st = state.steps.get(stepId);
            st.compInFlight = false;
            if (error == null) {
                ObjectNode data = Jsons.obj();
                data.put("stepId", stepId);
                append(state, EventType.COMPENSATION_COMPLETED, data);
                st.compStatus = RunState.CompStatus.DONE;
                state.compensatedSteps.add(stepId);
            } else {
                failCompensation(state, stepId, st, error);
            }
            lock.notifyAll();
        }
    }

    private void failCompensation(RunState state, String stepId, StepRuntime st, Throwable error) {
        CompensationDef compensation = state.definition.step(stepId).compensation();
        RetryPolicy retry = compensation.retry();
        boolean exhausted = st.compAttempt >= retry.maxAttempts();
        ObjectNode data = Jsons.obj();
        data.put("stepId", stepId);
        data.put("attempt", st.compAttempt);
        data.put("error", String.valueOf(error.getMessage() != null ? error.getMessage() : error));
        data.put("final", exhausted);
        if (!exhausted) {
            long dueAt = System.currentTimeMillis() + retry.backoffAfter(st.compAttempt);
            data.put("dueAt", dueAt);
            st.compStatus = RunState.CompStatus.WAITING;
            st.compDueAt = dueAt;
        } else {
            st.compStatus = RunState.CompStatus.FAILED;
            state.failedCompensations.add(stepId);
        }
        append(state, EventType.COMPENSATION_FAILED, data);
    }

    private void failRun(RunState state) {
        ObjectNode data = Jsons.obj();
        data.put("error", state.error != null ? state.error : "workflow failed");
        data.set("compensatedSteps", Jsons.toNode(state.compensatedSteps));
        data.set("failedCompensations", Jsons.toNode(state.failedCompensations));
        append(state, EventType.WORKFLOW_FAILED, data);
        state.status = RunStatus.FAILED;
        lock.notifyAll();
    }

    // ---------------------------------------------------------------- 工具

    private Map<String, Object> resultsOf(RunState state) {
        Map<String, Object> results = new LinkedHashMap<>();
        for (StepDefinition step : state.definition.steps()) {
            StepRuntime st = state.steps.get(step.id());
            if (st.status == StepStatus.COMPLETED) {
                results.put(step.id(), st.result);
            }
        }
        return results;
    }

    private static String idempotencyKey(String runId, String stepId, int attempt) {
        return runId + ":" + stepId + ":" + attempt;
    }

    private static String idempotencyKey(String runId, String stepId, String attemptTag) {
        return runId + ":" + stepId + ":" + attemptTag;
    }

    private void append(RunState state, EventType type, ObjectNode data) {
        state.journal.append(Event.of(type, data));
    }

    private void ensureRunning() {
        if (!running) {
            throw new IllegalStateException("引擎已关闭");
        }
    }

    private Path journalPath(String runId) {
        return workflowsDir(config.dataDir()).resolve(runId + ".journal");
    }

    private Path effectsPath(String runId) {
        return workflowsDir(config.dataDir()).resolve(runId + ".effects");
    }

    private static Path workflowsDir(Path dataDir) {
        return dataDir.resolve("workflows");
    }

    private static WorkflowSnapshot toSnapshot(RunState state) {
        List<WorkflowSnapshot.StepSnapshot> steps = new ArrayList<>();
        if (state.definition != null) {
            for (StepDefinition step : state.definition.steps()) {
                StepRuntime st = state.steps.get(step.id());
                steps.add(new WorkflowSnapshot.StepSnapshot(
                        step.id(), st.status, st.attempt, st.result));
            }
        }
        return new WorkflowSnapshot(state.runId,
                state.definition != null ? state.definition.name() : null,
                state.status, steps, state.result, state.error,
                List.copyOf(state.compensatedSteps), List.copyOf(state.failedCompensations));
    }

    // ---------------------------------------------------------------- 生命周期

    /**
     * 模拟进程崩溃：立即停止调度与工作线程，不再写入任何事件，
     * 仅关闭文件句柄（已写入的事件均已 fsync，不会丢失）。
     * 之后可在同一数据目录上创建新引擎实例进行恢复。
     */
    public void crash() {
        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;
            workers.shutdownNow();
            for (RunState state : runs.values()) {
                closeQuietly(state);
            }
            lock.notifyAll();
        }
    }

    /**
     * 优雅关闭：停止调度，等待在执行的步骤完成后关闭。
     */
    @Override
    public void close() {
        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;
            lock.notifyAll();
        }
        workers.shutdown();
        try {
            workers.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (lock) {
            for (RunState state : runs.values()) {
                closeQuietly(state);
            }
        }
    }

    private void closeQuietly(RunState state) {
        try {
            if (state.journal != null) {
                state.journal.close();
            }
        } catch (RuntimeException ignored) {
            // 关闭失败不影响崩溃语义
        }
        try {
            if (state.effects != null) {
                state.effects.close();
            }
        } catch (RuntimeException ignored) {
            // 同上
        }
    }
}
