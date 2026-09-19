package com.brixdata.durableflow.engine;

import com.brixdata.durableflow.config.EngineConfig;
import com.brixdata.durableflow.model.RetryPolicy;
import com.brixdata.durableflow.model.StepDefinition;
import com.brixdata.durableflow.model.StepInstance;
import com.brixdata.durableflow.model.StepStatus;
import com.brixdata.durableflow.model.StepType;
import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.model.WorkflowInstance;
import com.brixdata.durableflow.model.WorkflowStatus;
import com.brixdata.durableflow.persistence.IdempotencyStore;
import com.brixdata.durableflow.persistence.JournalStore;
import com.brixdata.durableflow.persistence.StateRebuilder;
import com.brixdata.durableflow.persistence.WorkflowEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 持久化工作流执行引擎。
 *
 * <p>核心机制：</p>
 * <ul>
 *   <li>每次状态迁移先写事件日志（fsync）再推进内存状态，崩溃后通过事件回放恢复；</li>
 *   <li>定时等待与重试退避持久化绝对唤醒时间，重启后按原计划继续而非重新计时；</li>
 *   <li>崩溃时处于 RUNNING 的步骤恢复后重新派发，处理器通过幂等键去重，
 *       避免“已执行成功但状态未落盘”导致重复副作用；</li>
 *   <li>步骤最终失败时按完成逆序执行补偿，补偿自身支持重试与崩溃恢复。</li>
 * </ul>
 */
public class WorkflowEngine implements AutoCloseable {

    private final EngineConfig config;
    private final HandlerRegistry handlers;
    private final JournalStore journal;
    private final IdempotencyStore idempotency;
    private final Map<String, WorkflowInstance> instances = new ConcurrentHashMap<>();
    private final ExecutorService pool;
    private final ScheduledExecutorService timers;
    private final Object stateLock = new Object();
    private volatile boolean closed;

    /** 测试钩子：步骤/补偿处理器成功之后、完成事件写入之前触发，用于模拟崩溃窗口。 */
    private volatile Runnable afterHandlerSuccessHook;

    public WorkflowEngine(EngineConfig config, HandlerRegistry handlers) {
        this.config = config;
        this.handlers = handlers;
        this.journal = new JournalStore(config.getDataDir());
        this.idempotency = new IdempotencyStore(config.getDataDir());
        AtomicInteger seq = new AtomicInteger();
        this.pool = Executors.newFixedThreadPool(config.getMaxConcurrency(), r -> {
            Thread t = new Thread(r, "durableflow-worker-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        this.timers = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "durableflow-timer");
            t.setDaemon(true);
            return t;
        });
    }

    // ------------------------------------------------------------------
    // 提交与恢复
    // ------------------------------------------------------------------

    /**
     * 提交工作流。实例 id 由定义 id + 规范化输入派生：
     * 相同定义与相同输入重复提交返回已有实例，不会重复执行。
     */
    public String submit(WorkflowDefinition definition, Map<String, Object> input) {
        definition.validate();
        ensureOpen();
        Map<String, Object> safeInput = input == null ? Map.of() : new LinkedHashMap<>(input);
        String id = JsonUtil.deriveInstanceId(definition.getId(), safeInput);
        synchronized (stateLock) {
            if (instances.containsKey(id) || journal.exists(id)) {
                return id;
            }
            WorkflowInstance instance = new WorkflowInstance(id, definition, safeInput);
            instances.put(id, instance);
            journal.append(id, WorkflowEvent.of(WorkflowEvent.Types.WORKFLOW_SUBMITTED, null,
                    Map.of("definition", definition, "input", safeInput)));
        }
        scheduleReady(id);
        return id;
    }

    /** 从事件日志恢复所有未完成实例并继续执行，返回恢复的实例数。 */
    public int recover() {
        ensureOpen();
        List<String> toResume = loadInstances();
        toResume.forEach(this::resume);
        return toResume.size();
    }

    /** 仅加载状态而不继续执行（用于状态查询）。 */
    public List<String> load() {
        ensureOpen();
        return loadInstances();
    }

    private List<String> loadInstances() {
        List<String> loaded = new ArrayList<>();
        synchronized (stateLock) {
            for (var entry : journal.loadAll().entrySet()) {
                if (instances.containsKey(entry.getKey()) || entry.getValue().isEmpty()) {
                    continue;
                }
                WorkflowInstance instance = StateRebuilder.rebuild(entry.getKey(), entry.getValue());
                instances.put(instance.getId(), instance);
                if (!instance.getStatus().isTerminal()) {
                    loaded.add(instance.getId());
                }
            }
        }
        return loaded;
    }

    /** 恢复单个实例：按持久化的状态/时间继续，已完成的步骤不会重跑。 */
    private void resume(String instanceId) {
        WorkflowInstance instance = instances.get(instanceId);
        if (instance == null) {
            return;
        }
        if (instance.getStatus() == WorkflowStatus.COMPENSATING) {
            scheduleCompensation(instanceId);
            return;
        }
        if (instance.getStatus() != WorkflowStatus.RUNNING) {
            return;
        }
        boolean hasDead;
        synchronized (stateLock) {
            hasDead = instance.getSteps().stream()
                    .anyMatch(s -> s.getStatus() == StepStatus.DEAD);
            if (!hasDead) {
                for (StepInstance step : instance.getSteps()) {
                    switch (step.getStatus()) {
                        // 崩溃时正在执行：重新派发，幂等机制保证副作用不重复
                        case READY, RUNNING -> dispatch(instanceId, step.getStepId());
                        case WAITING -> scheduleWakeup(instanceId, step.getStepId(), step.getResumeAt());
                        default -> {
                        }
                    }
                }
            }
        }
        if (hasDead) {
            // 崩溃发生在“步骤已最终失败、工作流失败事件尚未写入”的窗口
            failWorkflow(instance);
        } else {
            scheduleReady(instanceId);
        }
    }

    // ------------------------------------------------------------------
    // 调度
    // ------------------------------------------------------------------

    /** 让所有依赖已满足的 PENDING 步骤进入执行（条件不满足则跳过）。 */
    private void scheduleReady(String instanceId) {
        List<String> toDispatch = new ArrayList<>();
        synchronized (stateLock) {
            WorkflowInstance instance = instances.get(instanceId);
            if (instance == null || closed || instance.getStatus() != WorkflowStatus.RUNNING) {
                return;
            }
            boolean progressed = true;
            while (progressed) {
                progressed = false;
                for (StepInstance step : instance.getSteps()) {
                    if (step.getStatus() != StepStatus.PENDING || !depsSatisfied(instance, step)) {
                        continue;
                    }
                    StepDefinition def = instance.getDefinition().step(step.getStepId());
                    if (!ConditionEvaluator.evaluate(def.getCondition(), instance.getInput(),
                            stepResults(instance))) {
                        step.markSkipped();
                        journal.append(instanceId, WorkflowEvent.of(
                                WorkflowEvent.Types.STEP_SKIPPED, step.getStepId()));
                    } else if (def.getType() == StepType.WAIT) {
                        long resumeAt = System.currentTimeMillis() + def.getWaitMillis();
                        step.markWaiting(resumeAt);
                        journal.append(instanceId, WorkflowEvent.of(
                                WorkflowEvent.Types.STEP_WAITING, step.getStepId(),
                                Map.of("resumeAt", resumeAt)));
                        scheduleWakeup(instanceId, step.getStepId(), resumeAt);
                    } else {
                        step.markReady();
                        journal.append(instanceId, WorkflowEvent.of(
                                WorkflowEvent.Types.STEP_READY, step.getStepId()));
                        toDispatch.add(step.getStepId());
                    }
                    progressed = true;
                }
            }
            completeIfFinished(instance);
        }
        toDispatch.forEach(stepId -> dispatch(instanceId, stepId));
    }

    private boolean depsSatisfied(WorkflowInstance instance, StepInstance step) {
        StepDefinition def = instance.getDefinition().step(step.getStepId());
        for (String dep : def.getDependsOn()) {
            StepStatus depStatus = instance.step(dep).getStatus();
            if (depStatus != StepStatus.SUCCEEDED && depStatus != StepStatus.SKIPPED) {
                return false;
            }
        }
        return true;
    }

    private Map<String, String> stepResults(WorkflowInstance instance) {
        Map<String, String> results = new LinkedHashMap<>();
        for (StepInstance s : instance.getSteps()) {
            if (s.getStatus() == StepStatus.SUCCEEDED) {
                results.put(s.getStepId(), s.getResult());
            }
        }
        return results;
    }

    /** 全部步骤到达终态且无失败 → 工作流成功。调用时需持有 stateLock。 */
    private void completeIfFinished(WorkflowInstance instance) {
        if (instance.getStatus() == WorkflowStatus.RUNNING && instance.allStepsFinished()) {
            instance.setStatus(WorkflowStatus.SUCCEEDED);
            journal.append(instance.getId(), WorkflowEvent.of(WorkflowEvent.Types.WORKFLOW_SUCCEEDED));
        }
    }

    private void dispatch(String instanceId, String stepId) {
        if (closed) {
            return;
        }
        pool.execute(() -> executeStep(instanceId, stepId));
    }

    private void scheduleWakeup(String instanceId, String stepId, long at) {
        long delay = Math.max(0, at - System.currentTimeMillis());
        timers.schedule(() -> onWakeup(instanceId, stepId), delay, TimeUnit.MILLISECONDS);
    }

    /** 定时器触发：WAIT 步骤到时完成，TASK 步骤退避结束重新派发。 */
    private void onWakeup(String instanceId, String stepId) {
        WorkflowInstance instance = instances.get(instanceId);
        if (instance == null) {
            return;
        }
        StepDefinition def = instance.getDefinition().step(stepId);
        synchronized (stateLock) {
            if (closed || instance.getStatus() != WorkflowStatus.RUNNING) {
                return;
            }
            StepInstance step = instance.step(stepId);
            if (step.getStatus() != StepStatus.WAITING) {
                return;
            }
            if (def.getType() == StepType.WAIT) {
                step.markSucceeded("waited " + def.getWaitMillis() + "ms");
                journal.append(instanceId, WorkflowEvent.of(WorkflowEvent.Types.STEP_SUCCEEDED,
                        stepId, Map.of("result", step.getResult())));
            } else {
                step.markReady();
                journal.append(instanceId, WorkflowEvent.of(WorkflowEvent.Types.STEP_READY, stepId));
            }
        }
        if (def.getType() == StepType.WAIT) {
            scheduleReady(instanceId);
        } else {
            dispatch(instanceId, stepId);
        }
    }

    // ------------------------------------------------------------------
    // 步骤执行与重试
    // ------------------------------------------------------------------

    private void executeStep(String instanceId, String stepId) {
        WorkflowInstance instance = instances.get(instanceId);
        if (instance == null) {
            return;
        }
        StepDefinition def = instance.getDefinition().step(stepId);
        StepInstance step = instance.step(stepId);
        int attempt;
        synchronized (stateLock) {
            if (closed || instance.getStatus() != WorkflowStatus.RUNNING) {
                return;
            }
            StepStatus status = step.getStatus();
            if (status != StepStatus.READY && status != StepStatus.RUNNING) {
                return; // SUCCEEDED 等状态：不重复执行
            }
            attempt = step.getAttempts() + 1;
            step.markRunning(attempt);
            journal.append(instanceId, WorkflowEvent.of(WorkflowEvent.Types.STEP_STARTED,
                    stepId, Map.of("attempt", attempt)));
        }
        StepContext ctx = new StepContext(instanceId, stepId, attempt, def.getArgs(),
                instance.getInput(), idempotency, config.getDataDir());
        String result;
        try {
            result = handlers.require(def.getHandler()).execute(ctx);
            if (result == null) {
                result = "";
            }
        } catch (Throwable t) {
            handleStepFailure(instance, def, step, attempt, t);
            return;
        }
        Runnable hook = afterHandlerSuccessHook;
        if (hook != null) {
            hook.run(); // 测试：模拟“执行成功但完成状态尚未写入”的崩溃
        }
        synchronized (stateLock) {
            if (closed) {
                return; // 崩溃：完成状态丢失，恢复后依赖幂等去重
            }
            step.markSucceeded(result);
            journal.append(instanceId, WorkflowEvent.of(WorkflowEvent.Types.STEP_SUCCEEDED,
                    stepId, Map.of("result", result)));
            completeIfFinished(instance);
        }
        scheduleReady(instanceId);
    }

    private void handleStepFailure(WorkflowInstance instance, StepDefinition def,
                                   StepInstance step, int attempt, Throwable t) {
        RetryPolicy retry = def.retryOr(config.getDefaultRetry());
        String error = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
        boolean dead;
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            if (attempt < retry.maxAttempts()) {
                long retryAt = System.currentTimeMillis() + retry.delayMillis(attempt);
                step.markFailed(error, retryAt);
                journal.append(instance.getId(), WorkflowEvent.of(WorkflowEvent.Types.STEP_FAILED,
                        step.getStepId(), Map.of("attempt", attempt, "error", error,
                                "retryAt", retryAt)));
                scheduleWakeup(instance.getId(), step.getStepId(), retryAt);
                dead = false;
            } else {
                step.markFailed(error, -1);
                journal.append(instance.getId(), WorkflowEvent.of(WorkflowEvent.Types.STEP_FAILED,
                        step.getStepId(), Map.of("attempt", attempt, "error", error,
                                "retryAt", -1L)));
                dead = true;
            }
        }
        if (dead) {
            failWorkflow(instance);
        }
    }

    /** 步骤最终失败：工作流进入失败态，有需要补偿的步骤则启动补偿。 */
    private void failWorkflow(WorkflowInstance instance) {
        boolean compensate;
        synchronized (stateLock) {
            if (closed || instance.getStatus() != WorkflowStatus.RUNNING) {
                return;
            }
            journal.append(instance.getId(), WorkflowEvent.of(WorkflowEvent.Types.WORKFLOW_FAILED));
            compensate = compensationPlan(instance).findAny().isPresent();
            // 一次性落到目标状态，避免外部观察到 FAILED→COMPENSATING 的中间态
            instance.setStatus(compensate ? WorkflowStatus.COMPENSATING : WorkflowStatus.FAILED);
            if (compensate) {
                journal.append(instance.getId(),
                        WorkflowEvent.of(WorkflowEvent.Types.COMPENSATION_STARTED));
            }
        }
        if (compensate) {
            scheduleCompensation(instance.getId());
        }
    }

    // ------------------------------------------------------------------
    // 补偿
    // ------------------------------------------------------------------

    /** 补偿计划：已成功且配置了补偿处理器的步骤，按完成时间逆序。 */
    private java.util.stream.Stream<StepInstance> compensationPlan(WorkflowInstance instance) {
        List<String> definitionOrder = instance.getDefinition().getSteps().stream()
                .map(StepDefinition::getId).toList();
        return instance.getSteps().stream()
                .filter(s -> s.getStatus() == StepStatus.SUCCEEDED)
                .filter(s -> instance.getDefinition().step(s.getStepId()).getCompensationHandler() != null)
                .sorted(Comparator.comparingLong(StepInstance::getFinishedAt).reversed()
                        .thenComparing((StepInstance s) -> definitionOrder.indexOf(s.getStepId()),
                                Comparator.reverseOrder()));
    }

    /** 顺序执行补偿计划中的下一个步骤；全部完成则工作流进入 COMPENSATED。 */
    private void scheduleCompensation(String instanceId) {
        StepInstance next;
        WorkflowInstance instance;
        synchronized (stateLock) {
            instance = instances.get(instanceId);
            if (instance == null || closed || instance.getStatus() != WorkflowStatus.COMPENSATING) {
                return;
            }
            next = compensationPlan(instance).filter(s -> !s.isCompensated()).findFirst().orElse(null);
            if (next == null) {
                instance.setStatus(WorkflowStatus.COMPENSATED);
                journal.append(instanceId, WorkflowEvent.of(WorkflowEvent.Types.WORKFLOW_COMPENSATED));
                return;
            }
            long retryAt = next.getCompensationRetryAt();
            if (retryAt > System.currentTimeMillis()) {
                // 补偿退避尚未到期（含崩溃恢复场景）：按原时间唤醒
                StepInstance pending = next;
                timers.schedule(() -> scheduleCompensation(instanceId),
                        retryAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                return;
            }
        }
        StepInstance target = next;
        pool.execute(() -> executeCompensation(instanceId, target.getStepId()));
    }

    private void executeCompensation(String instanceId, String stepId) {
        WorkflowInstance instance = instances.get(instanceId);
        if (instance == null) {
            return;
        }
        StepDefinition def = instance.getDefinition().step(stepId);
        StepInstance step = instance.step(stepId);
        int attempt;
        synchronized (stateLock) {
            if (closed || instance.getStatus() != WorkflowStatus.COMPENSATING || step.isCompensated()) {
                return;
            }
            attempt = step.getCompensationAttempts() + 1;
            step.markCompensationRunning(attempt);
            step.setCompensationRetryAt(0);
            journal.append(instanceId, WorkflowEvent.of(WorkflowEvent.Types.COMPENSATION_STEP_STARTED,
                    stepId, Map.of("attempt", attempt)));
        }
        StepContext ctx = new StepContext(instanceId, stepId, attempt, def.getCompensationArgs(),
                instance.getInput(), idempotency, config.getDataDir());
        try {
            handlers.require(def.getCompensationHandler()).execute(ctx);
        } catch (Throwable t) {
            handleCompensationFailure(instance, def, step, attempt, t);
            return;
        }
        Runnable hook = afterHandlerSuccessHook;
        if (hook != null) {
            hook.run();
        }
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            step.markCompensated();
            journal.append(instanceId, WorkflowEvent.of(
                    WorkflowEvent.Types.COMPENSATION_STEP_SUCCEEDED, stepId));
        }
        scheduleCompensation(instanceId);
    }

    private void handleCompensationFailure(WorkflowInstance instance, StepDefinition def,
                                           StepInstance step, int attempt, Throwable t) {
        RetryPolicy retry = def.retryOr(config.getDefaultRetry());
        String error = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
        synchronized (stateLock) {
            if (closed || instance.getStatus() != WorkflowStatus.COMPENSATING) {
                return;
            }
            if (attempt < retry.maxAttempts()) {
                long retryAt = System.currentTimeMillis() + retry.delayMillis(attempt);
                step.setCompensationRetryAt(retryAt);
                journal.append(instance.getId(), WorkflowEvent.of(
                        WorkflowEvent.Types.COMPENSATION_STEP_FAILED, step.getStepId(),
                        Map.of("attempt", attempt, "error", error, "retryAt", retryAt)));
            } else {
                journal.append(instance.getId(), WorkflowEvent.of(
                        WorkflowEvent.Types.COMPENSATION_STEP_FAILED, step.getStepId(),
                        Map.of("attempt", attempt, "error", error, "retryAt", -1L)));
                instance.setStatus(WorkflowStatus.COMPENSATION_FAILED);
                journal.append(instance.getId(), WorkflowEvent.of(
                        WorkflowEvent.Types.WORKFLOW_COMPENSATION_FAILED));
                return;
            }
        }
        scheduleCompensation(instance.getId());
    }

    // ------------------------------------------------------------------
    // 状态查询与等待
    // ------------------------------------------------------------------

    public WorkflowInstance instance(String instanceId) {
        return instances.get(instanceId);
    }

    public List<WorkflowInstance> instances() {
        return List.copyOf(instances.values());
    }

    /** 等待实例到达终态，返回最终状态；超时返回当前状态。 */
    public WorkflowStatus awaitTerminal(String instanceId, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            WorkflowInstance instance = instances.get(instanceId);
            if (instance != null && instance.getStatus().isTerminal()) {
                return instance.getStatus();
            }
            sleep(10);
        }
        WorkflowInstance instance = instances.get(instanceId);
        return instance == null ? null : instance.getStatus();
    }

    /** 等待所有已加载实例到达终态；超时返回 false。 */
    public boolean awaitAllTerminal(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (instances.values().stream().allMatch(i -> i.getStatus().isTerminal())) {
                return true;
            }
            sleep(10);
        }
        return false;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------
    // 生命周期与测试钩子
    // ------------------------------------------------------------------

    /** 测试专用：注册“处理器成功后、完成事件写入前”的钩子，用于模拟崩溃窗口。 */
    public void setAfterHandlerSuccessHook(Runnable hook) {
        this.afterHandlerSuccessHook = hook;
    }

    /** 测试专用：等待工作线程池与定时器终止。 */
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        if (!pool.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            return false;
        }
        long remaining = Math.max(1, deadline - System.currentTimeMillis());
        return timers.awaitTermination(remaining, TimeUnit.MILLISECONDS);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("engine is closed");
        }
    }

    @Override
    public void close() {
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            closed = true;
            journal.close();
            idempotency.close();
        }
        pool.shutdown();
        timers.shutdown();
    }
}
