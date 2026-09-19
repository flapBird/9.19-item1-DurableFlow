package com.brixdata.durableflow.engine;

import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.persistence.DurableEffectStore;
import com.brixdata.durableflow.persistence.Journal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单个工作流运行的内存状态，由事件日志重放重建，仅被引擎调度线程
 * 与工作线程在引擎锁内访问。
 */
final class RunState {

    /** 补偿执行状态。 */
    enum CompStatus {
        NONE, RUNNING, WAITING, DONE, FAILED
    }

    /** 单个步骤的运行时状态。 */
    static final class StepRuntime {
        StepStatus status = StepStatus.PENDING;
        int attempt;
        long dueAt;
        Object result;
        long completedSeq;
        boolean inFlight;
        // 补偿状态
        CompStatus compStatus = CompStatus.NONE;
        int compAttempt;
        long compDueAt;
        boolean compInFlight;
    }

    final String runId;
    WorkflowDefinition definition;
    Map<String, Object> input = Map.of();
    RunStatus status = RunStatus.RUNNING;
    final Map<String, StepRuntime> steps = new LinkedHashMap<>();
    Journal journal;
    DurableEffectStore effects;
    long completionSeq;
    Map<String, Object> result;
    String error;
    // 补偿流程：按完成顺序逆序排列的待补偿步骤
    final List<String> compensationOrder = new ArrayList<>();
    int compensationIndex;
    final List<String> compensatedSteps = new ArrayList<>();
    final List<String> failedCompensations = new ArrayList<>();

    RunState(String runId) {
        this.runId = runId;
    }

    boolean isTerminal() {
        return status == RunStatus.COMPLETED || status == RunStatus.FAILED;
    }
}
