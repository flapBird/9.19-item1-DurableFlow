package com.brixdata.durableflow.persistence;

import com.brixdata.durableflow.json.Jsons;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 追加到工作流日志中的一条事件。事件是恢复执行状态的唯一事实来源：
 * 引擎重启后通过重放事件重建内存状态。
 *
 * @param type 事件类型
 * @param time 事件发生时间（epoch 毫秒）
 * @param data 事件负载
 */
public record Event(EventType type, long time, ObjectNode data) {

    /** 事件类型。 */
    public enum EventType {
        /** 工作流已提交，data 含 definition 与 input。 */
        WORKFLOW_SUBMITTED,
        /** 步骤开始执行，data 含 stepId、attempt、idempotencyKey。 */
        STEP_STARTED,
        /** 步骤执行成功，data 含 stepId、attempt、result、seq（完成序号）。 */
        STEP_COMPLETED,
        /**
         * 步骤单次尝试失败，data 含 stepId、attempt、error、final。
         * final=false 时含 dueAt（下次重试的绝对时间，持久化后重启不重计时）；
         * final=true 表示重试次数耗尽。
         */
        STEP_FAILED,
        /** 定时等待步骤已排期，data 含 stepId、resumeAt（绝对时间）。 */
        WAIT_SCHEDULED,
        /** 步骤因条件不满足或依赖被跳过而跳过，data 含 stepId、reason。 */
        STEP_SKIPPED,
        /** 补偿开始执行，data 含 stepId、attempt。 */
        COMPENSATION_STARTED,
        /** 补偿执行成功，data 含 stepId。 */
        COMPENSATION_COMPLETED,
        /**
         * 补偿单次尝试失败，data 含 stepId、attempt、error、final；
         * final=false 时含 dueAt（下次补偿重试的绝对时间）。
         */
        COMPENSATION_FAILED,
        /** 工作流成功完成，data 含 result。 */
        WORKFLOW_COMPLETED,
        /** 工作流最终失败，data 含 error、compensatedSteps、failedCompensations。 */
        WORKFLOW_FAILED
    }

    public static Event of(EventType type, ObjectNode data) {
        return new Event(type, System.currentTimeMillis(), data);
    }

    public String toLine() {
        ObjectNode node = Jsons.obj();
        node.put("type", type.name());
        node.put("time", time);
        node.set("data", data);
        return Jsons.write(node);
    }

    /**
     * 解析一行日志记录。
     *
     * @throws IllegalArgumentException 行不是合法事件时抛出
     */
    public static Event parse(String line) {
        JsonNode node = Jsons.read(line);
        if (!node.hasNonNull("type") || !node.hasNonNull("data")) {
            throw new IllegalArgumentException("非法事件记录: " + line);
        }
        return new Event(
                EventType.valueOf(node.get("type").asText()),
                node.path("time").asLong(0),
                (ObjectNode) node.get("data"));
    }
}
