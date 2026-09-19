package com.brixdata.durableflow.persistence;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 持久化事件。每个工作流实例对应一个 append-only 日志文件，
 * 每行一个 JSON 事件，状态通过顺序回放事件重建。
 */
public class WorkflowEvent {

    /** 事件类型常量。 */
    public static final class Types {
        public static final String WORKFLOW_SUBMITTED = "WORKFLOW_SUBMITTED";
        public static final String STEP_READY = "STEP_READY";
        public static final String STEP_WAITING = "STEP_WAITING";
        public static final String STEP_STARTED = "STEP_STARTED";
        public static final String STEP_SUCCEEDED = "STEP_SUCCEEDED";
        public static final String STEP_FAILED = "STEP_FAILED";
        public static final String STEP_SKIPPED = "STEP_SKIPPED";
        public static final String WORKFLOW_SUCCEEDED = "WORKFLOW_SUCCEEDED";
        public static final String WORKFLOW_FAILED = "WORKFLOW_FAILED";
        public static final String COMPENSATION_STARTED = "COMPENSATION_STARTED";
        public static final String COMPENSATION_STEP_STARTED = "COMPENSATION_STEP_STARTED";
        public static final String COMPENSATION_STEP_SUCCEEDED = "COMPENSATION_STEP_SUCCEEDED";
        public static final String COMPENSATION_STEP_FAILED = "COMPENSATION_STEP_FAILED";
        public static final String WORKFLOW_COMPENSATED = "WORKFLOW_COMPENSATED";
        public static final String WORKFLOW_COMPENSATION_FAILED = "WORKFLOW_COMPENSATION_FAILED";

        private Types() {
        }
    }

    private String type;
    private long timestamp;
    private String stepId;
    private Map<String, Object> data = new LinkedHashMap<>();

    public WorkflowEvent() {
    }

    private WorkflowEvent(String type, String stepId, Map<String, Object> data) {
        this.type = type;
        this.timestamp = System.currentTimeMillis();
        this.stepId = stepId;
        this.data = data;
    }

    public static WorkflowEvent of(String type) {
        return new WorkflowEvent(type, null, new LinkedHashMap<>());
    }

    public static WorkflowEvent of(String type, String stepId) {
        return new WorkflowEvent(type, stepId, new LinkedHashMap<>());
    }

    public static WorkflowEvent of(String type, String stepId, Map<String, Object> data) {
        return new WorkflowEvent(type, stepId, data);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data == null ? new LinkedHashMap<>() : data;
    }

    public long longData(String key, long fallback) {
        Object v = data.get(key);
        return v instanceof Number n ? n.longValue() : fallback;
    }

    public String stringData(String key) {
        Object v = data.get(key);
        return v == null ? null : v.toString();
    }
}
