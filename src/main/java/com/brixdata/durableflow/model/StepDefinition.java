package com.brixdata.durableflow.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 步骤定义（JavaBean，便于 JSON 反序列化）。
 *
 * <ul>
 *   <li>串行：通过 {@code dependsOn} 形成依赖链</li>
 *   <li>并行：多个步骤依赖同一组前置步骤</li>
 *   <li>条件分支：{@code condition} 表达式为假时步骤被跳过</li>
 *   <li>定时等待：{@code type=WAIT} + {@code waitMillis}</li>
 * </ul>
 */
public class StepDefinition {

    private String id;
    private StepType type = StepType.TASK;
    private String handler;
    private Map<String, String> args = new LinkedHashMap<>();
    private List<String> dependsOn = new ArrayList<>();
    private String condition;
    private long waitMillis;
    private RetryPolicy retry;
    private String compensationHandler;
    private Map<String, String> compensationArgs = new LinkedHashMap<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public StepType getType() {
        return type;
    }

    public void setType(StepType type) {
        this.type = type;
    }

    public String getHandler() {
        return handler;
    }

    public void setHandler(String handler) {
        this.handler = handler;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args == null ? new LinkedHashMap<>() : args;
    }

    public List<String> getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(List<String> dependsOn) {
        this.dependsOn = dependsOn == null ? new ArrayList<>() : dependsOn;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public long getWaitMillis() {
        return waitMillis;
    }

    public void setWaitMillis(long waitMillis) {
        this.waitMillis = waitMillis;
    }

    public RetryPolicy getRetry() {
        return retry;
    }

    public void setRetry(RetryPolicy retry) {
        this.retry = retry;
    }

    public String getCompensationHandler() {
        return compensationHandler;
    }

    public void setCompensationHandler(String compensationHandler) {
        this.compensationHandler = compensationHandler;
    }

    public Map<String, String> getCompensationArgs() {
        return compensationArgs;
    }

    public void setCompensationArgs(Map<String, String> compensationArgs) {
        this.compensationArgs = compensationArgs == null ? new LinkedHashMap<>() : compensationArgs;
    }

    /** 步骤未配置重试策略时返回引擎默认策略。 */
    public RetryPolicy retryOr(RetryPolicy fallback) {
        return retry != null ? retry : fallback;
    }

    // ---- 流式构造辅助（测试与内联定义用） ----

    public static StepDefinition task(String id, String handler) {
        StepDefinition d = new StepDefinition();
        d.setId(id);
        d.setHandler(handler);
        return d;
    }

    public static StepDefinition wait(String id, long waitMillis) {
        StepDefinition d = new StepDefinition();
        d.setId(id);
        d.setType(StepType.WAIT);
        d.setWaitMillis(waitMillis);
        return d;
    }

    public StepDefinition args(Map<String, String> args) {
        setArgs(args);
        return this;
    }

    public StepDefinition dependsOn(String... ids) {
        setDependsOn(new ArrayList<>(List.of(ids)));
        return this;
    }

    public StepDefinition condition(String condition) {
        setCondition(condition);
        return this;
    }

    public StepDefinition retry(RetryPolicy retry) {
        setRetry(retry);
        return this;
    }

    public StepDefinition compensation(String handler, Map<String, String> args) {
        setCompensationHandler(handler);
        setCompensationArgs(args);
        return this;
    }
}
