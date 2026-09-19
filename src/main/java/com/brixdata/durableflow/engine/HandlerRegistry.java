package com.brixdata.durableflow.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 处理器注册表：步骤定义中的 handler 名称 → 处理器实现。 */
public class HandlerRegistry {

    private final Map<String, StepHandler> handlers = new ConcurrentHashMap<>();

    public HandlerRegistry register(String name, StepHandler handler) {
        handlers.put(name, handler);
        return this;
    }

    public boolean contains(String name) {
        return handlers.containsKey(name);
    }

    public StepHandler require(String name) {
        StepHandler handler = handlers.get(name);
        if (handler == null) {
            throw new IllegalArgumentException("no handler registered for: " + name);
        }
        return handler;
    }

    /** 包含全部内置处理器的注册表。 */
    public static HandlerRegistry withBuiltins() {
        return BuiltinHandlers.create();
    }
}
