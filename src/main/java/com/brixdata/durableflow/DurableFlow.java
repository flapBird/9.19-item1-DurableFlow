package com.brixdata.durableflow;

/**
 * DurableFlow 持久化工作流执行引擎入口。
 */
public final class DurableFlow {

    /** 引擎名称。 */
    public static final String NAME = "DurableFlow";

    private DurableFlow() {
    }

    public static void main(String[] args) {
        System.out.println(NAME + " persistent workflow engine is initializing...");
    }
}
