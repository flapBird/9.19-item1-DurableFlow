package com.brixdata.durableflow;

import com.brixdata.durableflow.config.EngineConfig;
import com.brixdata.durableflow.engine.HandlerRegistry;
import com.brixdata.durableflow.engine.WorkflowEngine;
import com.brixdata.durableflow.model.StepInstance;
import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.model.WorkflowStatus;
import com.brixdata.durableflow.model.WorkflowInstance;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DurableFlow 命令行入口。
 *
 * <pre>
 *   durableflow [--config &lt;yaml&gt;] [--data-dir &lt;dir&gt;] &lt;command&gt; [args]
 *
 *   submit &lt;definition.json&gt; [key=value ...]  提交工作流并运行至完成
 *   recover                                    恢复所有未完成实例并运行至完成
 *   status [instanceId]                        查看执行状态（不触发执行）
 *   list                                       列出全部实例
 * </pre>
 *
 * 退出码：submit/recover 全部实例成功为 0，否则为 1；参数错误为 2。
 */
public final class DurableFlow {

    /** 引擎名称。 */
    public static final String NAME = "DurableFlow";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration COMPLETION_TIMEOUT = Duration.ofHours(1);

    private DurableFlow() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out));
    }

    static int run(String[] args, PrintStream out) {
        Path configPath = Path.of("config", "durableflow.yaml");
        Path dataDirOverride = null;
        String command = null;
        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
                case "--config" -> configPath = Path.of(args[++i]);
                case "--data-dir" -> dataDirOverride = Path.of(args[++i]);
                default -> {
                    command = args[i];
                    i++;
                    // 剩余参数全部留给命令
                    return execute(configPath, dataDirOverride, command,
                            java.util.Arrays.copyOfRange(args, i, args.length), out);
                }
            }
            i++;
        }
        usage(out);
        return command == null ? 2 : 0;
    }

    private static int execute(Path configPath, Path dataDirOverride, String command,
                               String[] args, PrintStream out) {
        EngineConfig config = EngineConfig.load(configPath);
        if (dataDirOverride != null) {
            config.setDataDir(dataDirOverride);
        }
        try {
            return switch (command) {
                case "submit" -> submit(config, args, out);
                case "recover" -> recover(config, out);
                case "status" -> status(config, args, out);
                case "list" -> status(config, new String[0], out);
                default -> {
                    out.println("unknown command: " + command);
                    usage(out);
                    yield 2;
                }
            };
        } catch (Exception e) {
            out.println(NAME + " error: " + e.getMessage());
            return 2;
        }
    }

    /** 提交工作流并运行至完成。 */
    private static int submit(EngineConfig config, String[] args, PrintStream out) throws Exception {
        if (args.length < 1) {
            out.println("usage: submit <definition.json> [key=value ...]");
            return 2;
        }
        Path definitionFile = Path.of(args[0]);
        if (!Files.exists(definitionFile)) {
            out.println("definition file not found: " + definitionFile);
            return 2;
        }
        WorkflowDefinition definition = MAPPER.readValue(Files.newInputStream(definitionFile),
                WorkflowDefinition.class);
        Map<String, Object> input = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            int eq = args[i].indexOf('=');
            if (eq <= 0) {
                out.println("invalid input (expect key=value): " + args[i]);
                return 2;
            }
            input.put(args[i].substring(0, eq), parseValue(args[i].substring(eq + 1)));
        }
        try (WorkflowEngine engine = new WorkflowEngine(config, HandlerRegistry.withBuiltins())) {
            String id = engine.submit(definition, input);
            out.println("submitted: " + id + " (definition=" + definition.getId() + ")");
            engine.awaitAllTerminal(COMPLETION_TIMEOUT);
            printInstance(engine.instance(id), out);
            return engine.instance(id).getStatus() == WorkflowStatus.SUCCEEDED ? 0 : 1;
        }
    }

    /** 恢复所有未完成实例并运行至完成。 */
    private static int recover(EngineConfig config, PrintStream out) {
        try (WorkflowEngine engine = new WorkflowEngine(config, HandlerRegistry.withBuiltins())) {
            int resumed = engine.recover();
            out.println("recovered " + resumed + " unfinished instance(s)");
            engine.awaitAllTerminal(COMPLETION_TIMEOUT);
            boolean allSucceeded = true;
            for (WorkflowInstance instance : engine.instances()) {
                printInstance(instance, out);
                if (instance.getStatus() != WorkflowStatus.SUCCEEDED) {
                    allSucceeded = false;
                }
            }
            return allSucceeded ? 0 : 1;
        }
    }

    /** 查看状态：仅加载事件日志，不触发任何执行。 */
    private static int status(EngineConfig config, String[] args, PrintStream out) {
        try (WorkflowEngine engine = new WorkflowEngine(config, HandlerRegistry.withBuiltins())) {
            engine.load();
            if (args.length > 0) {
                WorkflowInstance instance = engine.instance(args[0]);
                if (instance == null) {
                    out.println("no such instance: " + args[0]);
                    return 1;
                }
                printInstance(instance, out);
            } else if (engine.instances().isEmpty()) {
                out.println("no workflow instances found");
            } else {
                for (WorkflowInstance instance : engine.instances()) {
                    printInstance(instance, out);
                }
            }
            return 0;
        }
    }

    private static void printInstance(WorkflowInstance instance, PrintStream out) {
        out.printf("instance %s  definition=%s  status=%s  input=%s%n",
                instance.getId(), instance.getDefinition().getId(),
                instance.getStatus(), instance.getInput());
        for (StepInstance step : instance.getSteps()) {
            StringBuilder line = new StringBuilder();
            line.append(String.format("  step %-16s %-10s attempts=%d", step.getStepId(),
                    step.getStatus(), step.getAttempts()));
            if (step.getResult() != null) {
                line.append("  result=").append(step.getResult());
            }
            if (step.getError() != null) {
                line.append("  error=").append(step.getError());
            }
            if (step.isCompensated()) {
                line.append("  [compensated]");
            }
            out.println(line);
        }
    }

    private static Object parseValue(String raw) {
        if ("true".equalsIgnoreCase(raw) || "false".equalsIgnoreCase(raw)) {
            return Boolean.parseBoolean(raw);
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ignored) {
        }
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException ignored) {
        }
        return raw;
    }

    private static void usage(PrintStream out) {
        out.println(NAME + " - persistent workflow engine");
        out.println("usage: durableflow [--config <yaml>] [--data-dir <dir>] <command> [args]");
        out.println("  submit <definition.json> [key=value ...]  submit and run to completion");
        out.println("  recover                                   resume unfinished workflows");
        out.println("  status [instanceId]                       show execution status");
        out.println("  list                                      list all instances");
    }
}
