package com.brixdata.durableflow;

import com.brixdata.durableflow.builtin.BuiltinHandlers;
import com.brixdata.durableflow.config.EngineSettings;
import com.brixdata.durableflow.engine.EngineConfig;
import com.brixdata.durableflow.engine.RunStatus;
import com.brixdata.durableflow.engine.WorkflowEngine;
import com.brixdata.durableflow.engine.WorkflowSnapshot;
import com.brixdata.durableflow.json.Jsons;
import com.brixdata.durableflow.model.WorkflowDefinition;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DurableFlow 持久化工作流执行引擎命令行入口。
 *
 * <pre>
 *   durableflow submit &lt;definition.json&gt; [--input '&lt;json&gt;'|@file] [--wait]
 *   durableflow recover [--wait]
 *   durableflow status &lt;runId&gt; | --all
 *   durableflow list
 * </pre>
 * 全局选项：{@code --data-dir <路径>}、{@code --max-concurrency <n>}、{@code --config <路径>}。
 * 默认读取 config/durableflow.yaml（若存在），命令行选项优先。
 */
public final class DurableFlow {

    /** 引擎名称。 */
    public static final String NAME = "DurableFlow";

    private static final Path DEFAULT_CONFIG = Path.of("config", "durableflow.yaml");

    private DurableFlow() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        List<String> positional = new ArrayList<>();
        Path dataDir = null;
        Integer maxConcurrency = null;
        Path configFile = DEFAULT_CONFIG;
        boolean wait = false;
        String input = null;
        boolean all = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--data-dir" -> dataDir = Path.of(requireValue(args, ++i, "--data-dir"));
                case "--max-concurrency" ->
                        maxConcurrency = Integer.valueOf(requireValue(args, ++i, "--max-concurrency"));
                case "--config" -> configFile = Path.of(requireValue(args, ++i, "--config"));
                case "--input" -> input = requireValue(args, ++i, "--input");
                case "--wait" -> wait = true;
                case "--all" -> all = true;
                default -> positional.add(args[i]);
            }
        }
        if (positional.isEmpty()) {
            usage(err);
            return 2;
        }
        String command = positional.get(0);
        EngineSettings settings = EngineSettings.load(configFile, dataDir, maxConcurrency);
        try {
            return switch (command) {
                case "submit" -> submit(positional, input, wait, settings, out, err);
                case "recover" -> recover(wait, settings, out);
                case "status" -> status(positional, all, settings, out, err);
                case "list" -> list(settings, out);
                default -> {
                    err.println("未知命令: " + command);
                    usage(err);
                    yield 2;
                }
            };
        } catch (Exception e) {
            err.println("执行失败: " + e.getMessage());
            return 1;
        }
    }

    // ---------------------------------------------------------------- 命令

    private static int submit(List<String> positional, String input, boolean wait,
                              EngineSettings settings, PrintStream out, PrintStream err)
            throws Exception {
        if (positional.size() < 2) {
            err.println("submit 需要工作流定义文件路径");
            return 2;
        }
        Path defFile = Path.of(positional.get(1));
        WorkflowDefinition definition =
                WorkflowDefinition.fromJson(Jsons.read(Files.readString(defFile)));
        Map<String, Object> inputMap = parseInput(input);
        try (WorkflowEngine engine = newEngine(settings)) {
            String runId = engine.submit(definition, inputMap);
            out.println("已提交工作流 " + definition.name() + "，runId=" + runId);
            if (!wait) {
                out.println("使用 `recover --wait` 或 `status " + runId + "` 查看进度");
                return 0;
            }
            engine.await(runId, Duration.ofDays(1));
            WorkflowSnapshot snapshot = engine.snapshot(runId);
            printSnapshot(snapshot, out);
            return snapshot.status() == RunStatus.COMPLETED ? 0 : 1;
        }
    }

    private static int recover(boolean wait, EngineSettings settings, PrintStream out) {
        try (WorkflowEngine engine = newEngine(settings)) {
            List<WorkflowSnapshot> snapshots = engine.listSnapshots();
            out.println("恢复未完成工作流 " + snapshots.size() + " 个");
            if (!wait) {
                snapshots.forEach(s -> printSnapshot(s, out));
                return 0;
            }
            engine.awaitAll(Duration.ofDays(1));
            boolean allCompleted = true;
            for (WorkflowSnapshot snapshot : engine.listSnapshots()) {
                printSnapshot(snapshot, out);
                allCompleted &= snapshot.status() == RunStatus.COMPLETED;
            }
            return allCompleted ? 0 : 1;
        }
    }

    private static int status(List<String> positional, boolean all,
                              EngineSettings settings, PrintStream out, PrintStream err) {
        if (all) {
            return list(settings, out);
        }
        if (positional.size() < 2) {
            err.println("status 需要 runId 或 --all");
            return 2;
        }
        String runId = positional.get(1);
        Path journal = settings.dataDir().resolve("workflows").resolve(runId + ".journal");
        if (!Files.exists(journal)) {
            err.println("未找到运行记录: " + runId);
            return 1;
        }
        printSnapshot(WorkflowEngine.readSnapshot(journal), out);
        return 0;
    }

    private static int list(EngineSettings settings, PrintStream out) {
        List<Path> journals = WorkflowEngine.listJournalFiles(settings.dataDir());
        if (journals.isEmpty()) {
            out.println("暂无工作流运行记录");
            return 0;
        }
        for (Path journal : journals) {
            printSnapshot(WorkflowEngine.readSnapshot(journal), out);
        }
        return 0;
    }

    // ---------------------------------------------------------------- 工具

    private static WorkflowEngine newEngine(EngineSettings settings) {
        return new WorkflowEngine(
                new EngineConfig(settings.dataDir(), settings.maxConcurrency()),
                BuiltinHandlers.create(settings.dataDir()));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseInput(String input) {
        if (input == null) {
            return Map.of();
        }
        String json = input.startsWith("@") ? readFile(Path.of(input.substring(1))) : input;
        return Jsons.toMap(Jsons.read(json));
    }

    private static String readFile(Path file) {
        try {
            return Files.readString(file);
        } catch (Exception e) {
            throw new IllegalArgumentException("读取文件失败: " + file, e);
        }
    }

    private static void printSnapshot(WorkflowSnapshot snapshot, PrintStream out) {
        out.printf("run %s  workflow=%s  status=%s%n",
                snapshot.runId(), snapshot.workflowName(), snapshot.status());
        for (WorkflowSnapshot.StepSnapshot step : snapshot.steps()) {
            out.printf("  [%-9s] %s (attempts=%d)%s%n", step.status(), step.stepId(),
                    step.attempts(), step.result() != null ? " -> " + step.result() : "");
        }
        if (snapshot.result() != null) {
            out.println("  result: " + snapshot.result());
        }
        if (snapshot.error() != null) {
            out.println("  error: " + snapshot.error());
        }
        if (!snapshot.compensatedSteps().isEmpty()) {
            out.println("  已补偿: " + snapshot.compensatedSteps());
        }
        if (!snapshot.failedCompensations().isEmpty()) {
            out.println("  补偿失败: " + snapshot.failedCompensations());
        }
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException("选项 " + option + " 缺少参数");
        }
        return args[index];
    }

    private static void usage(PrintStream err) {
        err.println("""
                用法:
                  durableflow submit <definition.json> [--input '<json>'|@file] [--wait]
                  durableflow recover [--wait]
                  durableflow status <runId> | --all
                  durableflow list
                全局选项: --data-dir <路径> --max-concurrency <n> --config <路径>
                """);
    }
}
