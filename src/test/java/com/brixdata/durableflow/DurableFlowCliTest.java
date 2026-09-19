package com.brixdata.durableflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 命令行入口测试：提交、恢复、状态查询均通过 CLI 完成，无需外部基础设施。
 */
class DurableFlowCliTest {

    @TempDir
    Path dataDir;

    @TempDir
    Path workDir;

    private record Result(int code, String out, String err) {
    }

    private Result cli(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String[] full = new String[args.length + 2];
        full[0] = "--data-dir";
        full[1] = dataDir.toString();
        System.arraycopy(args, 0, full, 2, args.length);
        int code = DurableFlow.run(full,
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
        return new Result(code, out.toString(StandardCharsets.UTF_8),
                err.toString(StandardCharsets.UTF_8));
    }

    private String writeDefinition(String name, String json) throws Exception {
        Path file = workDir.resolve(name + ".json");
        Files.writeString(file, json);
        return file.toString();
    }

    @Test
    void submitWithWaitRunsToCompletion() throws Exception {
        String def = writeDefinition("ok", """
                {"name": "cli-ok", "steps": [
                  {"id": "a", "type": "TASK", "handler": "echo", "config": {"message": "hello"}},
                  {"id": "b", "type": "TASK", "handler": "echo", "dependsOn": ["a"]}
                ]}
                """);
        Result result = cli("submit", def, "--wait");
        assertEquals(0, result.code(), result.err());
        assertTrue(result.out().contains("status=COMPLETED"), result.out());
        assertTrue(result.out().contains("runId="), result.out());
    }

    @Test
    void submitFailingWorkflowReturnsNonZero() throws Exception {
        String def = writeDefinition("fail", """
                {"name": "cli-fail", "steps": [
                  {"id": "a", "type": "TASK", "handler": "fail"}
                ]}
                """);
        Result result = cli("submit", def, "--wait");
        assertEquals(1, result.code());
        assertTrue(result.out().contains("status=FAILED"), result.out());
    }

    @Test
    void submitWithoutWaitThenRecoverCompletesRun() throws Exception {
        String def = writeDefinition("deferred", """
                {"name": "cli-deferred", "steps": [
                  {"id": "a", "type": "TASK", "handler": "echo"}
                ]}
                """);
        // 提交后不等待：进程退出时工作流尚未执行
        Result submit = cli("submit", def);
        assertEquals(0, submit.code(), submit.err());
        Matcher matcher = Pattern.compile("runId=([0-9a-f-]+)").matcher(submit.out());
        assertTrue(matcher.find(), submit.out());
        String runId = matcher.group(1);

        // 模拟"重新启动"：恢复未完成的工作流并执行完成
        // （若提交瞬间步骤已被调度完成，recover 恢复 0 个，同样返回成功）
        Result recover = cli("recover", "--wait");
        assertEquals(0, recover.code(), recover.err());

        // 状态查询不需要启动引擎
        Result status = cli("status", runId);
        assertEquals(0, status.code());
        assertTrue(status.out().contains("status=COMPLETED"), status.out());

        Result list = cli("list");
        assertEquals(0, list.code());
        assertTrue(list.out().contains(runId), list.out());
    }

    @Test
    void retryAndWaitDefinitionsWorkThroughCli() throws Exception {
        String def = writeDefinition("retry-wait", """
                {"name": "cli-retry-wait", "steps": [
                  {"id": "w", "type": "WAIT", "waitMs": 300},
                  {"id": "f", "type": "TASK", "handler": "flaky", "dependsOn": ["w"],
                   "config": {"failTimes": 1},
                   "retry": {"maxAttempts": 3, "initialBackoffMs": 100, "multiplier": 1.0, "maxBackoffMs": 500}}
                ]}
                """);
        Result result = cli("submit", def, "--wait");
        assertEquals(0, result.code(), result.err());
        assertTrue(result.out().contains("attempts=2"), result.out());
    }

    @Test
    void unknownCommandAndMissingArgsReturnUsageError() {
        assertEquals(2, cli().code());
        assertEquals(2, cli("bogus").code());
        assertEquals(2, cli("status").code());
    }
}
