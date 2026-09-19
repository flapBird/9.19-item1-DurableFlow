package com.brixdata.durableflow;

import com.brixdata.durableflow.config.EngineConfig;
import com.brixdata.durableflow.engine.HandlerRegistry;
import com.brixdata.durableflow.engine.WorkflowEngine;
import com.brixdata.durableflow.model.StepDefinition;
import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.model.WorkflowStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableFlowCliTest {

    @TempDir
    Path dataDir;

    @TempDir
    Path workDir;

    private record Result(int exitCode, String output) {
    }

    private Result runCli(String... args) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8);
        String[] full = new String[args.length + 2];
        full[0] = "--data-dir";
        full[1] = dataDir.toString();
        System.arraycopy(args, 0, full, 2, args.length);
        int code = DurableFlow.run(full, out);
        return new Result(code, buffer.toString(StandardCharsets.UTF_8));
    }

    private Path writeDefinition(String json) throws Exception {
        Path file = workDir.resolve("flow.json");
        Files.writeString(file, json);
        return file;
    }

    @Test
    void submitRunsWorkflowToCompletion() throws Exception {
        Path def = writeDefinition("""
                {
                  "id": "cli-flow",
                  "steps": [
                    {"id": "a", "type": "TASK", "handler": "echo", "args": {"message": "hello"}},
                    {"id": "w", "type": "WAIT", "waitMillis": 50, "dependsOn": ["a"]},
                    {"id": "b", "type": "TASK", "handler": "echo", "dependsOn": ["w"],
                     "condition": "input.go == true"}
                  ]
                }
                """);
        Result submit = runCli("submit", def.toString(), "go=true");
        assertEquals(0, submit.exitCode(), submit.output());
        assertTrue(submit.output().contains("status=SUCCEEDED"), submit.output());

        Result status = runCli("status");
        assertEquals(0, status.exitCode());
        assertTrue(status.output().contains("cli-flow"), status.output());
        assertTrue(status.output().contains("SUCCEEDED"), status.output());
    }

    @Test
    void recoverResumesInterruptedWorkflow() throws Exception {
        // 先用引擎提交一个带定时等待的工作流并立即关闭，模拟中断
        EngineConfig config = new EngineConfig();
        config.setDataDir(dataDir);
        String id;
        try (WorkflowEngine engine = new WorkflowEngine(config, HandlerRegistry.withBuiltins())) {
            id = engine.submit(new WorkflowDefinition("cli-recover", List.of(
                    StepDefinition.wait("nap", 200),
                    StepDefinition.task("after", "echo").dependsOn("nap"))), Map.of());
        }
        Result recover = runCli("recover");
        assertEquals(0, recover.exitCode(), recover.output());
        assertTrue(recover.output().contains("recovered 1"), recover.output());
        assertTrue(recover.output().contains("status=SUCCEEDED"), recover.output());

        Result status = runCli("status", id);
        assertEquals(0, status.exitCode());
        assertTrue(status.output().contains("SUCCEEDED"), status.output());
    }

    @Test
    void invalidCommandAndMissingFileAreRejected() {
        Result unknown = runCli("frobnicate");
        assertEquals(2, unknown.exitCode());
        Result missing = runCli("submit", workDir.resolve("nope.json").toString());
        assertEquals(2, missing.exitCode());
    }

    @Test
    void failedWorkflowReturnsNonZeroExitCode() throws Exception {
        Path def = writeDefinition("""
                {
                  "id": "cli-fail",
                  "steps": [
                    {"id": "bad", "type": "TASK", "handler": "fail",
                     "retry": {"maxAttempts": 1, "backoffMillis": 0, "multiplier": 1.0}}
                  ]
                }
                """);
        Result result = runCli("submit", def.toString());
        assertEquals(1, result.exitCode(), result.output());
        assertTrue(result.output().contains("status=FAILED"), result.output());
    }
}
