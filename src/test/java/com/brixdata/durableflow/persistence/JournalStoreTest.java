package com.brixdata.durableflow.persistence;

import com.brixdata.durableflow.model.StepDefinition;
import com.brixdata.durableflow.model.WorkflowDefinition;
import com.brixdata.durableflow.model.WorkflowInstance;
import com.brixdata.durableflow.model.WorkflowStatus;
import com.brixdata.durableflow.model.StepStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JournalStoreTest {

    @TempDir
    Path dataDir;

    @Test
    void appendAndLoadRoundTrip() {
        try (JournalStore store = new JournalStore(dataDir)) {
            store.append("wf-1", WorkflowEvent.of(WorkflowEvent.Types.WORKFLOW_SUBMITTED, null,
                    Map.of("definition", Map.of("id", "f"), "input", Map.of())));
            store.append("wf-1", WorkflowEvent.of(WorkflowEvent.Types.STEP_READY, "a"));
            store.append("wf-2", WorkflowEvent.of(WorkflowEvent.Types.STEP_SKIPPED, "b"));

            assertTrue(store.exists("wf-1"));
            assertFalse(store.exists("wf-3"));
            assertEquals(2, store.load("wf-1").size());
            assertEquals(2, store.instanceIds().size());
            assertEquals(1, store.load("wf-2").size());
        }
    }

    @Test
    void truncatedTailLineIsTolerated() throws Exception {
        try (JournalStore store = new JournalStore(dataDir)) {
            store.append("wf-1", WorkflowEvent.of(WorkflowEvent.Types.STEP_READY, "a"));
            store.append("wf-1", WorkflowEvent.of(WorkflowEvent.Types.STEP_STARTED, "a"));
        }
        // 模拟崩溃：状态写入过程中被终止，留下半行
        Path file = dataDir.resolve("workflows").resolve("wf-1.journal");
        Files.writeString(file, "{\"type\":\"STEP_SUCCEEDED\",\"stepId\":\"a\",\"da",
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);

        try (JournalStore store = new JournalStore(dataDir)) {
            List<WorkflowEvent> events = store.load("wf-1");
            assertEquals(2, events.size());
            assertEquals(WorkflowEvent.Types.STEP_STARTED, events.get(1).getType());
        }
    }

    @Test
    void stateRebuilderRestoresInstance() {
        WorkflowDefinition def = new WorkflowDefinition("flow", List.of(
                StepDefinition.task("a", "echo"),
                StepDefinition.task("b", "echo").dependsOn("a")));
        try (JournalStore store = new JournalStore(dataDir)) {
            store.append("wf-1", WorkflowEvent.of(WorkflowEvent.Types.WORKFLOW_SUBMITTED, null,
                    Map.of("definition", def, "input", Map.of("k", "v"))));
            store.append("wf-1", WorkflowEvent.of(WorkflowEvent.Types.STEP_READY, "a"));
            store.append("wf-1", WorkflowEvent.of(WorkflowEvent.Types.STEP_STARTED, "a",
                    Map.of("attempt", 1)));
            store.append("wf-1", WorkflowEvent.of(WorkflowEvent.Types.STEP_SUCCEEDED, "a",
                    Map.of("result", "ok")));

            WorkflowInstance inst = StateRebuilder.rebuild("wf-1", store.load("wf-1"));
            assertEquals("flow", inst.getDefinition().getId());
            assertEquals("v", inst.getInput().get("k"));
            assertEquals(WorkflowStatus.RUNNING, inst.getStatus());
            assertEquals(StepStatus.SUCCEEDED, inst.step("a").getStatus());
            assertEquals("ok", inst.step("a").getResult());
            assertEquals(StepStatus.PENDING, inst.step("b").getStatus());
        }
    }
}
