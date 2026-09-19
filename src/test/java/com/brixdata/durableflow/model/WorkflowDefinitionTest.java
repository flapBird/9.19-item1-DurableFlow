package com.brixdata.durableflow.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class WorkflowDefinitionTest {

    @Test
    void acceptsSerialParallelAndConditionalSteps() {
        WorkflowDefinition def = new WorkflowDefinition("order", List.of(
                StepDefinition.task("a", "echo").build(),
                StepDefinition.task("b", "echo").dependsOn("a").build(),
                StepDefinition.task("c", "echo").dependsOn("a").condition("input.vip == true").build(),
                StepDefinition.task("d", "echo").dependsOn("b", "c").build()));
        assertEquals("order", def.name());
        assertEquals(4, def.steps().size());
        assertEquals(List.of("b", "c"), List.of(def.step("b").id(), def.step("c").id()));
    }

    @Test
    void rejectsDuplicateIds() {
        assertThrows(IllegalArgumentException.class, () -> new WorkflowDefinition("dup", List.of(
                StepDefinition.task("a", "echo").build(),
                StepDefinition.task("a", "echo").build())));
    }

    @Test
    void rejectsMissingDependency() {
        assertThrows(IllegalArgumentException.class, () -> new WorkflowDefinition("missing", List.of(
                StepDefinition.task("a", "echo").dependsOn("ghost").build())));
    }

    @Test
    void rejectsDependencyCycle() {
        assertThrows(IllegalArgumentException.class, () -> new WorkflowDefinition("cycle", List.of(
                StepDefinition.task("a", "echo").dependsOn("b").build(),
                StepDefinition.task("b", "echo").dependsOn("a").build())));
    }

    @Test
    void jsonRoundTripPreservesDefinition() {
        WorkflowDefinition def = new WorkflowDefinition("roundtrip", List.of(
                StepDefinition.task("a", "echo")
                        .config(Map.of("msg", "hello"))
                        .retry(new RetryPolicy(3, 100, 2.0, 1000))
                        .compensation(new CompensationDef("undo", Map.of("x", 1), RetryPolicy.NONE))
                        .build(),
                StepDefinition.waitStep("w", 500).dependsOn("a").build(),
                StepDefinition.task("b", "echo").dependsOn("w")
                        .condition("input.level >= 2").build()));
        WorkflowDefinition restored = WorkflowDefinition.fromJson(def.toJson());
        assertEquals(def, restored);
    }
}
