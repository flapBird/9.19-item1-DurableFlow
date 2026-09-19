package com.brixdata.durableflow.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowDefinitionTest {

    @Test
    void validDefinitionPasses() {
        WorkflowDefinition def = new WorkflowDefinition("flow", List.of(
                StepDefinition.task("a", "echo"),
                StepDefinition.task("b", "echo").dependsOn("a"),
                StepDefinition.wait("w", 100).dependsOn("b")));
        assertDoesNotThrow(def::validate);
    }

    @Test
    void duplicateStepIdRejected() {
        WorkflowDefinition def = new WorkflowDefinition("flow", List.of(
                StepDefinition.task("a", "echo"),
                StepDefinition.task("a", "echo")));
        assertThrows(IllegalArgumentException.class, def::validate);
    }

    @Test
    void unknownDependencyRejected() {
        WorkflowDefinition def = new WorkflowDefinition("flow", List.of(
                StepDefinition.task("a", "echo").dependsOn("missing")));
        assertThrows(IllegalArgumentException.class, def::validate);
    }

    @Test
    void dependencyCycleRejected() {
        WorkflowDefinition def = new WorkflowDefinition("flow", List.of(
                StepDefinition.task("a", "echo").dependsOn("c"),
                StepDefinition.task("b", "echo").dependsOn("a"),
                StepDefinition.task("c", "echo").dependsOn("b")));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, def::validate);
        assertTrue(e.getMessage().contains("cycle"));
    }

    @Test
    void taskWithoutHandlerRejected() {
        WorkflowDefinition def = new WorkflowDefinition("flow", List.of(
                StepDefinition.task("a", null)));
        assertThrows(IllegalArgumentException.class, def::validate);
    }

    @Test
    void retryPolicyBackoff() {
        RetryPolicy policy = new RetryPolicy(5, 100, 2.0);
        assertEquals(100, policy.delayMillis(1));
        assertEquals(200, policy.delayMillis(2));
        assertEquals(400, policy.delayMillis(3));
        assertEquals(800, policy.delayMillis(4));
    }

    @Test
    void retryPolicyValidation() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(0, 0, 1.0));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(1, -1, 1.0));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(1, 0, 0.5));
    }
}
