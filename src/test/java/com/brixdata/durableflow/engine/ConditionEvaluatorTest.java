package com.brixdata.durableflow.engine;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionEvaluatorTest {

    @Test
    void blankConditionIsTrue() {
        assertTrue(ConditionEvaluator.evaluate(null, Map.of(), Map.of()));
        assertTrue(ConditionEvaluator.evaluate("  ", Map.of(), Map.of()));
    }

    @Test
    void literals() {
        assertTrue(ConditionEvaluator.evaluate("true", Map.of(), Map.of()));
        assertFalse(ConditionEvaluator.evaluate("false", Map.of(), Map.of()));
    }

    @Test
    void truthiness() {
        assertTrue(ConditionEvaluator.evaluate("input.flag", Map.of("flag", true), Map.of()));
        assertFalse(ConditionEvaluator.evaluate("input.flag", Map.of("flag", false), Map.of()));
        assertFalse(ConditionEvaluator.evaluate("input.missing", Map.of(), Map.of()));
        assertTrue(ConditionEvaluator.evaluate("!input.missing", Map.of(), Map.of()));
    }

    @Test
    void equality() {
        Map<String, Object> input = Map.of("env", "prod", "replicas", 3);
        assertTrue(ConditionEvaluator.evaluate("input.env == prod", input, Map.of()));
        assertTrue(ConditionEvaluator.evaluate("input.env == 'prod'", input, Map.of()));
        assertTrue(ConditionEvaluator.evaluate("input.env != dev", input, Map.of()));
        assertTrue(ConditionEvaluator.evaluate("input.replicas == 3", input, Map.of()));
        assertFalse(ConditionEvaluator.evaluate("input.env == dev", input, Map.of()));
    }

    @Test
    void stepResultComparison() {
        Map<String, String> results = Map.of("check", "fast");
        assertTrue(ConditionEvaluator.evaluate("steps.check.result == fast", Map.of(), results));
        assertFalse(ConditionEvaluator.evaluate("steps.check.result == slow", Map.of(), results));
    }

    @Test
    void unsupportedPathRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ConditionEvaluator.evaluate("system.env == x", Map.of(), Map.of()));
    }
}
