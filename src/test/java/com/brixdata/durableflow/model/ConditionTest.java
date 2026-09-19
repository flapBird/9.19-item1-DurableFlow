package com.brixdata.durableflow.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class ConditionTest {

    private final Map<String, Object> input = Map.of(
            "vip", true,
            "level", 3,
            "name", "alice",
            "nested", Map.of("count", 10));
    private final Map<String, Object> results = Map.of(
            "check", Map.of("stock", 5));

    @Test
    void evaluatesBooleanAndNumericComparisons() {
        assertTrue(Condition.parse("input.vip == true").evaluate(input, results));
        assertFalse(Condition.parse("input.vip != true").evaluate(input, results));
        assertTrue(Condition.parse("input.level >= 3").evaluate(input, results));
        assertTrue(Condition.parse("input.level > 2").evaluate(input, results));
        assertFalse(Condition.parse("input.level < 3").evaluate(input, results));
        assertTrue(Condition.parse("input.nested.count == 10").evaluate(input, results));
    }

    @Test
    void evaluatesStringComparisons() {
        assertTrue(Condition.parse("input.name == 'alice'").evaluate(input, results));
        assertTrue(Condition.parse("input.name != \"bob\"").evaluate(input, results));
        assertTrue(Condition.parse("input.name == alice").evaluate(input, results));
    }

    @Test
    void evaluatesAgainstStepResults() {
        assertTrue(Condition.parse("steps.check.stock > 0").evaluate(input, results));
        assertFalse(Condition.parse("steps.check.stock == 0").evaluate(input, results));
    }

    @Test
    void missingPathComparesAsNull() {
        assertTrue(Condition.parse("input.absent == null").evaluate(input, results));
        assertTrue(Condition.parse("input.absent != 'x'").evaluate(input, results));
        assertFalse(Condition.parse("input.absent == 'x'").evaluate(input, results));
    }

    @Test
    void rejectsMalformedExpressions() {
        assertThrows(IllegalArgumentException.class, () -> Condition.parse("input.vip =="));
        assertThrows(IllegalArgumentException.class, () -> Condition.parse("vip == true"));
        assertThrows(IllegalArgumentException.class, () -> Condition.parse("input.vip === true"));
        assertThrows(IllegalArgumentException.class, () -> Condition.parse("  "));
    }
}
