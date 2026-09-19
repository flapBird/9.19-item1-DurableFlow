package com.brixdata.durableflow.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    @Test
    void backoffGrowsExponentiallyAndIsCapped() {
        RetryPolicy policy = new RetryPolicy(5, 100, 2.0, 250);
        assertEquals(100, policy.backoffAfter(1));
        assertEquals(200, policy.backoffAfter(2));
        assertEquals(250, policy.backoffAfter(3));
        assertEquals(250, policy.backoffAfter(4));
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(0, 0, 1.0, 0));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(1, -1, 1.0, 0));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(1, 0, 0.5, 0));
    }

    @Test
    void jsonRoundTrip() {
        RetryPolicy policy = new RetryPolicy(3, 50, 1.5, 1000);
        assertEquals(policy, RetryPolicy.fromJson(policy.toJson()));
    }
}
