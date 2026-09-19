package com.brixdata.durableflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class DurableFlowTest {

    @Test
    void engineNameIsDurableFlow() {
        assertEquals("DurableFlow", DurableFlow.NAME);
    }

    @Test
    void engineEntryPointExists() {
        assertNotNull(DurableFlow.class);
    }
}
