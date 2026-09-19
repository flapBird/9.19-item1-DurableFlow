package com.brixdata.durableflow.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.brixdata.durableflow.json.Jsons;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JournalTest {

    @TempDir
    Path dir;

    private static Event event(Event.EventType type, String stepId) {
        ObjectNode data = Jsons.obj();
        data.put("stepId", stepId);
        return Event.of(type, data);
    }

    @Test
    void appendedEventsSurviveReopen() {
        Path file = dir.resolve("run-1.journal");
        try (Journal journal = Journal.open(file)) {
            journal.append(event(Event.EventType.STEP_STARTED, "a"));
            journal.append(event(Event.EventType.STEP_COMPLETED, "a"));
        }
        List<Event> events = Journal.replay(file);
        assertEquals(2, events.size());
        assertEquals(Event.EventType.STEP_STARTED, events.get(0).type());
        assertEquals("a", events.get(0).data().get("stepId").asText());
        assertEquals(Event.EventType.STEP_COMPLETED, events.get(1).type());
    }

    @Test
    void replayIgnoresTruncatedTailLine() throws Exception {
        Path file = dir.resolve("run-2.journal");
        try (Journal journal = Journal.open(file)) {
            journal.append(event(Event.EventType.STEP_STARTED, "a"));
        }
        // 模拟崩溃时写了一半的记录
        Files.writeString(file, "{\"type\":\"STEP_COMPLETED\",\"ti",
                StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);
        List<Event> events = Journal.replay(file);
        assertEquals(1, events.size());
        assertEquals(Event.EventType.STEP_STARTED, events.get(0).type());
    }

    @Test
    void replayMissingFileYieldsNoEvents() {
        assertTrue(Journal.replay(dir.resolve("nope.journal")).isEmpty());
    }

    @Test
    void eventRoundTripPreservesPayload() {
        Event original = event(Event.EventType.STEP_FAILED, "b");
        Event parsed = Event.parse(original.toLine());
        assertEquals(original.type(), parsed.type());
        assertEquals(original.time(), parsed.time());
        assertEquals(original.data(), parsed.data());
    }
}
