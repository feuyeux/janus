package org.janus.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JanusMessageTest {

    @Test
    void methodIndexAndBackAreConsistent() {
        assertEquals(0, JanusMessage.request(JanusMessage.METHOD_TALK, "0", "m").methodIndex());
        assertEquals(1, JanusMessage.request(JanusMessage.METHOD_TALK_ONE_ANSWER_MORE, "0", "m").methodIndex());
        assertEquals(2, JanusMessage.request(JanusMessage.METHOD_TALK_MORE_ANSWER_ONE, "0", "m").methodIndex());
        assertEquals(3, JanusMessage.request(JanusMessage.METHOD_TALK_BIDIRECTIONAL, "0", "m").methodIndex());

        assertEquals(JanusMessage.METHOD_TALK, JanusMessage.methodFromIndex(0));
        assertEquals(JanusMessage.METHOD_TALK_ONE_ANSWER_MORE, JanusMessage.methodFromIndex(1));
        assertEquals(JanusMessage.METHOD_TALK_MORE_ANSWER_ONE, JanusMessage.methodFromIndex(2));
        assertEquals(JanusMessage.METHOD_TALK_BIDIRECTIONAL, JanusMessage.methodFromIndex(3));
        // Out-of-range index falls back to TALK.
        assertEquals(JanusMessage.METHOD_TALK, JanusMessage.methodFromIndex(99));
    }

    @Test
    void nullMethodDefaultsToTalkIndex() {
        JanusMessage m = new JanusMessage(
                null, JanusMessage.MODE_REQUEST, "0", "m",
                null, null, null, null, null, 0, true, null);
        assertEquals(0, m.methodIndex());
    }

    @Test
    void factoriesSetExpectedModes() {
        assertTrue(JanusMessage.request(JanusMessage.METHOD_TALK, "0", "m").isRequest());
        assertTrue(JanusMessage.response(JanusMessage.METHOD_TALK, 200, List.of()).isResponse());
        assertTrue(JanusMessage.error(JanusMessage.METHOD_TALK, "boom").isError());
    }

    @Test
    void withRequestIdPreservesOtherFieldsAndSetsId() {
        JanusMessage base = JanusMessage.request(JanusMessage.METHOD_TALK, "3", "meta");
        JanusMessage withId = base.withRequestId("corr-123");

        assertNull(base.requestId(), "wither must not mutate the original");
        assertEquals("corr-123", withId.requestId());
        assertEquals("3", withId.data());
        assertEquals("meta", withId.meta());
        assertEquals(JanusMessage.MODE_REQUEST, withId.mode());
    }

    @Test
    void withTracePreservesRequestId() {
        JanusMessage msg = JanusMessage.request(JanusMessage.METHOD_TALK, "0", "m")
                .withRequestId("corr-9")
                .withTrace("trace-abc", "span-def");

        assertEquals("trace-abc", msg.traceId());
        assertEquals("span-def", msg.spanId());
        assertEquals("corr-9", msg.requestId(), "withTrace must keep the correlation id");
    }
}
