package org.janus.ws;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link JanusWsClient#selectLeastLoadedIndex(int[], boolean[], int)},
 * the pure core of the pool's least-in-flight connection selection (P2-1).
 */
class LeastLoadedSelectionTest {

    @Test
    void picksTheOpenConnectionWithFewestInFlight() {
        int[] loads = {5, 2, 8};
        boolean[] open = {true, true, true};
        assertEquals(1, JanusWsClient.selectLeastLoadedIndex(loads, open, 0));
    }

    @Test
    void skipsClosedConnectionsEvenWhenTheyAreLessLoaded() {
        int[] loads = {0, 1, 2};   // slot 0 is idle but closed
        boolean[] open = {false, true, true};
        assertEquals(1, JanusWsClient.selectLeastLoadedIndex(loads, open, 0));
    }

    @Test
    void returnsMinusOneWhenNoConnectionIsOpen() {
        int[] loads = {0, 0, 0};
        boolean[] open = {false, false, false};
        assertEquals(-1, JanusWsClient.selectLeastLoadedIndex(loads, open, 0));
    }

    @Test
    void returnsMinusOneForEmptyPool() {
        assertEquals(-1, JanusWsClient.selectLeastLoadedIndex(new int[0], new boolean[0], 0));
    }

    @Test
    void rotatingStartBreaksTiesFairly() {
        int[] loads = {3, 3, 3};   // all equal → tie resolved by start offset
        boolean[] open = {true, true, true};
        assertEquals(0, JanusWsClient.selectLeastLoadedIndex(loads, open, 0));
        assertEquals(1, JanusWsClient.selectLeastLoadedIndex(loads, open, 1));
        assertEquals(2, JanusWsClient.selectLeastLoadedIndex(loads, open, 2));
    }

    @Test
    void idleConnectionShortCircuitsScanFromStart() {
        int[] loads = {2, 0, 0};
        boolean[] open = {true, true, true};
        // From start 0: slot 0 (load 2) then slot 1 (idle) wins and stops.
        assertEquals(1, JanusWsClient.selectLeastLoadedIndex(loads, open, 0));
        // From start 2: slot 2 is idle immediately.
        assertEquals(2, JanusWsClient.selectLeastLoadedIndex(loads, open, 2));
    }

    @Test
    void wrapsAroundWhenStartIsNonZero() {
        int[] loads = {1, 9, 9};
        boolean[] open = {true, true, true};
        // Start at slot 1, scan 1→2→0; slot 0 (load 1) is the minimum.
        assertEquals(0, JanusWsClient.selectLeastLoadedIndex(loads, open, 1));
    }

    @Test
    void reserveReleaseTrackInFlightCount() {
        JanusWsClient.MultiplexClient c =
                new JanusWsClient.MultiplexClient(java.net.URI.create("ws://localhost:1"));
        assertEquals(0, c.inFlight());
        c.reserve();
        c.reserve();
        assertEquals(2, c.inFlight(), "reservations counted at selection time");
        c.release();
        assertEquals(1, c.inFlight());
        c.release();
        assertEquals(0, c.inFlight(), "balanced after release");
    }
}
