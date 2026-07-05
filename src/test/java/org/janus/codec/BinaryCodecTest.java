package org.janus.codec;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BinaryCodecTest {

    @Test
    void janusFrameRoundTrips() throws Exception {
        BinaryCodec.EchoResult[] results = {
                new BinaryCodec.EchoResult(42L, 0, Map.of("k", "v", "idx", "3"))
        };
        byte[] frame = BinaryCodec.encodeJanus(
                1, 1, 5, false, 200, "data", "meta", "tid", "sid", "", results);

        BinaryCodec.JanusFrame tf = BinaryCodec.decodeJanus(frame);

        assertEquals(1, tf.method());
        assertEquals(1, tf.mode());
        assertEquals(5, tf.seq());
        assertFalse(tf.streamEnd());
        assertEquals(200, tf.status());
        assertEquals("data", tf.data());
        assertEquals("meta", tf.meta());
        assertEquals("tid", tf.traceId());
        assertEquals("sid", tf.spanId());
        assertEquals(1, tf.results().length);
        assertEquals(42L, tf.results()[0].idx());
        assertEquals("v", tf.results()[0].kv().get("k"));
    }

    @Test
    void echoRequestRoundTrips() throws Exception {
        BinaryCodec.Message m = BinaryCodec.echoRequest(7L, "meta", "3", "tid", "sid");
        byte[] enc = m.encode();

        BinaryCodec.Message dec = BinaryCodec.decodeMessage(enc);

        assertEquals(BinaryCodec.MSG_ECHO_REQUEST, dec.type);
        assertEquals(7L, dec.echoId);
        assertEquals("meta", dec.echoMeta);
        assertEquals("3", dec.echoData);
        assertEquals("tid", dec.traceId);
        assertEquals("sid", dec.spanId);
    }

    @Test
    void decodeFrameRejectsBadMagic() {
        byte[] bad = new byte[BinaryCodec.HEADER_LEN];
        bad[0] = 0x00; // not MAGIC
        bad[1] = BinaryCodec.VERSION;
        assertThrows(BinaryCodec.DecodeException.class, () -> BinaryCodec.decodeFrame(bad));
    }

    @Test
    void decodeFrameRejectsTruncatedPayload() {
        BinaryCodec.ByteWriter w = new BinaryCodec.ByteWriter();
        w.writeString("hi");
        byte[] frame = BinaryCodec.encodeFrame(BinaryCodec.MSG_HELLO, w.toByteArray());
        // Corrupt declared payload length to be larger than actual bytes.
        frame[7] = (byte) 0xFF;
        assertThrows(BinaryCodec.DecodeException.class, () -> BinaryCodec.decodeFrame(frame));
    }

    @Test
    void readCountRejectsOversizedElementCount() {
        BinaryCodec.ByteWriter w = new BinaryCodec.ByteWriter();
        w.writeI32(200);            // echoStatus
        w.writeU32(0x7fffffffL);    // absurd result count, no backing bytes
        byte[] frame = BinaryCodec.encodeFrame(BinaryCodec.MSG_ECHO_RESPONSE, w.toByteArray());

        // Must fail fast instead of attempting a huge allocation.
        assertThrows(BinaryCodec.DecodeException.class, () -> BinaryCodec.decodeMessage(frame));
    }
}
