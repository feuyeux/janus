package org.janus.codec;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binary WebSocket protocol codec.
 * Implements the canonical binary protocol from hello-websocket PROTOCOL.md.
 */
public final class BinaryCodec {

    private BinaryCodec() {}

    public static final byte MAGIC = 0x48;
    public static final byte VERSION = 0x01;
    public static final int HEADER_LEN = 8;

    /** Upper bound on any single length-prefixed count/size to reject malicious frames. */
    public static final int MAX_ELEMENTS = 1 << 20; // 1,048,576

    // Message types
    public static final byte MSG_HELLO = 0x01;
    public static final byte MSG_BONJOUR = 0x02;
    public static final byte MSG_ECHO_REQUEST = 0x03;
    public static final byte MSG_ECHO_RESPONSE = 0x04;
    public static final byte MSG_PING = 0x07;
    public static final byte MSG_PONG = 0x08;
    public static final byte MSG_DISCONNECT = 0x0C;
    public static final byte MSG_JANUS = 0x10;
    public static final byte MSG_ERROR = 0x7F;

    // Error codes
    public static final int ERR_DECODE = 0x01;
    public static final int ERR_UNKNOWN_MSG_TYPE = 0x02;
    public static final int ERR_TRUNCATED_PAYLOAD = 0x03;
    public static final int ERR_BAD_MAGIC = 0x04;
    public static final int ERR_BAD_VERSION = 0x05;

    // ─── ByteWriter ──────────────────────────────────────────────────────────

    public static final class ByteWriter {
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

        public void writeU8(int v) { buf.write(v & 0xFF); }

        public void writeU32(long v) {
            buf.write((int)((v >> 24) & 0xFF));
            buf.write((int)((v >> 16) & 0xFF));
            buf.write((int)((v >> 8) & 0xFF));
            buf.write((int)(v & 0xFF));
        }

        public void writeI32(int v) { writeU32(v & 0xFFFFFFFFL); }

        public void writeI64(long v) {
            buf.write((int)((v >> 56) & 0xFF));
            buf.write((int)((v >> 48) & 0xFF));
            buf.write((int)((v >> 40) & 0xFF));
            buf.write((int)((v >> 32) & 0xFF));
            buf.write((int)((v >> 24) & 0xFF));
            buf.write((int)((v >> 16) & 0xFF));
            buf.write((int)((v >> 8) & 0xFF));
            buf.write((int)(v & 0xFF));
        }

        public void writeString(String s) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            writeU32(b.length);
            buf.writeBytes(b);
        }

        public void writeKV(Map<String, String> m) {
            writeU32(m.size());
            for (var e : m.entrySet()) {
                writeString(e.getKey());
                writeString(e.getValue());
            }
        }

        public byte[] toByteArray() { return buf.toByteArray(); }
    }

    // ─── ByteReader ─────────────────────────────────────────────────────────

    public static final class ByteReader {
        private final byte[] data;
        private int pos;

        public ByteReader(byte[] data) { this.data = data; this.pos = 0; }

        public int readU8() throws DecodeException {
            if (pos + 1 > data.length) throw new DecodeException("unexpected end of data reading u8");
            return data[pos++] & 0xFF;
        }

        public long readU32() throws DecodeException {
            if (pos + 4 > data.length) throw new DecodeException("unexpected end of data reading u32");
            long v = ((long)(data[pos] & 0xFF) << 24) | ((long)(data[pos + 1] & 0xFF) << 16)
                   | ((long)(data[pos + 2] & 0xFF) << 8) | (data[pos + 3] & 0xFF);
            pos += 4;
            return v;
        }

        public int readI32() throws DecodeException { return (int) readU32(); }

        /**
         * Read a u32 element count and validate it against a sane upper bound and
         * the number of bytes still available, so a malicious frame cannot trigger
         * a huge/negative allocation (OOM / NegativeArraySizeException).
         */
        public int readCount() throws DecodeException {
            long count = readU32();
            if (count > MAX_ELEMENTS) {
                throw new DecodeException("element count " + count + " exceeds maximum " + MAX_ELEMENTS);
            }
            if (count > data.length - pos) {
                throw new DecodeException("element count " + count + " exceeds remaining data " + (data.length - pos));
            }
            return (int) count;
        }

        public long readI64() throws DecodeException {
            if (pos + 8 > data.length) throw new DecodeException("unexpected end of data reading i64");
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v = (v << 8) | (data[pos + i] & 0xFF);
            }
            pos += 8;
            return v;
        }

        public String readString() throws DecodeException {
            long ln = readU32();
            if (pos + ln > data.length) throw new DecodeException("string length " + ln + " exceeds remaining data");
            String s = new String(data, pos, (int) ln, StandardCharsets.UTF_8);
            pos += ln;
            return s;
        }

        public Map<String, String> readKV() throws DecodeException {
            int count = readCount();
            Map<String, String> m = new LinkedHashMap<>(count);
            for (int i = 0; i < count; i++) {
                String k = readString();
                String v = readString();
                m.put(k, v);
            }
            return m;
        }
    }

    @SuppressWarnings("serial")
    public static final class DecodeException extends Exception {
        public DecodeException(String msg) { super(msg); }
    }

    // ─── Frame Codec ─────────────────────────────────────────────────────────

    public static byte[] encodeFrame(byte msgType, byte[] payload) {
        byte[] buf = new byte[HEADER_LEN + payload.length];
        buf[0] = MAGIC;
        buf[1] = VERSION;
        buf[2] = msgType;
        buf[3] = 0x00;
        buf[4] = (byte)((payload.length >> 24) & 0xFF);
        buf[5] = (byte)((payload.length >> 16) & 0xFF);
        buf[6] = (byte)((payload.length >> 8) & 0xFF);
        buf[7] = (byte)(payload.length & 0xFF);
        System.arraycopy(payload, 0, buf, HEADER_LEN, payload.length);
        return buf;
    }

    public record Frame(byte msgType, byte[] payload) {}

    public static Frame decodeFrame(byte[] data) throws DecodeException {
        if (data.length < HEADER_LEN) throw new DecodeException("frame too short: " + data.length);
        if (data[0] != MAGIC) throw new DecodeException("bad magic: 0x" + String.format("%02x", data[0]));
        if (data[1] != VERSION) throw new DecodeException("bad version: 0x" + String.format("%02x", data[1]));
        byte msgType = data[2];
        long payloadLen = ((long)(data[4] & 0xFF) << 24) | ((long)(data[5] & 0xFF) << 16)
                        | ((long)(data[6] & 0xFF) << 8) | (data[7] & 0xFF);
        if (payloadLen > data.length - HEADER_LEN)
            throw new DecodeException("truncated payload: declared " + payloadLen + ", available " + (data.length - HEADER_LEN));
        byte[] payload = new byte[(int) payloadLen];
        System.arraycopy(data, HEADER_LEN, payload, 0, (int) payloadLen);
        return new Frame(msgType, payload);
    }

    // ─── Message ─────────────────────────────────────────────────────────────

    public record EchoResult(long idx, int type, Map<String, String> kv) {}

    public static final class Message {
        public byte type;
        public String clientLanguage;
        public String serverLanguage;
        public long echoId; public String echoMeta; public String echoData;
        public int echoStatus; public EchoResult[] echoResults;
        public long timestampMs;
        public String disconnectReason;
        public int errorCode; public String errorMessage;
        // Trace propagation
        public String traceId;
        public String spanId;

        public byte[] encode() {
            ByteWriter w = new ByteWriter();
            switch (type) {
                case MSG_HELLO -> { w.writeString(clientLanguage); return encodeFrame(MSG_HELLO, w.toByteArray()); }
                case MSG_BONJOUR -> { w.writeString(serverLanguage); return encodeFrame(MSG_BONJOUR, w.toByteArray()); }
                case MSG_ECHO_REQUEST -> {
                    w.writeI64(echoId); w.writeString(echoMeta); w.writeString(echoData);
                    w.writeString(traceId != null ? traceId : "");
                    w.writeString(spanId != null ? spanId : "");
                    return encodeFrame(MSG_ECHO_REQUEST, w.toByteArray());
                }
                case MSG_ECHO_RESPONSE -> {
                    w.writeI32(echoStatus); w.writeU32(echoResults.length);
                    for (EchoResult r : echoResults) { w.writeI64(r.idx()); w.writeU8(r.type()); w.writeKV(r.kv()); }
                    return encodeFrame(MSG_ECHO_RESPONSE, w.toByteArray());
                }
                case MSG_PING -> { w.writeI64(timestampMs); return encodeFrame(MSG_PING, w.toByteArray()); }
                case MSG_PONG -> { w.writeI64(timestampMs); return encodeFrame(MSG_PONG, w.toByteArray()); }
                case MSG_DISCONNECT -> { w.writeString(disconnectReason); return encodeFrame(MSG_DISCONNECT, w.toByteArray()); }
                case MSG_ERROR -> { w.writeI32(errorCode); w.writeString(errorMessage); return encodeFrame(MSG_ERROR, w.toByteArray()); }
                default -> throw new IllegalArgumentException("unknown message type: 0x" + String.format("%02x", type));
            }
        }
    }

    public static Message decodeMessage(byte[] data) throws DecodeException {
        Frame frame = decodeFrame(data);
        ByteReader r = new ByteReader(frame.payload());
        Message m = new Message();
        m.type = frame.msgType();
        switch (frame.msgType()) {
            case MSG_HELLO -> { m.clientLanguage = r.readString(); }
            case MSG_BONJOUR -> { m.serverLanguage = r.readString(); }
            case MSG_ECHO_REQUEST -> {
                m.echoId = r.readI64(); m.echoMeta = r.readString(); m.echoData = r.readString();
                m.traceId = r.readString(); m.spanId = r.readString();
            }
            case MSG_ECHO_RESPONSE -> {
                m.echoStatus = r.readI32();
                int count = r.readCount();
                m.echoResults = new EchoResult[count];
                for (int i = 0; i < count; i++) {
                    long idx = r.readI64();
                    int type = r.readU8();
                    Map<String, String> kv = r.readKV();
                    m.echoResults[i] = new EchoResult(idx, type, kv);
                }
            }
            case MSG_PING -> { m.timestampMs = r.readI64(); }
            case MSG_PONG -> { m.timestampMs = r.readI64(); }
            case MSG_DISCONNECT -> { m.disconnectReason = r.readString(); }
            case MSG_ERROR -> { m.errorCode = r.readI32(); m.errorMessage = r.readString(); }
            default -> throw new DecodeException("unknown message type: 0x" + String.format("%02x", frame.msgType()));
        }
        return m;
    }

    // ─── Message Factory Helpers ─────────────────────────────────────────────
    public static Message hello(String lang) { Message m = new Message(); m.type = MSG_HELLO; m.clientLanguage = lang; return m; }
    public static Message bonjour(String lang) { Message m = new Message(); m.type = MSG_BONJOUR; m.serverLanguage = lang; return m; }
    public static Message ping(long ts) { Message m = new Message(); m.type = MSG_PING; m.timestampMs = ts; return m; }
    public static Message pong(long ts) { Message m = new Message(); m.type = MSG_PONG; m.timestampMs = ts; return m; }
    public static Message disconnect(String reason) { Message m = new Message(); m.type = MSG_DISCONNECT; m.disconnectReason = reason; return m; }
    public static Message error(int code, String msg) { Message m = new Message(); m.type = MSG_ERROR; m.errorCode = code; m.errorMessage = msg; return m; }

    public static Message echoRequest(long id, String meta, String data, String traceId, String spanId) {
        Message m = new Message();
        m.type = MSG_ECHO_REQUEST;
        m.echoId = id;
        m.echoMeta = meta;
        m.echoData = data;
        m.traceId = traceId;
        m.spanId = spanId;
        return m;
    }

    public static Message echoResponse(int status, EchoResult[] results) {
        Message m = new Message();
        m.type = MSG_ECHO_RESPONSE;
        m.echoStatus = status;
        m.echoResults = results;
        return m;
    }

    // ─── Unified Janus Message (MSG_JANUS 0x10) ────────────────────────

    /**
     * Encode a unified Janus envelope into a MSG_JANUS binary frame.
     */
    public static byte[] encodeJanus(
            int method, int mode, int seq, boolean streamEnd,
            int status, String data, String meta,
            String traceId, String spanId, String errorMsg,
            EchoResult[] results) {
        ByteWriter w = new ByteWriter();
        w.writeU8(method);
        w.writeU8(mode);
        w.writeI32(seq);
        w.writeU8(streamEnd ? 1 : 0);
        w.writeI32(status);
        w.writeString(data != null ? data : "");
        w.writeString(meta != null ? meta : "");
        w.writeString(traceId != null ? traceId : "");
        w.writeString(spanId != null ? spanId : "");
        w.writeString(errorMsg != null ? errorMsg : "");
        if (results != null) {
            w.writeU32(results.length);
            for (EchoResult r : results) {
                w.writeI64(r.idx());
                w.writeU8(r.type());
                w.writeKV(r.kv());
            }
        } else {
            w.writeU32(0);
        }
        return encodeFrame(MSG_JANUS, w.toByteArray());
    }

    /**
     * Unified Janus envelope decoded from binary frame.
     */
    public record JanusFrame(
            int method, int mode, int seq, boolean streamEnd,
            int status, String data, String meta,
            String traceId, String spanId, String errorMsg,
            EchoResult[] results) {}

    /**
     * Decode a MSG_JANUS binary frame into a JanusFrame.
     */
    public static JanusFrame decodeJanus(byte[] data) throws DecodeException {
        Frame frame = decodeFrame(data);
        if (frame.msgType() != MSG_JANUS) {
            throw new DecodeException("not a MSG_JANUS frame: 0x" + String.format("%02x", frame.msgType()));
        }
        ByteReader r = new ByteReader(frame.payload());
        int method = r.readU8();
        int mode = r.readU8();
        int seq = r.readI32();
        boolean streamEnd = r.readU8() == 1;
        int status = r.readI32();
        String dataStr = r.readString();
        String meta = r.readString();
        String traceId = r.readString();
        String spanId = r.readString();
        String errorMsg = r.readString();
        int count = r.readCount();
        EchoResult[] results = new EchoResult[count];
        for (int i = 0; i < count; i++) {
            long idx = r.readI64();
            int type = r.readU8();
            Map<String, String> kv = r.readKV();
            results[i] = new EchoResult(idx, type, kv);
        }
        return new JanusFrame(method, mode, seq, streamEnd, status,
                dataStr, meta, traceId, spanId, errorMsg, results);
    }
}
