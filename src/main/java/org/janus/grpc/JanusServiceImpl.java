package org.janus.grpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.common.HelloUtils;
import org.janus.config.ServerConfig;
import org.janus.handler.ChainHandler;
import org.janus.model.JanusMessage;
import org.janus.observability.OtelSupport;
import org.janus.proto.ResultType;
import org.janus.proto.TalkRequest;
import org.janus.proto.TalkResponse;
import org.janus.proto.TalkResult;
import org.janus.proto.JanusServiceGrpc;

import java.util.*;

/**
 * gRPC service implementation supporting all 4 RPC models.
 *
 * <p>Resolution order per call:
 * <ol>
 *   <li>gRPC downstream configured ({@code blockingStub}/{@code asyncStub} set) →
 *       forward to the next hop via gRPC (preserving native streaming).</li>
 *   <li>WS downstream configured ({@code chainHandler} + {@code JANUS_DOWNSTREAM_PROTOCOL=ws})
 *       → route through {@link ChainHandler} so a request received on gRPC is
 *       forwarded to the next hop over WebSocket (the gRPC-in → WS-out middle
 *       node). Streaming RPCs collapse to a unary WS round-trip.</li>
 *   <li>Otherwise → process locally (terminal node).</li>
 * </ol>
 */
public class JanusServiceImpl extends JanusServiceGrpc.JanusServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(JanusServiceImpl.class);

    private JanusServiceGrpc.JanusServiceBlockingStub blockingStub;
    private JanusServiceGrpc.JanusServiceStub asyncStub;
    // When set (and no gRPC downstream), gRPC-received requests are routed through
    // the unified chain so they can be forwarded over WebSocket.
    private ChainHandler chainHandler;

    public JanusServiceImpl() {}

    public void setBlockingStub(JanusServiceGrpc.JanusServiceBlockingStub blockingStub) {
        this.blockingStub = blockingStub;
    }

    public void setAsyncStub(JanusServiceGrpc.JanusServiceStub asyncStub) {
        this.asyncStub = asyncStub;
    }

    public void setChainHandler(ChainHandler chainHandler) {
        this.chainHandler = chainHandler;
    }

    /** True when this node should forward gRPC-received requests over WebSocket. */
    private boolean wsForward() {
        return chainHandler != null && blockingStub == null && ServerConfig.isWsDownstream();
    }

    // ─── Unary RPC ────────────────────────────────────────────────────────────

    @Override
    public void talk(TalkRequest request, StreamObserver<TalkResponse> responseObserver) {
        log.debug("Unary call - data: {}, meta: {}, trace: {}", request.getData(), request.getMeta(), request.getTraceId());
        OtelSupport.recordRpcCall("Talk");

        TalkResponse response;
        if (blockingStub != null) {
            response = blockingStub.talk(request);
        } else if (wsForward()) {
            response = chainHandler.handleGrpcRequest(JanusMessage.METHOD_TALK, request);
        } else {
            response = TalkResponse.newBuilder()
                    .setStatus(200)
                    .addResults(createResult(request.getData(), request.getMeta()))
                    .setSeq(request.getSeq())
                    .setStreamEnd(true)
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // ─── Server Streaming RPC ──────────────────────────────────────────────────

    @Override
    public void talkOneAnswerMore(TalkRequest request, StreamObserver<TalkResponse> responseObserver) {
        log.debug("Server streaming - data: {}, meta: {}", request.getData(), request.getMeta());
        OtelSupport.recordRpcCall("TalkOneAnswerMore");

        if (blockingStub != null) {
            Iterator<TalkResponse> it = blockingStub.talkOneAnswerMore(request);
            it.forEachRemaining(responseObserver::onNext);
        } else if (wsForward()) {
            // gRPC-in → WS-out: the WS round-trip collapses to a single unary
            // reply, so emit one response carrying all aggregated results.
            responseObserver.onNext(
                    chainHandler.handleGrpcRequest(JanusMessage.METHOD_TALK_ONE_ANSWER_MORE, request));
        } else {
            String[] dataItems = request.getData().split(",");
            int seq = 0;
            for (String dataItem : dataItems) {
                TalkResponse response = TalkResponse.newBuilder()
                        .setStatus(200)
                        .addResults(createResult(dataItem, request.getMeta()))
                        .setSeq(seq)
                        .setStreamEnd(++seq == dataItems.length)
                        .build();
                responseObserver.onNext(response);
            }
        }

        responseObserver.onCompleted();
    }

    // ─── Client Streaming RPC ──────────────────────────────────────────────────

    @Override
    public StreamObserver<TalkRequest> talkMoreAnswerOne(StreamObserver<TalkResponse> responseObserver) {
        OtelSupport.recordRpcCall("TalkMoreAnswerOne");

        if (wsForward()) {
            // gRPC-in → WS-out: client streaming collapses to a single unary WS
            // round-trip. Accumulate the inbound requests, then forward one
            // aggregated request (comma-joined data) on completion.
            return new StreamObserver<>() {
                private final List<String> dataItems = new ArrayList<>();
                private TalkRequest last;

                @Override
                public void onNext(TalkRequest request) {
                    log.debug("Client streaming (WS forward) - data: {}", request.getData());
                    dataItems.add(request.getData());
                    last = request;
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error in client streaming", t);
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    TalkRequest base = last != null ? last : TalkRequest.getDefaultInstance();
                    TalkRequest aggregated = base.toBuilder()
                            .setData(String.join(",", dataItems))
                            .build();
                    responseObserver.onNext(
                            chainHandler.handleGrpcRequest(JanusMessage.METHOD_TALK_MORE_ANSWER_ONE, aggregated));
                    responseObserver.onCompleted();
                }
            };
        }

        if (asyncStub == null) {
            return new StreamObserver<>() {
                private final List<TalkResult> results = new ArrayList<>();

                @Override
                public void onNext(TalkRequest request) {
                    log.debug("Client streaming - data: {}, meta: {}", request.getData(), request.getMeta());
                    results.add(createResult(request.getData(), request.getMeta()));
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error in client streaming", t);
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    TalkResponse response = TalkResponse.newBuilder()
                            .setStatus(200)
                            .addAllResults(results)
                            .setSeq(0)
                            .setStreamEnd(true)
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            };
        } else {
            StreamObserver<TalkResponse> forwarder = createForwarder(responseObserver);
            StreamObserver<TalkRequest> requestObserver = asyncStub.talkMoreAnswerOne(forwarder);
            return new StreamObserver<>() {
                @Override
                public void onNext(TalkRequest request) {
                    log.debug("Client streaming (forwarding) - data: {}", request.getData());
                    requestObserver.onNext(request);
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error in client streaming", t);
                    requestObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    requestObserver.onCompleted();
                }
            };
        }
    }

    // ─── Bidirectional Streaming RPC ───────────────────────────────────────────

    @Override
    public StreamObserver<TalkRequest> talkBidirectional(StreamObserver<TalkResponse> responseObserver) {
        OtelSupport.recordRpcCall("TalkBidirectional");

        if (wsForward()) {
            // gRPC-in → WS-out: each inbound message triggers a unary WS
            // round-trip, and its reply is streamed straight back to the caller.
            return new StreamObserver<>() {
                @Override
                public void onNext(TalkRequest request) {
                    log.debug("Bidi streaming (WS forward) - data: {}", request.getData());
                    responseObserver.onNext(
                            chainHandler.handleGrpcRequest(JanusMessage.METHOD_TALK_BIDIRECTIONAL, request));
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error in bidi streaming", t);
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        if (asyncStub == null) {
            return new StreamObserver<>() {
                @Override
                public void onNext(TalkRequest request) {
                    log.debug("Bidi streaming - data: {}, meta: {}", request.getData(), request.getMeta());
                    TalkResponse response = TalkResponse.newBuilder()
                            .setStatus(200)
                            .addResults(createResult(request.getData(), request.getMeta()))
                            .setSeq(request.getSeq())
                            .setStreamEnd(request.getStreamEnd())
                            .build();
                    responseObserver.onNext(response);
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error in bidi streaming", t);
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        } else {
            StreamObserver<TalkResponse> forwarder = createForwarder(responseObserver);
            StreamObserver<TalkRequest> requestObserver = asyncStub.talkBidirectional(forwarder);
            return new StreamObserver<>() {
                @Override
                public void onNext(TalkRequest request) {
                    log.debug("Bidi streaming (forwarding) - data: {}", request.getData());
                    requestObserver.onNext(request);
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error in bidi streaming", t);
                    requestObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    requestObserver.onCompleted();
                }
            };
        }
    }

    // ─── Helpers ───────────────────────────────────────────────────────────────

    private StreamObserver<TalkResponse> createForwarder(StreamObserver<TalkResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(TalkResponse response) {
                responseObserver.onNext(response);
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error from backend", t);
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private TalkResult createResult(String id, String meta) {
        String greeting = HelloUtils.getGreeting(id);
        String answer = HelloUtils.getAnswer(greeting);

        Map<String, String> kv = new HashMap<>();
        kv.put("id", UUID.randomUUID().toString());
        kv.put("idx", id);
        kv.put("data", greeting + "," + answer);
        kv.put("meta", meta != null && !meta.isEmpty() ? meta : "JAVA");

        return TalkResult.newBuilder()
                .setId(System.nanoTime())
                .setType(ResultType.OK)
                .putAllKv(kv)
                .build();
    }
}
