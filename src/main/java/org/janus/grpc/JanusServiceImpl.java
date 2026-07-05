package org.janus.grpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.common.HelloUtils;
import org.janus.observability.OtelSupport;
import org.janus.proto.ResultType;
import org.janus.proto.TalkRequest;
import org.janus.proto.TalkResponse;
import org.janus.proto.TalkResult;
import org.janus.proto.JanusServiceGrpc;
import lombok.Setter;

import java.util.*;

/**
 * gRPC service implementation supporting all 4 RPC models.
 * Forwards requests to downstream when configured.
 */
public class JanusServiceImpl extends JanusServiceGrpc.JanusServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(JanusServiceImpl.class);

    @Setter private JanusServiceGrpc.JanusServiceBlockingStub blockingStub;
    @Setter private JanusServiceGrpc.JanusServiceStub asyncStub;

    public JanusServiceImpl() {}

    // ─── Unary RPC ────────────────────────────────────────────────────────────

    @Override
    public void talk(TalkRequest request, StreamObserver<TalkResponse> responseObserver) {
        log.info("Unary call - data: {}, meta: {}, trace: {}", request.getData(), request.getMeta(), request.getTraceId());
        OtelSupport.recordRpcCall("Talk");

        TalkResponse response;
        if (blockingStub == null) {
            response = TalkResponse.newBuilder()
                    .setStatus(200)
                    .addResults(createResult(request.getData(), request.getMeta()))
                    .setSeq(request.getSeq())
                    .setStreamEnd(true)
                    .build();
        } else {
            response = blockingStub.talk(request);
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // ─── Server Streaming RPC ──────────────────────────────────────────────────

    @Override
    public void talkOneAnswerMore(TalkRequest request, StreamObserver<TalkResponse> responseObserver) {
        log.info("Server streaming - data: {}, meta: {}", request.getData(), request.getMeta());
        OtelSupport.recordRpcCall("TalkOneAnswerMore");

        if (blockingStub == null) {
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
        } else {
            Iterator<TalkResponse> it = blockingStub.talkOneAnswerMore(request);
            it.forEachRemaining(responseObserver::onNext);
        }

        responseObserver.onCompleted();
    }

    // ─── Client Streaming RPC ──────────────────────────────────────────────────

    @Override
    public StreamObserver<TalkRequest> talkMoreAnswerOne(StreamObserver<TalkResponse> responseObserver) {
        OtelSupport.recordRpcCall("TalkMoreAnswerOne");

        if (asyncStub == null) {
            return new StreamObserver<>() {
                private final List<TalkResult> results = new ArrayList<>();

                @Override
                public void onNext(TalkRequest request) {
                    log.info("Client streaming - data: {}, meta: {}", request.getData(), request.getMeta());
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
                    log.info("Client streaming (forwarding) - data: {}", request.getData());
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

        if (asyncStub == null) {
            return new StreamObserver<>() {
                @Override
                public void onNext(TalkRequest request) {
                    log.info("Bidi streaming - data: {}, meta: {}", request.getData(), request.getMeta());
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
                    log.info("Bidi streaming (forwarding) - data: {}", request.getData());
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
