package org.janus.grpc;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.janus.Constants;
import org.janus.proto.TalkRequest;
import org.janus.proto.TalkResponse;
import org.janus.proto.JanusServiceGrpc;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression test for the header interceptors. Before the fix, both interceptors
 * looped up to TRACING_KEYS.size() (7) while indexing CONTEXT_KEYS (3), throwing
 * IndexOutOfBoundsException on every gRPC call. This exercises a full round trip
 * through both interceptors and asserts the trace context propagates.
 */
class HeaderInterceptorPropagationTest {

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
    }

    @Test
    void interceptorsPropagateTraceContextWithoutThrowing() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        AtomicReference<String> observedTraceId = new AtomicReference<>();

        JanusServiceGrpc.JanusServiceImplBase svc =
                new JanusServiceGrpc.JanusServiceImplBase() {
                    @Override
                    public void talk(TalkRequest request, StreamObserver<TalkResponse> obs) {
                        observedTraceId.set(Constants.CTX_TRACE_ID.get());
                        obs.onNext(TalkResponse.newBuilder().setStatus(200).build());
                        obs.onCompleted();
                    }
                };

        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(ServerInterceptors.intercept(svc, new HeaderServerInterceptor()))
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        Channel intercepted = ClientInterceptors.intercept(channel, new HeaderClientInterceptor());
        JanusServiceGrpc.JanusServiceBlockingStub stub =
                JanusServiceGrpc.newBlockingStub(intercepted);

        Context ctx = Context.current().withValue(Constants.CTX_TRACE_ID, "trace-abc");
        TalkResponse resp = ctx.call(() -> stub.talk(TalkRequest.newBuilder().setData("0").build()));

        assertEquals(200, resp.getStatus());
        assertEquals("trace-abc", observedTraceId.get(),
                "x-trace-id should propagate client->server via the interceptors");
    }
}
