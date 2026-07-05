package org.janus.grpc;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.Constants;

public class HeaderClientInterceptor implements ClientInterceptor {
    private static final Logger log = LoggerFactory.getLogger("HeaderClientInterceptor");

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(
                next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                // Only the first CONTEXT_KEYS.size() tracing keys have a matching
                // context key; iterating over TRACING_KEYS.size() (7) would index
                // CONTEXT_KEYS (3) out of bounds and abort every gRPC call.
                for (int i = 0; i < Constants.CONTEXT_KEYS.size(); i++) {
                    Context.Key<String> k = Constants.CONTEXT_KEYS.get(i);
                    if (k != null) {
                        String metadata = k.get();
                        if (metadata != null) {
                            Metadata.Key<String> key = Constants.TRACING_KEYS.get(i);
                            log.info("<-T {}:{}", key, metadata);
                            headers.put(key, metadata);
                        }
                    }
                }
                super.start(
                        new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
                                responseListener) {
                            @Override
                            public void onHeaders(Metadata headers) {
                                log.info("<-H {}", headers);
                                super.onHeaders(headers);
                            }
                        },
                        headers);
            }
        };
    }
}
