package org.janus.grpc;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.Constants;

public class HeaderServerInterceptor implements ServerInterceptor {
    private static final Logger log = LoggerFactory.getLogger("HeaderServerInterceptor");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            final Metadata requestHeaders,
            ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Context current = Context.current();
        // Only the first CONTEXT_KEYS.size() tracing keys have a matching context
        // key; iterating over TRACING_KEYS.size() (7) would index CONTEXT_KEYS (3)
        // out of bounds when a b3 header is present.
        for (int i = 0; i < Constants.CONTEXT_KEYS.size(); i++) {
            Metadata.Key<String> tracingKey = Constants.TRACING_KEYS.get(i);
            String metadata = requestHeaders.get(tracingKey);
            if (metadata != null) {
                Context.Key<String> key = Constants.CONTEXT_KEYS.get(i);
                log.debug("->T {}:{}", key, metadata);
                current = current.withValue(key, metadata);
            }
        }
        return Contexts.interceptCall(current, call, requestHeaders, serverCallHandler);
    }
}
