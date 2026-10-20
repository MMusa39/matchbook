package dev.matchbook.gateway.grpc;

import dev.matchbook.gateway.api.ApiKeyAuthenticator;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.stereotype.Component;

import java.util.OptionalLong;

/** Authenticates gRPC calls by the {@code x-api-key} metadata header and puts the client id in the context. */
@Component
@GlobalServerInterceptor
public class ApiKeyServerInterceptor implements ServerInterceptor {

    public static final Context.Key<Long> CLIENT_ID = Context.key("matchbook.clientId");
    private static final Metadata.Key<String> API_KEY =
            Metadata.Key.of(ApiKeyAuthenticator.HEADER, Metadata.ASCII_STRING_MARSHALLER);

    private final ApiKeyAuthenticator authenticator;

    public ApiKeyServerInterceptor(ApiKeyAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                                                       ServerCallHandler<Q, R> next) {
        if (!call.getMethodDescriptor().getServiceName().equals("matchbook.v1.OrderEntry")) {
            return next.startCall(call, headers); // health, reflection
        }
        OptionalLong client = authenticator.authenticate(headers.get(API_KEY));
        if (client.isEmpty()) {
            call.close(Status.UNAUTHENTICATED.withDescription("missing or invalid x-api-key"), new Metadata());
            return new ServerCall.Listener<>() {
            };
        }
        return Contexts.interceptCall(Context.current().withValue(CLIENT_ID, client.getAsLong()), call, headers, next);
    }
}
