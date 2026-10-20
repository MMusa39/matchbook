package dev.matchbook.gateway.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.OptionalLong;

/** Authenticates REST calls by the {@code X-API-Key} header and stores the client id on the request. */
@Component
public class ApiKeyInterceptor implements HandlerInterceptor {

    public static final String CLIENT_ID = "matchbook.clientId";

    private final ApiKeyAuthenticator authenticator;

    public ApiKeyInterceptor(ApiKeyAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        OptionalLong client = authenticator.authenticate(request.getHeader(ApiKeyAuthenticator.HEADER));
        if (client.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType("application/problem+json");
            response.getWriter().write("{\"title\":\"Unauthorized\",\"status\":401,\"detail\":\"missing or invalid X-API-Key\"}");
            return false;
        }
        request.setAttribute(CLIENT_ID, client.getAsLong());
        return true;
    }
}
