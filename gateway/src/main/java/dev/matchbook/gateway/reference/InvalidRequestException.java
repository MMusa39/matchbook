package dev.matchbook.gateway.reference;

/** A request that is malformed before any business rule applies (HTTP 400 / gRPC INVALID_ARGUMENT). */
public class InvalidRequestException extends RuntimeException {

    public InvalidRequestException(String message) {
        super(message);
    }
}
