package dev.matchbook.engine.pipeline;

import java.util.concurrent.CompletableFuture;

/** A request, carried through the inbound ring, to capture engine state at that point in the stream. */
final class SnapshotRequest {

    final boolean persist;
    final CompletableFuture<Snapshot> result = new CompletableFuture<>();

    SnapshotRequest(boolean persist) {
        this.persist = persist;
    }
}
