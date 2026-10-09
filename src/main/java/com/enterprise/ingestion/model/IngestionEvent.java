package com.enterprise.ingestion.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable Ingestion Event.
 *
 * Implemented as a Java 21 Record for:
 * 1. Compact memory footprint (no getter/setter bytecode bloat).
 * 2. Thread-safety through immutability.
 * 3. JIT scalar replacement enablement via escape analysis.
 */
public record IngestionEvent(
        String eventId,
        String tenantId,
        String eventType,
        long payloadSizeInBytes,
        Instant timestamp
) {
    public IngestionEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        if (payloadSizeInBytes < 0) {
            throw new IllegalArgumentException("payloadSizeInBytes cannot be negative");
        }
    }
}
