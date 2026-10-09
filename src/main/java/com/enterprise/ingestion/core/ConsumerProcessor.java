package com.enterprise.ingestion.core;

/**
 * Functional callback interface for processing ingested items.
 *
 * @param <T> Item payload type
 */
@FunctionalInterface
public interface ConsumerProcessor<T> {
    /**
     * Processes an individual item consumed from the bounded queue.
     *
     * @param item The item to process
     * @throws Exception If an unrecoverable processing error occurs
     */
    void process(T item) throws Exception;
}
