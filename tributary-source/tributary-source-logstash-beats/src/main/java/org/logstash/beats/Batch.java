package org.logstash.beats;

import org.zicat.tributary.source.logstash.base.Message;

/** Interface representing a Batch of {@link Message}. */
public interface Batch extends Iterable<Message<Object>> {
    /**
     * Returns the protocol of the send messages that this batch was constructed from.
     *
     * @return byte - either '1' or '2'
     */
    byte getProtocol();

    /**
     * Number of messages that the batch is expected to contain.
     *
     * @return int - number of messages
     */
    int getBatchSize();

    /**
     * Set the number of messages that the batch is expected to contain.
     *
     * @param batchSize int - number of messages
     */
    void setBatchSize(int batchSize);

    /**
     * Returns the highest sequence number of the batch.
     *
     * @return int - highest sequence number
     */
    int getHighestSequence();

    /**
     * Current number of messages in the batch.
     *
     * @return int
     */
    int size();

    /**
     * Is the batch currently empty.
     *
     * @return boolean
     */
    boolean isEmpty();

    /**
     * Is the batch complete.
     *
     * @return boolean
     */
    boolean isComplete();

    /**
     * Release the resources associated with the batch. Consumers of the batch *must* release after
     * use.
     */
    void release();
}
