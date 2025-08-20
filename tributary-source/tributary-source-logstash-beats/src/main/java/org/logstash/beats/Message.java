package org.logstash.beats;

import io.netty.buffer.ByteBuf;

import java.util.Map;

/** Message. */
public class Message implements Comparable<Message> {

    private final int sequence;
    private byte[] bytes;
    private Batch batch;
    private ByteBuf buffer;

    /**
     * Create a message using a map of key, value pairs.
     *
     * @param sequence sequence number of the message
     * @param bytes key/value pairs representing the message
     */
    public Message(int sequence, byte[] bytes) {
        this.sequence = sequence;
        this.bytes = bytes;
    }

    /**
     * Create a message using a ByteBuf holding a Json object. Note that this ctr is *lazy* - it
     * will not deserialize the Json object until it is needed.
     *
     * @param sequence sequence number of the message
     * @param buffer {@link ByteBuf} buffer containing Json object
     */
    public Message(int sequence, ByteBuf buffer) {
        this.sequence = sequence;
        this.buffer = buffer;
    }

    /**
     * Returns the sequence number of this message.
     *
     * @return sequence number of the message
     */
    public int getSequence() {
        return sequence;
    }

    /**
     * Returns a list of key/value pairs representing the contents of the message. Note that this
     * method is lazy if the Message was created using a {@link ByteBuf}.
     *
     * @return {@link Map} Map of key/value pairs
     */
    public byte[] getData() {
        if (bytes == null && buffer != null) {
            this.bytes = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), bytes);
            buffer = null;
        }
        return bytes;
    }

    @Override
    public int compareTo(Message o) {
        return Integer.compare(getSequence(), o.getSequence());
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }
}
