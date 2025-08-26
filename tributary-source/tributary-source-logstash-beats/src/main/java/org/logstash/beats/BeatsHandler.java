package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.source.logstash.base.Message;
import org.zicat.tributary.source.logstash.beats.BatchMessageListener;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** BeatsHandler. */
public class BeatsHandler extends SimpleChannelInboundHandler<Batch> {
    private static final Logger logger = LoggerFactory.getLogger(BeatsHandler.class);
    private static final String executorTerminatedMessage = "event executor terminated";

    private final BatchMessageListener messageListener;
    private final AtomicBoolean isQuietPeriod = new AtomicBoolean(false);

    public BeatsHandler(BatchMessageListener listener) {
        messageListener = listener;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Batch batch)
            throws IOException, InterruptedException {
        logger.debug("Received a new payload");
        try {
            if (isQuietPeriod.get()) {
                logger.debug("Received batch but no executors available, ignoring...");
            } else {
                processBatchAndSendAck(ctx, batch);
            }
        } finally {
            // this channel is done processing this payload, instruct the connection handler to stop
            // sending TCP keep alive
            ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().set(false);
            batch.release();
            ctx.flush();
        }
    }

    /*
     * Do not propagate the SSL handshake exception down to the ruby layer handle it locally instead and close the connection
     * if the channel is still active. Calling `onException` will flush the content of the codec's buffer, this call
     * may block the thread in the event loop until completion, this should only affect LS 5 because it still supports
     * the multiline codec, v6 drop support for buffering codec in the beats input.
     *
     * For v5, I cannot drop the content of the buffer because this will create data loss because multiline content can
     * overlap Filebeat transmission; we were recommending multiline at the source in v5 and in v6 we enforce it.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            if (isNoisyException(cause)) {
                logger.info("closing", cause);
            } else {
                final Throwable realCause = extractCause(cause, 0);
                logger.info("Handling exception: {} (caused by: {})", cause, realCause.toString());
                // when execution tasks rejected, no need to forward the exception to netty channel
                // handlers
                if (cause instanceof RejectedExecutionException) {
                    // we no longer have event executors available since they are terminated, mostly
                    // by shutdown process
                    if (Objects.nonNull(cause.getMessage())
                            && cause.getMessage().contains(executorTerminatedMessage)) {
                        this.isQuietPeriod.compareAndSet(false, true);
                    }
                } else {
                    super.exceptionCaught(ctx, cause);
                }
            }
        } finally {
            ctx.flush();
            ctx.close();
        }
    }

    private void processBatchAndSendAck(ChannelHandlerContext ctx, Batch batch)
            throws IOException, InterruptedException {
        if (batch.isEmpty()) {
            logger.debug("Sending 0-seq ACK for empty batch");
            writeAck(ctx, batch.getProtocol(), 0);
        }
        final Iterator<Message<Object>> iterator = batch.iterator();
        messageListener.consume(
                new Iterator<Message<Object>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Message<Object> next() {
                        final Message<Object> message = iterator.next();
                        logger.debug(
                                "Sending a new message for listener, sequence: {}",
                                message.getSequence());
                        if (needAck(message)) {
                            logger.trace("Ack message number {}", message.getSequence());
                            writeAck(
                                    ctx,
                                    ((Batch) message.getPayload()).getProtocol(),
                                    message.getSequence());
                        }
                        return message;
                    }
                });
    }

    private boolean isNoisyException(final Throwable ex) {
        if (ex instanceof IOException) {
            final String message = ex.getMessage();
            return "Connection reset by peer".equals(message);
        }
        return false;
    }

    private boolean needAck(Message<Object> message) {
        return message.getSequence() == ((Batch) message.getPayload()).getHighestSequence();
    }

    private void writeAck(ChannelHandlerContext ctx, byte protocol, int sequence) {
        ctx.write(new Ack(protocol, sequence));
    }

    private static final int MAX_CAUSE_NESTING = 10;

    private static Throwable extractCause(final Throwable ex, final int nesting) {
        final Throwable cause = ex.getCause();
        if (cause == null || cause == ex) {
            return ex;
        }
        if (nesting >= MAX_CAUSE_NESTING) {
            return cause; // do not recurse infinitely
        }
        return extractCause(cause, nesting + 1);
    }
}
