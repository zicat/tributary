package org.logstash.beats;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * This Class is mostly used in the test suite to make the right assertions with the encoded data
 * frame. This class support creating v1 or v2 lumberjack frames.
 */
public class BatchEncoder extends MessageToByteEncoder<Batch> {

    public static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new AfterburnerModule());
    private static final TypeReference<Map<String, String>> MAPPER_TYPE_REF =
            new TypeReference<Map<String, String>>() {};

    private static final Logger logger = LoggerFactory.getLogger(BatchEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, Batch batch, ByteBuf out) throws Exception {
        out.writeByte(batch.getProtocol());
        out.writeByte('W');
        out.writeInt(batch.size());

        ByteBuf buffer = getPayload(ctx, batch);

        try {
            out.writeBytes(buffer);
        } finally {
            buffer.release();
        }
    }

    protected ByteBuf getPayload(ChannelHandlerContext ctx, Batch batch) throws IOException {
        ByteBuf payload = ctx.alloc().buffer();

        // Aggregates the payload that we could decide to compress or not.
        for (Message message : batch) {
            if (batch.getProtocol() == Protocol.VERSION_2) {
                encodeMessageWithJson(payload, message);
            } else {
                encodeMessageWithFields(payload, message);
            }
        }
        return payload;
    }

    public static void encodeMessageWithJson(ByteBuf payload, Message message) {
        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('J');
        payload.writeInt(message.getSequence());
        byte[] bs = message.getData();
        payload.writeInt(bs.length);
        payload.writeBytes(bs);
    }

    public static void encodeMessageWithFields(ByteBuf payload, Message message)
            throws IOException {
        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte('D');
        payload.writeInt(message.getSequence());
        final byte[] bytes = message.getData();
        final Map<String, String> data = MAPPER.readValue(bytes, MAPPER_TYPE_REF);
        payload.writeInt(data.size());
        for (Map.Entry<String, String> e : data.entrySet()) {
            final byte[] key = e.getKey().getBytes(StandardCharsets.UTF_8);
            final byte[] value = e.getValue().getBytes(StandardCharsets.UTF_8);
            logger.debug("New entry: key: {}, value: {}", key, value);
            payload.writeInt(key.length);
            payload.writeBytes(key);
            payload.writeInt(value.length);
            payload.writeBytes(value);
        }
    }
}
