package org.logstash.beats;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.zicat.tributary.source.logstash.base.Message;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/** BeatsParserTest. */
@SuppressWarnings({"deprecation"})
public class BeatsParserTest {

    private V1Batch v1Batch;
    public static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new AfterburnerModule());

    private final int numberOfMessage = 20;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        this.v1Batch = new V1Batch();

        for (int i = 1; i <= numberOfMessage; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("line", "Another world");
            map.put("from", "Little big Adventure");

            Message<Object> message = new Message<>(v1Batch, i, map);
            this.v1Batch.addMessage(message);
        }

        V2Batch byteBufBatch = new V2Batch();

        for (int i = 1; i <= numberOfMessage; i++) {
            Map<String, String> map = new HashMap<>();
            map.put("line", "Another world");
            map.put("from", "Little big Adventure");
            ByteBuf bytebuf = Unpooled.wrappedBuffer(MAPPER.writeValueAsBytes(map));
            byteBufBatch.addMessage(i, bytebuf, bytebuf.readableBytes());
        }
    }

    @Test
    public void testEncodingDecodingJson() throws IOException {
        Batch decodedBatch = decodeBatch(v1Batch);
        assertMessages(v1Batch, decodedBatch);
    }

    @Test
    public void testCompressedEncodingDecodingJson() throws IOException {
        Batch decodedBatch = decodeCompressedBatch(v1Batch);
        assertMessages(v1Batch, decodedBatch);
    }

    @Test
    public void testEncodingDecodingFields() throws IOException {
        Batch decodedBatch = decodeBatch(v1Batch);
        assertMessages(v1Batch, decodedBatch);
    }

    @Test
    public void testEncodingDecodingFieldWithUTFCharacters() throws Exception {
        V2Batch v2Batch = new V2Batch();

        // Generate Data with Keys and String with UTF-8
        for (int i = 0; i < numberOfMessage; i++) {
            ByteBuf payload = Unpooled.buffer();

            Map<String, String> map = new HashMap<>();
            map.put("étoile", "mystère");
            map.put("from", "ÉeèAççï");

            byte[] json = MAPPER.writeValueAsBytes(map);
            payload.writeBytes(json);

            v2Batch.addMessage(i, payload, payload.readableBytes());
        }

        Batch decodedBatch = decodeBatch(v2Batch);
        assertMessages(v2Batch, decodedBatch);
    }

    @Test
    public void testV1EncodingDecodingFieldWithUTFCharacters() throws IOException {
        V1Batch batch = new V1Batch();

        // Generate Data with Keys and String with UTF-8
        for (int i = 0; i < numberOfMessage; i++) {

            Map<String, Object> map = new HashMap<>();
            map.put("étoile", "mystère");
            map.put("from", "ÉeèAççï");
            Message<Object> message = new Message<>(batch, i + 1, map);
            batch.addMessage(message);
        }

        Batch decodedBatch = decodeBatch(batch);
        assertMessages(batch, decodedBatch);
    }

    @Test
    public void testCompressedEncodingDecodingFields() throws IOException {
        Batch decodedBatch = decodeCompressedBatch(v1Batch);
        assertMessages(this.v1Batch, decodedBatch);
    }

    @Test
    public void testShouldNotCrashOnGarbageData() {
        thrown.expectCause(isA(InvalidFrameProtocolException.class));

        byte[] n = new byte[10000];
        new Random().nextBytes(n);
        ByteBuf randomBufferData = Unpooled.wrappedBuffer(n);

        sendPayloadToParser(randomBufferData);
    }

    @Test
    public void testV1EmptyWindowEmitsEmptyBatch() {
        Batch decodedBatch = decodeBatch(new V1Batch());

        assertNotNull(decodedBatch);
        assertTrue(decodedBatch.isEmpty());
        assertEquals(0, decodedBatch.getBatchSize());
        assertEquals(0, decodedBatch.size());
    }

    @Test
    public void testV2EmptyWindowEmitsEmptyBatch() {
        Batch decodedBatch = decodeBatch(new V2Batch());

        assertNotNull(decodedBatch);
        assertTrue(decodedBatch.isEmpty());
        assertEquals(0, decodedBatch.getBatchSize());
        assertEquals(0, decodedBatch.size());
    }

    @Test
    public void testV1CompressedFrameEmptyWindowEmitsEmptyBatch() {
        Batch decodedBatch = decodeCompressedBatch(new V1Batch());

        assertNotNull(decodedBatch);
        assertTrue(decodedBatch.isEmpty());
        assertEquals(0, decodedBatch.getBatchSize());
        assertEquals(0, decodedBatch.size());
    }

    @Test
    public void testV2CompressedFrameEmptyWindowEmitsEmptyBatch() {
        Batch decodedBatch = decodeCompressedBatch(new V2Batch());

        assertNotNull(decodedBatch);
        assertTrue(decodedBatch.isEmpty());
        assertEquals(0, decodedBatch.getBatchSize());
        assertEquals(0, decodedBatch.size());
    }

    @Test
    public void testNegativeJsonPayloadShouldRaiseAnException() throws JsonProcessingException {
        sendInvalidJSonPayload(-1);
    }

    @Test
    public void testZeroSizeJsonPayloadShouldRaiseAnException() throws JsonProcessingException {
        sendInvalidJSonPayload(0);
    }

    @Test
    public void testInvalidCompression() throws JsonProcessingException {
        thrown.expectCause(isA(InvalidFrameProtocolException.class));
        thrown.expectMessage("Insufficient bytes in compressed content");
        ByteBuf payload = Unpooled.buffer();

        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('W');
        payload.writeInt(1);
        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('C');
        payload.writeInt(9);

        int[] next = {
            0x78, 0x9c, 0x33, 0xf2, 0x62, 0x60, 0x60, 0x60, 0x04, 0x62, 0x66, 0x17, 0xff, 0x60,
            0x00, 0x07, 0xe0, 0x01, 0x67
        };
        for (int n : next) {
            payload.writeByte(n);
        }
        sendPayloadToParser(payload);
    }

    @Test
    public void testNegativeFieldsCountShouldRaiseAnException() {
        sendInvalidV1Payload(-1);
    }

    @Test
    public void testZeroFieldsCountShouldRaiseAnException() {
        sendInvalidV1Payload(0);
    }

    private void sendInvalidV1Payload(int size) {
        thrown.expectCause(isA(InvalidFrameProtocolException.class));
        thrown.expectMessage("Invalid number of fields, received: " + size);

        ByteBuf payload = Unpooled.buffer();

        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte('W');
        payload.writeInt(1);
        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte('D');
        payload.writeInt(1);
        payload.writeInt(size);

        byte[] key = "message".getBytes();
        byte[] value = "Hola".getBytes();

        payload.writeInt(key.length);
        payload.writeBytes(key);
        payload.writeInt(value.length);
        payload.writeBytes(value);

        sendPayloadToParser(payload);
    }

    private void sendInvalidJSonPayload(int size) throws JsonProcessingException {
        thrown.expectCause(isA(InvalidFrameProtocolException.class));
        thrown.expectMessage("Invalid json length, received: " + size);

        Map<String, String> mapData = Collections.singletonMap("message", "hola");

        ByteBuf payload = Unpooled.buffer();

        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('W');
        payload.writeInt(1);
        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('J');
        payload.writeInt(1);
        payload.writeInt(size);

        byte[] json = MAPPER.writeValueAsBytes(mapData);
        payload.writeBytes(json);

        sendPayloadToParser(payload);
    }

    private void sendPayloadToParser(ByteBuf payload) {
        EmbeddedChannel channel = new EmbeddedChannel(new BeatsParser());
        channel.writeOutbound(payload);
        Object o = channel.readOutbound();
        channel.writeInbound(o);
    }

    private void assertMessages(Batch expected, Batch actual) {

        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());

        Iterator<Message<Object>> expectedMessages = expected.iterator();
        for (Message<Object> actualMessage : actual) {
            Message<Object> expectedMessage = expectedMessages.next();
            assertEquals(expectedMessage.getSequence(), actualMessage.getSequence());

            Map<String, Object> expectedData = expectedMessage.getData();
            Map<String, Object> actualData = actualMessage.getData();

            assertEquals(expectedData.size(), actualData.size());

            for (Map.Entry<String, Object> e : expectedData.entrySet()) {
                String key = e.getKey();
                Object value = e.getValue();
                assertEquals(value, actualData.get(key));
            }
        }
    }

    private Batch decodeCompressedBatch(Batch batch) {
        EmbeddedChannel channel =
                new EmbeddedChannel(new CompressedBatchEncoder(), new BeatsParser());
        channel.writeOutbound(batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        return channel.readInbound();
    }

    private Batch decodeBatch(Batch batch) {
        EmbeddedChannel channel = new EmbeddedChannel(new BatchEncoder(), new BeatsParser());
        channel.writeOutbound(batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        return channel.readInbound();
    }
}
