/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.tributary.source.http.test;

import com.github.luben.zstd.Zstd;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import org.xerial.snappy.Snappy;
import org.zicat.tributary.common.config.ReadableConfigBuilder;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.records.Record0;
import static org.zicat.tributary.common.util.IOUtils.compressionGZip;
import static org.zicat.tributary.common.util.IOUtils.decompressionGZip;
import org.zicat.tributary.source.base.netty.NettySource;
import static org.zicat.tributary.source.http.HttpMessageDecoder.*;

import com.fasterxml.jackson.core.JsonParseException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils;
import org.zicat.tributary.channel.test.ChannelBaseTest;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;
import org.zicat.tributary.source.base.test.netty.NettySourceMock;
import org.zicat.tributary.source.http.HttpPipelineInitializationFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** HttpPipelineInitializationFactoryTest. */
public class HttpPipelineInitializationFactoryTest {

    public static final String HTTP_UNAUTHORIZED = "HTTP/1.1 401 Unauthorized";
    public static final String HTTP_NOT_FOUNT_REQUEST = "HTTP/1.1 404 Not Found";
    public static final String HTTP_OK_REQUEST = "HTTP/1.1 200 OK";
    public static final String PATH = "/my/path";
    public static final String GROUP_ID = "g1";
    public static final String TOPIC = "t1";

    @Test
    public void test() throws Exception {
        final PipelineInitializationFactory factory =
                SpiFactory.findFactory(
                        HttpPipelineInitializationFactory.IDENTITY,
                        PipelineInitializationFactory.class);
        final ReadableConfig config =
                new ReadableConfigBuilder()
                        .addConfig(HttpPipelineInitializationFactory.OPTIONS_PATH, PATH)
                        .addConfig(HttpPipelineInitializationFactory.OPTION_HTTP_WORKER_THREADS, -1)
                        .build();

        try (Channel channel =
                        MemoryChannelTestUtils.memoryChannelFactory(GROUP_ID)
                                .createChannel(TOPIC, null);
                NettySource source = new NettySourceMock(config, channel)) {
            final PipelineInitialization pipelineInitialization =
                    factory.createPipelineInitialization(source);
            assertNotPostRequest(pipelineInitialization);
            assertPathNotMatch(pipelineInitialization);
            assertErrorContentType(pipelineInitialization);
            assertTopicNotFound(pipelineInitialization);
            assertErrorJson(pipelineInitialization);
            assertOkRequest(pipelineInitialization, channel);
        }

        try (Channel channel =
                        MemoryChannelTestUtils.memoryChannelFactory(GROUP_ID)
                                .createChannel(TOPIC, null);
                NettySource source = new NettySourceMock(config, channel)) {
            final PipelineInitialization pipelineInitialization =
                    factory.createPipelineInitialization(source);
            assertContentEncodingRequest(
                    pipelineInitialization,
                    channel,
                    new Compression() {
                        @Override
                        public byte[] compression(byte[] data) throws IOException {
                            return compressionGZip(data);
                        }

                        @Override
                        public byte[] decompression(byte[] data) throws IOException {
                            return decompressionGZip(data);
                        }
                    },
                    "gzip");

            assertContentEncodingRequest(
                    pipelineInitialization,
                    channel,
                    new Compression() {
                        @Override
                        public byte[] compression(byte[] data) throws IOException {
                            return Snappy.compress(data);
                        }

                        @Override
                        public byte[] decompression(byte[] data) throws IOException {
                            return Snappy.uncompress(data);
                        }
                    },
                    "snappy");

            assertContentEncodingRequest(
                    pipelineInitialization,
                    channel,
                    new Compression() {
                        @Override
                        public byte[] compression(byte[] data) {
                            return Zstd.compress(data);
                        }

                        @Override
                        public byte[] decompression(byte[] data) {
                            return Zstd.decompress(data, (int) Zstd.decompressedSize(data));
                        }
                    },
                    "zstd");

            assertContentEncodingRequest(
                    pipelineInitialization,
                    channel,
                    new Compression() {
                        @Override
                        public byte[] compression(byte[] data) {
                            return Zstd.compress(data);
                        }

                        @Override
                        public byte[] decompression(byte[] data) {
                            return Zstd.decompress(data, (int) Zstd.decompressedSize(data));
                        }
                    },
                    "zstd");

            assertContentEncodingRequest(
                    pipelineInitialization,
                    channel,
                    new Compression() {
                        @Override
                        public byte[] compression(byte[] data) {
                            return data;
                        }

                        @Override
                        public byte[] decompression(byte[] data) {
                            return data;
                        }
                    },
                    null);
        }
    }

    private void assertContentEncodingRequest(
            PipelineInitialization pipelineInitialization,
            Channel channel,
            Compression compression,
            String contentEncoding)
            throws Exception {

        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        pipelineInitialization.init(embeddedChannel);
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add("Content-Type", "application/json; charset=UTF-8");
        if (contentEncoding != null) {
            headers.add("Content-Encoding", contentEncoding);
        }

        final byte[] body =
                compression.compression(
                        "[{\"key\":\"key2\",\"value\":\"value2\",\"headers\":{\"header2\":\"value2\",\"header22\":\"value22\"}}]"
                                .getBytes(StandardCharsets.UTF_8));
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.POST,
                        PATH + "?topic=" + TOPIC,
                        Unpooled.buffer(body.length).writeBytes(body),
                        headers,
                        headers));
        final ByteBuf response2 = embeddedChannel.readOutbound();
        try (final BufferedReader reader = parse(response2)) {
            final String protocol = reader.readLine();
            Assert.assertEquals(HTTP_OK_REQUEST, protocol);
        } finally {
            response2.release();
        }
        channel.flush();

        final Offset offset = Offset.ZERO;
        final List<byte[]> data = ChannelBaseTest.readChannel(channel, 0, offset, 1).data;

        final Records record = Records.parse(data.get(0));
        Assert.assertEquals(TOPIC, record.topic());
        Assert.assertEquals(1, record.count());
        final Record0 record2 = record.iterator().next();
        Assert.assertEquals("key2", new String(record2.key(), StandardCharsets.UTF_8));
        Assert.assertEquals("value2", new String(record2.value(), StandardCharsets.UTF_8));
        Assert.assertEquals(2, record2.headers().size());
        Assert.assertEquals(
                "value2", new String(record2.headers().get("header2"), StandardCharsets.UTF_8));
        Assert.assertEquals(
                "value22", new String(record2.headers().get("header22"), StandardCharsets.UTF_8));
    }

    private void assertOkRequest(PipelineInitialization pipelineInitialization, Channel channel)
            throws Exception {
        final byte[] body1 =
                "[{\"key\":\"key1\",\"value\":\"value1\",\"headers\":{\"header1\":\"value1\",\"header11\":\"value11\"}}]"
                        .getBytes(StandardCharsets.UTF_8);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        pipelineInitialization.init(embeddedChannel);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.POST,
                        PATH + "?topic=" + TOPIC,
                        Unpooled.buffer(body1.length).writeBytes(body1),
                        getHeaders(),
                        getHeaders()));
        final ByteBuf response = embeddedChannel.readOutbound();
        try (final BufferedReader reader = parse(response)) {
            final String protocol = reader.readLine();
            Assert.assertEquals(HTTP_OK_REQUEST, protocol);
        } finally {
            response.release();
        }

        final byte[] body2 =
                "[{\"key\":\"key2\",\"value\":\"value2\",\"headers\":{\"header2\":\"value2\",\"header22\":\"value22\"}}]"
                        .getBytes(StandardCharsets.UTF_8);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.POST,
                        PATH + "?topic=" + TOPIC,
                        Unpooled.buffer(body2.length).writeBytes(body2),
                        getHeaders(),
                        getHeaders()));
        final ByteBuf response2 = embeddedChannel.readOutbound();
        try (final BufferedReader reader = parse(response2)) {
            final String protocol = reader.readLine();
            Assert.assertEquals(HTTP_OK_REQUEST, protocol);
        } finally {
            response2.release();
        }
        channel.flush();

        final Offset offset = Offset.ZERO;
        final List<byte[]> data = ChannelBaseTest.readChannel(channel, 0, offset, 2).data;

        final Records records1 = Records.parse(data.get(0));
        Assert.assertEquals(TOPIC, records1.topic());
        Assert.assertEquals(1, records1.count());
        final Record0 record1 = records1.iterator().next();
        Assert.assertEquals("key1", new String(record1.key(), StandardCharsets.UTF_8));
        Assert.assertEquals("value1", new String(record1.value(), StandardCharsets.UTF_8));
        Assert.assertEquals(2, record1.headers().size());
        Assert.assertEquals(
                "value1", new String(record1.headers().get("header1"), StandardCharsets.UTF_8));
        Assert.assertEquals(
                "value11", new String(record1.headers().get("header11"), StandardCharsets.UTF_8));

        final Records records2 = Records.parse(data.get(1));
        Assert.assertEquals(TOPIC, records2.topic());
        Assert.assertEquals(1, records2.count());
        final Record0 record2 = records2.iterator().next();
        Assert.assertEquals("key2", new String(record2.key(), StandardCharsets.UTF_8));
        Assert.assertEquals("value2", new String(record2.value(), StandardCharsets.UTF_8));
        Assert.assertEquals(2, record2.headers().size());
        Assert.assertEquals(
                "value2", new String(record2.headers().get("header2"), StandardCharsets.UTF_8));
        Assert.assertEquals(
                "value22", new String(record2.headers().get("header22"), StandardCharsets.UTF_8));
    }

    private void assertErrorJson(PipelineInitialization pipelineInitialization) throws Exception {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        pipelineInitialization.init(embeddedChannel);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.POST,
                        PATH + "?topic=aa",
                        Unpooled.buffer(1026).writeBytes(new byte[12060]),
                        getHeaders(),
                        getHeaders()));
        final ByteBuf response = embeddedChannel.readOutbound();
        try (final BufferedReader reader = parse(response)) {
            final String protocol = reader.readLine();
            Assert.assertTrue(protocol.contains(INTERNAL_SERVER_ERROR.toString()));
            final String body = readBody(reader);
            Assert.assertEquals(JsonParseException.class.getName(), body);
        } finally {
            response.release();
        }
    }

    private void assertTopicNotFound(PipelineInitialization pipelineInitialization)
            throws Exception {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        pipelineInitialization.init(embeddedChannel);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.POST,
                        PATH,
                        Unpooled.buffer(0),
                        getHeaders(),
                        getHeaders()));
        final ByteBuf response = embeddedChannel.readOutbound();
        try (final BufferedReader reader = parse(response)) {
            final String protocol = reader.readLine();
            Assert.assertTrue(protocol.contains(BAD_REQUEST.toString()));
            Assert.assertEquals(RESPONSE_BAD_TOPIC_NOT_IN_PARAMS, readBody(reader));
        } finally {
            response.release();
        }
    }

    private void assertErrorContentType(PipelineInitialization pipelineInitialization)
            throws Exception {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        pipelineInitialization.init(embeddedChannel);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, PATH));
        final ByteBuf response = embeddedChannel.readOutbound();
        try (final BufferedReader reader = parse(response)) {
            final String protocol = reader.readLine();
            Assert.assertTrue(protocol.contains(BAD_REQUEST.toString()));
            Assert.assertEquals(RESPONSE_BAD_CONTENT_TYPE, readBody(reader));

        } finally {
            response.release();
        }
    }

    private void assertPathNotMatch(PipelineInitialization pipelineInitialization)
            throws Exception {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        pipelineInitialization.init(embeddedChannel);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, PATH + "aa"));
        final ByteBuf response = embeddedChannel.readOutbound();
        try (final BufferedReader reader = parse(response)) {
            final String protocol = reader.readLine();
            Assert.assertEquals(HTTP_NOT_FOUNT_REQUEST, protocol);
            Assert.assertEquals(PATH + "aa not found", readBody(reader));
        } finally {
            response.release();
        }
    }

    private void assertNotPostRequest(PipelineInitialization pipelineInitialization)
            throws Exception {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        pipelineInitialization.init(embeddedChannel);
        embeddedChannel.writeInbound(
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, PATH));
        final ByteBuf response = embeddedChannel.readOutbound();
        try (final BufferedReader reader = parse(response)) {
            final String protocol = reader.readLine();
            Assert.assertTrue(protocol.contains(BAD_REQUEST.toString()));
            Assert.assertEquals(RESPONSE_BAD_METHOD, readBody(reader));
        } finally {
            response.release();
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public static String readBody(BufferedReader reader) throws IOException {
        while (!reader.readLine().isEmpty()) {}
        final StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line);
            builder.append(System.lineSeparator());
        }
        return builder.toString().trim();
    }

    public static BufferedReader parse(ByteBuf byteBuf) {
        final int size = byteBuf.readableBytes();
        final byte[] bytes = new byte[size];
        byteBuf.readBytes(bytes).discardReadBytes();
        return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
    }

    public static HttpHeaders getHeaders() {
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add("Content-Type", "application/json; charset=UTF-8");
        return headers;
    }

    /** Compression. */
    private interface Compression {

        byte[] compression(byte[] data) throws IOException;

        byte[] decompression(byte[] data) throws IOException;
    }
}
