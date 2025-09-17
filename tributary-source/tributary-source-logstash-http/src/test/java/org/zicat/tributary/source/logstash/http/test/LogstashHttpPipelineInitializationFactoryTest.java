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

package org.zicat.tributary.source.logstash.http.test;

import org.zicat.tributary.common.records.RecordsUtils;
import org.zicat.tributary.source.base.netty.NettySource;
import static org.zicat.tributary.source.http.HttpMessageDecoder.MAPPER;
import static org.zicat.tributary.source.http.test.HttpPipelineInitializationFactoryTest.*;
import org.zicat.tributary.source.logstash.base.LocalFileMessageFilterFactory;
import static org.zicat.tributary.source.logstash.base.LocalFileMessageFilterFactory.OPTION_LOCAL_FILE_PATH;
import static org.zicat.tributary.source.logstash.base.MessageFilterFactoryBuilder.OPTION_MESSAGE_FILTER_FACTORY_ID;
import static org.zicat.tributary.source.logstash.http.LogstashHttpPipelineInitializationFactory.*;

import com.fasterxml.jackson.core.type.TypeReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils;
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;
import org.zicat.tributary.source.base.test.netty.NettySourceMock;
import org.zicat.tributary.source.http.test.HttpPipelineInitializationFactoryTest;
import org.zicat.tributary.source.logstash.http.Codec;
import org.zicat.tributary.source.logstash.http.LogstashHttpPipelineInitializationFactory;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** LogstashHttpPipelineInitializationFactoryTest. */
public class LogstashHttpPipelineInitializationFactoryTest {

    private static final String groupId = "g1";
    private static final String topic = "t1";
    private static final TypeReference<Map<String, Object>> MAP_STRING_TYPE =
            new TypeReference<Map<String, Object>>() {};

    @Test
    public void test() throws Exception {

        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_LOGSTASH_HTTP_WORKER_THREADS, -1);
        config.put(OPTION_LOGSTASH_CODEC, Codec.PLAIN);
        config.put(OPTION_LOGSTASH_HTTP_REMOTE_HOST_TARGET_FIELD, "remote_host");
        config.put(OPTION_LOGSTASH_HTTP_REQUEST_HEADERS_TARGET_FIELD, "request_headers");
        config.put(OPTION_LOGSTASH_HTTP_TAGS, Arrays.asList("t1", "t2"));
        config.put(OPTION_LOGSTASH_HTTP_USER, "user1");
        config.put(OPTION_LOGSTASH_HTTP_PASSWORD, "password1");

        final PipelineInitializationFactory factory =
                SpiFactory.findFactory(
                        LogstashHttpPipelineInitializationFactory.IDENTITY,
                        PipelineInitializationFactory.class);

        try (Channel channel =
                        MemoryChannelTestUtils.memoryChannelFactory(groupId)
                                .createChannel(topic, null);
                NettySource source = new NettySourceMock(config, channel)) {
            final PipelineInitialization pipelineInitialization =
                    factory.createPipelineInitialization(source);
            final EmbeddedChannel serverChannel = new EmbeddedChannel();
            pipelineInitialization.init(serverChannel);
            serverChannel.writeInbound(
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            ByteBufAllocator.DEFAULT
                                    .buffer()
                                    .writeBytes("test".getBytes(StandardCharsets.UTF_8)),
                            EmptyHttpHeaders.INSTANCE,
                            EmptyHttpHeaders.INSTANCE));

            ByteBuf response = serverChannel.readOutbound();
            try (final BufferedReader reader =
                    HttpPipelineInitializationFactoryTest.parse(response)) {
                final String protocol = reader.readLine();
                Assert.assertEquals(HTTP_UNAUTHORIZED, protocol);
            } finally {
                response.release();
            }

            final HttpHeaders headers = new DefaultHttpHeaders();
            headers.add("Authorization", "Basic dXNlcjE6cGFzc3dvcmQx");
            headers.add("my_header1", "1111");
            serverChannel.writeInbound(
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            ByteBufAllocator.DEFAULT
                                    .buffer()
                                    .writeBytes("test".getBytes(StandardCharsets.UTF_8)),
                            headers,
                            EmptyHttpHeaders.INSTANCE));
            response = serverChannel.readOutbound();
            try (final BufferedReader reader =
                    HttpPipelineInitializationFactoryTest.parse(response)) {
                final String protocol = reader.readLine();
                Assert.assertEquals(HTTP_OK_REQUEST, protocol);
                Assert.assertEquals("ok", readBody(reader));
            } finally {
                response.release();
            }

            channel.flush();
            RecordsResultSet resultSet = channel.poll(0, Offset.ZERO, 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());
            final Records records = Records.parse(resultSet.next());
            Assert.assertEquals(1, records.count());
            RecordsUtils.foreachRecord(
                    records,
                    (key, value, allHeaders) -> {
                        Map<String, Object> data = MAPPER.readValue(value, MAP_STRING_TYPE);
                        Assert.assertEquals("test", data.get("message"));
                        Assert.assertEquals(1, ((Map<?, ?>) data.get("request_headers")).size());
                        Assert.assertNotNull(data.get("remote_host"));
                        Assert.assertArrayEquals(
                                new String[] {"t1", "t2"},
                                ((List<?>) data.get("tags")).toArray(new Object[] {}));
                    });
        }
    }

    @Test
    public void test2() throws Exception {

        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_LOGSTASH_HTTP_WORKER_THREADS, -1);
        config.put(OPTION_LOGSTASH_CODEC, Codec.JSON);

        final PipelineInitializationFactory factory =
                SpiFactory.findFactory(
                        LogstashHttpPipelineInitializationFactory.IDENTITY,
                        PipelineInitializationFactory.class);

        try (Channel channel =
                        MemoryChannelTestUtils.memoryChannelFactory(groupId)
                                .createChannel(topic, null);
                NettySource source = new NettySourceMock(config, channel)) {
            final PipelineInitialization pipelineInitialization =
                    factory.createPipelineInitialization(source);
            final EmbeddedChannel serverChannel = new EmbeddedChannel();
            pipelineInitialization.init(serverChannel);

            final HttpHeaders headers = new DefaultHttpHeaders();
            headers.add("my_header1", "1111");
            serverChannel.writeInbound(
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            ByteBufAllocator.DEFAULT
                                    .buffer()
                                    .writeBytes("{\"id\":1}".getBytes(StandardCharsets.UTF_8)),
                            headers,
                            EmptyHttpHeaders.INSTANCE));
            ByteBuf response = serverChannel.readOutbound();
            try (final BufferedReader reader =
                    HttpPipelineInitializationFactoryTest.parse(response)) {
                final String protocol = reader.readLine();
                Assert.assertEquals(HTTP_OK_REQUEST, protocol);
                Assert.assertEquals("ok", readBody(reader));
            } finally {
                response.release();
            }

            channel.flush();
            RecordsResultSet resultSet = channel.poll(0, Offset.ZERO, 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());
            final Records records = Records.parse(resultSet.next());
            Assert.assertEquals(1, records.count());
            RecordsUtils.foreachRecord(
                    records,
                    (key, value, allHeaders) -> {
                        Map<String, Object> data = MAPPER.readValue(value, MAP_STRING_TYPE);
                        Assert.assertEquals(1, data.size());
                        Assert.assertEquals((Integer) 1, data.get("id"));
                    });
        }
    }

    @Test
    public void testContentType() throws Exception {

        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_LOGSTASH_HTTP_WORKER_THREADS, -1);
        config.put(OPTION_LOGSTASH_CODEC, Codec.PLAIN);

        final PipelineInitializationFactory factory =
                SpiFactory.findFactory(
                        LogstashHttpPipelineInitializationFactory.IDENTITY,
                        PipelineInitializationFactory.class);

        try (Channel channel =
                        MemoryChannelTestUtils.memoryChannelFactory(groupId)
                                .createChannel(topic, null);
                NettySource source = new NettySourceMock(config, channel)) {
            final PipelineInitialization pipelineInitialization =
                    factory.createPipelineInitialization(source);
            final EmbeddedChannel serverChannel = new EmbeddedChannel();
            pipelineInitialization.init(serverChannel);

            final HttpHeaders headers = new DefaultHttpHeaders();
            headers.add("my_header1", "1111");
            headers.add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
            serverChannel.writeInbound(
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            ByteBufAllocator.DEFAULT
                                    .buffer()
                                    .writeBytes("{\"id\":1}".getBytes(StandardCharsets.UTF_8)),
                            headers,
                            EmptyHttpHeaders.INSTANCE));
            ByteBuf response = serverChannel.readOutbound();
            try (final BufferedReader reader =
                    HttpPipelineInitializationFactoryTest.parse(response)) {
                final String protocol = reader.readLine();
                Assert.assertEquals(HTTP_OK_REQUEST, protocol);
                Assert.assertEquals("ok", readBody(reader));
            } finally {
                response.release();
            }

            channel.flush();
            RecordsResultSet resultSet = channel.poll(0, Offset.ZERO, 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());
            final Records records = Records.parse(resultSet.next());
            Assert.assertEquals(1, records.count());
            RecordsUtils.foreachRecord(
                    records,
                    (key, value, allHeaders) -> {
                        Map<String, Object> data = MAPPER.readValue(value, MAP_STRING_TYPE);
                        Assert.assertEquals(1, data.size());
                        Assert.assertEquals((Integer) 1, data.get("id"));
                    });
        }
    }

    @Test
    public void testMessageFilter() throws Exception {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_LOGSTASH_HTTP_WORKER_THREADS, -1);
        config.put(OPTION_LOGSTASH_CODEC, Codec.JSON);
        config.put(
                CONFIG_PREFIX + OPTION_MESSAGE_FILTER_FACTORY_ID.key(),
                LocalFileMessageFilterFactory.IDENTITY);
        config.put(CONFIG_PREFIX + OPTION_LOCAL_FILE_PATH.key(), "DefaultMessageFilterTest.txt");

        final PipelineInitializationFactory factory =
                SpiFactory.findFactory(
                        LogstashHttpPipelineInitializationFactory.IDENTITY,
                        PipelineInitializationFactory.class);

        try (Channel channel =
                        MemoryChannelTestUtils.memoryChannelFactory(groupId)
                                .createChannel(topic, null);
                NettySource source = new NettySourceMock(config, channel)) {
            final PipelineInitialization pipelineInitialization =
                    factory.createPipelineInitialization(source);
            final EmbeddedChannel serverChannel = new EmbeddedChannel();
            pipelineInitialization.init(serverChannel);

            final HttpHeaders headers = new DefaultHttpHeaders();
            headers.add("my_header1", "1111");

            serverChannel.writeInbound(
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            ByteBufAllocator.DEFAULT
                                    .buffer()
                                    .writeBytes("{\"id\":1}".getBytes(StandardCharsets.UTF_8)),
                            headers,
                            EmptyHttpHeaders.INSTANCE));
            ByteBuf response = serverChannel.readOutbound();
            try (final BufferedReader reader =
                    HttpPipelineInitializationFactoryTest.parse(response)) {
                final String protocol = reader.readLine();
                Assert.assertEquals(HTTP_OK_REQUEST, protocol);
                Assert.assertEquals("ok", readBody(reader));
            } finally {
                response.release();
            }

            channel.flush();
            RecordsResultSet resultSet = channel.poll(0, Offset.ZERO, 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());
            final Records records = Records.parse(resultSet.next());
            Assert.assertEquals(1, records.count());
            RecordsUtils.foreachRecord(
                    records,
                    (key, value, allHeaders) -> {
                        Map<String, Object> data = MAPPER.readValue(value, MAP_STRING_TYPE);
                        Assert.assertEquals((Integer) 1, data.get("id"));
                        Assert.assertEquals("_test_value", data.get("_test"));
                        Assert.assertEquals(2, data.size());
                    });

            // test filter return false
            serverChannel.writeInbound(
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            ByteBufAllocator.DEFAULT
                                    .buffer()
                                    .writeBytes(
                                            "{\"id\":1, \"_test_test\":123}"
                                                    .getBytes(StandardCharsets.UTF_8)),
                            headers,
                            EmptyHttpHeaders.INSTANCE));
            serverChannel.writeInbound(
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            ByteBufAllocator.DEFAULT
                                    .buffer()
                                    .writeBytes(
                                            "{\"id\":2, \"test_test\":123}"
                                                    .getBytes(StandardCharsets.UTF_8)),
                            headers,
                            EmptyHttpHeaders.INSTANCE));
            ByteBuf response2 = serverChannel.readOutbound();
            try (final BufferedReader reader =
                    HttpPipelineInitializationFactoryTest.parse(response2)) {
                final String protocol = reader.readLine();
                Assert.assertEquals(HTTP_OK_REQUEST, protocol);
                Assert.assertEquals("ok", readBody(reader));
            } finally {
                response2.release();
            }
            channel.flush();
            RecordsResultSet resultSet2 =
                    channel.poll(0, resultSet.nexOffset(), 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet2.isEmpty());
            final Records records2 = Records.parse(resultSet2.next());
            Assert.assertEquals(1, records2.count());
            RecordsUtils.foreachRecord(
                    records2,
                    (key, value, allHeaders) -> {
                        Map<String, Object> data = MAPPER.readValue(value, MAP_STRING_TYPE);
                        Assert.assertEquals((Integer) 2, data.get("id"));
                        Assert.assertEquals("_test_value", data.get("_test"));
                        Assert.assertEquals((Integer) 123, data.get("test_test"));
                        Assert.assertEquals(3, data.size());
                    });
        }
    }
}
