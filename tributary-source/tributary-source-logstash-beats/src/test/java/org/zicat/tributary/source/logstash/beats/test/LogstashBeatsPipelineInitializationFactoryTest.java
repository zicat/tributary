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

package org.zicat.tributary.source.logstash.beats.test;

import static org.zicat.tributary.source.base.netty.ssl.PemSslContextBuilder.SUPPORTED_CIPHERS;
import static org.zicat.tributary.source.base.netty.ssl.PemSslContextBuilder.SUPPORT_PROTOCOL;
import static org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils.memoryChannelFactory;
import static org.zicat.tributary.common.ResourceUtils.getResourcePath;
import org.zicat.tributary.source.base.netty.NettySource;
import org.zicat.tributary.source.logstash.base.LocalFileMessageFilterFactory;
import static org.zicat.tributary.source.logstash.base.LocalFileMessageFilterFactory.OPTION_LOCAL_FILE_PATH;
import static org.zicat.tributary.source.logstash.base.MessageFilterFactoryBuilder.OPTION_MESSAGE_FILTER_FACTORY_ID;
import static org.zicat.tributary.source.logstash.beats.LogstashBeatsPipelineInitializationFactory.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

import org.junit.Assert;
import org.junit.Test;
import org.logstash.beats.*;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.common.records.RecordsUtils;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;
import org.zicat.tributary.source.base.test.netty.NettySourceMock;
import org.zicat.tributary.source.logstash.beats.LogstashBeatsPipelineInitialization;
import org.zicat.tributary.source.logstash.beats.LogstashBeatsPipelineInitializationFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** LogstashBeatsPipelineInitializationFactoryTest. */
public class LogstashBeatsPipelineInitializationFactoryTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new AfterburnerModule());
    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, String>>() {};

    private LogstashBeatsPipelineInitialization beatsPipelineInitialization(
            NettySource source) throws Exception {
        return (LogstashBeatsPipelineInitialization)
                SpiFactory.findFactory(
                                LogstashBeatsPipelineInitializationFactory.IDENTITY,
                                PipelineInitializationFactory.class)
                        .createPipelineInitialization(source);
    }

    @Test
    public void testSsl() throws Exception {
        final String topic = "t1";
        final String caCertPath = "ca.crt";
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_LOGSTASH_BEATS_WORKER_THREADS, -1);
        config.put(OPTION_LOGSTASH_BEATS_SSL, true);
        config.put(OPTION_LOGSTASH_BEATS_SSL_CERTIFICATE, "server.crt");
        config.put(OPTION_LOGSTASH_BEATS_SSL_KEY, "server.key");
        config.put(OPTION_LOGSTASH_BEATS_SSL_CERTIFICATE_AUTHORITIES, caCertPath);

        try (final Channel channel = memoryChannelFactory("g1").createChannel(topic, config);
                NettySource source = new NettySourceMock(config, topic, channel)) {
            final LogstashBeatsPipelineInitialization pipelineInitialization =
                    beatsPipelineInitialization(source);
            final EmbeddedChannel serverChannel = new EmbeddedChannel();
            pipelineInitialization.init(serverChannel);

            SslContext sslClientCtx =
                    SslContextBuilder.forClient()
                            .trustManager(new File(getResourcePath(caCertPath)))
                            .keyManager(
                                    new File(getResourcePath("client.crt")),
                                    new File(getResourcePath("client.key")))
                            .sslProvider(SslProvider.JDK)
                            .protocols(SUPPORT_PROTOCOL)
                            .ciphers(SUPPORTED_CIPHERS)
                            .build();

            final EmbeddedChannel clientChannel = new EmbeddedChannel();
            final AtomicBoolean clientReceivedAck = new AtomicBoolean();
            clientChannel
                    .pipeline()
                    .addLast(
                            new SslHandler(sslClientCtx.newEngine(ByteBufAllocator.DEFAULT), false),
                            new AckDecoder(clientReceivedAck, Protocol.VERSION_2, 1),
                            new BatchEncoder(),
                            new DummyV2Sender());
            final SslHandler clientSslHandler = clientChannel.pipeline().get(SslHandler.class);
            final SslHandler serverSslHandler = serverChannel.pipeline().get(SslHandler.class);

            // 通过手动触发链接active事件来执行ssl握手
            clientChannel.pipeline().fireChannelActive();
            serverChannel.pipeline().fireChannelActive();
            // 5. Process handshake messages
            while (!clientSslHandler.handshakeFuture().isDone()
                    || !serverSslHandler.handshakeFuture().isDone()) {
                ByteBuf handshakeData = clientChannel.readOutbound();
                if (handshakeData != null) {
                    serverChannel.writeInbound(handshakeData);
                }
                ByteBuf handsResponse = serverChannel.readOutbound();
                if (handsResponse != null) {
                    clientChannel.writeInbound(handsResponse);
                }
            }

            Assert.assertTrue(clientSslHandler.handshakeFuture().isSuccess());
            Assert.assertTrue(serverSslHandler.handshakeFuture().isSuccess());

            clientChannel.writeInbound("test2");
            ByteBuf encryptedData = clientChannel.readOutbound();
            serverChannel.writeInbound(encryptedData);

            final ByteBuf encryptedAckResponse = serverChannel.readOutbound();
            clientChannel.writeInbound(encryptedAckResponse);
            Assert.assertTrue(clientReceivedAck.get());

            channel.flush();
            RecordsResultSet resultSet = channel.poll(0, Offset.ZERO, 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());

            final Records records = Records.parse(resultSet.next());
            Assert.assertEquals(1, records.count());
            Assert.assertEquals(topic, records.topic());

            RecordsUtils.foreachRecord(
                    records,
                    (key, value, allHeaders) ->
                            Assert.assertEquals(
                                    "test2", MAPPER.readValue(value, MAP_TYPE_REFERENCE).get("k")));
        }
    }

    @Test
    public void testMessageFilter() throws Exception {
        final String topic = "t1";
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_LOGSTASH_BEATS_WORKER_THREADS, -1); // set to sync for test
        config.put(
                CONFIG_PREFIX + OPTION_MESSAGE_FILTER_FACTORY_ID.key(),
                LocalFileMessageFilterFactory.IDENTITY);
        config.put(CONFIG_PREFIX + OPTION_LOCAL_FILE_PATH.key(), "DefaultMessageFilterTest.txt");

        final AtomicBoolean clientReceivedAck = new AtomicBoolean();
        try (final Channel channel = memoryChannelFactory("g1").createChannel(topic, config);
                NettySource source = new NettySourceMock(config, topic, channel)) {
            final LogstashBeatsPipelineInitialization pipelineInitialization =
                    beatsPipelineInitialization(source);
            final EmbeddedChannel serverChannel = new EmbeddedChannel();
            pipelineInitialization.init(serverChannel);

            final EmbeddedChannel clientChannel =
                    new EmbeddedChannel(
                            new AckDecoder(clientReceivedAck, Protocol.VERSION_2, 1),
                            new BatchEncoder(),
                            new DummyV2JsonSender());
            clientChannel.writeInbound("{\"a\":\"b\"}");
            final ByteBuf byteBuf = clientChannel.readOutbound();

            serverChannel.pipeline().fireChannelActive();
            serverChannel.writeInbound(byteBuf);

            final Object serverAckResponse = serverChannel.readOutbound();
            clientChannel.writeInbound(serverAckResponse);
            Assert.assertTrue(clientReceivedAck.get());

            channel.flush();
            RecordsResultSet resultSet = channel.poll(0, Offset.ZERO, 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());

            final Records records = Records.parse(resultSet.next());
            Assert.assertEquals(1, records.count());
            Assert.assertEquals(topic, records.topic());

            RecordsUtils.foreachRecord(
                    records,
                    (key, value, allHeaders) -> {
                        Map<String, String> data = MAPPER.readValue(value, MAP_TYPE_REFERENCE);
                        Assert.assertEquals("b", data.get("a"));
                        Assert.assertEquals("_test_value", data.get("_test"));
                        Assert.assertEquals(2, data.size());
                    });
        }
    }

    @Test
    public void test() throws Exception {

        final String topic = "t1";
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_LOGSTASH_BEATS_WORKER_THREADS, -1); // set to sync for test
        final AtomicBoolean clientReceivedAck = new AtomicBoolean();
        try (final Channel channel = memoryChannelFactory("g1").createChannel(topic, config);
                NettySource source = new NettySourceMock(config, topic, channel)) {
            final LogstashBeatsPipelineInitialization pipelineInitialization =
                    beatsPipelineInitialization(source);
            final EmbeddedChannel serverChannel = new EmbeddedChannel();
            pipelineInitialization.init(serverChannel);

            final EmbeddedChannel clientChannel =
                    new EmbeddedChannel(
                            new AckDecoder(clientReceivedAck, Protocol.VERSION_2, 1),
                            new BatchEncoder(),
                            new DummyV2Sender());
            clientChannel.writeInbound("test1");
            final ByteBuf byteBuf = clientChannel.readOutbound();

            serverChannel.pipeline().fireChannelActive();
            serverChannel.writeInbound(byteBuf);

            final Object serverAckResponse = serverChannel.readOutbound();
            clientChannel.writeInbound(serverAckResponse);
            Assert.assertTrue(clientReceivedAck.get());

            channel.flush();
            RecordsResultSet resultSet = channel.poll(0, Offset.ZERO, 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());

            final Records records = Records.parse(resultSet.next());
            Assert.assertEquals(1, records.count());
            Assert.assertEquals(topic, records.topic());

            RecordsUtils.foreachRecord(
                    records,
                    (key, value, allHeaders) ->
                            Assert.assertEquals(
                                    "test1", MAPPER.readValue(value, MAP_TYPE_REFERENCE).get("k")));
        }
    }
}
