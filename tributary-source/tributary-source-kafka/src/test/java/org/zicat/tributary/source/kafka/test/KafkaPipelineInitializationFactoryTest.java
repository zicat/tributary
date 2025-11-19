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

package org.zicat.tributary.source.kafka.test;

import static org.apache.kafka25.ApiKeys.*;
import static org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils.memoryChannelFactory;
import org.zicat.tributary.common.config.ReadableConfigBuilder;
import org.zicat.tributary.source.base.netty.NettySource;
import static org.zicat.tributary.source.kafka.KafkaPipelineInitializationFactory.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;

import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka25.*;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.common.records.Record0;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;
import org.zicat.tributary.source.base.test.netty.NettySourceMock;
import org.zicat.tributary.source.kafka.KafkaMessageDecoder;
import org.zicat.tributary.source.kafka.KafkaPipelineInitializationFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** KafkaPipelineInitializationFactoryTest. */
public class KafkaPipelineInitializationFactoryTest {

    private static final int partitions = 40;
    private static final int port1 = 9000;
    private static final int port2 = 9001;
    private static final String HOST = "127.0.0.1";
    private static final String AUTH_USER = "user1";
    private static final String AUTH_PASS = "16Ew658jjzvmxDqk";
    private static final TopicPartition tp = new TopicPartition("t1", 0);
    private static final String groupId = "g1";
    private static final Offset START_OFFSET = Offset.ZERO;

    @Test
    public void test() throws Exception {
        final ReadableConfigBuilder builder =
                new ReadableConfigBuilder()
                        .addConfig(OPTION_TOPIC_PARTITION_COUNT, partitions)
                        .addConfig(OPTION_KAFKA_SASL_PLAIN, true)
                        .addConfig(OPTION_SASL_USERS, AUTH_USER + "_" + AUTH_PASS)
                        .addConfig(OPTION_KAFKA_WORKER_THREADS, -1)
                        .addConfig(OPTION_KAFKA_ADVERTISED_HOST_PATTERN, "localhost");
        final String zkPath = "/kafka_test_register";
        try (TestingServer server = new TestingServer();
                Channel channel =
                        memoryChannelFactory(groupId).createChannel(tp.topic(), builder.build())) {
            server.start();
            test(
                    channel,
                    builder.addConfig(OPTION_ZOOKEEPER_CONNECT, server.getConnectString() + zkPath)
                            .build());
        }
    }

    private void test(Channel channel, ReadableConfig config) throws Exception {
        try (NettySource source = createSource(config, channel, port1)) {
            final EmbeddedChannel nettyChannel1 = new EmbeddedChannel();
            initChannel(source, nettyChannel1);
            assertApiVersionResponse(nettyChannel1);
            // not login so only api version request allowed
            nettyChannel1.writeInbound(serialize(metadataRequest()));
            Assert.assertFalse(nettyChannel1.isActive());

            final EmbeddedChannel nettyChannel2 = new EmbeddedChannel();
            initChannel(source, nettyChannel2);
            // api version
            assertApiVersionResponse(nettyChannel2);
            // sasl api
            nettyChannel2.writeInbound(serialize(saslHandshakeRequest()));
            final SaslHandshakeResponse saslHandshakeResponse =
                    saslHandshakeResponse(nettyChannel2);
            Assert.assertArrayEquals(
                    KafkaMessageDecoder.SUPPORTED_MECHANISMS.toArray(new String[] {}),
                    saslHandshakeResponse.enabledMechanisms().toArray(new String[] {}));
            login(nettyChannel2);

            // meta api
            nettyChannel2.writeInbound(serialize(metadataRequest()));
            final MetadataResponse metaResponse = metadataResponse(nettyChannel2);
            Assert.assertEquals(1, metaResponse.brokers().size());
            final Node oneNode = metaResponse.brokers().iterator().next();
            Assert.assertEquals(HOST, oneNode.host());
            Assert.assertEquals(0, oneNode.id());
            Assert.assertEquals(port1, oneNode.port());

            Assert.assertEquals(1, metaResponse.topicMetadata().size());
            final MetadataResponse.TopicMetadata topicMetadata =
                    metaResponse.topicMetadata().iterator().next();
            Assert.assertEquals(partitions, topicMetadata.partitionMetadata().size());
            for (MetadataResponse.PartitionMetadata partitionMetadata :
                    topicMetadata.partitionMetadata()) {
                Assert.assertTrue(partitionMetadata.leaderEpoch.isPresent());
                Assert.assertEquals(0, partitionMetadata.leaderEpoch.get().intValue());
                Assert.assertTrue(partitionMetadata.leaderId.isPresent());
            }

            nettyChannel2.writeInbound(serialize(produceRequest()));
            final ProduceResponse produceResponse = produceResponse(nettyChannel2);
            Assert.assertEquals(Errors.NONE, produceResponse.responses().get(tp).error);
            channel.flush();
            final RecordsResultSet resultSet =
                    channel.poll(tp.partition(), START_OFFSET, 10, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());

            final Records records = Records.parse(resultSet.next());
            Assert.assertEquals(tp.topic(), records.topic());
            final Record0 record = records.iterator().next();
            Assert.assertEquals("k1", new String(record.key()));
            Assert.assertEquals("v1", new String(record.value()));
            Assert.assertEquals(1, record.headers().size());
            Assert.assertTrue(record.headers().containsKey("h1"));
            Assert.assertEquals("hv1", new String(record.headers().get("h1")));

            try (NettySource source2 = createSource(config, channel, port2)) {
                final EmbeddedChannel nettyChannel3 = new EmbeddedChannel();
                initChannel(source2, nettyChannel3);
                login(nettyChannel3);

                assertApiVersionResponse(nettyChannel3);
                nettyChannel3.writeInbound(serialize(metadataRequest()));
                final MetadataResponse metaResponse2 = metadataResponse(nettyChannel3);
                Assert.assertEquals(2, metaResponse2.brokers().size());
                final List<Node> sortedNodes =
                        metaResponse2.brokers().stream()
                                .sorted(Comparator.comparingInt(Node::id))
                                .collect(Collectors.toList());
                final Node firstNode = sortedNodes.get(0);
                Assert.assertEquals(HOST, firstNode.host());
                Assert.assertEquals(0, firstNode.id());
                Assert.assertEquals(port1, firstNode.port());
                final Node secondNode = sortedNodes.get(1);
                Assert.assertEquals(HOST, secondNode.host());
                Assert.assertEquals(1, secondNode.id());
                Assert.assertEquals(port2, secondNode.port());

                Assert.assertEquals(1, metaResponse2.topicMetadata().size());
                final MetadataResponse.TopicMetadata topicMeta2 =
                        metaResponse2.topicMetadata().iterator().next();
                Assert.assertEquals(partitions, topicMeta2.partitionMetadata().size());
                for (MetadataResponse.PartitionMetadata partitionMetadata :
                        topicMeta2.partitionMetadata()) {
                    Assert.assertTrue(partitionMetadata.leaderEpoch.isPresent());
                    Assert.assertEquals(0, partitionMetadata.leaderEpoch.get().intValue());
                    Assert.assertTrue(partitionMetadata.leaderId.isPresent());
                }

                final Set<Integer> nodeIds =
                        topicMeta2.partitionMetadata().stream()
                                .filter(v -> v.leaderId.isPresent())
                                .map(v -> v.leaderId.get())
                                .collect(Collectors.toSet());
                Assert.assertEquals(2, nodeIds.size());
                Assert.assertTrue(nodeIds.contains(0) && nodeIds.contains(1));
            }
        }
    }

    private static ApiVersionsResponse apiVersionsResponse(EmbeddedChannel nettyChannel) {
        return ApiVersionsResponse.parse(
                read(nettyChannel, API_VERSIONS), API_VERSIONS.latestVersion());
    }

    private static ApiVersionsRequest apiVersionsRequest() {
        return new ApiVersionsRequest(new ApiVersionsRequestData(), API_VERSIONS.latestVersion());
    }

    private static void login(EmbeddedChannel nettyChannel) {
        nettyChannel.writeInbound(serialize(saslAuthenticateRequest()));
        final SaslAuthenticateResponse saslAuthenticateResponse =
                SaslAuthenticateResponse.parse(
                        read(nettyChannel, SASL_AUTHENTICATE), SASL_AUTHENTICATE.latestVersion());
        Assert.assertEquals(Errors.NONE, saslAuthenticateResponse.error());
    }

    private static MetadataResponse metadataResponse(EmbeddedChannel nettyChannel) {
        return MetadataResponse.parse(read(nettyChannel, METADATA), METADATA.latestVersion());
    }

    private static ProduceResponse produceResponse(EmbeddedChannel nettyChannel) {
        return ProduceResponse.parse(read(nettyChannel, PRODUCE), PRODUCE.latestVersion());
    }

    private static MetadataRequest metadataRequest() {
        return new MetadataRequest(
                new MetadataRequestData()
                        .setTopics(
                                Collections.singletonList(
                                        new MetadataRequestData.MetadataRequestTopic()
                                                .setName(tp.topic()))),
                METADATA.latestVersion());
    }

    private static SaslAuthenticateRequest saslAuthenticateRequest() {
        final byte[] bs = (AUTH_USER + "\u0000" + AUTH_USER + "\u0000" + AUTH_PASS).getBytes();
        return new SaslAuthenticateRequest(new SaslAuthenticateRequestData().setAuthBytes(bs));
    }

    private static SaslHandshakeRequest saslHandshakeRequest() {
        return new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"));
    }

    private static ProduceRequest produceRequest() {
        final Map<TopicPartition, MemoryRecords> records = new HashMap<>();
        records.put(
                tp,
                MemoryRecords.withRecords(
                        CompressionType.SNAPPY,
                        new SimpleRecord(
                                1L,
                                "k1".getBytes(),
                                "v1".getBytes(),
                                new Header[] {new RecordHeader("h1", "hv1".getBytes())})));
        return new ProduceRequest.Builder(
                        PRODUCE.oldestVersion(),
                        PRODUCE.latestVersion(),
                        (short) 1,
                        60000,
                        records,
                        "")
                .build(PRODUCE.latestVersion());
    }

    private static SaslHandshakeResponse saslHandshakeResponse(EmbeddedChannel nettyChannel) {
        return SaslHandshakeResponse.parse(
                read(nettyChannel, SASL_HANDSHAKE), SASL_HANDSHAKE.latestVersion());
    }

    private static ByteBuf serialize(AbstractRequest request) {
        final ByteBuffer byteBuffer = request.serialize(requestHeader(request.api));
        return ByteBufAllocator.DEFAULT
                .buffer()
                .writeInt(byteBuffer.remaining())
                .writeBytes(byteBuffer);
    }

    private static ByteBuffer readAsBytes(ByteBuf received) {
        final int size = received.readInt();
        final byte[] bs = new byte[size];
        received.readBytes(bs);
        return ByteBuffer.wrap(bs);
    }

    private static ByteBuffer read(EmbeddedChannel channel, ApiKeys apiKey) {
        final short responseHeadVersion = apiKey.responseHeaderVersion(apiKey.latestVersion());
        final ByteBuffer bf = readAsBytes(channel.readOutbound());
        final ResponseHeader header = ResponseHeader.parse(bf, responseHeadVersion);
        Assert.assertNotNull(header);
        return bf;
    }

    /**
     * request header.
     *
     * @param apiKeys apiKeys
     * @return RequestHeader
     */
    private static RequestHeader requestHeader(ApiKeys apiKeys) {
        return new RequestHeader(
                new RequestHeaderData()
                        .setRequestApiKey(apiKeys.id)
                        .setRequestApiVersion(apiKeys.latestVersion()),
                apiKeys.requestHeaderVersion(apiKeys.latestVersion()));
    }

    private static NettySource createSource(ReadableConfig config, Channel channel, int port)
            throws Exception {
        return new NettySourceMock(config, HOST, port, channel);
    }

    private static void initChannel(NettySource source, EmbeddedChannel nettyChannel)
            throws Exception {
        PipelineInitialization pipelineInit =
                SpiFactory.findFactory(
                                KafkaPipelineInitializationFactory.IDENTITY,
                                PipelineInitializationFactory.class)
                        .createPipelineInitialization(source);
        pipelineInit.init(nettyChannel);
    }

    private static void assertApiVersionResponse(EmbeddedChannel channel) {
        final ApiVersionsRequest apiVersionsRequest = apiVersionsRequest();
        channel.writeInbound(serialize(apiVersionsRequest));
        final ApiVersionsResponse apiVersionsResponse = apiVersionsResponse(channel);
        Assert.assertEquals(0, apiVersionsResponse.data.errorCode());
        Assert.assertFalse(apiVersionsResponse.data.apiKeys().isEmpty());
    }
}
