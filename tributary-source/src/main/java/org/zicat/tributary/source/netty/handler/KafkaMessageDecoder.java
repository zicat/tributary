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

package org.zicat.tributary.source.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import io.prometheus.client.Counter;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.BytesUtils;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.source.netty.AbstractNettySource;
import org.zicat.tributary.source.netty.handler.kafka.*;
import org.zicat.tributary.source.netty.handler.kafka.MetadataResponse.PartitionMetadata;
import org.zicat.tributary.source.netty.handler.kafka.MetadataResponse.TopicMetadata;
import org.zicat.tributary.source.netty.handler.kafka.ProduceResponse.PartitionResponse;

import javax.security.sasl.SaslServer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.zicat.tributary.source.netty.handler.kafka.ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE;
import static org.zicat.tributary.source.utils.SourceHeaders.sourceHeaders;

/** KafkaMessageDecoder. */
@Sharable
public abstract class KafkaMessageDecoder extends SimpleChannelInboundHandler<byte[]>
        implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageDecoder.class);
    public static final List<String> SUPPORTED_MECHANISMS = Collections.singletonList("PLAIN");

    private static final AttributeKey<String> KEY_AUTHENTICATED_USER =
            AttributeKey.valueOf("authenticated_user");

    private static final Counter REQUEST_COUNTER =
            Counter.build()
                    .name("kafka_request_type")
                    .help("kafka request type")
                    .labelNames("host", "port", "request_name")
                    .register();

    protected final AbstractNettySource source;
    protected final String clusterId;
    protected final int partitions;
    protected volatile List<Node> nodes;
    protected volatile Node currentNode;
    protected final String host;
    protected final String port;
    protected final SaslServer saslServer;
    protected final ScheduledExecutorService executor;

    public KafkaMessageDecoder(
            AbstractNettySource source,
            HostPort hostPort,
            String clusterId,
            int partitions,
            long metaTTL,
            SaslServer saslServer,
            ScheduledExecutorService executor)
            throws Exception {
        this.source = source;
        this.host = hostPort.getHost();
        this.port = String.valueOf(hostPort.getPort());
        this.clusterId = clusterId;
        this.partitions = partitions;
        this.saslServer = saslServer;
        this.executor = executor;
        this.nodes = getNodes();
        this.currentNode = currentNode(nodes, hostPort);
        executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        final List<Node> nodes = getNodes();
                        final Node currentNode = currentNode(nodes, hostPort);
                        this.nodes = nodes;
                        this.currentNode = currentNode;
                    } catch (Exception e) {
                        LOG.error("schedule update meta error", e);
                    }
                },
                metaTTL,
                metaTTL,
                TimeUnit.MILLISECONDS);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) throws Exception {
        final ByteBuffer requestBuffer = ByteBuffer.wrap(msg);
        final RequestHeader header = RequestHeader.parse(requestBuffer);
        final Struct struct = header.apiKey().parseRequest(header.apiVersion(), requestBuffer);
        final AbstractRequest request =
                AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), struct);
        incRequestCount(request.api.name);
        if (request instanceof ApiVersionsRequest) {
            ctx.writeAndFlush(toByteBuf(DEFAULT_API_VERSIONS_RESPONSE, header));
            return;
        }
        if (request instanceof ProduceRequest) {
            checkAuthenticated(ctx);
            final ProduceRequest produceRequest = (ProduceRequest) request;
            handleProduceRequest(produceRequest, ctx, header);
            return;
        }
        if (request instanceof MetadataRequest) {
            checkAuthenticated(ctx);
            final List<Node> nodes = this.nodes;
            final MetadataRequest metadataRequest = (MetadataRequest) request;
            final AbstractResponse res = topicsMetaResponse(nodes, metadataRequest.topics());
            ctx.writeAndFlush(toByteBuf(res, header));
            return;
        }
        if (request instanceof SaslHandshakeRequest) {
            final SaslHandshakeRequest handshakeRequest = (SaslHandshakeRequest) request;
            final SaslHandshakeResponseData data =
                    new SaslHandshakeResponseData().setMechanisms(SUPPORTED_MECHANISMS);
            if (!SUPPORTED_MECHANISMS.contains(handshakeRequest.data().mechanism())) {
                data.setErrorCode(Short.parseShort("1"));
                ctx.writeAndFlush(toByteBuf(new SaslHandshakeResponse(data), header));
                ctx.close();
            } else {
                ctx.writeAndFlush(toByteBuf(new SaslHandshakeResponse(data), header));
            }
            return;
        }
        if (request instanceof SaslAuthenticateRequest) {
            final SaslAuthenticateRequest authenticateRequest = (SaslAuthenticateRequest) request;
            final byte[] token =
                    saslServer.evaluateResponse(authenticateRequest.data().authBytes());
            final SaslAuthenticateResponse res =
                    new SaslAuthenticateResponse(
                            new SaslAuthenticateResponseData()
                                    .setErrorCode(Errors.NONE.code())
                                    .setAuthBytes(token));
            ctx.channel().attr(KEY_AUTHENTICATED_USER).set(saslServer.getAuthorizationID());
            ctx.writeAndFlush(toByteBuf(res, header));
            return;
        }
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ctx.channel().attr(KEY_AUTHENTICATED_USER).set(null);
    }

    /**
     * check whether login.
     *
     * @param ctx ctx
     */
    private void checkAuthenticated(ChannelHandlerContext ctx) {
        if (saslServer == null) {
            return;
        }
        final String authenticatedUser = ctx.channel().attr(KEY_AUTHENTICATED_USER).get();
        if (authenticatedUser == null) {
            throw new SaslAuthenticationException("Authentication first please");
        }
    }

    /**
     * increase error request count.
     *
     * @param apiName apiName
     */
    private void incRequestCount(String apiName) {
        REQUEST_COUNTER.labels(host, port, apiName).inc();
    }

    /**
     * find current node.
     *
     * @param nodes nodes
     * @param hostPort hostPort
     * @return node
     */
    private static Node currentNode(List<Node> nodes, HostPort hostPort) {
        final Node current = findCurrentNode(nodes, hostPort);
        if (current == null) {
            throw new IllegalStateException("current node not found");
        }
        return current;
    }

    /**
     * find current node.
     *
     * @param nodes nodes
     * @param hostPort hostPort
     * @return return null if not found
     */
    public static Node findCurrentNode(List<Node> nodes, HostPort hostPort) {
        for (Node node : nodes) {
            if (node.host().equals(hostPort.getHost()) && node.port() == hostPort.getPort()) {
                return node;
            }
        }
        return null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!(cause.getMessage() != null && cause.getMessage().contains("Connection reset by peer"))
                && !(cause instanceof SaslAuthenticationException)) {
            LOG.error("channel error", cause);
        }
        ctx.close();
    }

    /**
     * get nodes from discovery service.
     *
     * @return nodes.
     * @throws Exception Exception
     */
    protected abstract List<Node> getNodes() throws Exception;

    /**
     * handle produce request.
     *
     * @param request produce request.
     * @param ctx context
     * @param header header
     */
    private void handleProduceRequest(
            ProduceRequest request, ChannelHandlerContext ctx, RequestHeader header)
            throws IOException {
        final Map<TopicPartition, PartitionResponse> partitionRes = new HashMap<>();
        final Map<TopicPartition, MemoryRecords> allRecords = request.partitionRecordsOrFail();
        final List<Node> nodes = this.nodes;
        final Node currentNode = this.currentNode;
        for (Entry<TopicPartition, MemoryRecords> entry : allRecords.entrySet()) {
            final TopicPartition tp = entry.getKey();
            // check if balance
            if (!selectNode(tp.partition(), nodes).equals(currentNode)) {
                ctx.close();
                return;
            }
            partitionRes.put(tp, new PartitionResponse(Errors.NONE));
            final DefaultRecords defaultRecords = createRecords(ctx, tp);
            final AbstractIterator<MutableRecordBatch> it = entry.getValue().batchIterator();
            while (it.hasNext()) {
                final MutableRecordBatch batch = it.next();
                for (Record record : batch) {
                    defaultRecords.addRecord(toRecord(record));
                }
            }
            source.append(tp.partition() % source.partition(), defaultRecords);
        }
        ctx.writeAndFlush(toByteBuf(new ProduceResponse(partitionRes), header));
    }

    /**
     * create records.
     *
     * @param ctx ctx
     * @param tp tp
     * @return DefaultRecords
     */
    private static DefaultRecords createRecords(ChannelHandlerContext ctx, TopicPartition tp) {
        final long receivedTs = System.currentTimeMillis();
        final String user = ctx.channel().attr(KEY_AUTHENTICATED_USER).get();
        final Map<String, byte[]> headers = sourceHeaders(receivedTs, user);
        return new DefaultRecords(tp.topic(), headers);
    }

    /**
     * kafka record to tributary record.
     *
     * @param record kafka record
     * @return tributary record
     */
    private static org.zicat.tributary.common.records.Record toRecord(Record record) {
        final Map<String, byte[]> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header.value());
        }
        return new DefaultRecord(
                headers,
                record.hasKey() ? BytesUtils.toBytes(record.key()) : null,
                record.hasValue() ? BytesUtils.toBytes(record.value()) : null);
    }

    /**
     * abstract response to byte buf.
     *
     * @param response response
     * @param header header
     * @return ByteBuf
     */
    private static ByteBuf toByteBuf(AbstractResponse response, RequestHeader header) {
        final ByteBuffer buffer =
                response.serialize(header.apiKey(), header.apiVersion(), header.correlationId());
        return ByteBufAllocator.DEFAULT
                .buffer(buffer.remaining() + 4)
                .writeInt(buffer.remaining())
                .writeBytes(buffer);
    }

    /**
     * create topics meta response.
     *
     * @param topics topics
     * @return AbstractResponse
     */
    private AbstractResponse topicsMetaResponse(List<Node> nodes, List<String> topics) {
        final List<String> newTopics = topics == null ? Collections.emptyList() : topics;
        final List<TopicMetadata> topicMetadataList =
                newTopics.stream()
                        .map(topic -> topicMetadata(topic, nodes))
                        .collect(Collectors.toList());
        return MetadataResponse.prepareResponse(
                nodes, clusterId, nodes.get(0).id(), topicMetadataList);
    }

    /**
     * create topic metadata.
     *
     * <p>the partition count is equals nodes count
     *
     * <p>the topic always no replica and the leader id is always 0
     *
     * @param topic topic
     * @param nodes nodes
     * @return response
     */
    private TopicMetadata topicMetadata(String topic, List<Node> nodes) {
        final List<PartitionMetadata> partitionMetas = new ArrayList<>();
        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            final int id = selectNode(partitionId, nodes).id();
            final PartitionMetadata partitionMeta = createPartitionMetadata(topic, partitionId, id);
            partitionMetas.add(partitionMeta);
        }
        return createTopicMetadata(topic, partitionMetas);
    }

    /**
     * select node by partition id and nodes.
     *
     * @param partitionId partitionId
     * @param nodes nodes
     * @return node
     */
    private static Node selectNode(int partitionId, List<Node> nodes) {
        return nodes.get(partitionId % nodes.size());
    }

    /**
     * create topic metadata.
     *
     * @param topic topic
     * @param partitionMetas partitionMetas
     * @return TopicMetadata
     */
    private static TopicMetadata createTopicMetadata(
            String topic, List<PartitionMetadata> partitionMetas) {
        return new TopicMetadata(Errors.NONE, topic, false, partitionMetas);
    }

    /**
     * create partition meta data.
     *
     * @param topic topic
     * @param nodeId nodeId
     * @return PartitionMetadata
     */
    private static PartitionMetadata createPartitionMetadata(
            String topic, int partition, int nodeId) {
        final TopicPartition tp = new TopicPartition(topic, partition);
        // never changed leaderEpoch, change leaderEpoch will cause client meta update fail
        return new PartitionMetadata(
                Errors.NONE,
                tp,
                Optional.of(nodeId),
                Optional.of(0),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
    }
}
