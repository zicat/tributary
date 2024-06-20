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

package org.zicat.tributary.source.netty.pipeline;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.source.netty.DefaultNettySource;
import org.zicat.tributary.source.netty.handler.IdleCloseHandler;
import org.zicat.tributary.source.netty.handler.kafka.HostPort;
import org.zicat.tributary.source.netty.handler.KafkaMessageDecoder;
import org.zicat.tributary.source.netty.handler.kafka.PlainServerCallbackHandler;
import org.zicat.tributary.source.netty.handler.LengthDecoder;

import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/** KafkaPipelineInitialization. */
public class KafkaPipelineInitialization extends AbstractPipelineInitialization {

    public static final ConfigOption<String> OPTION_KAFKA_CLUSTER_ID =
            ConfigOptions.key("netty.decoder.kafka.cluster.id").stringType().defaultValue(null);

    public static final ConfigOption<String> OPTION_ZOOKEEPER_CONNECT =
            ConfigOptions.key("netty.decoder.kafka.zookeeper.connect")
                    .stringType()
                    .noDefaultValue();
    public static final ConfigOption<Integer> OPTION_CONNECTION_TIMEOUT_MS =
            ConfigOptions.key("netty.decoder.kafka.zookeeper.connection.timeout.ms")
                    .integerType()
                    .defaultValue(15000);
    public static final ConfigOption<Integer> OPTION_SESSION_TIMEOUT_MS =
            ConfigOptions.key("netty.decoder.kafka.zookeeper.session.timeout.ms")
                    .integerType()
                    .defaultValue(60000);
    public static final ConfigOption<Integer> OPTION_RETRY_TIMES =
            ConfigOptions.key("netty.decoder.kafka.zookeeper.retry.times")
                    .integerType()
                    .defaultValue(3);
    public static final ConfigOption<Integer> OPTION_FAIL_BASE_SLEEP_TIME_MS =
            ConfigOptions.key("netty.decoder.kafka.zookeeper.fail.base.sleep.time.ms")
                    .integerType()
                    .defaultValue(1000);

    public static final ConfigOption<Long> OPTION_META_CACHE_TTL_MS =
            ConfigOptions.key("netty.decoder.kafka.meta.ttl.ms").longType().defaultValue(10000L);

    public static final ConfigOption<Integer> OPTION_TOPIC_PARTITION_COUNT =
            ConfigOptions.key("netty.decoder.kafka.topic.partitions")
                    .integerType()
                    .defaultValue(60);

    public static final ConfigOption<Boolean> OPTION_KAFKA_SASL_PLAIN =
            ConfigOptions.key("netty.decoder.kafka.sasl.plain").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTION_SASL_USERS =
            ConfigOptions.key("netty.decoder.kafka.sasl.plain.usernames")
                    .stringType()
                    .defaultValue(null);

    protected final DefaultNettySource source;
    protected final KafkaMessageDecoder kafkaMessageDecoder;

    public KafkaPipelineInitialization(DefaultNettySource source) throws Exception {
        super(source);
        this.source = source;
        this.kafkaMessageDecoder = createKafkaMessageDeCoder(source);
    }

    @Override
    public void init(ChannelPipeline pipeline) {
        pipeline.addLast(source.idleStateHandler());
        pipeline.addLast(new IdleCloseHandler());
        pipeline.addLast(new LengthDecoder());
        pipeline.addLast(kafkaMessageDecoder);
    }

    /**
     * create KafkaMessageDecoder.
     *
     * @param source source
     * @return KafkaMessageDecoder
     */
    private static KafkaMessageDecoder createKafkaMessageDeCoder(DefaultNettySource source)
            throws Exception {

        final ReadableConfig config = source.getConfig();
        final String clusterId =
                config.get(OPTION_KAFKA_CLUSTER_ID) == null
                        ? source.sourceId()
                        : config.get(OPTION_KAFKA_CLUSTER_ID);
        final String hostName = oneHostName(source.getHostNames());
        final int port = source.getPort();
        final String zk = config.get(OPTION_ZOOKEEPER_CONNECT);
        final String zkHostPort = zk.substring(0, zk.indexOf("/"));
        final String path = zk.substring(zk.indexOf("/"));
        final int connectionTimeout = config.get(OPTION_CONNECTION_TIMEOUT_MS);
        final int sessionTimeout = config.get(OPTION_SESSION_TIMEOUT_MS);
        final int retryTimes = config.get(OPTION_RETRY_TIMES);
        final int baseSleepTimeMs =
                Math.max(
                        config.get(OPTION_FAIL_BASE_SLEEP_TIME_MS),
                        OPTION_FAIL_BASE_SLEEP_TIME_MS.defaultValue());
        final int partitions = config.get(OPTION_TOPIC_PARTITION_COUNT);
        final long metaTTL = config.get(OPTION_META_CACHE_TTL_MS);
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        final HostPort hostPort = new HostPort(hostName, port);
        final ServiceInstance<HostPort> currentInstance =
                ServiceInstance.<HostPort>builder().name(clusterId).payload(hostPort).build();
        CuratorFramework client = null;
        ServiceDiscovery<HostPort> serviceDiscovery = null;
        try {
            client =
                    CuratorFrameworkFactory.newClient(
                            zkHostPort,
                            sessionTimeout,
                            connectionTimeout,
                            new ExponentialBackoffRetry(baseSleepTimeMs, retryTimes));
            client.start();
            final JsonInstanceSerializer<HostPort> serializer =
                    new JsonInstanceSerializer<>(HostPort.class);
            serviceDiscovery =
                    ServiceDiscoveryBuilder.builder(HostPort.class)
                            .client(client)
                            .basePath(path)
                            .serializer(serializer)
                            .build();
            serviceDiscovery.start();
            serviceDiscovery.registerService(currentInstance);
            final SaslServer saslServer = createSaslServer(config);
            final ServiceDiscovery<HostPort> finalServiceDiscovery = serviceDiscovery;
            final CuratorFramework finalClient = client;
            return new KafkaMessageDecoder(
                    source, hostPort, clusterId, partitions, metaTTL, saslServer, executor) {
                @Override
                protected List<Node> getNodes() throws Exception {

                    for (int j = 0; j < retryTimes; j++) {
                        final Collection<ServiceInstance<HostPort>> instances =
                                finalServiceDiscovery.queryForInstances(clusterId);
                        final Set<HostPort> hostPorts = new HashSet<>();
                        for (ServiceInstance<HostPort> instance : instances) {
                            hostPorts.add(instance.getPayload());
                        }
                        final List<HostPort> sortedHostPorts =
                                hostPorts.stream()
                                        .sorted(
                                                Comparator.comparing(HostPort::getHost)
                                                        .thenComparingInt(HostPort::getPort))
                                        .collect(Collectors.toList());
                        final List<Node> nodes = new ArrayList<>(sortedHostPorts.size());
                        for (int i = 0; i < sortedHostPorts.size(); i++) {
                            final HostPort hostPort = sortedHostPorts.get(i);
                            nodes.add(new Node(i, hostPort.getHost(), hostPort.getPort()));
                        }
                        if (findCurrentNode(nodes, hostPort) == null) {
                            Thread.sleep(baseSleepTimeMs + (j + 1));
                            continue;
                        }
                        return nodes;
                    }
                    throw new RuntimeException("can not find current node " + hostPort);
                }

                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        try {
                            executor.shutdown();
                        } finally {
                            try {
                                finalServiceDiscovery.unregisterService(currentInstance);
                            } catch (Exception ignore) {
                            } finally {
                                IOUtils.closeQuietly(finalServiceDiscovery, finalClient);
                            }
                        }
                    }
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                        throws Exception {
                    try {
                        Thread.sleep(baseSleepTimeMs);
                    } finally {
                        super.exceptionCaught(ctx, cause);
                    }
                }
            };
        } catch (Exception e) {
            try {
                executor.shutdown();
            } finally {
                try {
                    if (serviceDiscovery != null) {
                        serviceDiscovery.unregisterService(currentInstance);
                    }
                } catch (Exception ignore) {
                } finally {
                    IOUtils.closeQuietly(serviceDiscovery, client);
                }
            }
            throw e;
        }
    }

    /**
     * create sasl server.
     *
     * @param config config.
     * @return SaslServer
     */
    private static SaslServer createSaslServer(ReadableConfig config) {
        final boolean saslPlain = config.get(OPTION_KAFKA_SASL_PLAIN);
        if (!saslPlain) {
            return null;
        }
        final String users = config.get(OPTION_SASL_USERS);
        if (users == null) {
            throw new RuntimeException(OPTION_SASL_USERS.key() + " not config");
        }
        final Map<String, String> userPassword = new HashMap<>();
        final String[] userList = users.split(",");
        for (String user : userList) {
            String[] split = user.split("_");
            if (split.length != 2) {
                throw new RuntimeException("parse user error " + user);
            }
            userPassword.put(split[0], split[1]);
        }
        return new PlainSaslServer(new PlainServerCallbackHandler(userPassword));
    }

    /**
     * one host name.
     *
     * @param hosts hosts
     * @return string
     */
    private static String oneHostName(List<String> hosts) {
        if (hosts.size() != 1) {
            throw new RuntimeException("kafka only support one host");
        }
        return hosts.get(0);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(kafkaMessageDecoder);
    }
}
