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

package org.zicat.tributary.source.kafka;

import io.netty.channel.ChannelHandler.Sharable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.kafka.common.Node;
import org.apache.kafka25.HostPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.ReadableConfig;
import static org.zicat.tributary.common.util.IOUtils.closeQuietly;
import org.zicat.tributary.source.base.Source;
import static org.zicat.tributary.source.kafka.KafkaPipelineInitializationFactory.CONFIG_PREFIX;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.security.sasl.SaslServer;

/** ZookeeperMetaKafkaMessageDecoder. */
@Sharable
public class ZookeeperMetaKafkaMessageDecoder extends KafkaMessageDecoder {

    public static final ConfigOption<String> OPTION_CONNECT =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.connect").stringType().noDefaultValue();
    public static final ConfigOption<Duration> OPTION_META_CACHE_TTL =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.meta.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10));
    public static final ConfigOption<Duration> OPTION_CONNECTION_TIMEOUT =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.connection.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(15));
    public static final ConfigOption<Duration> OPTION_SESSION_TIMEOUT =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.session.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60));
    public static final ConfigOption<Integer> OPTION_RETRY_TIMES =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.retry.times")
                    .integerType()
                    .defaultValue(3);
    public static final ConfigOption<Duration> OPTION_FAIL_BASE_SLEEP_TIME =
            ConfigOptions.key(CONFIG_PREFIX + "zookeeper.fail.base.sleep.time")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1));

    private static final Logger LOG =
            LoggerFactory.getLogger(ZookeeperMetaKafkaMessageDecoder.class);
    private final int retryTimes;
    private final long baseSleepTimeMs;
    private final ScheduledExecutorService executor;
    private final ServiceInstance<HostPort> instance;
    private CuratorFramework client = null;
    private ServiceDiscovery<HostPort> serviceDiscovery = null;

    public ZookeeperMetaKafkaMessageDecoder(
            Source source,
            HostPort hostPort,
            String clusterId,
            int partitions,
            SaslServer saslServer,
            ReadableConfig config)
            throws Exception {
        super(source, hostPort, clusterId, partitions, saslServer);
        this.retryTimes = config.get(OPTION_RETRY_TIMES);
        this.baseSleepTimeMs =
                Math.max(
                        config.get(OPTION_FAIL_BASE_SLEEP_TIME).toMillis(),
                        OPTION_FAIL_BASE_SLEEP_TIME.defaultValue().toMillis());
        final String zk = config.get(OPTION_CONNECT);
        final String zkHostPort = zk.substring(0, zk.indexOf("/"));
        final String path = zk.substring(zk.indexOf("/"));
        final int connectionTimeout = (int) config.get(OPTION_CONNECTION_TIMEOUT).toMillis();
        final int sessionTimeout = (int) config.get(OPTION_SESSION_TIMEOUT).toMillis();
        final long metaTTL = config.get(OPTION_META_CACHE_TTL).toMillis();
        this.executor = Executors.newScheduledThreadPool(1);
        this.instance =
                ServiceInstance.<HostPort>builder().name(clusterId).payload(hostPort).build();
        try {
            client =
                    CuratorFrameworkFactory.newClient(
                            zkHostPort,
                            sessionTimeout,
                            connectionTimeout,
                            new ExponentialBackoffRetry((int) baseSleepTimeMs, retryTimes));
            client.start();
            serviceDiscovery =
                    ServiceDiscoveryBuilder.builder(HostPort.class)
                            .client(client)
                            .basePath(path)
                            .serializer(new JsonInstanceSerializer<>(HostPort.class))
                            .build();
            serviceDiscovery.start();
            serviceDiscovery.registerService(instance);

            updateNodes();
            executor.scheduleWithFixedDelay(
                    () -> {
                        try {
                            updateNodes();
                        } catch (Exception e) {
                            LOG.error("schedule update meta error", e);
                        }
                    },
                    metaTTL,
                    metaTTL,
                    TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            closeQuietly(this);
            throw e;
        }
    }

    @Override
    protected List<Node> getNodes() throws Exception {
        for (int j = 0; j < retryTimes; j++) {
            final Collection<ServiceInstance<HostPort>> instances =
                    serviceDiscovery.queryForInstances(clusterId);
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
            executor.shutdown();
        } finally {
            try {
                if (serviceDiscovery != null) {
                    serviceDiscovery.unregisterService(instance);
                }
            } catch (Exception ignore) {
            } finally {
                closeQuietly(serviceDiscovery, client);
            }
        }
    }
}
