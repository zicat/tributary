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

import static org.zicat.tributary.common.util.HostUtils.getFirstMatchedHostAddress;
import static org.zicat.tributary.common.util.ResourceUtils.getResourcePath;
import org.zicat.tributary.source.base.netty.NettySource;
import org.zicat.tributary.source.base.netty.ssl.AbstractSslContextBuilder;
import org.zicat.tributary.source.base.netty.ssl.AbstractSslContextBuilder.SslClientVerifyMode;
import org.zicat.tributary.source.base.netty.ssl.KeystoreSslContextBuilder;
import org.zicat.tributary.source.base.netty.ssl.KeystoreSslContextBuilder.KeystoreType;
import org.zicat.tributary.source.base.netty.ssl.SslHandlerProvider;
import static org.zicat.tributary.source.base.netty.EventExecutorGroups.createEventExecutorGroup;
import static org.zicat.tributary.source.kafka.KafkaPipelineInitializationFactory.*;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutorGroup;

import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka25.HostPort;
import org.apache.kafka25.PlainServerCallbackHandler;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.source.base.netty.handler.LengthDecoder;
import org.zicat.tributary.source.base.netty.pipeline.AbstractPipelineInitialization;

import java.util.*;

import javax.security.sasl.SaslServer;

/** KafkaPipelineInitialization. */
public class KafkaPipelineInitialization extends AbstractPipelineInitialization {

    private static final String SSL_HANDLER = "ssl-handler";

    protected final KafkaMessageDecoder kafkaMessageDecoder;
    protected final EventExecutorGroup kafkaHandlerExecutorGroup;
    protected final SslHandlerProvider sslHandlerProvider;

    public KafkaPipelineInitialization(NettySource source) throws Exception {
        super(source);
        final ReadableConfig config = source.config();
        this.sslHandlerProvider = createSslHandlerProvider(config);
        this.kafkaMessageDecoder = createKafkaMessageDeCoder(source);
        this.kafkaHandlerExecutorGroup =
                createEventExecutorGroup(
                        source.sourceId() + "-kafkaHandler",
                        config.get(OPTION_KAFKA_WORKER_THREADS));
    }

    @Override
    public void init(Channel channel) {
        final ChannelPipeline pipeline = channel.pipeline();
        if (sslHandlerProvider != null) {
            pipeline.addLast(SSL_HANDLER, sslHandlerProvider.sslHandlerForChannel(channel));
        }
        idleClosedChannelPipeline(pipeline)
                .addLast(new LengthDecoder())
                .addLast(kafkaHandlerExecutorGroup, kafkaMessageDecoder);
    }

    /**
     * create KafkaMessageDecoder.
     *
     * @param source source
     * @return KafkaMessageDecoder
     */
    protected KafkaMessageDecoder createKafkaMessageDeCoder(NettySource source)
            throws Exception {
        final ReadableConfig config = source.config();
        final String clusterId =
                config.get(OPTION_KAFKA_CLUSTER_ID) == null
                        ? source.sourceId()
                        : config.get(OPTION_KAFKA_CLUSTER_ID);
        final String hostName = getFirstMatchedHostAddress(config.get(OPTION_KAFKA_ADVERTISED_HOST_PATTERN));
        final int port = source.getPort();
        final int partitions = config.get(OPTION_TOPIC_PARTITION_COUNT);
        final HostPort hostPort = new HostPort(hostName, port);
        final SaslServer saslServer = createSaslServer(config);
        return new ZookeeperMetaKafkaMessageDecoder(
                source, hostPort, clusterId, partitions, saslServer, config);
    }

    /**
     * create sasl server.
     *
     * @param config config.
     * @return SaslServer
     */
    private static SaslServer createSaslServer(ReadableConfig config) {
        final SaslMechanism mechanism = config.get(OPTION_KAFKA_SASL_MECHANISM);
        if (mechanism == null || mechanism == SaslMechanism.NONE) {
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

    @Override
    public void close() {
        try {
            if (kafkaHandlerExecutorGroup != null) {
                kafkaHandlerExecutorGroup.shutdownGracefully();
            }
        } finally {
            IOUtils.closeQuietly(kafkaMessageDecoder);
        }
    }

    private SslHandlerProvider createSslHandlerProvider(ReadableConfig config) throws Exception {
        final String keystoreLocation = config.get(OPTION_SSL_KEYSTORE_LOCATION);
        final String keystorePassword = config.get(OPTION_SSL_KEYSTORE_PASSWORD);
        final String keyPassword = config.get(OPTION_SSL_KEY_PASSWORD);
        final List<String> enableProtocols = config.get(OPTION_SSL_ENABLE_PROTOCOLS);
        final SslClientVerifyMode verifyMode = config.get(OPTION_SSL_CLIENT_AUTH);
        final KeystoreType keystoreType = config.get(OPTION_SSL_KEYSTORE_TYPE);
        if (keystoreLocation == null || keystorePassword == null || keyPassword == null) {
            return null;
        }
        final AbstractSslContextBuilder builder =
                new KeystoreSslContextBuilder()
                        .keystoreLocation(getResourcePath(keystoreLocation))
                        .keystorePassword(keystorePassword)
                        .keyPassword(keyPassword)
                        .keystoreType(keystoreType)
                        .verifyMode(verifyMode)
                        .protocols(enableProtocols);
        return new SslHandlerProvider(
                builder.buildContext(), config.get(OPTION_SSL_TIMEOUT).toMillis());
    }
}
