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

package org.zicat.tributary.source.beats;

import static org.zicat.tributary.common.ResourceUtils.getResourcePath;
import static org.zicat.tributary.common.Threads.createThreadFactoryByName;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import org.logstash.beats.*;
import org.logstash.netty.SslContextBuilder;
import org.logstash.netty.SslHandlerProvider;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.source.base.netty.DefaultNettySource;
import org.zicat.tributary.source.base.netty.pipeline.AbstractPipelineInitialization;

import java.time.Duration;

/** BeatsPipelineInitialization. */
public class BeatsPipelineInitialization extends AbstractPipelineInitialization {

    private static final String SSL_HANDLER = "ssl-handler";
    private static final String BEATS_ACKER = "beats-acker";
    private static final String CONNECTION_HANDLER = "connection-handler";
    private static final String IDLESTATE_HANDLER = "idlestate-handler";

    public static final ConfigOption<Integer> OPTION_BEATS_WORKER_THREADS =
            ConfigOptions.key("netty.decoder.beats.worker-threads")
                    .integerType()
                    .description("The number of worker threads for the Beats handler.")
                    .defaultValue(10);

    public static final ConfigOption<Boolean> OPTION_BEATS_SSL =
            ConfigOptions.key("netty.decoder.beats.ssl")
                    .booleanType()
                    .description("Whether to use SSL for the Beats connection.")
                    .defaultValue(false);

    public static final ConfigOption<String> OPTION_BEATS_SSL_CERTIFICATE_AUTHORITIES =
            ConfigOptions.key("netty.decoder.beats.ssl.certificate.authorities")
                    .stringType()
                    .description("The certificate authorities for the SSL connection.")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_BEATS_SSL_CERTIFICATE =
            ConfigOptions.key("netty.decoder.beats.ssl.certificate")
                    .stringType()
                    .description("The certificate for the SSL connection.")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_BEATS_SSL_KEY =
            ConfigOptions.key("netty.decoder.beats.ssl.key")
                    .stringType()
                    .description("The key for the SSL connection.")
                    .defaultValue(null);

    public static final ConfigOption<Duration> OPTION_BEATS_SSL_TIMEOUT =
            ConfigOptions.key("netty.decoder.beats.ssl.timeout")
                    .durationType()
                    .description("The timeout for the SSL handshake, default 10s.")
                    .defaultValue(Duration.ofSeconds(10));

    protected final SslHandlerProvider sslHandlerProvider;
    protected final DefaultNettySource source;
    protected final EventExecutorGroup beatsHandlerExecutorGroup;

    public BeatsPipelineInitialization(DefaultNettySource source) throws Exception {
        super(source);
        this.source = source;
        this.sslHandlerProvider = createSslHandlerProvider(source.getConfig());
        final int workerThreads = source.getConfig().get(OPTION_BEATS_WORKER_THREADS);
        this.beatsHandlerExecutorGroup =
                workerThreads == -1
                        ? null
                        : new DefaultEventExecutorGroup(
                                workerThreads,
                                createThreadFactoryByName(
                                        source.sourceId() + "-beatsHandler", true));
    }

    @Override
    public void init(Channel channel) {
        final ChannelPipeline pipeline = channel.pipeline();
        if (sslHandlerProvider != null) {
            pipeline.addLast(SSL_HANDLER, sslHandlerProvider.sslHandlerForChannel(channel));
        }
        pipeline.addLast(IDLESTATE_HANDLER, source.idleStateHandler());
        pipeline.addLast(BEATS_ACKER, new AckEncoder());
        pipeline.addLast(CONNECTION_HANDLER, new ConnectionHandler());
        final BatchMessageListener listener =
                new Message2ChannelListener(source, selectPartition());
        pipeline.addLast(beatsHandlerExecutorGroup, new BeatsParser(), new BeatsHandler(listener));
    }

    /**
     * Create an SslHandlerProvider based on the provided configuration.
     *
     * @param config config
     * @return SslHandlerProvider
     * @throws Exception Exception
     */
    private SslHandlerProvider createSslHandlerProvider(ReadableConfig config) throws Exception {
        if (!config.get(OPTION_BEATS_SSL)) {
            return null;
        }
        final String sslCertificate = getResourcePath(config.get(OPTION_BEATS_SSL_CERTIFICATE));
        if (sslCertificate == null) {
            throw new IllegalStateException(
                    "SSL certificate is required when SSL is enabled for Beats source.");
        }
        final String sslKey = getResourcePath(config.get(OPTION_BEATS_SSL_KEY));
        if (sslKey == null) {
            throw new IllegalStateException(
                    "SSL key is required when SSL is enabled for Beats source.");
        }
        final String sslCertificateAuthorities =
                getResourcePath(config.get(OPTION_BEATS_SSL_CERTIFICATE_AUTHORITIES));
        if (sslCertificateAuthorities == null) {
            throw new IllegalStateException(
                    "SSL certificate authorities are required when SSL is enabled for Beats source.");
        }
        final String[] certificateAuthorities = new String[] {sslCertificateAuthorities};
        final SslContextBuilder sslBuilder =
                new SslContextBuilder(sslCertificate, sslKey, null)
                        .setClientAuthentication(
                                SslContextBuilder.SslClientVerifyMode.REQUIRED,
                                certificateAuthorities);
        return new SslHandlerProvider(
                sslBuilder.buildContext(), config.get(OPTION_BEATS_SSL_TIMEOUT).toMillis());
    }
}
