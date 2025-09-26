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

package org.zicat.tributary.source.logstash.beats;

import static org.zicat.tributary.common.ResourceUtils.getResourcePath;
import org.zicat.tributary.source.base.Source;
import static org.zicat.tributary.source.base.netty.EventExecutorGroups.createEventExecutorGroup;
import static org.zicat.tributary.source.logstash.beats.LogstashBeatsPipelineInitializationFactory.*;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutorGroup;

import org.logstash.beats.*;
import org.zicat.tributary.source.base.netty.ssl.PemSslContextBuilder;
import org.zicat.tributary.source.base.netty.ssl.SslHandlerProvider;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.source.base.netty.pipeline.AbstractPipelineInitialization;
import org.zicat.tributary.source.logstash.base.MessageFilterFactory;
import org.zicat.tributary.source.logstash.base.MessageFilterFactoryBuilder;

import java.io.IOException;

/** LogstashBeatsPipelineInitialization. */
public class LogstashBeatsPipelineInitialization extends AbstractPipelineInitialization {

    private static final String SSL_HANDLER = "ssl-handler";
    private static final String BEATS_ACKER = "beats-acker";
    private static final String CONNECTION_HANDLER = "connection-handler";

    protected final SslHandlerProvider sslHandlerProvider;
    protected final EventExecutorGroup beatsHandlerExecutorGroup;
    protected final MessageFilterFactory messageFilterFactory;

    public LogstashBeatsPipelineInitialization(Source source) throws Exception {
        super(source);
        final ReadableConfig conf = source.config();
        this.sslHandlerProvider = createSslHandlerProvider(conf);
        this.messageFilterFactory =
                MessageFilterFactoryBuilder.newBuilder()
                        .config(conf.filterAndRemovePrefixKey(CONFIG_PREFIX))
                        .buildAndOpen();
        this.beatsHandlerExecutorGroup =
                createEventExecutorGroup(
                        source.sourceId() + "-logstashBeatsHandler",
                        conf.get(OPTION_LOGSTASH_BEATS_WORKER_THREADS));
    }

    @Override
    public void init(Channel channel) {
        final ChannelPipeline pipeline = channel.pipeline();
        if (sslHandlerProvider != null) {
            pipeline.addLast(SSL_HANDLER, sslHandlerProvider.sslHandlerForChannel(channel));
        }
        final BatchMessageListener listener =
                new Message2ChannelListener(source, messageFilterFactory);
        idleClosedChannelPipeline(pipeline)
                .addLast(BEATS_ACKER, new AckEncoder())
                .addLast(CONNECTION_HANDLER, new ConnectionHandler())
                .addLast(beatsHandlerExecutorGroup, new BeatsParser(), new BeatsHandler(listener));
    }

    /**
     * Create an SslHandlerProvider based on the provided configuration.
     *
     * @param config config
     * @return SslHandlerProvider
     * @throws Exception Exception
     */
    private SslHandlerProvider createSslHandlerProvider(ReadableConfig config) throws Exception {
        if (!config.get(OPTION_LOGSTASH_BEATS_SSL)) {
            return null;
        }
        final String sslCertificate =
                getResourcePath(config.get(OPTION_LOGSTASH_BEATS_SSL_CERTIFICATE));
        if (sslCertificate == null) {
            throw new IllegalStateException(
                    "SSL certificate is required when SSL is enabled for logstash beats source.");
        }
        final String sslKey = getResourcePath(config.get(OPTION_LOGSTASH_BEATS_SSL_KEY));
        if (sslKey == null) {
            throw new IllegalStateException(
                    "SSL key is required when SSL is enabled for logstash beats source.");
        }
        final String sslCertificateAuthorities =
                getResourcePath(config.get(OPTION_LOGSTASH_BEATS_SSL_CERTIFICATE_AUTHORITIES));
        if (sslCertificateAuthorities == null) {
            throw new IllegalStateException(
                    "SSL certificate authorities are required when SSL is enabled for logstash beats source.");
        }
        final String[] certificateAuthorities = new String[] {sslCertificateAuthorities};
        final PemSslContextBuilder sslBuilder =
                new PemSslContextBuilder(sslCertificate, sslKey, null)
                        .setClientAuthentication(
                                PemSslContextBuilder.SslClientVerifyMode.REQUIRED,
                                certificateAuthorities);
        return new SslHandlerProvider(
                sslBuilder.buildContext(),
                config.get(OPTION_LOGSTASH_BEATS_SSL_TIMEOUT).toMillis());
    }

    @Override
    public void close() throws IOException {
        try {
            if (beatsHandlerExecutorGroup != null) {
                beatsHandlerExecutorGroup.shutdownGracefully();
            }
        } finally {
            IOUtils.closeQuietly(messageFilterFactory);
        }
    }
}
