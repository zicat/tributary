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

package org.zicat.tributary.server;

import io.prometheus.client.CollectorRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.server.component.*;
import org.zicat.tributary.server.config.PropertiesConfigBuilder;
import org.zicat.tributary.server.config.PropertiesLoader;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

/** Server. */
public class Starter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Starter.class);
    private final ReadableConfig channelConfig;
    private final ReadableConfig sourceConfig;
    private final ReadableConfig sinkConfig;
    private final ReadableConfig serverConfig;
    protected transient ChannelComponent channelComponent;
    protected transient SinkComponent sinkComponent;
    protected transient SourceComponent sourceComponent;
    protected transient MetricsHttpServer metricsHttpServer;

    public Starter(Properties properties) {
        this.channelConfig = PropertiesConfigBuilder.channelConfig(properties);
        this.sourceConfig = PropertiesConfigBuilder.sourceConfig(properties);
        this.sinkConfig = PropertiesConfigBuilder.sinkConfig(properties);
        this.serverConfig = PropertiesConfigBuilder.serverConfig(properties);
    }

    /** open. this method will park current thread if open success. */
    public void start() throws InterruptedException, UnknownHostException {
        initComponent();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> IOUtils.closeQuietly(this)));
        LockSupport.park();
    }

    /** init component. */
    protected void initComponent() throws InterruptedException, UnknownHostException {
        final CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        this.metricsHttpServer = new MetricsHttpServer(registry, serverConfig);
        final String metricsHost = metricsHttpServer.metricHost();
        this.channelComponent = channelComponentFactory(metricsHost).create().register(registry);
        this.sinkComponent =
                sinkComponentFactory(metricsHost, channelComponent).create().register(registry);
        this.sourceComponent =
                sourceComponentFactory(metricsHost, channelComponent).create().register(registry);
        metricsHttpServer.start();
    }

    /**
     * create ChannelComponentFactory.
     *
     * @param metricsHost metricsHost
     * @return ChannelComponentFactory
     */
    protected ChannelComponentFactory channelComponentFactory(String metricsHost) {
        return new ChannelComponentFactory(channelConfig, metricsHost);
    }

    /**
     * create SinkComponentFactory.
     *
     * @param metricsHost metricsHost
     * @param channelComponent channelComponent
     * @return SinkComponentFactory
     */
    protected SinkComponentFactory sinkComponentFactory(
            String metricsHost, ChannelComponent channelComponent) {
        return new SinkComponentFactory(sinkConfig, channelComponent, metricsHost);
    }

    /**
     * create SourceComponentFactory.
     *
     * @param metricsHost metricsHost
     * @param channelComponent channelComponent
     * @return SourceComponentFactory
     */
    protected SourceComponentFactory sourceComponentFactory(
            String metricsHost, ChannelComponent channelComponent) {
        return new SourceComponentFactory(sourceConfig, channelComponent, metricsHost);
    }

    @Override
    public void close() {
        LOG.info("start to close tributary....");
        IOUtils.closeQuietly(metricsHttpServer);
        IOUtils.closeQuietly(sourceComponent);
        try {
            if (channelComponent != null) {
                channelComponent.flush();
            }
        } finally {
            IOUtils.closeQuietly(sinkComponent);
            IOUtils.closeQuietly(channelComponent);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (Starter starter = new Starter(new PropertiesLoader().load())) {
            starter.start();
        }
    }
}
