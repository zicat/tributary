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

import io.netty.channel.ChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.server.component.*;
import org.zicat.tributary.server.config.PropertiesConfigBuilder;
import org.zicat.tributary.server.config.PropertiesLoader;
import org.zicat.tributary.server.metrics.TributaryCollectorRegistry;
import org.zicat.tributary.server.rest.DispatcherHttpHandlerBuilder;
import org.zicat.tributary.server.rest.HttpServer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

/** Starter. */
public class Starter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Starter.class);
    private final ReadableConfig channelConfig;
    private final ReadableConfig sourceConfig;
    private final ReadableConfig sinkConfig;
    private final ReadableConfig serverConfig;
    protected transient ChannelComponent channelComponent;
    protected transient SinkComponent sinkComponent;
    protected transient SourceComponent sourceComponent;
    protected transient HttpServer httpServer;

    public Starter(Properties properties) {
        this.channelConfig = PropertiesConfigBuilder.channelConfig(properties);
        this.sourceConfig = PropertiesConfigBuilder.sourceConfig(properties);
        this.sinkConfig = PropertiesConfigBuilder.sinkConfig(properties);
        this.serverConfig = PropertiesConfigBuilder.serverConfig(properties);
    }

    /** open. this method will park current thread if open success. */
    public void start() throws Exception {
        start0();
        LockSupport.park();
    }

    /**
     * start0 without park.
     *
     * @throws Exception Exception
     */
    protected void start0() throws Exception {
        this.httpServer = new HttpServer(serverConfig);
        final TributaryCollectorRegistry registry =
                new TributaryCollectorRegistry(httpServer.host());
        this.channelComponent = createChannelComponent(registry);
        this.sinkComponent = createSinkComponent(registry);
        this.sourceComponent = createSourceComponent(registry);
        httpServer.start(createDispatcherHttpHandler(registry));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> IOUtils.closeQuietly(this)));
    }

    @Override
    public void close() {
        LOG.info("start to close tributary....");
        IOUtils.closeQuietly(httpServer);
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

    /**
     * create Channel Component.
     *
     * @param registry registry
     * @return ChannelComponent
     */
    protected ChannelComponent createChannelComponent(TributaryCollectorRegistry registry) {
        return new ChannelComponentFactory(channelConfig, registry).create();
    }

    /**
     * create Sink Component.
     *
     * @param registry registry
     * @return SinkComponent
     */
    protected SinkComponent createSinkComponent(TributaryCollectorRegistry registry) {
        if (channelComponent == null) {
            throw new IllegalStateException("channel component is null");
        }
        return new SinkComponentFactory(sinkConfig, channelComponent, registry).create();
    }

    /**
     * create Source Component.
     *
     * @param registry registry
     * @return SourceComponent
     */
    protected SourceComponent createSourceComponent(TributaryCollectorRegistry registry) {
        if (channelComponent == null) {
            throw new IllegalStateException("channel component is null");
        }
        return new SourceComponentFactory(sourceConfig, channelComponent, registry).create();
    }

    /**
     * create Dispatcher Http Handler.
     *
     * @param registry registry
     * @return ChannelHandler
     */
    protected ChannelHandler createDispatcherHttpHandler(TributaryCollectorRegistry registry) {
        if (channelComponent == null) {
            throw new IllegalStateException("channel component is null");
        }
        return new DispatcherHttpHandlerBuilder(serverConfig)
                .metricCollectorRegistry(registry)
                .channelComponent(channelComponent)
                .build();
    }

    public static void main(String[] args) throws Exception {
        try (Starter starter = new Starter(new PropertiesLoader().load())) {
            starter.start();
        }
    }
}
