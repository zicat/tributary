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

package org.zicat.tributary.source.base.netty;

import io.netty.bootstrap.AbstractBootstrap;
import static org.zicat.tributary.common.util.HostUtils.getHostAddresses;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.exception.TributaryRuntimeException;
import org.zicat.tributary.source.base.AbstractSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** NettySource. */
public abstract class NettySource extends AbstractSource {

    private static final Logger LOG = LoggerFactory.getLogger(NettySource.class);
    private static final String OPTION_PREFIX = "netty.option.";
    private static final String CHILD_OPTION_PREFIX = "netty.child-option.";

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final int port;
    protected final int eventThreads;
    protected final EventLoopGroup bossGroup;
    protected final EventLoopGroup workGroup;
    protected final ServerBootstrap serverBootstrap;
    protected final PipelineInitialization pipelineInitialization;
    protected final List<Channel> channelList;
    protected final List<String> hostNames;

    public NettySource(
            String sourceId,
            ReadableConfig config,
            org.zicat.tributary.channel.Channel channel,
            String hostPattern,
            int port,
            int eventThreads)
            throws Exception {
        super(sourceId, config, channel);
        this.port = port;
        this.eventThreads = eventThreads;
        this.hostNames = getHostAddresses(hostPattern);
        try {
            this.bossGroup = createEventLoopGroup(Math.max(1, eventThreads / 4));
            this.workGroup = createEventLoopGroup(eventThreads);
            this.serverBootstrap =
                    createServerBootstrap(
                            bossGroup,
                            workGroup,
                            config.filterAndRemovePrefixKey(OPTION_PREFIX).toProperties(),
                            config.filterAndRemovePrefixKey(CHILD_OPTION_PREFIX).toProperties());
            this.pipelineInitialization = createPipelineInitialization();
            this.serverBootstrap.childHandler(
                    new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            pipelineInitialization.init(ch);
                        }
                    });
            this.channelList = createChannelList();
        } catch (Exception e) {
            IOUtils.closeQuietly(this);
            throw e;
        }
    }

    /**
     * get netty decoder.
     *
     * @return NettyDecoder
     */
    protected abstract PipelineInitialization createPipelineInitialization() throws Exception;

    /**
     * create channel by host.
     *
     * @param host host
     */
    private Channel createChannel(String host) throws InterruptedException {
        final ChannelFuture syncFuture =
                host == null
                        ? serverBootstrap.bind(port).sync()
                        : serverBootstrap.bind(host, port).sync();
        if (!syncFuture.isSuccess()) {
            throw new TributaryRuntimeException(
                    "TcpServer started fail on port " + port, syncFuture.cause());
        }
        LOG.info("TcpServer started on {}:{}", prettyHost(host), port);
        return syncFuture.channel();
    }

    /**
     * init server channel list.
     *
     * @return channel list
     */
    private List<Channel> createChannelList() throws InterruptedException {
        final List<Channel> channelList = new ArrayList<>();
        if (hostNames.isEmpty()) {
            channelList.add(createChannel(null));
            return channelList;
        }
        for (String h : hostNames) {
            channelList.add(createChannel(h));
        }
        return channelList;
    }

    /**
     * create server bootstrap.
     *
     * @param bossGroup bossGroup
     * @param workGroup workGroup
     * @return ServerBootstrap
     */
    public static ServerBootstrap createServerBootstrap(
            EventLoopGroup bossGroup,
            EventLoopGroup workGroup,
            Properties properties,
            Properties childProperties)
            throws IllegalAccessException {
        final ServerBootstrap serverBootstrap = new ServerBootstrap();
        final Class<? extends ServerChannel> channelClass =
                SystemUtils.IS_OS_LINUX
                        ? EpollServerSocketChannel.class
                        : NioServerSocketChannel.class;
        serverBootstrap.group(bossGroup, workGroup).channel(channelClass);
        nettyOptions(serverBootstrap, properties);
        nettyChildOptions(serverBootstrap, childProperties);
        return serverBootstrap;
    }

    /**
     * create server bootstrap.
     *
     * @param bossGroup bossGroup
     * @param workGroup workGroup
     * @return ServerBootstrap
     */
    public static ServerBootstrap createServerBootstrap(
            EventLoopGroup bossGroup, EventLoopGroup workGroup) throws IllegalAccessException {
        return createServerBootstrap(bossGroup, workGroup, new Properties(), new Properties());
    }

    /**
     * create boss groups.
     *
     * @return EventLoopGroup
     */
    public static EventLoopGroup createEventLoopGroup(int nThreads) {
        return SystemUtils.IS_OS_LINUX
                ? new EpollEventLoopGroup(nThreads)
                : new NioEventLoopGroup(nThreads);
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            closeServerChannelsAndGroup();
        } finally {
            try {
                flush();
            } finally {
                IOUtils.closeQuietly(pipelineInitialization);
            }
        }
    }

    private void closeServerChannelsAndGroup() {
        if (channelList != null) {
            channelList.forEach(NettySource::closeChannelSync);
            LOG.info("close netty source, id:{}, port:{}", sourceId, port);
        }
        if (workGroup != null) {
            workGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }

    /**
     * pretty host.
     *
     * @param host host
     * @return String
     */
    private String prettyHost(String host) {
        return host == null || host.isEmpty() ? "*" : host;
    }

    /**
     * get port.
     *
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * get event threads.
     *
     * @return threads
     */
    public int getEventThreads() {
        return eventThreads;
    }

    /**
     * close channel sync.
     *
     * @param channel channel
     */
    private static void closeChannelSync(Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close().sync();
        } catch (Exception e) {
            LOG.warn("wait close channel sync fail", e);
        }
    }

    /**
     * set netty options.
     *
     * @param bootstrap bootstrap
     * @param properties properties
     * @throws IllegalAccessException IllegalAccessException
     */
    public static void nettyOptions(ServerBootstrap bootstrap, Properties properties)
            throws IllegalAccessException {
        nettyOptions(bootstrap, properties, AbstractBootstrap::option);
    }

    /**
     * set netty child options.
     *
     * @param bootstrap bootstrap
     * @param properties properties
     * @throws IllegalAccessException IllegalAccessException
     */
    public static void nettyChildOptions(ServerBootstrap bootstrap, Properties properties)
            throws IllegalAccessException {
        nettyOptions(bootstrap, properties, ServerBootstrap::childOption);
    }

    @SuppressWarnings("unchecked")
    private static void nettyOptions(
            ServerBootstrap bootstrap, Properties props, OptionConsumer consumer)
            throws IllegalAccessException {
        final Map<String, Field> fieldNameMapping =
                Arrays.stream(ChannelOption.class.getDeclaredFields())
                        .collect(Collectors.toMap(Field::getName, field -> field));
        for (String optionName : props.stringPropertyNames()) {
            final Field field = fieldNameMapping.get(optionName);
            if (field == null) {
                LOG.warn("ChannelOption {} not exists, ignore it.", optionName);
                continue;
            }
            final Object option = field.get(null);
            if (!(option instanceof ChannelOption)) {
                continue;
            }
            final ChannelOption<Object> channelOption = (ChannelOption<Object>) option;
            final Type type =
                    ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
            final String value = props.get(optionName).toString().trim();
            if (type == Integer.class) {
                consumer.consume(bootstrap, channelOption, Integer.parseInt(value));
            } else if (type == Boolean.class) {
                consumer.consume(bootstrap, channelOption, Boolean.parseBoolean(value));
            } else if (type == Long.class) {
                consumer.consume(bootstrap, channelOption, Long.parseLong(value));
            } else if (type == Double.class) {
                consumer.consume(bootstrap, channelOption, Double.parseDouble(value));
            } else if (type == Float.class) {
                consumer.consume(bootstrap, channelOption, Float.parseFloat(value));
            } else if (type == Short.class) {
                consumer.consume(bootstrap, channelOption, Short.parseShort(value));
            } else if (type == String.class) {
                consumer.consume(bootstrap, channelOption, value);
            } else {
                LOG.warn("ChannelOption {} type {} not support, ignore it.", optionName, type);
            }
        }
    }

    /** OptionConsumer. */
    public interface OptionConsumer {

        /**
         * consume option.
         *
         * @param bootstrap bootstrap
         * @param channelOption channelOption
         * @param value value
         */
        void consume(ServerBootstrap bootstrap, ChannelOption<Object> channelOption, Object value);
    }
}
