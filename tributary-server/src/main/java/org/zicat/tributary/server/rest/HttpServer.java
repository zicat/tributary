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

package org.zicat.tributary.server.rest;

import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.ReadableConfig;
import static org.zicat.tributary.source.base.netty.NettySource.*;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.stream.ChunkedWriteHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/** HttpServer. */
public class HttpServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);

    public static final int MAX_CONTENT_LENGTH = 10240;
    public static final ConfigOption<Integer> OPTION_PORT =
            ConfigOptions.key("port").integerType().defaultValue(9090);
    public static final ConfigOption<Integer> OPTION_THREADS =
            ConfigOptions.key("worker-threads").integerType().defaultValue(1);

    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private transient Channel channel;

    public HttpServer(ReadableConfig serverConfig) {
        this.port = serverConfig.get(OPTION_PORT);
        final int threads = serverConfig.get(OPTION_THREADS);
        this.bossGroup = createEventLoopGroup(Math.max(1, threads / 4));
        this.workerGroup = createEventLoopGroup(threads);
    }

    /** start. */
    public void start(ChannelHandler dispatcherHttpHandler)
            throws InterruptedException, IllegalAccessException {
        final ServerBootstrap b = createServerBootstrap(bossGroup, workerGroup);
        b.childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpRequestDecoder());
                        ch.pipeline().addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
                        ch.pipeline().addLast(new HttpResponseEncoder());
                        ch.pipeline().addLast(new ChunkedWriteHandler());
                        ch.pipeline().addLast(dispatcherHttpHandler);
                    }
                });
        this.channel = b.bind(port).sync().channel();
        LOG.info("MetricHttpServer started on *:{}", port);
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close().sync();
                LOG.info("close metric http server listen {}", port);
            }
        } catch (InterruptedException ignored) {
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
