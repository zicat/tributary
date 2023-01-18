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

package org.zicat.tributary.service.source.netty;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.zicat.tributary.channel.Channel;

import java.util.Map;

import static org.zicat.tributary.service.source.netty.NettyDecoder.lengthDecoder;

/** DefaultTributaryServerFactory. */
public class NettyTributaryServerFactory extends AbstractTributaryServerFactory {

    public static final String KEY_NETTY_IDLE_SECOND = "netty.idle.second";
    public static final String DEFAULT_NETTY_IDLE_SECOND = "120";

    public static final String KEY_NETTY_DECODER = "netty.decoder";

    @Override
    public String identity() {
        return "netty";
    }

    @Override
    public AbstractTributaryServer createAbstractTributaryServer(
            String host, int port, int eventThreads, Channel channel, Map<String, String> config) {
        final int idleSecond =
                Integer.parseInt(
                        config.getOrDefault(KEY_NETTY_IDLE_SECOND, DEFAULT_NETTY_IDLE_SECOND));

        final NettyDecoder nettyDecoder =
                config.get(KEY_NETTY_DECODER) == null
                        ? lengthDecoder
                        : NettyDecoder.valueOf(config.get(KEY_NETTY_DECODER));

        return new NettyTributaryServer(host, port, eventThreads, channel, idleSecond) {
            @Override
            protected void initChannel(SocketChannel ch, Channel channel) {
                ch.pipeline().addLast(new IdleStateHandler(idleSecond, 0, 0));
                ch.pipeline().addLast(nettyDecoder.createSourceDecoder());
                ch.pipeline().addLast(new FileChannelHandler(channel));
            }
        };
    }
}
