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

package org.zicat.tributary.service.source;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.zicat.tributary.channel.Channel;

/** DefaultTributaryServer. */
public class DefaultTributaryServer extends TributaryServer {

    protected final int idleSecond;

    public DefaultTributaryServer(
            String host, int port, int eventThreads, Channel channel, int idleSecond) {
        super(host, port, eventThreads, channel);
        this.idleSecond = idleSecond;
    }

    /**
     * init channel.
     *
     * @param ch ch
     */
    @Override
    protected void initChannel(SocketChannel ch, Channel channel) {
        ch.pipeline().addLast(new IdleStateHandler(idleSecond, 0, 0));
        ch.pipeline().addLast(new LengthDecoder());
        ch.pipeline().addLast(new FileChannelHandler(channel));
    }
}
