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

package org.zicat.tributary.demo.source;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.source.netty.AbstractNettyTributaryServer;

/** HttpTributaryServer. */
public class HttpTributaryServer extends AbstractNettyTributaryServer {

    public HttpTributaryServer(String host, int port, int eventThreads, Channel channel) {
        super(host, port, eventThreads, channel);
    }

    /**
     * init channel.
     *
     * @param ch ch
     */
    @Override
    protected void initChannel(SocketChannel ch, Channel channel) {
        final ChannelPipeline pip = ch.pipeline();
        pip.addLast(new HttpResponseEncoder());
        pip.addLast(new HttpRequestDecoder());
        pip.addLast(new HttpObjectAggregator(512 * 1024));
        pip.addLast(new SimpleHttpHandler(channel));
    }
}
