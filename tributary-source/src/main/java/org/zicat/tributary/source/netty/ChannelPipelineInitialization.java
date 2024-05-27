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

package org.zicat.tributary.source.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import org.zicat.tributary.channel.Channel;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/** ChannelPipelineInitialization. */
public abstract class ChannelPipelineInitialization implements PipelineInitialization {

    protected final AtomicInteger count = new AtomicInteger();
    protected final Channel channel;
    protected final AbstractNettySource source;

    public ChannelPipelineInitialization(AbstractNettySource source) {
        this.source = source;
        this.channel = source.getChannel();
    }

    /**
     * select partition id.
     *
     * @return partition id
     */
    protected int selectPartition() {
        return (count.getAndIncrement() & 0x7fffffff) % channel.partition();
    }

    /**
     * create length channel handler.
     *
     * @return ChannelHandler
     */
    protected ChannelHandler createLengthChannelHandler() {
        return new ChannelHandler(source, selectPartition()) {
            @Override
            public void ackSuccess(byte[] receivedData, ChannelHandlerContext ctx) {
                ctx.writeAndFlush(ByteBufAllocator.DEFAULT.buffer(4).writeInt(receivedData.length));
            }
        };
    }

    /**
     * create mute channel handler.
     *
     * @return ChannelHandler
     */
    protected ChannelHandler createMuteChannelHandler() {
        return new ChannelHandler(source, selectPartition()) {
            @Override
            public void ackSuccess(byte[] receivedData, ChannelHandlerContext ctx) {}
        };
    }

    @Override
    public void close() throws IOException {}
}
