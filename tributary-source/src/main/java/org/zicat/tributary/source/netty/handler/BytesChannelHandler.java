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

package org.zicat.tributary.source.netty.handler;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.netty.AbstractNettySource;
import org.zicat.tributary.source.utils.SourceHeaders;

import java.io.IOException;
import java.util.Collections;

import static org.zicat.tributary.common.records.RecordsUtils.createBytesRecords;

/** ChannelHandler. */
public abstract class BytesChannelHandler extends SimpleChannelInboundHandler<byte[]> {

    private final AbstractNettySource source;
    private final int partition;

    public BytesChannelHandler(AbstractNettySource source, int partition) {
        this.source = source;
        this.partition = partition;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, byte[] packet)
            throws IOException, InterruptedException {
        final long receivedTs = System.currentTimeMillis();
        final Records records =
                createBytesRecords(
                        source.sourceId(),
                        SourceHeaders.sourceHeaders(receivedTs),
                        Collections.singletonList(packet));
        source.append(partition, records);
        ackSuccess(packet, ctx);
    }

    /**
     * ack success to client.
     *
     * @param receivedData receivedData
     * @param ctx ctx
     */
    public abstract void ackSuccess(byte[] receivedData, ChannelHandlerContext ctx);

    /** MuteBytesChannelHandler. */
    public static class MuteBytesChannelHandler extends BytesChannelHandler {

        public MuteBytesChannelHandler(AbstractNettySource source, int partition) {
            super(source, partition);
        }

        @Override
        public void ackSuccess(byte[] receivedData, ChannelHandlerContext ctx) {}
    }

    /** LengthResponseBytesChannelHandler. */
    public static class LengthResponseBytesChannelHandler extends BytesChannelHandler {

        public LengthResponseBytesChannelHandler(AbstractNettySource source, int partition) {
            super(source, partition);
        }

        @Override
        public void ackSuccess(byte[] receivedData, ChannelHandlerContext ctx) {
            ctx.writeAndFlush(ByteBufAllocator.DEFAULT.buffer(4).writeInt(receivedData.length));
        }
    }
}
