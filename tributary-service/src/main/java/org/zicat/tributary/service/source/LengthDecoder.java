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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/** LengthDecoder. */
public class LengthDecoder extends SourceDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(LengthDecoder.class);
    private static final String FAIL_RES_FORMAT =
            "failed response pong to host:{}, port:{}, cause:{}";

    @Override
    protected byte[] decode(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        final int readableBytes = byteBuf.readableBytes();
        final int headLength = headLength();
        if (readableBytes < headLength) {
            return null;
        }
        byteBuf.markReaderIndex();
        final int len = readHead(byteBuf);
        if (readableBytes - headLength < len) {
            byteBuf.resetReaderIndex();
            return null;
        }
        final byte[] bytes = new byte[len];
        byteBuf.readBytes(bytes);
        byteBuf.discardReadBytes();
        response(len, ctx);
        return bytes;
    }

    /**
     * response received data.
     *
     * @param len len
     * @param ctx ctx
     */
    protected void response(int len, ChannelHandlerContext ctx) {

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(4);
        byteBuf.writeInt(len);
        final ChannelFuture future = ctx.writeAndFlush(byteBuf);
        future.addListener(
                f -> {
                    if (!f.isSuccess()) {
                        final InetSocketAddress address =
                                (InetSocketAddress) ctx.channel().remoteAddress();
                        final String remoteHost = address.getAddress().getHostAddress();
                        final int remotePort = address.getPort();
                        LOG.warn(FAIL_RES_FORMAT, remoteHost, remotePort, f.cause());
                    }
                });
    }

    /**
     * read data length.
     *
     * @param byteBuf byteBuf
     * @return int length
     */
    protected int readHead(ByteBuf byteBuf) {
        return byteBuf.readInt();
    }

    /**
     * set head length.
     *
     * @return int
     */
    protected int headLength() {
        return 4;
    }
}
