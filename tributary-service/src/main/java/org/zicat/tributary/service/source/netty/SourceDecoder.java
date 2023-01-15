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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/** SourceDecoder. */
public abstract class SourceDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(SourceDecoder.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        final InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
        final String remoteHost = address.getAddress().getHostAddress();
        final int remotePort = address.getPort();
        LOG.info("channel active, client is {}:{}", remoteHost, remotePort);
        ctx.fireChannelActive();
    }

    /**
     * decode data.
     *
     * @param ctx ctx.
     * @param in in
     * @return byte, return null if not ready
     */
    protected abstract byte[] decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception;

    @Override
    protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        final byte[] data = decode(ctx, in);
        if (data != null) {
            out.add(data);
        }
    }
}
