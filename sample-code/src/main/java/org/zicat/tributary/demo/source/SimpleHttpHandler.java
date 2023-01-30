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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.zicat.tributary.channel.Channel;

import java.io.IOException;
import java.util.Random;

/** SimpleHttpHandler. */
public class SimpleHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    final Random random = new Random();
    private final Channel channel;

    public SimpleHttpHandler(Channel channel) {
        this.channel = channel;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws IOException {
        // read body and append to file channel
        final ByteBuf byteBuf = msg.content();
        final byte[] bytes = new byte[byteBuf.readableBytes()];
        final int readerIndex = byteBuf.readerIndex();
        byteBuf.getBytes(readerIndex, bytes);
        byteBuf.discardReadBytes();
        channel.append(random.nextInt(channel.partition()), bytes);

        // response
        final String resBody = "response, length " + bytes.length;
        FullHttpResponse response =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1,
                        HttpResponseStatus.OK,
                        Unpooled.wrappedBuffer(resBody.getBytes()));
        response.headers()
                .add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN + "; charset=UTF-8")
                .add(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
                .add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        ctx.write(response);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
