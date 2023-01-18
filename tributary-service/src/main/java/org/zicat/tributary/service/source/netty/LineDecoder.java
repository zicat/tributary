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
import io.netty.handler.codec.LineBasedFrameDecoder;

/** LineDecoder. */
public class LineDecoder extends SourceDecoder {

    private final NettyLineDecoderWrapper wrapper = new NettyLineDecoderWrapper();

    @Override
    protected byte[] decode(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
        final ByteBuf frame = (ByteBuf) wrapper.decode(ctx, byteBuf);
        if (frame == null) {
            return null;
        }
        final int length = frame.readableBytes();
        final byte[] result = new byte[length];
        frame.readBytes(result);
        frame.discardReadBytes();
        if (clientQuit(result)) {
            response(-1, ctx);
            ctx.close();
            return null;
        }
        response(length, ctx);
        return result;
    }

    /**
     * boolean quit.
     *
     * @param result result
     * @return boolean
     */
    protected boolean clientQuit(final byte[] result) {
        return (result.length == 1 && result[0] == 27)
                || (result.length == 4 && new String(result).equals("quit"));
    }

    /** NettyLineDecoderWrapper. */
    private static class NettyLineDecoderWrapper extends LineBasedFrameDecoder {

        public NettyLineDecoderWrapper() {
            super(10240);
        }

        @Override
        public Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
            return super.decode(ctx, buffer);
        }
    }
}
