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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/** LengthDecoder. */
public class LengthDecoder extends SourceDecoder {

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
        return bytes;
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
