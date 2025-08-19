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

package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import org.junit.Assert;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** AckDecoder. */
public class AckDecoder extends ByteToMessageDecoder {

    private final AtomicBoolean clientReceivedAck;
    private final byte expectedVersion;
    private final int seq;

    public AckDecoder(AtomicBoolean clientReceivedAck, byte expectedVersion, int seq) {
        this.clientReceivedAck = clientReceivedAck;
        this.expectedVersion = expectedVersion;
        this.seq = seq;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        clientReceivedAck.set(true);
        Assert.assertEquals(expectedVersion, in.readByte());
        Assert.assertEquals('A', in.readByte());
        Assert.assertEquals(seq, in.readInt());
    }
}
