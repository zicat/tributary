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

package org.zicat.tributary.source.base.test.netty.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.source.base.netty.handler.LengthDecoder;

import java.nio.charset.StandardCharsets;

/** LengthDecoderTest. */
public class LengthDecoderTest {

    @Test
    public void testSplitHeader() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(new LengthDecoder());
        final byte[] data = "lynn".getBytes(StandardCharsets.UTF_8);
        try {
            Assert.assertFalse(channel.writeInbound(Unpooled.buffer().writeShort(0)));
            Assert.assertNull(channel.readInbound());

            Assert.assertFalse(channel.writeInbound(Unpooled.buffer().writeShort(data.length)));
            Assert.assertNull(channel.readInbound());

            Assert.assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(data)));
            Assert.assertArrayEquals(data, (byte[]) channel.readInbound());
            Assert.assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testSplitBody() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(new LengthDecoder());
        final byte[] data = "zhangjun".getBytes(StandardCharsets.UTF_8);
        try {
            Assert.assertFalse(
                    channel.writeInbound(
                            Unpooled.buffer().writeInt(data.length).writeBytes(data, 0, 3)));
            Assert.assertNull(channel.readInbound());

            Assert.assertTrue(
                    channel.writeInbound(Unpooled.wrappedBuffer(data, 3, data.length - 3)));
            Assert.assertArrayEquals(data, (byte[]) channel.readInbound());
            Assert.assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testCompleteFrameFollowedByPartialFrame() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(new LengthDecoder());
        final byte[] first = "lynn".getBytes(StandardCharsets.UTF_8);
        final byte[] second = "zhangjun".getBytes(StandardCharsets.UTF_8);
        final byte[] third = "tributary".getBytes(StandardCharsets.UTF_8);
        try {
            Assert.assertTrue(
                    channel.writeInbound(
                            Unpooled.buffer()
                                    .writeInt(first.length)
                                    .writeBytes(first)
                                    .writeInt(second.length)
                                    .writeBytes(second, 0, 3)));
            Assert.assertArrayEquals(first, (byte[]) channel.readInbound());
            Assert.assertNull(channel.readInbound());

            Assert.assertTrue(
                    channel.writeInbound(
                            Unpooled.buffer()
                                    .writeBytes(second, 3, second.length - 3)
                                    .writeInt(third.length)
                                    .writeBytes(third)));
            Assert.assertArrayEquals(second, (byte[]) channel.readInbound());
            Assert.assertArrayEquals(third, (byte[]) channel.readInbound());
            Assert.assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
