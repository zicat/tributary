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

package org.zicat.tributary.source.test.netty;

import static org.zicat.tributary.source.netty.NettyDecoder.lineDecoder;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import org.apache.commons.net.telnet.TelnetClient;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.memory.MemoryChannel;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.source.netty.ChannelHandler;
import org.zicat.tributary.source.netty.DefaultNettySource;
import org.zicat.tributary.source.netty.NettyDecoder;
import org.zicat.tributary.source.netty.ack.LengthAckHandler;
import org.zicat.tributary.source.netty.client.LengthDecoderClient;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** DefaultNettySourceTest. */
public class DefaultNettySourceTest {

    @Test
    public void testLineDecoder() throws Exception {
        final String groupId = "test_group";
        final DefaultChannel<MemoryChannel> channel =
                new DefaultChannel<>(
                        new DefaultChannel.AbstractChannelArrayFactory<MemoryChannel>() {
                            @Override
                            public String topic() {
                                return "t1";
                            }

                            @Override
                            public Set<String> groups() {
                                return Collections.singleton(groupId);
                            }

                            @Override
                            public MemoryChannel[] create() {
                                return MemoryChannelFactory.createChannels(
                                        "t1",
                                        1,
                                        Collections.singleton(groupId),
                                        1024 * 3,
                                        102400L,
                                        CompressionType.SNAPPY);
                            }
                        },
                        0);
        final GroupOffset groupOffset = channel.committedGroupOffset(groupId, 0);
        try (DefaultNettySource source =
                new DefaultNettySource(channel) {
                    @Override
                    protected void initChannel(SocketChannel ch, Channel channel) {
                        ch.pipeline().addLast(new IdleStateHandler(idleSecond, 0, 0));
                        ch.pipeline().addLast(lineDecoder.createSourceDecoder());
                        ch.pipeline()
                                .addLast(
                                        new ChannelHandler(
                                                channel, selectPartition(), LengthAckHandler::new));
                    }
                }) {

            source.start();

            final String data1 = "lynn";
            final TelnetClient telnet = new TelnetClient();
            try {
                telnet.connect("localhost", source.getPort());
                telnet.getOutputStream().write(data1.getBytes());
                telnet.getOutputStream().write("\r".getBytes());
                telnet.getOutputStream().write("zhangjun".getBytes());
                telnet.getOutputStream().write("\r".getBytes());
                telnet.getOutputStream().write("quit".getBytes());
                telnet.getOutputStream().write("\r".getBytes());
                telnet.getOutputStream().flush();
                // block reading 2 length response
                int totalCount = 0;
                int readCount;
                while (totalCount < 8
                        && (readCount = telnet.getInputStream().read(new byte[8])) != -1) {
                    totalCount += readCount;
                }
                Assert.assertEquals(8, totalCount);
            } finally {
                telnet.disconnect();
            }

            channel.flush();
            final RecordsResultSet recordsResultSet =
                    channel.poll(0, groupOffset, 10, TimeUnit.MILLISECONDS);
            Assert.assertEquals("lynn", new String(recordsResultSet.next()));
            Assert.assertEquals("zhangjun", new String(recordsResultSet.next()));
        }
    }

    @Test
    public void testLengthDecoder() throws Exception {

        final String groupId = "test_group";
        final DefaultChannel<MemoryChannel> channel =
                new DefaultChannel<>(
                        new DefaultChannel.AbstractChannelArrayFactory<MemoryChannel>() {
                            @Override
                            public String topic() {
                                return "t1";
                            }

                            @Override
                            public Set<String> groups() {
                                return Collections.singleton(groupId);
                            }

                            @Override
                            public MemoryChannel[] create() {
                                return MemoryChannelFactory.createChannels(
                                        "t1",
                                        1,
                                        Collections.singleton(groupId),
                                        1024 * 3,
                                        102400L,
                                        CompressionType.SNAPPY);
                            }
                        },
                        0);
        final GroupOffset groupOffset = channel.committedGroupOffset(groupId, 0);
        try (DefaultNettySource source =
                new DefaultNettySource(channel) {
                    @Override
                    protected NettyDecoder nettyDecoder() {
                        return NettyDecoder.lengthDecoder;
                    }
                }) {

            source.start();

            final byte[] data1 = "lyn".getBytes();
            final byte[] data2 = "zhangjun".getBytes();
            try (LengthDecoderClient client = new LengthDecoderClient(source.getPort())) {
                Assert.assertEquals(data1.length, client.append(data1));
                Assert.assertEquals(data2.length, client.append(data2));
            }
            channel.flush();

            final RecordsResultSet recordsResultSet =
                    channel.poll(0, groupOffset, 10, TimeUnit.MILLISECONDS);
            Assert.assertArrayEquals(data1, recordsResultSet.next());
            Assert.assertArrayEquals(data2, recordsResultSet.next());
        }
    }
}
