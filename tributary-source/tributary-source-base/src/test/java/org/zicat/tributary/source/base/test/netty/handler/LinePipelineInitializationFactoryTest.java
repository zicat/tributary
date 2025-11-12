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

import static org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils.memoryChannelFactory;
import static org.zicat.tributary.channel.test.ChannelBaseTest.readChannel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.config.ReadableConfigConfigBuilder;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.base.netty.NettySource;
import org.zicat.tributary.source.base.netty.pipeline.LinePipelineInitializationFactory;
import static org.zicat.tributary.source.base.netty.pipeline.LinePipelineInitializationFactory.OPTION_LINE_WORKER_THREADS;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;
import org.zicat.tributary.source.base.test.netty.NettySourceMock;

import java.util.List;

/** LinePipelineInitializationFactoryTest. */
public class LinePipelineInitializationFactoryTest {

    @Test
    public void test() throws Exception {
        final PipelineInitializationFactory factory =
                SpiFactory.findFactory(
                        LinePipelineInitializationFactory.IDENTITY,
                        PipelineInitializationFactory.class);
        final String groupId = "g1";
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        final ReadableConfig config =
                new ReadableConfigConfigBuilder().addConfig(OPTION_LINE_WORKER_THREADS, -1).build();
        try (Channel channel = memoryChannelFactory(groupId).createChannel("t1", null);
                NettySource source = new NettySourceMock(config, channel)) {
            final PipelineInitialization pipelineInitialization =
                    factory.createPipelineInitialization(source);
            pipelineInitialization.init(embeddedChannel);
            final ByteBuf byteBuf = embeddedChannel.alloc().buffer();
            byteBuf.writeBytes("lynn".getBytes());
            byteBuf.writeBytes(System.lineSeparator().getBytes());
            byteBuf.writeBytes("zhangjun".getBytes());
            byteBuf.writeBytes(System.lineSeparator().getBytes());
            byteBuf.writeBytes("quit".getBytes());
            byteBuf.writeBytes(System.lineSeparator().getBytes());
            embeddedChannel.writeInbound(byteBuf);
            channel.flush();
            final Offset offset = Offset.ZERO;
            final List<byte[]> data = readChannel(channel, 0, offset, 2).data;

            Assert.assertEquals(
                    "lynn", new String(Records.parse(data.get(0)).iterator().next().value()));
            Assert.assertEquals(
                    "zhangjun", new String(Records.parse(data.get(1)).iterator().next().value()));
        }
    }
}
