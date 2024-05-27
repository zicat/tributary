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

package org.zicat.tributary.source.test.netty.length;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.source.netty.DefaultNettySource;
import org.zicat.tributary.source.netty.PipelineInitialization;
import org.zicat.tributary.source.netty.PipelineInitializationFactory;
import org.zicat.tributary.source.netty.length.LengthPipelineInitializationFactory;
import org.zicat.tributary.source.test.netty.ChannelTestUtils;

import java.util.concurrent.TimeUnit;

/** LengthPipelineInitializationFactoryTest. */
public class LengthPipelineInitializationFactoryTest {

    @SuppressWarnings("VulnerableCodeUsages")
    @Test
    public void test() throws Exception {
        final PipelineInitializationFactory factory =
                SpiFactory.findFactory(
                        LengthPipelineInitializationFactory.IDENTITY,
                        PipelineInitializationFactory.class);
        final String groupId = "g1";
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        try (Channel channel =
                        ChannelTestUtils.memoryChannelFactory(groupId).createChannel("t1", null);
                DefaultNettySource source = new DefaultNettySource(channel)) {
            final PipelineInitialization pipelineInitialization =
                    factory.createPipelineInitialization(source);
            pipelineInitialization.init(embeddedChannel.pipeline());
            final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
            final String s1 = "lynn";
            byteBuf.writeInt(s1.getBytes().length);
            byteBuf.writeBytes(s1.getBytes());

            final String s2 = "zhangjun";
            byteBuf.writeInt(s2.getBytes().length);
            byteBuf.writeBytes(s2.getBytes());

            embeddedChannel.writeInbound(byteBuf);

            channel.flush();
            final GroupOffset groupOffset = new GroupOffset(0L, 0L, groupId);
            final RecordsResultSet recordsResultSet =
                    channel.poll(0, groupOffset, 10, TimeUnit.MILLISECONDS);
            Assert.assertEquals("lynn", new String(recordsResultSet.next()));
            Assert.assertEquals("zhangjun", new String(recordsResultSet.next()));
        }
    }
}
