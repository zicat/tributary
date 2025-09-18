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

package org.zicat.tributary.sink.test.handler;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.function.PrintFunctionFactory;
import org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory;
import org.zicat.tributary.sink.handler.DirectPartitionHandler;
import org.zicat.tributary.sink.handler.MultiThreadPartitionHandler;
import org.zicat.tributary.sink.handler.PartitionHandler;

import java.io.IOException;

/** DefaultPartitionHandlerFactoryTest. */
public class DefaultPartitionHandlerFactoryTest {

    @Test
    public void test() throws IOException {
        final String groupId = "g1";
        final String topic = "t1";
        try (Channel channel = MemoryChannelTestUtils.createChannel(topic, groupId)) {
            final DefaultPartitionHandlerFactory factory = new DefaultPartitionHandlerFactory();
            final SinkGroupConfigBuilder builder =
                    SinkGroupConfigBuilder.newBuilder()
                            .functionIdentity(PrintFunctionFactory.IDENTITY);
            builder.addCustomProperty(DefaultPartitionHandlerFactory.OPTION_THREADS, 1);
            try (PartitionHandler handler =
                    factory.createHandler(groupId, channel, 0, builder.build())) {
                handler.open();
                Assert.assertEquals(DirectPartitionHandler.class, handler.getClass());
            }

            builder.addCustomProperty(DefaultPartitionHandlerFactory.OPTION_THREADS, 2);
            try (PartitionHandler handler =
                    factory.createHandler(groupId, channel, 0, builder.build())) {
                handler.open();
                Assert.assertEquals(MultiThreadPartitionHandler.class, handler.getClass());
            }
        }
    }
}
