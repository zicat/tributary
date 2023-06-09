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
import org.zicat.tributary.channel.AbstractChannel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.DefaultChannel;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.handler.MultiThreadPartitionHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.zicat.tributary.sink.handler.MultiThreadPartitionHandler.OPTION_THREADS;

/** DisruptorMultiSinkHandlerTest. */
public class MultiThreadPartitionHandlerTest {

    @Test
    public void testThreadCount() throws IOException {
        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder().functionIdentity("dummy");
        int threads = 0;
        builder.addCustomProperty(OPTION_THREADS.key(), threads);
        MultiThreadPartitionHandler handler =
                new MultiThreadPartitionHandler(
                        "g1",
                        new DefaultChannel<>(
                                new DefaultChannel.AbstractChannelArrayFactory<
                                        AbstractChannel<?>>() {
                                    @Override
                                    public String topic() {
                                        return "t1";
                                    }

                                    @Override
                                    public Set<String> groups() {
                                        return Collections.singleton("g1");
                                    }

                                    @Override
                                    public AbstractChannel<?>[] create() {
                                        return MemoryChannelFactory.createChannels(
                                                "t1",
                                                1,
                                                Collections.singleton("g1"),
                                                1024 * 3,
                                                102400L,
                                                CompressionType.SNAPPY);
                                    }
                                },
                                0),
                        0,
                        builder.build());
        try {
            handler.open();
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(true);
        }

        threads = 10;
        builder.addCustomProperty(OPTION_THREADS.key(), threads);
        handler =
                new MultiThreadPartitionHandler(
                        "g1",
                        new DefaultChannel<>(
                                new DefaultChannel.AbstractChannelArrayFactory<
                                        AbstractChannel<?>>() {
                                    @Override
                                    public String topic() {
                                        return "t1";
                                    }

                                    @Override
                                    public Set<String> groups() {
                                        return Collections.singleton("g1");
                                    }

                                    @Override
                                    public AbstractChannel<?>[] create() {
                                        return MemoryChannelFactory.createChannels(
                                                "t1",
                                                1,
                                                Collections.singleton("g1"),
                                                1024 * 3,
                                                102400L,
                                                CompressionType.SNAPPY);
                                    }
                                },
                                0),
                        0,
                        builder.build());
        handler.open();
        Assert.assertEquals(threads, handler.handlers().length);
        IOUtils.closeQuietly(handler);
    }

    @Test
    public void testFunctionId() throws IOException {
        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder().functionIdentity("dummy");
        final int threads = 4;
        builder.addCustomProperty(OPTION_THREADS.key(), threads);
        MultiThreadPartitionHandler handler =
                new MultiThreadPartitionHandler(
                        "g1",
                        new DefaultChannel<>(
                                new DefaultChannel.AbstractChannelArrayFactory<
                                        AbstractChannel<?>>() {
                                    @Override
                                    public String topic() {
                                        return "t1";
                                    }

                                    @Override
                                    public Set<String> groups() {
                                        return Collections.singleton("g1");
                                    }

                                    @Override
                                    public AbstractChannel<?>[] create() {
                                        return MemoryChannelFactory.createChannels(
                                                "t1",
                                                1,
                                                Collections.singleton("g1"),
                                                1024 * 3,
                                                102400L,
                                                CompressionType.SNAPPY);
                                    }
                                },
                                0),
                        0,
                        builder.build());
        handler.open();
        Assert.assertEquals(threads, handler.handlers().length);
        Set<String> distinctIds = new HashSet<>();
        for (MultiThreadPartitionHandler.DataHandler dataHandler : handler.handlers()) {
            distinctIds.add(dataHandler.functionId());
        }
        Assert.assertEquals(threads, distinctIds.size());
        IOUtils.closeQuietly(handler);
    }
}
