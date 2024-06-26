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

package org.zicat.tributary.service.test.component;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.service.component.DynamicChannel;
import org.zicat.tributary.service.component.DynamicSinkGroupManager;
import org.zicat.tributary.service.component.DynamicSource;
import org.zicat.tributary.service.configuration.ChannelConfiguration;
import org.zicat.tributary.service.configuration.SinkGroupManagerConfiguration;
import org.zicat.tributary.service.configuration.SourceConfiguration;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.function.CollectionFunction;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

import static org.zicat.tributary.common.IOUtils.writeData;

/** DynamicSourceTest. */
@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = {DynamicChannel.class, DynamicSinkGroupManager.class, DynamicSource.class})
@EnableConfigurationProperties({
    ChannelConfiguration.class,
    SinkGroupManagerConfiguration.class,
    SourceConfiguration.class
})
@ActiveProfiles("source-test")
public class DynamicSourceTest {

    @Autowired DynamicChannel dynamicChannel;
    @Autowired DynamicSinkGroupManager dynamicSinkGroupManager;

    @After
    public void after() throws IOException {
        dynamicChannel.flushAll();
        IOUtils.closeQuietly(dynamicSinkGroupManager, dynamicChannel);
        cleanup();
    }

    @Before
    public void before() throws IOException {
        cleanup();
    }

    /**
     * clean up.
     *
     * @throws IOException IOException
     */
    private void cleanup() throws IOException {
        IOUtils.deleteDir(new File("tributary_source_test").getCanonicalFile());
    }

    @Test
    public void test() throws IOException {

        final byte[] data1 = "lyn".getBytes();

        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(57132))) {
            writeData(channel, data1);
        }
        final byte[] data2 = "zicat".getBytes();
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(57133))) {
            writeData(channel, data2);
        }
        dynamicChannel.flushAll();
        final Map<String, List<SinkGroupManager>> sinkGroupManagerMap =
                dynamicSinkGroupManager.getSinkGroupManagerMap();
        final List<SinkGroupManager> sinkGroupManagers = sinkGroupManagerMap.get("group_1");
        for (SinkGroupManager sinkGroupManager : sinkGroupManagers) {
            IOUtils.closeQuietly(sinkGroupManager);
            final CollectionFunction collectionFunction =
                    (CollectionFunction) sinkGroupManager.getFunctions().get(0).get(0);
            Assert.assertEquals(1, collectionFunction.history.size());
            if (sinkGroupManager.topic().equals("c1")) {
                Assert.assertArrayEquals(
                        "lyn".getBytes(),
                        collectionFunction.history.get(0).iterator().next().value());
            } else {
                Assert.assertArrayEquals(
                        "zicat".getBytes(),
                        collectionFunction.history.get(0).iterator().next().value());
            }
        }
    }
}
