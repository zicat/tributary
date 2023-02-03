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
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.service.component.DynamicChannel;
import org.zicat.tributary.service.component.DynamicSinkGroupManager;
import org.zicat.tributary.service.configuration.ChannelConfiguration;
import org.zicat.tributary.service.configuration.SinkGroupManagerConfiguration;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.function.CollectionFunction;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** DynamicSinkGroupManagerTest. */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {DynamicChannel.class, DynamicSinkGroupManager.class})
@EnableConfigurationProperties({ChannelConfiguration.class, SinkGroupManagerConfiguration.class})
@ActiveProfiles("sink-test")
public class DynamicSinkGroupManagerTest {

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
        IOUtils.deleteDir(new File("tributary_sink_test").getCanonicalFile());
    }

    @Test
    public void test() throws IOException, InterruptedException {
        final Map<String, List<SinkGroupManager>> sinkGroupManagerMap =
                dynamicSinkGroupManager.getSinkGroupManagerMap();

        Assert.assertEquals(2, sinkGroupManagerMap.size());

        final List<SinkGroupManager> c1GroupManagers = sinkGroupManagerMap.get("group_1");
        Assert.assertEquals(1, c1GroupManagers.size());
        final List<SinkGroupManager> c2GroupManagers = sinkGroupManagerMap.get("group_2");
        Assert.assertEquals(2, c2GroupManagers.size());

        final Channel channel1 = dynamicChannel.getChannel("c1");
        channel1.append(0, "zhangjun".getBytes());
        channel1.flush();
        final Channel channel2 = dynamicChannel.getChannel("c2");
        channel2.append(0, "zicat".getBytes());
        channel2.flush();

        Thread.sleep(100);
        final SinkGroupManager c1GroupManager = c1GroupManagers.get(0);
        CollectionFunction function =
                (CollectionFunction) c1GroupManager.getFunctions().get(0).get(0);
        Assert.assertArrayEquals("zhangjun".getBytes(), function.history.get(0));
        Assert.assertEquals("c1", c1GroupManagers.get(0).topic());

        for (SinkGroupManager c2GroupManager : c2GroupManagers) {
            CollectionFunction function2 =
                    (CollectionFunction) c2GroupManager.getFunctions().get(0).get(0);
            if (c2GroupManager.topic().equals("c1")) {
                Assert.assertArrayEquals("zhangjun".getBytes(), function2.history.get(0));
            } else {
                Assert.assertArrayEquals("zicat".getBytes(), function2.history.get(0));
                Assert.assertEquals("c2", c2GroupManager.topic());
            }
        }
    }
}
