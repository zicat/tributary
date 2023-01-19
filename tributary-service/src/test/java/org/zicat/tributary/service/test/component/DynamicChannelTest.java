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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.zicat.tributary.service.component.DynamicChannel;
import org.zicat.tributary.service.configuration.ChannelConfiguration;

import static org.zicat.tributary.channel.utils.IOUtils.deleteDir;

/** DynamicChannelTest. */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = DynamicChannel.class)
@EnableConfigurationProperties(ChannelConfiguration.class)
@ActiveProfiles("channel-test")
public class DynamicChannelTest {

    @Autowired DynamicChannel dynamicChannel;

    @After
    public void after() {
        if (dynamicChannel.getTempDir() != null) {
            deleteDir(dynamicChannel.getTempDir());
        }
    }

    @Test
    public void test() {

        Assert.assertEquals("c1", dynamicChannel.getChannel("c1").topic());
        Assert.assertEquals("c2", dynamicChannel.getChannel("c2").topic());

        Assert.assertEquals(2, dynamicChannel.getChannel("c1").groups().size());
        Assert.assertTrue(dynamicChannel.getChannel("c1").groups().contains("group_1"));
        Assert.assertTrue(dynamicChannel.getChannel("c1").groups().contains("group_2"));

        Assert.assertEquals(1, dynamicChannel.getChannel("c2").groups().size());
        Assert.assertTrue(dynamicChannel.getChannel("c1").groups().contains("group_2"));

        Assert.assertEquals(2, dynamicChannel.size());
    }
}
