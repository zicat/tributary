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

package org.zicat.tributary.server.test.component;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.test.ChannelBaseTest;
import org.zicat.tributary.channel.test.ChannelBaseTest.DataOffset;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.server.component.ChannelComponent;
import org.zicat.tributary.server.component.ChannelComponentFactory;
import org.zicat.tributary.server.config.PropertiesConfigBuilder;
import org.zicat.tributary.server.config.PropertiesLoader;

import java.io.IOException;
import java.util.Properties;

/** ChannelComponentTest. */
public class ChannelComponentTest {

    private static final String profile = "channel-test";

    @Test
    public void test() throws IOException, InterruptedException {

        final Properties properties = new PropertiesLoader(profile).load();
        final ReadableConfig config = PropertiesConfigBuilder.channelConfig(properties);
        final Offset offset = Offset.ZERO;
        try (ChannelComponent channelComponent = new ChannelComponentFactory(config, "").create()) {
            Assert.assertEquals(2, channelComponent.size());
            final Channel channel1 = channelComponent.get("c1");
            final Channel channel2 = channelComponent.get("c2");
            Assert.assertNotNull(channel1);
            Assert.assertNotNull(channel1);

            Assert.assertEquals(1, channelComponent.findChannels("group_1").size());
            Assert.assertSame(channel1, channelComponent.findChannels("group_1").get(0));

            Assert.assertEquals(2, channelComponent.findChannels("group_2").size());
            Assert.assertTrue(channelComponent.findChannels("group_2").contains(channel1));
            Assert.assertTrue(channelComponent.findChannels("group_2").contains(channel2));

            Assert.assertTrue(channelComponent.collect().size() >= channelComponent.size());

            channel1.append(0, "aa".getBytes());
            channelComponent.flush();

            final DataOffset dataOffset = ChannelBaseTest.readChannel(channel1, 0, offset, 1);
            Assert.assertEquals("aa", new String(dataOffset.data.get(0)));
        }
    }
}
