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
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.server.component.ChannelComponent;
import org.zicat.tributary.server.component.ChannelComponentFactory;
import org.zicat.tributary.server.component.SourceComponent;
import org.zicat.tributary.server.component.SourceComponentFactory;
import org.zicat.tributary.server.config.PropertiesConfigBuilder;
import org.zicat.tributary.server.config.PropertiesLoader;
import org.zicat.tributary.source.base.netty.NettySource;

import java.io.IOException;
import java.util.Properties;

/** SourceComponentTest. */
public class SourceComponentTest {

    private static final String profile = "source-test";

    @Test
    public void test() throws IOException {

        final Properties properties = new PropertiesLoader(profile).load();
        final ReadableConfig channelConfig = PropertiesConfigBuilder.channelConfig(properties);
        final ReadableConfig sourceConfig = PropertiesConfigBuilder.sourceConfig(properties);
        final String metricsHost = "";
        try (ChannelComponent channelComponent =
                        new ChannelComponentFactory(channelConfig, metricsHost).create();
                SourceComponent sourceComponent =
                        new SourceComponentFactory(sourceConfig, channelComponent, metricsHost)
                                .create()) {
            Assert.assertEquals(3, sourceComponent.size());

            final NettySource source1 = (NettySource) sourceComponent.get("s1");
            final NettySource source2 = (NettySource) sourceComponent.get("s2");
            final NettySource source3 = (NettySource) sourceComponent.get("s3");

            Assert.assertEquals("c1", source1.topic());
            Assert.assertEquals("c2", source2.topic());
            Assert.assertEquals("c2", source3.topic());

            Assert.assertEquals(57132, source1.getPort());
            Assert.assertEquals(57133, source2.getPort());
            Assert.assertEquals(57134, source3.getPort());

            Assert.assertEquals(10, source1.getEventThreads());
            Assert.assertEquals(11, source2.getEventThreads());
            Assert.assertEquals(12, source3.getEventThreads());
        }
    }
}
