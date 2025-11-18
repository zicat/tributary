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
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.server.component.ChannelComponent;
import org.zicat.tributary.server.component.ChannelComponentFactory;
import org.zicat.tributary.server.component.SinkComponent;
import org.zicat.tributary.server.component.SinkComponentFactory;
import org.zicat.tributary.server.config.PropertiesConfigBuilder;
import org.zicat.tributary.server.config.PropertiesLoader;
import org.zicat.tributary.server.metrics.TributaryCollectorRegistry;
import org.zicat.tributary.sink.SinkGroupManager;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/** SinkComponentTest. */
public class SinkComponentTest {

    private static final String profile = "sink-test";

    @Test
    public void test() throws IOException {

        final Properties properties = new PropertiesLoader(profile).load();
        final ReadableConfig channelConfig = PropertiesConfigBuilder.channelConfig(properties);
        final ReadableConfig sinkConfig = PropertiesConfigBuilder.sinkConfig(properties);
        final TributaryCollectorRegistry registry = new TributaryCollectorRegistry("local");
        try (ChannelComponent channelComponent =
                        new ChannelComponentFactory(channelConfig, registry).create();
                SinkComponent sinkComponent =
                        new SinkComponentFactory(sinkConfig, channelComponent, registry).create()) {

            Assert.assertEquals(4, sinkComponent.size());
            Assert.assertTrue(sinkComponent.collect().size() >= sinkComponent.size());
            final List<SinkGroupManager> gs1 = sinkComponent.get("g1");
            final List<SinkGroupManager> gs2 = sinkComponent.get("g2");
            final List<SinkGroupManager> gs3 = sinkComponent.get("g3");

            Assert.assertEquals(1, gs1.size());
            Assert.assertEquals(2, gs2.size());
            Assert.assertEquals(1, gs3.size());
            final SinkGroupManager g1 = gs1.get(0);
            final SinkGroupManager g21 = gs2.get(0);
            final SinkGroupManager g22 = gs2.get(1);
            final SinkGroupManager g3 = gs3.get(0);

            Assert.assertEquals("c1", g1.topic());
            Assert.assertEquals("c2", g3.topic());
            Assert.assertTrue(
                    (g21.topic().equals("c1") && g22.topic().equals("c2"))
                            || (g21.topic().equals("c2") && g22.topic().equals("c1")));
        }
    }
}
