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

package org.zicat.tributary.server.test.config;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.server.config.PropertiesConfigBuilder;

import java.util.Properties;
import java.util.Set;

import static org.zicat.tributary.common.ReadableConfig.DEFAULT_KEY_HANDLER;

/** PropertiesConfigBuilderTest. */
public class PropertiesConfigBuilderTest {

    @Test
    public void test() {
        final Properties properties = new Properties();
        properties.put("source.a", "1");
        properties.put("source.b", "2");
        properties.put("channel.c.a", "3");
        properties.put("channel.c2.a", "4");
        properties.put("sink.d", "5");
        properties.put("sink.d2", "6");
        properties.put("server.e.c", "7");
        assertProperties(properties);
    }

    /**
     * assert properties.
     *
     * @param properties properties
     */
    public static void assertProperties(Properties properties) {
        final ReadableConfig channelConfig = PropertiesConfigBuilder.channelConfig(properties);
        final Set<String> channelKey = channelConfig.groupKeys(DEFAULT_KEY_HANDLER);
        Assert.assertTrue(channelKey.contains("c"));
        Assert.assertTrue(channelKey.contains("c2"));
        Assert.assertEquals(2, channelKey.size());
        Assert.assertEquals(
                "3", channelConfig.get(ConfigOptions.key("c.a").stringType().noDefaultValue()));
        Assert.assertEquals(
                "4", channelConfig.get(ConfigOptions.key("c2.a").stringType().noDefaultValue()));

        final ReadableConfig sourceConfig = PropertiesConfigBuilder.sourceConfig(properties);
        final Set<String> sourceKey = sourceConfig.groupKeys(DEFAULT_KEY_HANDLER);
        Assert.assertTrue(sourceKey.contains("a"));
        Assert.assertTrue(sourceKey.contains("b"));
        Assert.assertEquals(2, sourceKey.size());
        Assert.assertEquals(
                "1", sourceConfig.get(ConfigOptions.key("a").stringType().noDefaultValue()));
        Assert.assertEquals(
                "2", sourceConfig.get(ConfigOptions.key("b").stringType().noDefaultValue()));

        final ReadableConfig sinkConfig = PropertiesConfigBuilder.sinkConfig(properties);
        final Set<String> sinkKey = sinkConfig.groupKeys(DEFAULT_KEY_HANDLER);
        Assert.assertTrue(sinkKey.contains("d"));
        Assert.assertTrue(sinkKey.contains("d2"));
        Assert.assertEquals(2, sinkKey.size());
        Assert.assertEquals(
                "5", sinkConfig.get(ConfigOptions.key("d").stringType().noDefaultValue()));
        Assert.assertEquals(
                "6", sinkConfig.get(ConfigOptions.key("d2").stringType().noDefaultValue()));

        final ReadableConfig serverConfig = PropertiesConfigBuilder.serverConfig(properties);
        final Set<String> serverKey = serverConfig.groupKeys(DEFAULT_KEY_HANDLER);
        Assert.assertTrue(serverKey.contains("e"));
        Assert.assertEquals(1, serverKey.size());
        Assert.assertEquals(
                "7", serverConfig.get(ConfigOptions.key("e.c").stringType().noDefaultValue()));
    }
}
