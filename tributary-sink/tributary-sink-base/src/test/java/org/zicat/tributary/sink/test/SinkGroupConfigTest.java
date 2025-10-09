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

package org.zicat.tributary.sink.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory;
import org.zicat.tributary.sink.test.function.AssertFunctionFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** SinkGroupConfigBuilder. */
public class SinkGroupConfigTest {

    @Test
    public void test() {
        final SinkGroupConfigBuilder builder = new SinkGroupConfigBuilder();
        final Map<String, Object> mapValue = new HashMap<>();
        mapValue.put("m_1", "m_v_1");
        builder.functionIdentity(AssertFunctionFactory.IDENTITY)
                .handlerIdentity(DefaultPartitionHandlerFactory.IDENTITY);
        builder.addCustomProperty("aa", "bb")
                .addCustomPropertyIfContainKey("aa", "cc", "dd")
                .addCustomPropertyIfContainKey("bb", "ee", "ff")
                .addCustomProperty("kafka.aa", "hh")
                .addCustomProperty("kafka.bb", "jj")
                .addAll(mapValue)
                .addCustomProperty("kk", 3);
        final SinkGroupConfig sinkGroupConfig = builder.build();
        Assert.assertEquals(
                DefaultPartitionHandlerFactory.IDENTITY, sinkGroupConfig.handlerIdentity());
        Assert.assertEquals(AssertFunctionFactory.IDENTITY, sinkGroupConfig.functionIdentity());
        Assert.assertEquals("bb", sinkGroupConfig.get("aa"));
        Assert.assertEquals("dd", sinkGroupConfig.get("cc"));
        Assert.assertNull(sinkGroupConfig.get("ee"));
        final Properties properties =
                sinkGroupConfig.filterAndRemovePrefixKey("kafka.").toProperties();
        Assert.assertEquals("hh", properties.getProperty("aa"));
        Assert.assertEquals("jj", properties.getProperty("bb"));

        int value = sinkGroupConfig.get(ConfigOptions.key("kk").integerType().defaultValue(4));
        Assert.assertEquals(3, value);

        value = sinkGroupConfig.get(ConfigOptions.key("empty").integerType().defaultValue(5));
        Assert.assertEquals(5, value);

        Assert.assertNull(
                sinkGroupConfig.get(ConfigOptions.key("ks").stringType().defaultValue(null)));
        Assert.assertEquals(
                "aaa",
                sinkGroupConfig.get(ConfigOptions.key("ks").stringType().defaultValue("aaa")));
        Assert.assertEquals(
                "bb",
                sinkGroupConfig.get(ConfigOptions.key("aa").stringType().defaultValue("aaa")));
        Assert.assertEquals(
                "m_v_1",
                sinkGroupConfig.get(ConfigOptions.key("m_1").stringType().defaultValue("aaa")));
    }
}
