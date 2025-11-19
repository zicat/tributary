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

package org.zicat.tributary.source.logstash.http.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.config.ReadableConfigBuilder;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.source.logstash.http.Codec;
import org.zicat.tributary.source.logstash.http.LogstashHttpPipelineInitializationFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** CodecTest. */
@SuppressWarnings("unchecked")
public class CodecTest {

    @Test
    public void testPlain() throws IOException {
        final ReadableConfig config =
                new ReadableConfigBuilder()
                        .addConfig(
                                LogstashHttpPipelineInitializationFactory.OPTION_LOGSTASH_CODEC
                                        .key(),
                                Codec.PLAIN.getName())
                        .build();
        final Codec codec =
                config.get(LogstashHttpPipelineInitializationFactory.OPTION_LOGSTASH_CODEC);
        Assert.assertEquals(Codec.PLAIN, codec);

        final List<Map<String, Object>> dataList =
                codec.encode("hello world".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(1, dataList.size());
        Assert.assertEquals("hello world", dataList.get(0).get("message"));
    }

    @Test
    public void testJson() throws IOException {
        final ReadableConfig config =
                new ReadableConfigBuilder()
                        .addConfig(
                                LogstashHttpPipelineInitializationFactory.OPTION_LOGSTASH_CODEC
                                        .key(),
                                Codec.JSON.getName())
                        .build();
        final Codec codec =
                config.get(LogstashHttpPipelineInitializationFactory.OPTION_LOGSTASH_CODEC);
        Assert.assertEquals(Codec.JSON, codec);

        final List<Map<String, Object>> dataList =
                codec.encode("{\"id\":1}".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(1, dataList.size());
        Assert.assertEquals(1, dataList.get(0).get("id"));

        // test nest
        final List<Map<String, Object>> dataList2 =
                codec.encode("{\"id\":{\"name\":\"zicat\"}}".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(
                "zicat", ((Map<String, Object>) dataList2.get(0).get("id")).get("name"));

        // test array
        final List<Map<String, Object>> dataList3 =
                codec.encode("[{\"id\":1},{\"id\":2}]".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(2, dataList3.size());
        Assert.assertEquals(1, dataList3.get(0).get("id"));
        Assert.assertEquals(2, dataList3.get(1).get("id"));
    }
}
