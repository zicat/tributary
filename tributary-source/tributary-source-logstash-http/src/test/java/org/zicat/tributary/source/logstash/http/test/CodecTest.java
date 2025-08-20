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
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.source.logstash.http.Codec;
import org.zicat.tributary.source.logstash.http.LogstashHttpPipelineInitializationFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** CodecTest. */
@SuppressWarnings("unchecked")
public class CodecTest {

    @Test
    public void testPlain() throws IOException {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(
                LogstashHttpPipelineInitializationFactory.OPTION_LOGSTASH_CODEC.key(),
                Codec.PLAIN.getName());
        final Codec codec =
                config.get(LogstashHttpPipelineInitializationFactory.OPTION_LOGSTASH_CODEC);
        Assert.assertEquals(Codec.PLAIN, codec);

        final Map<String, Object> data = new HashMap<>();
        codec.encode(data, "hello world".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("hello world", data.get("message"));
    }

    @Test
    public void testJson() throws IOException {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(
                LogstashHttpPipelineInitializationFactory.OPTION_LOGSTASH_CODEC.key(),
                Codec.JSON.getName());
        final Codec codec =
                config.get(LogstashHttpPipelineInitializationFactory.OPTION_LOGSTASH_CODEC);
        Assert.assertEquals(Codec.JSON, codec);

        final Map<String, Object> data = new HashMap<>();
        codec.encode(data, "{\"id\":1}".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(1, data.get("id"));

        // test nest
        final Map<String, Object> data2 = new HashMap<>();
        codec.encode(data2, "{\"id\":{\"name\":\"zicat\"}}".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("zicat", ((Map<String, Object>) data2.get("id")).get("name"));
    }
}
