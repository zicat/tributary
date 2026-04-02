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

package org.zicat.tributary.source.base.test.interceptor;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.config.ReadableConfigBuilder;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.common.records.SingleRecords;
import org.zicat.tributary.source.base.interceptor.SourceInterceptor;
import org.zicat.tributary.source.base.interceptor.SourceInterceptorFactory;
import org.zicat.tributary.source.base.interceptor.TimestampInterceptorFactory;
import static org.zicat.tributary.source.base.interceptor.TimestampInterceptorFactory.HEADER_KEY_REC_TS;

import java.nio.charset.StandardCharsets;

/** TimestampInterceptorFactoryTest. */
public class TimestampInterceptorFactoryTest {

    @Test
    public void testSpiIdentity() {
        final SourceInterceptorFactory factory =
                SpiFactory.findFactory(
                        TimestampInterceptorFactory.IDENTITY, SourceInterceptorFactory.class);
        Assert.assertTrue(factory instanceof TimestampInterceptorFactory);
    }

    @Test
    public void testDefaultKey() throws Exception {
        final ReadableConfig config = new ReadableConfigBuilder().build();
        final SourceInterceptor interceptor =
                new TimestampInterceptorFactory().createInterceptor(config);
        final SingleRecords records = new SingleRecords("t1", new DefaultRecord("v1".getBytes()));
        Assert.assertNull(records.headers().get(HEADER_KEY_REC_TS));
        final Records result = interceptor.intercept(records);
        Assert.assertNotNull(result);
        final byte[] ts = result.headers().get(HEADER_KEY_REC_TS);
        Assert.assertNotNull(ts);
        // should be a valid timestamp
        Assert.assertNotNull(Long.valueOf(new String(ts, StandardCharsets.UTF_8)));
    }

    @Test
    public void testCustomKey() throws Exception {
        final ReadableConfig config =
                new ReadableConfigBuilder()
                        .addConfig(TimestampInterceptorFactory.OPTION_TIMESTAMP_KEY, "custom_ts")
                        .build();
        final SourceInterceptor interceptor =
                new TimestampInterceptorFactory().createInterceptor(config);
        final SingleRecords records = new SingleRecords("t1", new DefaultRecord("v1".getBytes()));
        interceptor.intercept(records);
        Assert.assertNotNull(records.headers().get("custom_ts"));
        Assert.assertNull(records.headers().get(HEADER_KEY_REC_TS));
    }

    @Test
    public void testConcatOnMultipleIntercept() throws Exception {
        final ReadableConfig config = new ReadableConfigBuilder().build();
        final SourceInterceptor interceptor =
                new TimestampInterceptorFactory().createInterceptor(config);
        final SingleRecords records = new SingleRecords("t1", new DefaultRecord("v1".getBytes()));
        interceptor.intercept(records);
        interceptor.intercept(records);
        final String value =
                new String(records.headers().get(HEADER_KEY_REC_TS), StandardCharsets.UTF_8);
        // two timestamps concatenated with space
        final String[] parts = value.split(" ");
        Assert.assertEquals(2, parts.length);
        Assert.assertNotNull(Long.valueOf(parts[0]));
        Assert.assertNotNull(Long.valueOf(parts[1]));
    }
}
