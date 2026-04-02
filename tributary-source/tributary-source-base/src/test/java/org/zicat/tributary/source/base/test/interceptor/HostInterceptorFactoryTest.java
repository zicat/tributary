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
import org.zicat.tributary.source.base.interceptor.HostInterceptorFactory;
import org.zicat.tributary.source.base.interceptor.SourceInterceptor;
import org.zicat.tributary.source.base.interceptor.SourceInterceptorFactory;

import static org.zicat.tributary.source.base.interceptor.HostInterceptorFactory.HEADER_KEY_HOST;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

/** HostInterceptorFactoryTest. */
public class HostInterceptorFactoryTest {

    @Test
    public void testSpiIdentity() {
        final SourceInterceptorFactory factory =
                SpiFactory.findFactory(
                        HostInterceptorFactory.IDENTITY, SourceInterceptorFactory.class);
        Assert.assertTrue(factory instanceof HostInterceptorFactory);
    }

    @Test
    public void testDefaultKey() throws Exception {
        final ReadableConfig config = new ReadableConfigBuilder().build();
        final SourceInterceptor interceptor =
                new HostInterceptorFactory().createInterceptor(config);
        final SingleRecords records =
                new SingleRecords("t1", new DefaultRecord("v1".getBytes()));
        Assert.assertNull(records.headers().get(HEADER_KEY_HOST));
        final Records result = interceptor.intercept(records);
        Assert.assertNotNull(result);
        final byte[] hostBytes = result.headers().get(HEADER_KEY_HOST);
        Assert.assertNotNull(hostBytes);
        final String expected = InetAddress.getLocalHost().getHostName();
        Assert.assertEquals(expected, new String(hostBytes, StandardCharsets.UTF_8));
    }

    @Test
    public void testCustomKey() throws Exception {
        final ReadableConfig config =
                new ReadableConfigBuilder()
                        .addConfig(HostInterceptorFactory.OPTION_HOST_KEY, "custom_host")
                        .build();
        final SourceInterceptor interceptor =
                new HostInterceptorFactory().createInterceptor(config);
        final SingleRecords records =
                new SingleRecords("t1", new DefaultRecord("v1".getBytes()));
        interceptor.intercept(records);
        Assert.assertNotNull(records.headers().get("custom_host"));
        Assert.assertNull(records.headers().get(HEADER_KEY_HOST));
    }

    @Test
    public void testConcatOnMultipleIntercept() throws Exception {
        final ReadableConfig config = new ReadableConfigBuilder().build();
        final SourceInterceptor interceptor =
                new HostInterceptorFactory().createInterceptor(config);
        final SingleRecords records =
                new SingleRecords("t1", new DefaultRecord("v1".getBytes()));
        interceptor.intercept(records);
        interceptor.intercept(records);
        final String value =
                new String(records.headers().get(HEADER_KEY_HOST), StandardCharsets.UTF_8);
        final String expectedHost = InetAddress.getLocalHost().getHostName();
        // two identical hosts concatenated with space
        final String[] parts = value.split(" ");
        Assert.assertEquals(2, parts.length);
        Assert.assertEquals(expectedHost, parts[0]);
        Assert.assertEquals(expectedHost, parts[1]);
    }
}
