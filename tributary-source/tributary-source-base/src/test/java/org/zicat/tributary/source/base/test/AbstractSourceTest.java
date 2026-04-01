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

package org.zicat.tributary.source.base.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Segment.AppendResult;
import org.zicat.tributary.channel.test.ChannelAdapter;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.config.ReadableConfigBuilder;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.SingleRecords;
import org.zicat.tributary.source.base.AbstractSource;

import java.io.IOException;
import java.nio.ByteBuffer;

/** AbstractSourceTest. */
public class AbstractSourceTest {

    @Test
    public void testSourceLivenessGauge() throws Exception {
        final ReadableConfig config = new ReadableConfigBuilder().build();
        try (final Channel appendFailChannel =
                new ChannelAdapter() {
                    @Override
                    public AppendResult append(int partition, ByteBuffer byteBuffer)
                            throws IOException {
                        throw new IOException("testing");
                    }
                }) {
            final AbstractSource source =
                    new AbstractSource("s1", config, appendFailChannel) {
                        @Override
                        public void _open() {}

                        @Override
                        public void close() {}
                    };
            source.open();
            Assert.assertEquals(
                    1, source.gaugeFamily().get(source.sourceLivenessGauge()).intValue());
            try {
                source.append(new SingleRecords("t1", new DefaultRecord("value1".getBytes())));
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(
                        0, source.gaugeFamily().get(source.sourceLivenessGauge()).intValue());
            }
        }
    }
}
