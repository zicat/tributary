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

package org.zicat.tributary.channel.test;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.channel.Segment.AppendResult;
import org.zicat.tributary.common.metric.MetricKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** ChannelAdapter. */
public class ChannelAdapter implements Channel {
    @Override
    public AppendResult append(int partition, ByteBuffer byteBuffer)
            throws IOException, InterruptedException {
        return null;
    }

    @Override
    public RecordsResultSet poll(int partition, Offset offset, long time, TimeUnit unit) {
        return null;
    }

    @Override
    public void flush() throws IOException {}

    @Override
    public long lag(int partition, Offset offset) {
        return 0;
    }

    @Override
    public String topic() {
        return "";
    }

    @Override
    public int partition() {
        return 1;
    }

    @Override
    public Map<String, Map<Integer, Offset>> committedOffsets() {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> groups() {
        return Collections.emptySet();
    }

    @Override
    public Offset committedOffset(String groupId, int partition) {
        return null;
    }

    @Override
    public void commit(int partition, String groupId, Offset offset) {}

    @Override
    public void commit(int partition, Offset offset) {}

    @Override
    public void close() throws IOException {}

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        return Collections.emptyMap();
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        return Collections.emptyMap();
    }
}
