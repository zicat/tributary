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

package org.zicat.tributary.source.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.MetricKey;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.records.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** AbstractSourceChannel. */
public abstract class AbstractSource implements Source {

    public static final String HEADER_KEY_REC_TS = "_rec_ts";
    private static final Map<MetricKey, Double> empty = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSource.class);

    protected final AtomicInteger count = new AtomicInteger();
    protected final ReadableConfig config;
    protected final Channel channel;
    protected final String sourceId;

    public AbstractSource(String sourceId, ReadableConfig config, Channel channel) {
        this.sourceId = sourceId;
        this.config = config;
        this.channel = channel;
    }

    @Override
    public void append(Integer partition, Records records)
            throws IOException, InterruptedException {
        if (records == null || records.count() == 0) {
            return;
        }
        final Map<String, byte[]> headers = records.headers();
        headers.put(HEADER_KEY_REC_TS, String.valueOf(System.currentTimeMillis()).getBytes());
        final ByteBuffer byteBuffer = records.toByteBuffer();
        final int realPartition =
                partition == null
                        ? (count.getAndIncrement() & 0x7fffffff) % partition()
                        : partition;
        try {
            channel.append(realPartition, byteBuffer);
        } catch (IOException e) {
            LOG.error("append data error, close source", e);
            IOUtils.closeQuietly(this);
            throw e;
        }
    }

    @Override
    public void flush() throws IOException {
        channel.flush();
    }

    @Override
    public String topic() {
        return channel.topic();
    }

    @Override
    public int partition() {
        return channel.partition();
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        return empty;
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    /**
     * get config.
     *
     * @return config
     */
    public ReadableConfig config() {
        return config;
    }
}
