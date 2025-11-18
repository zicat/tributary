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
import org.zicat.tributary.common.Clock;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.metric.MetricKey;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.SystemClock;
import org.zicat.tributary.common.records.Records;
import static org.zicat.tributary.common.records.RecordsUtils.appendRecTs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/** AbstractSource. */
public abstract class AbstractSource implements Source {

    public static final ConfigOption<Clock> OPTION_SOURCE_CLOCK =
            ConfigOptions.key("_source_clock").<Clock>objectType().defaultValue(new SystemClock());

    public static final MetricKey APPEND_CHANNEL_RECORD_COUNTER =
            new MetricKey("tributary_source_append_channel_record_counter");

    public static final ConfigOption<AppendResultType> OPTION_CHANNEL_APPEND_RESULT_TYPE =
            ConfigOptions.key("channel.append-result-type")
                    .enumType(AppendResultType.class)
                    .defaultValue(AppendResultType.BLOCK);

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSource.class);

    protected final AtomicInteger count = new AtomicInteger();
    protected final ReadableConfig config;
    protected final Channel channel;
    protected final String sourceId;
    protected final Map<MetricKey, Double> gaugeFamily = new ConcurrentHashMap<>();
    protected final Map<MetricKey, Double> counterFamily = new ConcurrentHashMap<>();
    protected final Clock clock;
    protected final AppendResultType appendResultType;

    public AbstractSource(String sourceId, ReadableConfig config, Channel channel) {
        this.sourceId = sourceId;
        this.config = config;
        this.channel = channel;
        this.clock = config.get(OPTION_SOURCE_CLOCK);
        this.appendResultType = config.get(OPTION_CHANNEL_APPEND_RESULT_TYPE);
    }

    @Override
    public void append(Integer partition, Records records)
            throws IOException, InterruptedException {
        if (records == null || records.count() == 0) {
            return;
        }
        mergeCounter(APPEND_CHANNEL_RECORD_COUNTER, records.count(), Double::sum);
        appendRecTs(clock, records.headers());
        final ByteBuffer byteBuffer = records.toByteBuffer();
        final int realPartition = partition == null ? defaultPartition() : partition;
        try {
            appendResultType.dealAppendResult(channel.append(realPartition, byteBuffer));
        } catch (IOException e) {
            LOG.error("append data error, close source", e);
            IOUtils.closeQuietly(this);
            throw e;
        }
    }

    /**
     * default partition.
     *
     * @return int
     */
    protected int defaultPartition() {
        return (count.getAndIncrement() & 0x7fffffff) % partition();
    }

    @Override
    public void mergeGauge(
            MetricKey key,
            double value,
            BiFunction<? super Double, ? super Double, ? extends Double> remappingFunction) {
        gaugeFamily.merge(key, value, remappingFunction);
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        return gaugeFamily;
    }

    @Override
    public void mergeCounter(
            MetricKey key,
            double value,
            BiFunction<? super Double, ? super Double, ? extends Double> remappingFunction) {
        counterFamily.merge(key, value, remappingFunction);
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        return counterFamily;
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

    /**
     * get config.
     *
     * @return config
     */
    public ReadableConfig config() {
        return config;
    }
}
