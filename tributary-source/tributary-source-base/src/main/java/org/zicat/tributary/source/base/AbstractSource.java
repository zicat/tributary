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

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Segment.AppendResult;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import static org.zicat.tributary.common.config.ConfigOptions.COMMA_SPLIT_HANDLER;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.metric.MetricKey;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.base.interceptor.TimestampInterceptorFactory;
import org.zicat.tributary.source.base.interceptor.SourceInterceptor;
import org.zicat.tributary.source.base.interceptor.SourceInterceptorFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/** AbstractSource. */
public abstract class AbstractSource implements Source {

    public static final MetricKey APPEND_CHANNEL_RECORD_COUNTER =
            new MetricKey("tributary_source_append_channel_record_counter");

    public static final MetricKey SOURCE_LIVENESS_GAUGE =
            new MetricKey("tributary_source_liveness", "tributary source liveness");

    public static final ConfigOption<AppendResultType> OPTION_CHANNEL_APPEND_RESULT_TYPE =
            ConfigOptions.key("channel.append-result-type")
                    .enumType(AppendResultType.class)
                    .defaultValue(AppendResultType.BLOCK);

    public static final ConfigOption<List<String>> OPTION_INTERCEPTORS =
            ConfigOptions.key("interceptors")
                    .listType(COMMA_SPLIT_HANDLER)
                    .description("source interceptor identities, split by comma")
                    .defaultValue(Collections.singletonList(TimestampInterceptorFactory.IDENTITY));

    protected final AtomicInteger count = new AtomicInteger();
    protected final ReadableConfig config;
    protected final Channel channel;
    protected final String sourceId;
    protected final Map<MetricKey, Double> gaugeFamily = new ConcurrentHashMap<>();
    protected final Map<MetricKey, Double> counterFamily = new ConcurrentHashMap<>();
    protected transient AppendResultType appendResultType;
    protected transient MetricKey sourceLivenessGauge;
    protected transient List<SourceInterceptor> interceptors;

    public AbstractSource(String sourceId, ReadableConfig config, Channel channel) {
        this.sourceId = sourceId;
        this.config = config;
        this.channel = channel;
    }

    /**
     * create interceptors from config, subclass can override for testing or customization.
     *
     * @return interceptor list
     */
    protected List<SourceInterceptor> createInterceptors() throws Exception {
        final List<String> identities = config.get(OPTION_INTERCEPTORS);
        if (identities == null || identities.isEmpty()) {
            return Collections.emptyList();
        }
        final List<SourceInterceptor> result = new ArrayList<>();
        for (String identity : identities) {
            final SourceInterceptorFactory factory =
                    SpiFactory.findFactory(identity, SourceInterceptorFactory.class);
            result.add(factory.createInterceptor(config));
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * open method is used for open successful callback, this child class should ovrride _open.
     *
     * @throws Exception Exception
     */
    public abstract void _open() throws Exception;

    @Override
    public final void open() throws Exception {
        appendResultType = config.get(OPTION_CHANNEL_APPEND_RESULT_TYPE);
        interceptors = createInterceptors();
        _open();
        sourceLivenessGauge = SOURCE_LIVENESS_GAUGE.copyWithLabel("sourceId", sourceId);
        mergeGauge(sourceLivenessGauge, 1, (oldValue, newValue) -> newValue);
    }

    @Override
    public void append(Integer partition, Records records) throws Exception {
        if (records == null || records.count() == 0) {
            return;
        }
        records = applyInterceptors(records);
        if (records == null || records.count() == 0) {
            return;
        }
        mergeCounter(APPEND_CHANNEL_RECORD_COUNTER, records.count(), Double::sum);
        final ByteBuffer byteBuffer = records.toByteBuffer();
        final int realPartition = partition == null ? defaultPartition() : partition;
        AppendResult appendResult;
        try {
            appendResult = channel.append(realPartition, byteBuffer);
        } catch (IOException e) {
            IOUtils.closeQuietly(this);
            mergeGauge(sourceLivenessGauge, 0, (oldValue, newValue) -> newValue);
            throw new IOException("append data error, close source " + sourceId, e);
        }
        appendResultType.dealAppendResult(appendResult);
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

    /**
     * source liveness key.
     *
     * @return MetricKey
     */
    public MetricKey sourceLivenessGauge() {
        return sourceLivenessGauge;
    }

    /**
     * apply interceptors to records.
     *
     * @param records records
     * @return intercepted records, null if discarded
     */
    private Records applyInterceptors(Records records) throws Exception {
        for (SourceInterceptor interceptor : interceptors) {
            records = interceptor.intercept(records);
            if (records == null) {
                return null;
            }
        }
        return records;
    }
}
