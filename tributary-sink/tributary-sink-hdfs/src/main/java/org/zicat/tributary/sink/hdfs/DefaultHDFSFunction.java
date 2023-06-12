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

package org.zicat.tributary.sink.hdfs;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.sink.function.Clock;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.SystemClock;
import org.zicat.tributary.sink.function.Trigger;

import java.util.Iterator;

/** DefaultHDFSFunction. */
public class DefaultHDFSFunction extends AbstractHDFSFunction<Void> implements Trigger {

    public static final ConfigOption<Integer> OPTION_IDLE_MILLIS =
            ConfigOptions.key("idle.trigger.millis")
                    .integerType()
                    .description("idle trigger, default 30s")
                    .defaultValue(30 * 1000);

    public static final ConfigOption<String> OPTION_BUCKET_DATE_FORMAT =
            ConfigOptions.key("bucket.date.format")
                    .stringType()
                    .description("set process time bucket format, default yyyyMMdd_HH")
                    .defaultValue("yyyyMMdd_HH");

    public static final ConfigOption<String> OPTION_BUCKET_DATE_TIMEZONE =
            ConfigOptions.key("bucket.date.timezone")
                    .stringType()
                    .description("set process time bucket timezone, default UTC")
                    .defaultValue("UTC");

    public static final ConfigOption<Clock> OPTION_CLOCK =
            ConfigOptions.key("clock")
                    .<Clock>objectType()
                    .description("set clock instance")
                    .defaultValue(new SystemClock());

    private static final Counter HDFS_SINK_COUNTER =
            Counter.build()
                    .name("sink_hdfs_counter")
                    .help("sink hdfs counter")
                    .labelNames("host", "groupId", "topic")
                    .register();
    private static final Gauge HDFS_OPEN_FILES_GAUGE =
            Gauge.build()
                    .name("hdfs_opened_files")
                    .help("hdfs opened files")
                    .labelNames("host", "groupId", "threadName")
                    .register();

    protected int idleTriggerMillis;
    protected String bucketDateFormat = null;
    protected String bucketDateTimeZone = null;
    protected String timeBucket = null;
    protected GroupOffset lastGroupOffset;
    protected Clock clock;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        idleTriggerMillis = context.get(OPTION_IDLE_MILLIS);
        bucketDateFormat = context.get(OPTION_BUCKET_DATE_FORMAT);
        bucketDateTimeZone = context.get(OPTION_BUCKET_DATE_TIMEZONE);
        clock = context.get(OPTION_CLOCK);
        timeBucket = clock.currentTime(bucketDateFormat, bucketDateTimeZone);
    }

    /**
     * refresh.
     *
     * @param force force
     * @throws Exception Exception
     */
    public void refresh(boolean force) throws Exception {
        final String currentTimeBucket = clock.currentTime(bucketDateFormat, bucketDateTimeZone);
        if (force || !currentTimeBucket.equals(timeBucket)) {
            closeAllBuckets();
            commit(lastGroupOffset, null);
            timeBucket = currentTimeBucket;
        }
    }

    @Override
    public void process(GroupOffset groupOffset, Iterator<byte[]> iterator) throws Exception {

        refresh(false);
        int totalCount = 0;
        while (iterator.hasNext()) {
            final byte[] record = iterator.next();
            final String dataBucket = getBucket(record);
            final String bucket =
                    dataBucket == null || dataBucket.isEmpty()
                            ? timeBucket
                            : timeBucket + DIRECTORY_DELIMITER + getBucket(record);
            appendData(bucket, record, 0, record.length);
            totalCount++;
        }
        lastGroupOffset = groupOffset;
        updateMetrics(totalCount);
    }

    /**
     * update metrics count.
     *
     * @param count count
     */
    private void updateMetrics(int count) {
        HDFS_SINK_COUNTER.labels(metricsHost(), context.groupId(), context.topic()).inc(count);
        HDFS_OPEN_FILES_GAUGE
                .labels(metricsHost(), context.groupId(), Thread.currentThread().getName())
                .set(sfWriters.size());
    }

    /**
     * get bucket by item.
     *
     * @param record record
     * @return string
     */
    protected String getBucket(byte[] record) {
        return null;
    }

    @Override
    public long idleTimeMillis() {
        return idleTriggerMillis;
    }

    @Override
    public void idleTrigger() throws Throwable {
        LOG.info("idle triggered, idle time is {}", idleTimeMillis());
        refresh(true);
    }

    /**
     * get time bucket.
     *
     * @return time bucket
     */
    public String getTimeBucket() {
        return timeBucket;
    }
}
