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

package org.zicat.tributary.service.sink;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.Trigger;
import org.zicat.tributary.sink.hdfs.AbstractHDFSFunction;

import java.util.Iterator;

/** DefaultHDFSFunction. */
public class DefaultHDFSFunction extends AbstractHDFSFunction<Object> implements Trigger {

    public static final String KEY_IDLE_MILLIS = "idleTriggerMillis";
    public static final int DEFAULT_IDLE_MILLIS = 30 * 1000;
    private static final Counter HDFS_SINK_COUNTER =
            Counter.build()
                    .name("hdfs_sink_counter")
                    .help("hdfs sink counter")
                    .labelNames("host", "groupId")
                    .register();
    private static final Gauge HDFS_OPEN_FILES_GAUGE =
            Gauge.build()
                    .name("hdfs_opened_files")
                    .help("hdfs opened files")
                    .labelNames("host", "groupId", "threadName")
                    .register();

    private static final String DATE_FORMAT = "yyyyMMdd_HH";
    protected int idleTriggerMillis;
    protected String timeBucket = null;
    protected RecordsOffset lastRecordsOffset;

    @Override
    public void open(Context context) {
        super.open(context);
        idleTriggerMillis = context.getCustomProperty(KEY_IDLE_MILLIS, DEFAULT_IDLE_MILLIS);
        timeBucket = clock.currentTime(DATE_FORMAT);
    }

    /**
     * refresh.
     *
     * @param force forcex`
     * @throws Exception Exception
     */
    protected void refresh(boolean force) throws Exception {
        String currentTimeBucket = clock.currentTime(DATE_FORMAT);
        if (force || !currentTimeBucket.equals(timeBucket)) {
            closeBucket(timeBucket);
            timeBucket = currentTimeBucket;
        }
    }

    @Override
    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) throws Exception {

        refresh(false);
        int totalCount = 0;
        while (iterator.hasNext()) {
            final byte[] record = iterator.next();
            final String bucket = getBucket(record);
            appendData(bucket, record, 0, record.length);
            totalCount++;
        }
        lastRecordsOffset = recordsOffset;
        updateMetrics(totalCount);
    }

    /**
     * update metrics count.
     *
     * @param count count
     */
    private void updateMetrics(int count) {
        HDFS_SINK_COUNTER.labels(metricsHost(), context.groupId()).inc(count);
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
        return timeBucket;
    }

    @Override
    public long idleTimeMillis() {
        return idleTriggerMillis;
    }

    @Override
    public void idleTrigger() throws Throwable {
        LOG.info("idle triggered, idle time is {}", idleTimeMillis());
        refresh(true);
        flush(lastRecordsOffset);
    }
}
