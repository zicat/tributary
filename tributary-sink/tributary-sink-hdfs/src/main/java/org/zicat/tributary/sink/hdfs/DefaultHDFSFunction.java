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
import org.apache.hadoop.fs.FileSystem;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.Trigger;

import java.io.IOException;
import java.util.Iterator;

/** DefaultHDFSFunction. */
public class DefaultHDFSFunction extends AbstractHDFSFunction<Object> implements Trigger {

    public static final String KEY_IDLE_MILLIS = "idleTriggerMillis";
    public static final int DEFAULT_IDLE_MILLIS = 30 * 1000;

    public static final String KEY_BUCKET_DATE_FORMAT = "bucketDateFormat";
    public static final String DEFAULT_BUCKET_DATE_FORMAT = "yyyyMMdd_HH";

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
    protected String timeBucket = null;
    protected RecordsOffset lastRecordsOffset;

    @Override
    public void open(Context context) {
        super.open(context);
        idleTriggerMillis = context.getCustomProperty(KEY_IDLE_MILLIS, DEFAULT_IDLE_MILLIS);
        bucketDateFormat =
                context.getCustomProperty(KEY_BUCKET_DATE_FORMAT, DEFAULT_BUCKET_DATE_FORMAT);
        timeBucket = clock.currentTime(bucketDateFormat);
    }

    /**
     * refresh.
     *
     * @param force force
     * @throws Exception Exception
     */
    public void refresh(boolean force) throws Exception {
        String currentTimeBucket = clock.currentTime(bucketDateFormat);
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

    @Override
    protected BucketWriter<Object> initializeBucketWriter(String bucketPath, String realName) {
        return new BucketWriter<Object>(
                bucketPath,
                realName,
                snappyCodec,
                new HDFSCompressedDataStream(),
                privilegedExecutor,
                rollSize,
                maxRetry,
                null,
                clock) {
            protected void renameBucket(String bucketPath, String targetPath, final FileSystem fs)
                    throws IOException {
                super.renameBucket(bucketPath, targetPath, fs);
                DefaultHDFSFunction.this.flush(lastRecordsOffset, null);
            }
        };
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
    public String getBucket(byte[] record) {
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
    }
}
