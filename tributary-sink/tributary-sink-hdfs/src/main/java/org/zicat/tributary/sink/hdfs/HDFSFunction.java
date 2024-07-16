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
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.Trigger;
import org.zicat.tributary.sink.hdfs.bucket.BucketGenerator;
import org.zicat.tributary.sink.hdfs.bucket.BucketGenerator.RefreshHandler;
import org.zicat.tributary.sink.hdfs.bucket.ProcessTimeBucketGenerator;

import java.io.IOException;
import java.util.Iterator;

import static org.zicat.tributary.sink.hdfs.HDFSSinkOptions.OPTION_IDLE_TRIGGER;

/** HDFSFunction. */
public class HDFSFunction extends AbstractFunction implements Trigger {

    private static final Counter HDFS_SINK_COUNTER =
            Counter.build()
                    .name("sink_hdfs_counter")
                    .help("sink hdfs counter")
                    .labelNames("host", "id")
                    .register();
    private static final Gauge HDFS_OPEN_FILES_GAUGE =
            Gauge.build()
                    .name("hdfs_opened_files")
                    .help("hdfs opened files")
                    .labelNames("host", "id")
                    .register();

    protected transient Gauge.Child openFilesGaugeChild;
    protected transient Counter.Child sinkCounterChild;
    protected transient GroupOffset lastGroupOffset;
    protected transient RecordsWriterManager recordsWriterManager;
    protected transient BucketGenerator bucketGenerator;
    protected transient RefreshHandler refreshHandler;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        sinkCounterChild = labelHostId(HDFS_SINK_COUNTER);
        openFilesGaugeChild = labelHostId(HDFS_OPEN_FILES_GAUGE);
        recordsWriterManager = createRecordsWriterManager();
        recordsWriterManager.open(context);
        bucketGenerator = createBucketGenerator();
        bucketGenerator.open(context);
        refreshHandler =
                () -> {
                    recordsWriterManager.closeAllBuckets();
                    commit(lastGroupOffset, null);
                };
    }

    /**
     * refresh.
     *
     * @param force force
     * @throws Exception Exception
     */
    public void refresh(boolean force) throws Exception {
        bucketGenerator.checkRefresh(force, refreshHandler);
    }

    @Override
    public void process(GroupOffset groupOffset, Iterator<Records> iterator) throws Exception {

        refresh(false);
        int totalCount = 0;
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            final String bucket = bucketGenerator.getBucket(records);
            totalCount += recordsWriterManager.getOrCreateRecordsWriter(bucket).append(records);
        }
        lastGroupOffset = groupOffset;
        sinkCounterChild.inc(totalCount);
        openFilesGaugeChild.set(recordsWriterManager.bucketsCount());
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(bucketGenerator, recordsWriterManager);
    }

    @Override
    public long idleTimeMillis() {
        return context.get(OPTION_IDLE_TRIGGER).toMillis();
    }

    @Override
    public void idleTrigger() throws Throwable {
        refresh(true);
    }

    /**
     * create records writer manager.
     *
     * @return RecordsWriterManager
     */
    protected RecordsWriterManager createRecordsWriterManager() {
        return new DefaultRecordsWriterManager();
    }

    /**
     * create bucket generator.
     *
     * @return BucketGenerator
     */
    protected BucketGenerator createBucketGenerator() {
        return new ProcessTimeBucketGenerator();
    }
}
