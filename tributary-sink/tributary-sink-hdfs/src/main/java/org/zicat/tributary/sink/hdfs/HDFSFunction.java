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

import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.metric.MetricKey;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.config.Context;
import org.zicat.tributary.sink.hdfs.bucket.BucketGenerator;
import org.zicat.tributary.sink.hdfs.bucket.BucketGenerator.RefreshHandler;
import org.zicat.tributary.sink.hdfs.bucket.ProcessTimeBucketGenerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** HDFSFunction. */
public class HDFSFunction extends AbstractFunction {

    private static final MetricKey SINK_COUNTER = new MetricKey("tributary_sink_hdfs_counter");
    private static final MetricKey OPEN_FILES_GAUGE =
            new MetricKey("tributary_sink_hdfs_opened_files");

    protected transient long openFilesGauge;
    protected transient long sinkCounter;
    protected transient Offset lastOffset;
    protected transient RecordsWriterManager recordsWriterManager;
    protected transient BucketGenerator bucketGenerator;
    protected transient RefreshHandler refreshHandler;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        recordsWriterManager = createRecordsWriterManager();
        recordsWriterManager.open(context);
        bucketGenerator = createBucketGenerator();
        bucketGenerator.open(context);
        refreshHandler =
                () -> {
                    recordsWriterManager.closeAllBuckets();
                    commit(lastOffset);
                };
    }

    /**
     * refresh.
     *
     * @throws Exception Exception
     */
    public void refresh() throws Exception {
        bucketGenerator.checkRefresh(refreshHandler);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        refresh();
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            final String bucket = bucketGenerator.getBucket(records);
            sinkCounter += recordsWriterManager.getOrCreateRecordsWriter(bucket).append(records);
        }
        lastOffset = offset;
        openFilesGauge = recordsWriterManager.bucketsCount();
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.counterFamily());
        base.put(SINK_COUNTER, (double) sinkCounter);
        return base;
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.gaugeFamily());
        base.put(OPEN_FILES_GAUGE, (double) openFilesGauge);
        return base;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(bucketGenerator, recordsWriterManager);
    }

    @Override
    public void snapshot() throws Exception {
        refresh();
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
