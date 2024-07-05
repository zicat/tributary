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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.hdfs.bucket.BucketWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.zicat.tributary.common.Strings.removeLastIfMatch;
import static org.zicat.tributary.sink.hdfs.HDFSSinkOptions.OPTION_SINK_PATH;

/** DefaultBucketWriterManager. */
public class DefaultRecordsWriterManager implements RecordsWriterManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRecordsWriterManager.class);

    protected transient String basePath;
    protected transient Map<String, BucketWriter> bucketWriters;
    protected transient String prefixFileName;
    protected transient Context context;

    @Override
    public void open(Context context) throws Exception {
        this.context = context;
        this.bucketWriters = new HashMap<>();
        final String sinkPath = context.get(OPTION_SINK_PATH).trim();
        this.basePath = removeLastIfMatch(sinkPath, DIRECTORY_DELIMITER);
        this.prefixFileName = prefixFileNameInBucket();
    }

    @Override
    public BucketWriter getOrCreateRecordsWriter(String bucket) {
        BucketWriter writer = bucketWriters.get(bucket);
        if (writer != null) {
            return writer;
        }
        final String bucketPath = basePath + DIRECTORY_DELIMITER + bucket;
        writer = new BucketWriter(context, bucketPath, prefixFileName);
        bucketWriters.put(bucket, writer);
        return writer;
    }

    @Override
    public boolean contains(String bucket) {
        return bucketWriters.containsKey(bucket);
    }

    /**
     * create prefix file name.
     *
     * @return prefix file name
     */
    protected String prefixFileNameInBucket() {
        return UUID.randomUUID().toString().replace("-", "_") + "_" + context.id();
    }

    @Override
    public void closeAllBuckets() throws IOException {
        for (Map.Entry<String, BucketWriter> entry : bucketWriters.entrySet()) {
            entry.getValue().close();
        }
        bucketWriters.clear();
    }

    @Override
    public int bucketsCount() {
        return bucketWriters.size();
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<String, BucketWriter> entry : bucketWriters.entrySet()) {
            final String bucketPath = entry.getKey();
            final BucketWriter writer = entry.getValue();
            LOG.info("Closing {}", bucketPath);
            try {
                writer.close();
            } catch (Exception ex) {
                LOG.warn("Closing {}. Exception follows.", bucketPath, ex);
            }
        }
        bucketWriters.clear();
    }
}
