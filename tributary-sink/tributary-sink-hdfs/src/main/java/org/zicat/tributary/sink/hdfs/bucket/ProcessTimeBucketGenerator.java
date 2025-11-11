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

package org.zicat.tributary.sink.hdfs.bucket;

import static org.zicat.tributary.sink.hdfs.HDFSSinkOptions.*;

import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.common.Clock;
import org.zicat.tributary.sink.config.Context;
import org.zicat.tributary.sink.hdfs.RecordsWriterManager;

import java.io.IOException;

/** ProcessTimeBucketGenerator. */
public class ProcessTimeBucketGenerator implements BucketGenerator {

    protected transient String bucketDateFormat = null;
    protected transient String bucketDateTimeZone = null;
    protected transient String timeBucket = null;
    protected transient Clock clock;

    @Override
    public void open(Context context) {
        bucketDateFormat = context.get(OPTION_BUCKET_DATE_FORMAT);
        bucketDateTimeZone = context.get(OPTION_BUCKET_DATE_TIMEZONE);
        clock = context.get(OPTION_CLOCK);
        timeBucket = clock.currentTime(bucketDateFormat, bucketDateTimeZone);
    }

    @Override
    public String getBucket(Records records) {
        final String dataBucket = records.topic();
        return timeBucket + RecordsWriterManager.DIRECTORY_DELIMITER + dataBucket;
    }

    @Override
    public void checkRefresh(RefreshHandler handler) throws Exception {
        final String currentTimeBucket = clock.currentTime(bucketDateFormat, bucketDateTimeZone);
        if (!currentTimeBucket.equals(timeBucket)) {
            handler.doRefresh();
            timeBucket = currentTimeBucket;
        }
    }

    /**
     * time bucket.
     *
     * @return timeBucket
     */
    public String timeBucket() {
        return timeBucket;
    }

    @Override
    public void close() throws IOException {}
}
