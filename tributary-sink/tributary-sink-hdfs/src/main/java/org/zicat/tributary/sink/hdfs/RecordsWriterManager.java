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

import org.zicat.tributary.sink.config.Context;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;

/** BucketWriterManager. */
public interface RecordsWriterManager extends Closeable {

    String DIRECTORY_DELIMITER = FileSystems.getDefault().getSeparator();

    /**
     * open the function.
     *
     * @param context context
     * @throws Exception Exception
     */
    void open(Context context) throws Exception;

    /**
     * create bucket writer.
     *
     * @param bucket bucket
     * @return BucketWriter
     */
    RecordsWriter getOrCreateRecordsWriter(String bucket) throws IOException;

    /**
     * check whether contains bucket.
     *
     * @param bucket bucket
     * @return contains
     */
    boolean contains(String bucket);

    /** close all buckets. */
    void closeAllBuckets() throws Exception;

    /**
     * get bucket count.
     *
     * @return int
     */
    int bucketsCount();
}
