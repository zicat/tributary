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

import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.Context;

import java.io.Closeable;

/** BucketGenerator. */
public interface BucketGenerator extends Closeable {

    /**
     * open generator.
     *
     * @param context context
     */
    void open(Context context);

    /**
     * get bucket by records.
     *
     * @param records records
     * @return string
     */
    String getBucket(Records records);

    /**
     * check whether should refresh bucket, if should execute handler.
     *
     * @param force force
     * @param handler handler
     */
    void checkRefresh(boolean force, RefreshHandler handler) throws Exception;

    /** RefreshHandler. */
    interface RefreshHandler {

        /** do refresh. */
        void doRefresh() throws Exception;
    }
}
