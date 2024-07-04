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

import org.zicat.tributary.common.Threads;
import org.zicat.tributary.sink.function.Context;

/** BucketMeta. */
public class BucketMeta {

    public static final String IN_USE_SUFFIX = ".tmp";

    protected final String bucketPath;
    protected final String fileName;
    protected final long rollSize;
    protected final int maxRetries;
    protected final Context context;

    public BucketMeta(
            Context context, String bucketPath, String fileName, long rollSize, int maxRetries) {
        this.context = context;
        this.bucketPath = bucketPath;
        this.fileName = fileName;
        this.rollSize = rollSize;
        this.maxRetries = maxRetries;
    }

    /**
     * in use suffix.
     *
     * @return string
     */
    protected String inUseSuffix() {
        return IN_USE_SUFFIX;
    }

    /**
     * sleep on fail, wait for next retry.
     *
     * @return millis
     */
    protected long sleepOnFail() {
        return 10L;
    }

    /**
     * running with retry.
     *
     * @param runner runner
     */
    protected Throwable runWithRetry(BucketWriter.CallRunner runner, long sleepOnFail) {
        int retryCount = 0;
        Throwable exception = null;
        do {
            try {
                runner.call();
                return null;
            } catch (Throwable t) {
                if (exception == null) {
                    exception = t;
                }
                Threads.sleepQuietly(sleepOnFail);
            } finally {
                retryCount++;
            }
        } while (retryCount < maxRetries());
        return exception;
    }

    /**
     * get bucket path.
     *
     * @return string bucket path
     */
    public final String bucketPath() {
        return bucketPath;
    }

    /**
     * get file name.
     *
     * @return file name
     */
    public final String fileName() {
        return fileName;
    }

    /**
     * get roll size.
     *
     * @return roll size
     */
    public final long rollSize() {
        return rollSize;
    }

    /**
     * get max retries.
     *
     * @return long max retry
     */
    public final int maxRetries() {
        return maxRetries;
    }

    /** call runner. */
    public interface CallRunner {
        void call() throws Exception;
    }
}
