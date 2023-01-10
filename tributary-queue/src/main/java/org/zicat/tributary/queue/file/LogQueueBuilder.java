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

package org.zicat.tributary.queue.file;

import org.zicat.tributary.queue.CompressionType;

import java.util.List;
import java.util.concurrent.TimeUnit;

/** LogQueueBuilder. */
public class LogQueueBuilder {

    protected String topic;
    protected Long segmentSize;
    protected Integer blockSize;
    protected CompressionType compressionType;
    protected long cleanUpPeriod = 5;
    protected TimeUnit cleanUpUnit = TimeUnit.MINUTES;

    protected long flushPeriod = 1;
    protected TimeUnit flushTimeUnit = TimeUnit.SECONDS;
    protected long flushPageCacheSize = 1024L * 1024L * 32L;
    protected boolean flushForce = false;

    protected List<String> consumerGroups;

    /**
     * set cleanup period.
     *
     * @param cleanUpPeriod cleanUpPeriod
     * @param cleanUpUnit TimeUnit
     * @return this
     */
    public LogQueueBuilder cleanUpPeriod(long cleanUpPeriod, TimeUnit cleanUpUnit) {
        this.cleanUpPeriod = cleanUpPeriod;
        this.cleanUpUnit = cleanUpUnit;
        return this;
    }

    /**
     * set compression type.
     *
     * @param compressionType compressionType
     * @return this
     */
    public LogQueueBuilder compressionType(CompressionType compressionType) {
        if (compressionType != null) {
            this.compressionType = compressionType;
        }
        return this;
    }

    /**
     * set flush page cache size.
     *
     * @param flushPageCacheSize flushPageCacheSize
     * @return this
     */
    public LogQueueBuilder flushPageCacheSize(long flushPageCacheSize) {
        this.flushPageCacheSize = flushPageCacheSize;
        return this;
    }

    /**
     * set flush period .
     *
     * @param flushPeriod flushPeriod
     * @param flushTimeUnit flushTimeUnit
     * @return this
     */
    public LogQueueBuilder flushPeriod(long flushPeriod, TimeUnit flushTimeUnit) {
        this.flushPeriod = flushPeriod;
        this.flushTimeUnit = flushTimeUnit;
        return this;
    }

    /**
     * set segment size.
     *
     * @param segmentSize segmentSize
     * @return this
     */
    public LogQueueBuilder segmentSize(Long segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    /**
     * set block size.
     *
     * @param blockSize blockSize
     * @return this
     */
    public LogQueueBuilder blockSize(Integer blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    /**
     * set topic.
     *
     * @param topic topic
     * @return this
     */
    public LogQueueBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * set consumer groups.
     *
     * @param consumerGroups consumerGroups
     * @return this
     */
    public LogQueueBuilder consumerGroups(List<String> consumerGroups) {
        this.consumerGroups = consumerGroups;
        return this;
    }

    /**
     * set consumer groups.
     *
     * @param flushForce flushForce
     * @return this
     */
    public LogQueueBuilder flushForce(boolean flushForce) {
        this.flushForce = flushForce;
        return this;
    }
}
