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

package org.zicat.tributary.channel.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.AbstractSingleChannel;
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.CompressionType;

/** MemorySingleChannel. */
public class MemorySingleChannel extends AbstractSingleChannel<MemorySegment> {

    private static final Logger LOG = LoggerFactory.getLogger(MemorySingleChannel.class);
    private final Long segmentSize;
    private final CompressionType compressionType;
    private final BlockWriter blockWriter;

    protected MemorySingleChannel(
            String topic,
            SingleGroupManagerFactory singleGroupManagerFactory,
            int blockSize,
            Long segmentSize,
            CompressionType compressionType,
            int blockCacheCount) {
        super(topic, blockCacheCount, singleGroupManagerFactory);
        this.blockWriter = new BlockWriter(blockSize);
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        loadLastSegment();
    }

    @Override
    protected MemorySegment createSegment(long id) {
        LOG.info(
                "create segment id: {}, compression type:{}, segment size:{}, block size:{}, block cache count:{}",
                id,
                compressionType.name(),
                segmentSize,
                blockWriter.capacity(),
                bCache == null ? 0 : bCache.blockCount());
        return new MemorySegment(id, blockWriter, compressionType, segmentSize, bCache);
    }

    /** load last segment. */
    private void loadLastSegment() {
        initLastSegment(createSegment(0L));
    }
}
