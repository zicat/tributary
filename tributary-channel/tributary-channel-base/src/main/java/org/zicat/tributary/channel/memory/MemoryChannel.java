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

import org.zicat.tributary.channel.AbstractChannel;
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.common.IOUtils;

import java.util.Map;

import static org.zicat.tributary.channel.memory.MemoryGroupManager.createUnPersistGroupManager;

/** MemoryChannel. */
public class MemoryChannel extends AbstractChannel<MemorySegment> {

    private final Long segmentSize;
    private final CompressionType compressionType;
    private final BlockWriter blockWriter;

    protected MemoryChannel(
            String topic,
            Map<String, RecordsOffset> groupOffsets,
            int blockSize,
            Long segmentSize,
            CompressionType compressionType) {
        super(topic, createUnPersistGroupManager(groupOffsets));
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        this.blockWriter = new BlockWriter(blockSize);
    }

    @Override
    protected MemorySegment createSegment(long id) {
        return new MemorySegment(id, blockWriter, compressionType, segmentSize);
    }

    @Override
    public void close() {
        try {
            IOUtils.closeQuietly(groupManager);
        } finally {
            super.close();
        }
    }

    /** load last segment. */
    protected void loadLastSegment() {
        initLastSegment(createSegment(0L));
    }
}
