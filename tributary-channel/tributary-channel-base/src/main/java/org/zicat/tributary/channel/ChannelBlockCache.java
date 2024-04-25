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

package org.zicat.tributary.channel;

import org.zicat.tributary.common.CircularOrderedQueue;

/** ChannelBlockCache. */
public class ChannelBlockCache extends CircularOrderedQueue<ChannelBlockCache.Block> {

    private static final Compare<Block, Offset> HANDLER =
            (block, recordOffset) -> block.compare(recordOffset.segmentId(), recordOffset.offset);

    public ChannelBlockCache(int blockCount) {
        super(new Block[blockCount]);
    }

    /**
     * find.
     *
     * @param recordOffset recordOffset
     * @return BufferRecordsOffset
     */
    public BlockGroupOffset find(BlockGroupOffset recordOffset) {
        final Block block = super.find(recordOffset, HANDLER);
        if (block == null) {
            return null;
        }
        final BlockReader oldReader = recordOffset.blockReader;
        final BlockReader newReader = oldReader.cacheBlockReader(block.data, block.lengthInFile());
        return recordOffset.newOffsetReader(block.nextOffset, newReader);
    }

    /**
     * put the block.
     *
     * @param segmentId segmentId
     * @param currentOffset currentOffset
     * @param nextOffset nextOffset
     * @param data data
     */
    public void put(long segmentId, long currentOffset, long nextOffset, byte[] data) {
        super.put(new Block(segmentId, currentOffset, nextOffset, data));
    }

    /** Block. */
    public static class Block {
        private final long segmentId;
        private final long currentOffset;
        private final long nextOffset;
        private final byte[] data;

        public Block(long segmentId, long currentOffset, long nextOffset, byte[] data) {
            this.segmentId = segmentId;
            this.currentOffset = currentOffset;
            this.nextOffset = nextOffset;
            this.data = data;
        }

        public long lengthInFile() {
            return Math.max(nextOffset - currentOffset, 0);
        }

        /**
         * return 0 if equals, > 0 if this block is bigger, < 0 if this block is smaller.
         *
         * @param segmentId segmentId
         * @param offset offset
         * @return long
         */
        public long compare(long segmentId, long offset) {
            if (this.segmentId != segmentId) {
                return this.segmentId - segmentId;
            } else {
                return this.currentOffset - offset;
            }
        }
    }
}
