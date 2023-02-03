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

package org.zicat.tributary.channel.file;

import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.RecordsResultSet;

/** BlockRecordsOffset. */
public class BlockRecordsOffset extends RecordsOffset {

    protected final BlockReader blockReader;

    public BlockRecordsOffset(long segmentId, long offset, BlockReader blockReader) {
        super(segmentId, offset);
        this.blockReader = blockReader == null ? new BlockReader(null, null, 0) : blockReader;
    }

    private BlockRecordsOffset(long segmentId, long offset) {
        this(segmentId, offset, null);
    }

    /**
     * create result set.
     *
     * @return RecordsResultSet
     */
    public RecordsResultSet toResultSet() {
        return new RecordsResultSetImpl();
    }

    /**
     * get block.
     *
     * @return Block
     */
    public Block block() {
        return blockReader;
    }

    /**
     * create block by segment id.
     *
     * @param segmentId segmentId
     * @return BlockRecordsOffset
     */
    public static BlockRecordsOffset cast(long segmentId) {
        return cast(segmentId, 0);
    }

    /**
     * create block by segment id and offset.
     *
     * @param segmentId segmentId
     * @param offset offset
     * @return BlockRecordsOffset
     */
    public static BlockRecordsOffset cast(long segmentId, long offset) {
        return new BlockRecordsOffset(segmentId, offset);
    }

    /**
     * cast RecordsOffset as BlockRecordsOffset.
     *
     * @param recordsOffset recordsOffset
     * @return BlockRecordsOffset
     */
    public static BlockRecordsOffset cast(RecordsOffset recordsOffset) {
        if (recordsOffset instanceof BlockRecordsOffset) {
            return (BlockRecordsOffset) recordsOffset;
        }
        return new BlockRecordsOffset(recordsOffset.segmentId(), recordsOffset.offset());
    }

    /**
     * reset block.
     *
     * @return BlockRecordsOffset
     */
    public final BlockRecordsOffset reset() {
        blockReader.reset();
        return this;
    }

    @Override
    public BlockRecordsOffset skip2TargetOffset(long newOffset) {
        return skip2Target(segmentId(), newOffset);
    }

    @Override
    public BlockRecordsOffset skipNextSegmentHead() {
        return skip2TargetHead(segmentId() + 1);
    }

    @Override
    public BlockRecordsOffset skip2TargetHead(long segmentId) {
        return skip2Target(segmentId, 0);
    }

    @Override
    public BlockRecordsOffset skip2Target(RecordsOffset recordsOffset) {
        return skip2Target(recordsOffset.segmentId(), recordsOffset.offset());
    }

    @Override
    public BlockRecordsOffset skip2Target(long segmentId, long offset) {
        return new BlockRecordsOffset(segmentId, offset, blockReader);
    }

    /** RecordsResultSetImpl. */
    private class RecordsResultSetImpl implements RecordsResultSet {

        private byte[] nextData;

        RecordsResultSetImpl() {
            next();
        }

        @Override
        public final RecordsOffset nexRecordsOffset() {
            return BlockRecordsOffset.this;
        }

        @Override
        public final boolean hasNext() {
            return nextData != null;
        }

        @Override
        public final byte[] next() {
            final byte[] result = this.nextData;
            this.nextData = BlockRecordsOffset.this.blockReader.readNext();
            return result;
        }

        @Override
        public final long readBytes() {
            return BlockRecordsOffset.this.blockReader.readBytes();
        }
    }
}
