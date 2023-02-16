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

/** BlockGroupOffset. */
public class BlockGroupOffset extends GroupOffset {

    protected final BlockReader blockReader;

    public BlockGroupOffset(long segmentId, long offset, String groupId, BlockReader blockReader) {
        super(segmentId, offset, groupId);
        this.blockReader = blockReader == null ? new BlockReader(null, null, 0) : blockReader;
    }

    private BlockGroupOffset(long segmentId, long offset, String groupId) {
        this(segmentId, offset, groupId, null);
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
     * @param groupId groupId
     * @return BlockGroupOffset
     */
    public static BlockGroupOffset cast(long segmentId, String groupId) {
        return cast(segmentId, 0, groupId);
    }

    /**
     * create block by segment id and offset.
     *
     * @param segmentId segmentId
     * @param offset offset
     * @param groupId groupId
     * @return BlockGroupOffset
     */
    public static BlockGroupOffset cast(long segmentId, long offset, String groupId) {
        return new BlockGroupOffset(segmentId, offset, groupId);
    }

    /**
     * cast GroupOffset as BlockGroupOffset.
     *
     * @param groupOffset groupOffset
     * @return BlockGroupOffset
     */
    public static BlockGroupOffset cast(GroupOffset groupOffset) {
        if (groupOffset instanceof BlockGroupOffset) {
            return (BlockGroupOffset) groupOffset;
        }
        return new BlockGroupOffset(
                groupOffset.segmentId(), groupOffset.offset(), groupOffset.groupId());
    }

    /**
     * reset block.
     *
     * @return BlockGroupOffset
     */
    public final BlockGroupOffset reset() {
        blockReader.reset();
        return this;
    }

    @Override
    public BlockGroupOffset skip2TargetOffset(long newOffset) {
        return skip2Target(segmentId, newOffset, groupId);
    }

    @Override
    public BlockGroupOffset skipNextSegmentHead() {
        return skip2TargetHead(segmentId() + 1);
    }

    @Override
    public BlockGroupOffset skip2TargetHead(long segmentId) {
        return skip2Target(segmentId, 0, groupId);
    }

    @Override
    public BlockGroupOffset skip2Target(GroupOffset groupOffset) {
        return skip2Target(groupOffset.segmentId(), groupOffset.offset(), groupOffset.groupId);
    }

    @Override
    public BlockGroupOffset skip2Target(long segmentId, long offset, String groupId) {
        return new BlockGroupOffset(segmentId, offset, groupId, blockReader);
    }

    /** RecordsResultSetImpl. */
    private class RecordsResultSetImpl implements RecordsResultSet {

        private byte[] nextData;

        RecordsResultSetImpl() {
            next();
        }

        @Override
        public final GroupOffset nexGroupOffset() {
            return BlockGroupOffset.this;
        }

        @Override
        public final boolean hasNext() {
            return nextData != null;
        }

        @Override
        public final byte[] next() {
            final byte[] result = this.nextData;
            this.nextData = BlockGroupOffset.this.blockReader.readNext();
            return result;
        }

        @Override
        public final long readBytes() {
            return BlockGroupOffset.this.blockReader.readBytes();
        }
    }
}
