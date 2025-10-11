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

/** BlockReaderOffset. */
public class BlockReaderOffset extends Offset {

    protected final BlockReader blockReader;

    public BlockReaderOffset(long segmentId, long offset, BlockReader blockReader) {
        super(segmentId, offset);
        this.blockReader = blockReader == null ? BlockReader.emptyBlockReader() : blockReader;
    }

    private BlockReaderOffset(long segmentId, long offset) {
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
     * get block reader.
     *
     * @return Block
     */
    public BlockReader blockReader() {
        return blockReader;
    }

    /**
     * create block by segment id.
     *
     * @param segmentId segmentId
     * @return BlockReaderOffset
     */
    public static BlockReaderOffset cast(long segmentId) {
        return cast(segmentId, 0);
    }

    /**
     * create block by segment id and offset.
     *
     * @param segmentId segmentId
     * @param offset offset
     * @return BlockReaderOffset
     */
    public static BlockReaderOffset cast(long segmentId, long offset) {
        return new BlockReaderOffset(segmentId, offset);
    }

    /**
     * cast offset as BlockReaderOffset.
     *
     * @param offset offset
     * @return BlockReaderOffset
     */
    public static BlockReaderOffset cast(Offset offset) {
        if (offset instanceof BlockReaderOffset) {
            return (BlockReaderOffset) offset;
        }
        return new BlockReaderOffset(offset.segmentId(), offset.offset());
    }

    /**
     * reset block.
     *
     * @return BlockReaderOffset
     */
    public final BlockReaderOffset reset() {
        blockReader.reset();
        return this;
    }

    /**
     * new offset reader.
     *
     * @param offset offset
     * @param blockReader blockReader
     * @return BlockReaderOffset
     */
    public BlockReaderOffset newOffsetReader(long offset, BlockReader blockReader) {
        return new BlockReaderOffset(segmentId, offset, blockReader);
    }

    /** RecordsResultSetImpl. */
    private class RecordsResultSetImpl implements RecordsResultSet {

        private byte[] nextData;

        RecordsResultSetImpl() {
            next();
        }

        @Override
        public final Offset nexOffset() {
            return BlockReaderOffset.this;
        }

        @Override
        public final boolean hasNext() {
            return nextData != null;
        }

        @Override
        public final byte[] next() {
            final byte[] result = this.nextData;
            this.nextData = BlockReaderOffset.this.blockReader.readNext();
            return result;
        }

        @Override
        public final long readBytes() {
            return BlockReaderOffset.this.blockReader.readBytes();
        }
    }

    @Override
    public BlockReaderOffset skip2TargetOffset(long newOffset) {
        return skip2Target(segmentId, newOffset);
    }

    @Override
    public BlockReaderOffset skipNextSegmentHead() {
        return skip2TargetHead(segmentId() + 1);
    }

    @Override
    public BlockReaderOffset skip2TargetHead(long segmentId) {
        return skip2Target(segmentId, 0);
    }

    @Override
    public BlockReaderOffset skip2Target(Offset offset) {
        return skip2Target(offset.segmentId(), offset.offset());
    }

    @Override
    public BlockReaderOffset skip2Target(long segmentId, long offset) {
        if (segmentId == this.segmentId && offset == this.offset) {
            return this;
        }
        return new BlockReaderOffset(segmentId, offset, blockReader);
    }

    @Override
    public BlockReaderOffset skipOffset(long offset) {
        return skip2Target(segmentId, offset);
    }
}
