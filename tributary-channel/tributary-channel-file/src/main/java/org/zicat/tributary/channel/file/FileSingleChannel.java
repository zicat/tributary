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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.Segment.AppendResult;
import org.zicat.tributary.common.MemorySize;
import org.zicat.tributary.common.PercentSize;
import static org.zicat.tributary.common.PercentSize.memoryPercent;
import org.zicat.tributary.common.TributaryRuntimeException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.channel.file.FileSegmentUtil.getIdByName;
import static org.zicat.tributary.channel.file.FileSegmentUtil.isFileSegment;

/**
 * FileChannel implements {@link Channel} to Storage records and {@link Offset} in local file
 * system.
 *
 * <p>All public methods in FileChannel are @ThreadSafe.
 *
 * <p>FileChannel ignore partition params and append/pull all records in the {@link
 * FileSingleChannel#dir}. {@link Channel} support multi partitions operations.
 *
 * <p>Data files {@link FileSegment} name start with {@link FileSingleChannel#topic()}
 *
 * <p>Only one {@link FileSegment} is writeable and support multi threads write it, multi threads
 * can read writable segment or other segments tagged as finished(not writable).
 *
 * <p>FileChannel support commit group offset and support clean up expired segments(all group ids
 * has commit the offset over those segments) async}
 */
public class FileSingleChannel extends AbstractSingleChannel<FileSegment> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSingleChannel.class);

    private final File dir;
    private final Long segmentSize;
    private final CompressionType compressionType;
    private final BlockWriter blockWriter;
    private final long flushPageCacheSize;
    private final boolean appendSyncWait;
    private final long appendSyncWaitTimeoutMs;
    private final FileStore fileStore;

    protected FileSingleChannel(
            String topic,
            SingleGroupManagerFactory factory,
            int blockSize,
            Long segmentSize,
            CompressionType compressionType,
            File dir,
            int blockCacheCount,
            boolean appendSyncWait,
            long appendSyncWaitTimeoutMs,
            FileStore fileStore) {
        super(topic, blockCacheCount, factory);
        if (blockSize >= segmentSize) {
            throw new IllegalArgumentException("block size must less than segment size");
        }
        this.blockWriter = new BlockWriter(blockSize);
        this.flushPageCacheSize = blockSize * 10L;
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        this.dir = dir;
        this.appendSyncWait = appendSyncWait;
        this.appendSyncWaitTimeoutMs = appendSyncWaitTimeoutMs;
        this.fileStore = fileStore;
        createLastSegment();
    }

    @Override
    protected FileSegment createSegment(long id) {
        LOG.info(
                "create segment path: {}, compression type:{}, segment size:{}, block size:{}, block cache count:{}",
                dir.getPath(),
                compressionType.name(),
                segmentSize,
                blockWriter.capacity(),
                bCache == null ? 0 : bCache.blockCount());
        return new FileSegment.Builder()
                .fileId(id)
                .dir(dir)
                .segmentSize(segmentSize)
                .filePrefix(topic())
                .compressionType(compressionType)
                .blockCache(bCache)
                .build(blockWriter);
    }

    @Override
    public void append(ByteBuffer byteBuffer) throws IOException, InterruptedException {
        final AppendResult appendResult = innerAppend(byteBuffer);
        if (appendSyncWait) {
            appendResult.await2Storage(appendSyncWaitTimeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    protected AppendResult append2Segment(Segment segment, ByteBuffer byteBuffer)
            throws IOException {
        final AppendResult result = super.append2Segment(segment, byteBuffer);
        if (result.appended() && segment.cacheUsed() >= flushPageCacheSize) {
            segment.flush(false);
        }
        return result;
    }

    @Override
    public boolean checkCapacityExceed(PercentSize capacityPercent) throws IOException {
        final MemorySize used = new MemorySize(fileStore.getUsableSpace());
        final MemorySize capacity = new MemorySize(fileStore.getTotalSpace());
        final PercentSize usedPercent = memoryPercent(used, capacity);
        return usedPercent.compareTo(capacityPercent) > 0;
    }

    /** load segments. */
    private void createLastSegment() {

        final File[] files = dir.listFiles(file -> isFileSegment(topic(), file.getName()));
        if (files == null || files.length == 0) {
            initLastSegment(createSegment(0));
            return;
        }

        // load segment and find max.
        FileSegment maxSegment = null;
        final List<FileSegment> remainingSegment = new ArrayList<>();
        for (File file : files) {
            final String fileName = file.getName();
            final long id = getIdByName(topic(), fileName);
            final FileSegment segment = createSegment(id);
            addSegment(segment);
            remainingSegment.add(segment);
            maxSegment = SegmentUtil.max(segment, maxSegment);
        }
        for (FileSegment fileSegment : remainingSegment) {
            try {
                fileSegment.readonly();
            } catch (IOException e) {
                throw new TributaryRuntimeException("finish history segment fail", e);
            }
        }
        initLastSegment(createSegment(maxSegment.segmentId() + 1));
        cleanUpExpiredSegmentsQuietly();
    }
}
