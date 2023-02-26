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

import org.zicat.tributary.channel.*;
import org.zicat.tributary.common.TributaryRuntimeException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.zicat.tributary.channel.file.FileSegmentUtil.getIdByName;
import static org.zicat.tributary.channel.file.FileSegmentUtil.isFileSegment;

/**
 * FileChannel implements {@link Channel} to Storage records and {@link GroupOffset} in local file
 * system.
 *
 * <p>All public methods in FileChannel are @ThreadSafe.
 *
 * <p>FileChannel ignore partition params and append/pull all records in the {@link
 * FileChannel#dir}. {@link Channel} support multi partitions operations.
 *
 * <p>Data files {@link FileSegment} name start with {@link FileChannel#topic()}
 *
 * <p>Only one {@link FileSegment} is writeable and support multi threads write it, multi threads
 * can read writable segment or other segments tagged as finished(not writable).
 *
 * <p>FileChannel support commit groupOffset and support clean up expired segments(all group ids has
 * commit the offset over this segments) async}
 */
public class FileChannel extends AbstractChannel<FileSegment> {

    private final File dir;
    private final Long segmentSize;
    private final CompressionType compressionType;
    private final BlockWriter blockWriter;
    private final long flushPageCacheSize;

    protected FileChannel(
            String topic,
            MemoryGroupManagerFactory factory,
            int blockSize,
            Long segmentSize,
            CompressionType compressionType,
            File dir) {
        super(topic, factory);
        this.blockWriter = new BlockWriter(blockSize);
        this.flushPageCacheSize = blockSize * 10L;
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        this.dir = dir;
        createLastSegment();
    }

    @Override
    protected FileSegment createSegment(long id) {
        return new FileSegment.Builder()
                .fileId(id)
                .dir(dir)
                .segmentSize(segmentSize)
                .filePrefix(topic())
                .compressionType(compressionType)
                .build(blockWriter);
    }

    @Override
    protected boolean append2Segment(Segment segment, byte[] record, int offset, int length)
            throws IOException {
        final boolean appendSuccess = super.append2Segment(segment, record, offset, length);
        if (appendSuccess && segment.cacheUsed() >= flushPageCacheSize) {
            segment.flush(false);
        }
        return appendSuccess;
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
        cleanUp();
    }
}
