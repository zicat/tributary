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

package org.zicat.tributary.channle.file.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.file.FileSegment;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static org.zicat.tributary.channel.file.FileSegmentUtil.SEGMENT_HEAD_SIZE;
import static org.zicat.tributary.channel.file.FileSegmentUtil.legalOffset;
import static org.zicat.tributary.common.VIntUtil.putVInt;

/** BufferReaderWriterTest. */
public class BufferRecordsResultSetWriterTest {

    FileChannel fileChannel;
    File dir = FileUtils.createTmpDir("buffer_records_result_set_writer_test");
    File file = new File(dir, "foo.log");

    @Before
    public void before() throws IOException {
        IOUtils.deleteDir(dir);
        IOUtils.makeDir(dir);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        fileChannel = randomAccessFile.getChannel();
        ByteBuffer segmentHeader = ByteBuffer.allocate(SEGMENT_HEAD_SIZE);
        IOUtils.writeFull(fileChannel, segmentHeader);
    }

    @After
    public void after() {
        IOUtils.closeQuietly(fileChannel);
        IOUtils.deleteDir(dir);
    }

    static final class MockBlockFlushHandler implements BlockWriter.BlockFlushHandler {

        public int count;
        private final FileChannel fileChannel;
        private final CompressionType compressionType;

        public MockBlockFlushHandler(FileChannel fileChannel, CompressionType compressionType) {
            this.fileChannel = fileChannel;
            this.compressionType = compressionType;
        }

        @Override
        public void callback(Block block) throws IOException {
            block.reusedBuf(compressionType.compression(block.resultBuf(), block.reusedBuf()));
            count = IOUtils.writeFull(fileChannel, block.reusedBuf());
        }
    }

    @Test
    public void test() throws IOException {

        // test writer. append "foo"
        final MockBlockFlushHandler handler =
                new MockBlockFlushHandler(fileChannel, CompressionType.SNAPPY);
        final BlockWriter writer = new BlockWriter(6);
        final byte[] bs = "foo".getBytes(StandardCharsets.UTF_8);
        Assert.assertTrue(writer.put(bs, 0, bs.length));
        Assert.assertTrue(writer.remaining() > 0);
        Assert.assertFalse(writer.put(bs, 0, bs.length));
        writer.clear(handler);
        Assert.assertTrue(handler.count > 0);

        // test wrap. append "lynn:
        final byte[] bs2 = "lynn".getBytes(StandardCharsets.UTF_8);
        final BlockWriter writer2 = BlockWriter.wrap(bs2, 0, bs2.length);
        Assert.assertFalse(writer2.put(bs, 0, 1));
        writer2.clear(handler);
        Assert.assertTrue(handler.count > 0);

        // test reAllocate . append "foo"
        final BlockWriter writer3 = writer.reAllocate(6);
        Assert.assertSame(writer3.reusedBuf(), writer.reusedBuf());
        Assert.assertSame(writer3.resultBuf(), writer3.resultBuf());
        Assert.assertTrue(writer3.put(bs, 0, bs.length));
        Assert.assertTrue(writer3.remaining() > 0);
        Assert.assertFalse(writer3.put(bs, 0, bs.length));
        writer3.clear(handler);
        Assert.assertTrue(handler.count > 0);

        fileChannel.force(false);

        // test buffer reader
        BlockRecordsOffset bufferRecordsResultSet =
                BlockRecordsOffset.cast(new RecordsOffset(0, 0));
        long offset = legalOffset(bufferRecordsResultSet.offset());
        Assert.assertTrue(offset >= 2);
        Assert.assertFalse(offset >= 10);

        FileSegment fileSegment =
                new FileSegment(
                        1L,
                        new BlockWriter(1024),
                        CompressionType.SNAPPY,
                        10240,
                        SEGMENT_HEAD_SIZE,
                        file,
                        fileChannel);
        RecordsResultSet resultSet =
                fileSegment.read(bufferRecordsResultSet, fileChannel.position()).toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("foo", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        bufferRecordsResultSet = BlockRecordsOffset.cast(resultSet.nexRecordsOffset());
        Assert.assertSame(bufferRecordsResultSet, resultSet.nexRecordsOffset());
        resultSet = fileSegment.read(bufferRecordsResultSet, fileChannel.position()).toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("lynn", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        bufferRecordsResultSet = BlockRecordsOffset.cast(resultSet.nexRecordsOffset());
        resultSet = fileSegment.read(bufferRecordsResultSet, fileChannel.position()).toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("foo", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        // test over flow.
        resultSet =
                fileSegment.read(bufferRecordsResultSet, fileChannel.position() - 1).toResultSet();
        Assert.assertFalse(resultSet.hasNext());

        BlockWriter writer4 = writer.reAllocate(6);
        writer4.put(bs2, 0, bs2.length);
        writer4.clear(handler);
        fileChannel.force(false);

        RecordsOffset newOffset =
                resultSet
                        .nexRecordsOffset()
                        .skip2Target(
                                resultSet.nexRecordsOffset().segmentId(),
                                resultSet.nexRecordsOffset().offset() + 1);
        resultSet =
                fileSegment
                        .read(BlockRecordsOffset.cast(newOffset), fileChannel.position())
                        .toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("lynn", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        // test only length
        ByteBuffer lengthBuffer = ByteBuffer.allocate(5);
        putVInt(lengthBuffer, 2);
        lengthBuffer.flip();
        IOUtils.writeFull(fileChannel, lengthBuffer);
        fileChannel.force(false);

        resultSet =
                fileSegment
                        .read(
                                BlockRecordsOffset.cast(resultSet.nexRecordsOffset()),
                                fileChannel.position())
                        .toResultSet();
        Assert.assertFalse(resultSet.hasNext());

        writer4 = writer.reAllocate(6);
        writer4.put(bs2, 0, bs2.length);
        writer4.clear(handler);
        fileChannel.force(false);

        resultSet =
                fileSegment
                        .read(
                                BlockRecordsOffset.cast(resultSet.nexRecordsOffset()),
                                fileChannel.position())
                        .toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("lynn", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        Assert.assertEquals(fileChannel.position(), fileChannel.size());
        Assert.assertEquals(resultSet.nexRecordsOffset().offset(), fileChannel.position());
    }
}
