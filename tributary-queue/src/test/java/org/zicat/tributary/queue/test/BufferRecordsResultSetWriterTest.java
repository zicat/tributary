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

package org.zicat.tributary.queue.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zicat.tributary.queue.*;
import org.zicat.tributary.queue.file.FileBufferReader;
import org.zicat.tributary.queue.test.utils.FileUtils;
import org.zicat.tributary.queue.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static org.zicat.tributary.queue.file.LogSegmentUtil.SEGMENT_HEAD_SIZE;
import static org.zicat.tributary.queue.utils.VIntUtil.putVInt;

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

    static final class MockClearHandler implements BufferWriter.ClearHandler {

        public int count;
        private final FileChannel fileChannel;
        private final CompressionType compressionType;

        public MockClearHandler(FileChannel fileChannel, CompressionType compressionType) {
            this.fileChannel = fileChannel;
            this.compressionType = compressionType;
        }

        @Override
        public ByteBuffer clearCallback(ByteBuffer byteBuffer, ByteBuffer reusedBuffer)
                throws IOException {
            final ByteBuffer compressedBuffer =
                    compressionType.compression(byteBuffer, reusedBuffer);
            count = IOUtils.writeFull(fileChannel, compressedBuffer);
            return compressedBuffer;
        }
    }

    @Test
    public void test() throws IOException {

        // test writer. append "foo"
        final MockClearHandler handler = new MockClearHandler(fileChannel, CompressionType.SNAPPY);
        final BufferWriter writer = new BufferWriter(6);
        final byte[] bs = "foo".getBytes(StandardCharsets.UTF_8);
        Assert.assertTrue(writer.put(bs, 0, bs.length));
        Assert.assertTrue(writer.remaining() > 0);
        Assert.assertFalse(writer.put(bs, 0, bs.length));
        writer.clear(handler);
        Assert.assertTrue(handler.count > 0);

        // test wrap. append "lynn:
        final byte[] bs2 = "lynn".getBytes(StandardCharsets.UTF_8);
        final BufferWriter writer2 = BufferWriter.wrap(bs2, 0, bs2.length);
        Assert.assertFalse(writer2.put(bs, 0, 1));
        writer2.clear(handler);
        Assert.assertTrue(handler.count > 0);

        // test reAllocate . append "foo"
        final BufferWriter writer3 = writer.reAllocate(6);
        Assert.assertSame(writer3.compressionBuf(), writer.compressionBuf());
        Assert.assertSame(writer3.writeBuf(), writer3.writeBuf());
        Assert.assertTrue(writer3.put(bs, 0, bs.length));
        Assert.assertTrue(writer3.remaining() > 0);
        Assert.assertFalse(writer3.put(bs, 0, bs.length));
        writer3.clear(handler);
        Assert.assertTrue(handler.count > 0);

        fileChannel.force(false);

        // test buffer reader
        BufferRecordsOffset bufferRecordsResultSet =
                BufferRecordsOffset.cast(new RecordsOffset(0, 0));
        FileBufferReader fileBufferReader = FileBufferReader.from(bufferRecordsResultSet);
        Assert.assertTrue(fileBufferReader.reach(2));
        Assert.assertFalse(fileBufferReader.reach(10));

        RecordsResultSet resultSet =
                fileBufferReader
                        .readChannel(fileChannel, CompressionType.SNAPPY, fileChannel.position())
                        .toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("foo", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        bufferRecordsResultSet = BufferRecordsOffset.cast(resultSet.nexRecordsOffset());
        fileBufferReader = FileBufferReader.from(bufferRecordsResultSet);
        Assert.assertSame(bufferRecordsResultSet, resultSet.nexRecordsOffset());
        resultSet =
                fileBufferReader
                        .readChannel(fileChannel, CompressionType.SNAPPY, fileChannel.position())
                        .toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("lynn", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        bufferRecordsResultSet = BufferRecordsOffset.cast(resultSet.nexRecordsOffset());
        fileBufferReader = FileBufferReader.from(bufferRecordsResultSet);
        resultSet =
                fileBufferReader
                        .readChannel(fileChannel, CompressionType.SNAPPY, fileChannel.position())
                        .toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("foo", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        // test over flow.
        resultSet =
                fileBufferReader
                        .readChannel(
                                fileChannel, CompressionType.SNAPPY, fileChannel.position() - 1)
                        .toResultSet();
        Assert.assertFalse(resultSet.hasNext());

        BufferWriter writer4 = writer.reAllocate(6);
        writer4.put(bs2, 0, bs2.length);
        writer4.clear(handler);
        fileChannel.force(false);

        RecordsOffset newOffset =
                resultSet
                        .nexRecordsOffset()
                        .skip2Target(
                                resultSet.nexRecordsOffset().segmentId(),
                                resultSet.nexRecordsOffset().offset() + 1);
        fileBufferReader = FileBufferReader.from(BufferRecordsOffset.cast(newOffset));
        resultSet =
                fileBufferReader
                        .readChannel(fileChannel, CompressionType.SNAPPY, fileChannel.position())
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

        fileBufferReader =
                FileBufferReader.from(BufferRecordsOffset.cast(resultSet.nexRecordsOffset()));
        resultSet =
                fileBufferReader
                        .readChannel(fileChannel, CompressionType.SNAPPY, fileChannel.position())
                        .toResultSet();
        Assert.assertFalse(resultSet.hasNext());

        writer4 = writer.reAllocate(6);
        writer4.put(bs2, 0, bs2.length);
        writer4.clear(handler);
        fileChannel.force(false);

        fileBufferReader =
                FileBufferReader.from(BufferRecordsOffset.cast(resultSet.nexRecordsOffset()));
        resultSet =
                fileBufferReader
                        .readChannel(fileChannel, CompressionType.SNAPPY, fileChannel.position())
                        .toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals("lynn", new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());

        Assert.assertEquals(fileChannel.position(), fileChannel.size());
        Assert.assertEquals(resultSet.nexRecordsOffset().offset(), fileChannel.position());
    }
}
