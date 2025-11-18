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

package org.zicat.tributary.common.test.util;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.util.IOUtils;
import static org.zicat.tributary.common.util.IOUtils.concurrentCloseQuietly;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

/** IOUtilsTest. */
public class IOUtilsTest {

    @Test
    public void testMkDirDeleteDir() {
        final File dir = FileUtils.createTmpDir("test_mk_dir_delete_dir");
        IOUtils.deleteDir(dir);
        final File childDir = new File(dir, "cc");
        final File ccDir = new File(childDir, "c2");
        Assert.assertTrue(IOUtils.makeDir(ccDir));
        Assert.assertTrue(IOUtils.makeDir(childDir));
        Assert.assertTrue(IOUtils.deleteDir(ccDir));
        Assert.assertTrue(IOUtils.deleteDir(dir));
    }

    @Test
    public void testCloseable() {

        final AtomicBoolean close = new AtomicBoolean(false);
        Closeable closeable = () -> close.set(true);
        IOUtils.closeQuietly(closeable);
        Assert.assertTrue(close.get());

        IOUtils.closeQuietly(
                () -> {
                    throw new IOException("test");
                });
        try {
            IOUtils.closeQuietly(
                    () -> {
                        throw new Error("test error");
                    });
            Assert.fail("close quietly must not deal with error ");
        } catch (Throwable e) {
            Assert.assertTrue("", true);
        }
    }

    @Test
    public void testReadWriteChannel() throws IOException {
        final File dir = FileUtils.createTmpDir("test_read_writer_channel");
        final File file = new File(dir, "aa.txt");
        if (file.exists() && !file.delete()) {
            throw new IOException("file delete fail " + file.getPath());
        }
        if (!file.createNewFile()) {
            throw new IOException("file create fail " + file.getPath());
        }
        final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        final FileChannel channel = randomAccessFile.getChannel();

        final String testString = "aaaa";
        final byte[] testBs = testString.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(testBs);
        IOUtils.writeFull(channel, byteBuffer);
        Assert.assertFalse(byteBuffer.hasRemaining());

        final ByteBuffer readBuffer = ByteBuffer.allocate(testBs.length);
        IOUtils.readFully(channel, readBuffer, 0).flip();
        final byte[] result = new byte[testBs.length];
        readBuffer.get(result);
        Assert.assertEquals(testString, new String(result, StandardCharsets.UTF_8));

        IOUtils.closeQuietly(channel);
        IOUtils.closeQuietly(randomAccessFile);
        if (!file.delete()) {
            throw new IOException("file delete fail " + file.getPath());
        }
    }

    @Test
    public void testReAllocate() {

        final ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 20).capacity(), 20);
        byteBuffer.limit(5);

        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 5).capacity(), 10);
        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 5).limit(), 5);
        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 10).capacity(), 10);
        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 10).limit(), 10);

        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 20, 5, true), byteBuffer);
        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 20, 6, true).limit(), 6);
        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 20, 6, false).capacity(), 10);

        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 40, 30, false).limit(), 30);
        Assert.assertEquals(IOUtils.reAllocate(byteBuffer, 40, 30, false).capacity(), 40);
    }

    @Test
    public void testCompressionNone() {
        testCompressionNone("zhangjun zstd compression data");
        testCompressionNone("zicat zstd compression data");
    }

    /**
     * create compression none.
     *
     * @param data data
     */
    private void testCompressionNone(String data) {
        final byte[] bs = data.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer dataBuffer = createBuffer(bs);
        final ByteBuffer compressionBuffer = IOUtils.compressionNone(dataBuffer);
        compressionBuffer.getInt();
        final ByteBuffer decompressionBuffer = IOUtils.decompressionNone(compressionBuffer);
        final byte[] result = new byte[decompressionBuffer.remaining()];
        decompressionBuffer.get(result);
        Assert.assertEquals(data, new String(result, StandardCharsets.UTF_8));
    }

    @Test
    public void testCompressionZSTD() {
        testCompressionZSTD("zhangjun zstd compression data");
        testCompressionZSTD("zicat zstd compression data");
    }

    /**
     * test compression zstd.
     *
     * @param data data
     */
    private void testCompressionZSTD(final String data) {
        final byte[] bs = data.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer dataBuffer = createBuffer(bs);
        final ByteBuffer compressionBuffer = IOUtils.compressionZSTD(dataBuffer);
        compressionBuffer.getInt();
        final ByteBuffer decompressionBuffer = IOUtils.decompressionZSTD(compressionBuffer);
        final byte[] result = new byte[decompressionBuffer.remaining()];
        decompressionBuffer.get(result);
        Assert.assertEquals(data, new String(result, StandardCharsets.UTF_8));
    }

    @Test
    public void testCompressionSnappy() throws IOException {
        testCompressionSnappy("zhangjun snappy compression data");
        testCompressionSnappy("zicat snappy compression data");
    }

    /**
     * test compression snappy.
     *
     * @param data data
     * @throws IOException IOException
     */
    private void testCompressionSnappy(final String data) throws IOException {
        final byte[] bs = data.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer dataBuffer = createBuffer(bs);
        final ByteBuffer compressionBuffer = IOUtils.compressionSnappy(dataBuffer);
        compressionBuffer.getInt();
        final ByteBuffer decompressionBuffer = IOUtils.decompressionSnappy(compressionBuffer);
        final byte[] result = new byte[decompressionBuffer.remaining()];
        decompressionBuffer.get(result);
        Assert.assertEquals(data, new String(result, StandardCharsets.UTF_8));
    }

    /**
     * create buffer.
     *
     * @param bs bs
     * @return byte buffer
     */
    private ByteBuffer createBuffer(byte[] bs) {

        final ByteBuffer dataBuffer;
        dataBuffer = ByteBuffer.allocateDirect(bs.length + 4);
        dataBuffer.putInt(bs.length);
        dataBuffer.put(bs);
        dataBuffer.flip();
        dataBuffer.getInt();
        return dataBuffer;
    }

    @SuppressWarnings("resource")
    @Test
    public void testConcurrentCloseQuietly() {
        final TestCloseable[] closeables = {
            new TestCloseable(), new TestCloseable(), new TestCloseable()
        };
        concurrentCloseQuietly(closeables);
        for (TestCloseable closeable : closeables) {
            Assert.assertTrue(closeable.closed.get());
        }
        concurrentCloseQuietly((TestCloseable[]) null);
    }

    @Test
    public void testTransform() throws IOException {
        File source = Files.createTempFile("test_transform", ".txt").toFile();
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            for (int i = 0; i < 1024; i++) {
                bytes.write("aaaa".getBytes(StandardCharsets.UTF_8));
            }
            try (OutputStream os = Files.newOutputStream(source.toPath())) {
                os.write(bytes.toByteArray());
                os.flush();
            }
            File target = File.createTempFile("test_transform_target", ".txt");
            try {
                IOUtils.transform(source, target);
                try (InputStream is = Files.newInputStream(target.toPath())) {
                    byte[] targetBs = IOUtils.readFully(is);
                    Assert.assertArrayEquals(bytes.toByteArray(), targetBs);
                }
            } finally {
                Assert.assertTrue(target.delete());
            }
        } finally {
            Assert.assertTrue(source.delete());
        }
    }

    @Test
    public void testGzip() throws IOException {
        assertGzip("aa".getBytes(StandardCharsets.UTF_8));
        final byte[] data = new byte[10234];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        assertGzip(data);
    }

    private void assertGzip(byte[] data) throws IOException {
        byte[] compress = IOUtils.compressionGZip(data);
        byte[] decompress = IOUtils.decompressionGZip(compress);
        Assert.assertArrayEquals(data, decompress);
    }

    /** TestCloseable. */
    private static class TestCloseable implements Closeable {
        public final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
