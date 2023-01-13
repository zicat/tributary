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

package org.zicat.tributary.channel.test.utils;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.utils.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
    public void testCompressionSnappy() throws IOException {
        final String data = IOUtilsTest.class.getName();
        testCompressionSnappy(true, data);
        testCompressionSnappy(false, data);
    }

    /**
     * test compression snappy.
     *
     * @param direct direct
     * @param data data
     * @throws IOException IOException
     */
    private void testCompressionSnappy(boolean direct, final String data) throws IOException {
        final byte[] bs = data.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer dataBuffer = createBuffer(direct, bs);
        final ByteBuffer compressionBuffer = IOUtils.compressionSnappy(dataBuffer, null);
        compressionBuffer.getInt();
        final ByteBuffer decompressionBuffer = IOUtils.decompressionSnappy(compressionBuffer, null);
        final byte[] result = new byte[decompressionBuffer.remaining()];
        decompressionBuffer.get(result);
        Assert.assertEquals(data, new String(result, StandardCharsets.UTF_8));
    }

    /**
     * create buffer.
     *
     * @param direct direct
     * @param bs bs
     * @return byte buffer
     */
    private ByteBuffer createBuffer(boolean direct, byte[] bs) {

        final ByteBuffer dataBuffer;
        if (direct) {
            dataBuffer = ByteBuffer.allocateDirect(bs.length);
        } else {
            dataBuffer = ByteBuffer.allocate(bs.length);
        }
        dataBuffer.put(bs).flip();
        return dataBuffer;
    }
}
