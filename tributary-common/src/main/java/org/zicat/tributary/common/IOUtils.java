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

package org.zicat.tributary.common;

import com.github.luben.zstd.Zstd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.file.Files;

/** IOUtils. */
public class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);
    private static final int INT_LENGTH = 4;

    /**
     * to byte array.
     *
     * @param file file
     * @return array
     * @throws IOException IOException
     */
    public static byte[] readFull(File file) throws IOException {
        try (InputStream in = Files.newInputStream(file.toPath())) {
            return toByteArray(in);
        }
    }

    /**
     * to byte array.
     *
     * @param in in
     * @return array
     * @throws IOException IOException
     */
    public static byte[] toByteArray(InputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024 * 4];
        int n;
        while ((n = in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        return out.toByteArray();
    }

    /**
     * compression none.
     *
     * @param byteBuffer byteBuffer
     * @param reusedByteBuffer reusedByteBuffer
     * @return length + compression data
     */
    public static ByteBuffer compressionNone(ByteBuffer byteBuffer, ByteBuffer reusedByteBuffer) {
        reusedByteBuffer =
                IOUtils.reAllocate(reusedByteBuffer, byteBuffer.remaining() + INT_LENGTH);
        reusedByteBuffer.putInt(byteBuffer.remaining()).put(byteBuffer).flip();
        return reusedByteBuffer;
    }

    /**
     * compression none.
     *
     * @param byteBuffer byteBuffer
     * @return length + compression data
     */
    public static ByteBuffer compressionNone(ByteBuffer byteBuffer) {
        return compressionNone(byteBuffer, null);
    }

    /**
     * decompression none.
     *
     * @param byteBuffer byteBuffer
     * @param reusedByteBuffer reusedByteBuffer
     * @return ByteBuffer
     */
    public static ByteBuffer decompressionNone(ByteBuffer byteBuffer, ByteBuffer reusedByteBuffer) {
        reusedByteBuffer = IOUtils.reAllocate(reusedByteBuffer, byteBuffer.remaining());
        reusedByteBuffer.put(byteBuffer).flip();
        return reusedByteBuffer;
    }

    /**
     * decompression none.
     *
     * @param byteBuffer byteBuffer
     * @return ByteBuffer
     */
    public static ByteBuffer decompressionNone(ByteBuffer byteBuffer) {
        return decompressionNone(byteBuffer, null);
    }

    /**
     * compression zstd. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer must direct bytebuffer
     * @param reusedByteBuffer reusedByteBuffer
     * @return byteBuffer, length + compression data
     */
    public static ByteBuffer compressionZSTD(ByteBuffer byteBuffer, ByteBuffer reusedByteBuffer) {
        final ByteBuffer zstdBuffer = Zstd.compress(byteBuffer, 3);
        reusedByteBuffer =
                IOUtils.reAllocate(reusedByteBuffer, zstdBuffer.remaining() + INT_LENGTH);
        reusedByteBuffer.putInt(zstdBuffer.remaining()).put(zstdBuffer).flip();
        return reusedByteBuffer;
    }

    /**
     * compression zstd. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer must direct bytebuffer
     * @return byteBuffer, length + compression data
     */
    public static ByteBuffer compressionZSTD(ByteBuffer byteBuffer) {
        return compressionZSTD(byteBuffer, null);
    }

    /**
     * decompression zstd. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @param reusedByteBuffer reusedByteBuffer
     * @return ByteBuffer
     */
    public static ByteBuffer decompressionZSTD(ByteBuffer byteBuffer, ByteBuffer reusedByteBuffer) {
        final int size = (int) Zstd.decompressedSize(byteBuffer);
        reusedByteBuffer = IOUtils.reAllocate(reusedByteBuffer, size * 2, size);
        Zstd.decompress(reusedByteBuffer, byteBuffer);
        reusedByteBuffer.flip();
        return reusedByteBuffer;
    }

    /**
     * decompression zstd. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @return ByteBuffer
     */
    public static ByteBuffer decompressionZSTD(ByteBuffer byteBuffer) {
        return decompressionZSTD(byteBuffer, null);
    }

    /**
     * compression snappy. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @param reusedByteBuffer reusedByteBuffer
     * @return byteBuffer, length + compression data
     * @throws IOException IOException
     */
    public static ByteBuffer compressionSnappy(ByteBuffer byteBuffer, ByteBuffer reusedByteBuffer)
            throws IOException {
        final int size = byteBuffer.remaining();
        final int maxCompressedLength = Snappy.maxCompressedLength(size);
        reusedByteBuffer = IOUtils.reAllocate(reusedByteBuffer, maxCompressedLength + INT_LENGTH);
        reusedByteBuffer.position(INT_LENGTH);
        Snappy.compress(byteBuffer, reusedByteBuffer);
        final int length = reusedByteBuffer.remaining();
        reusedByteBuffer.position(0);
        reusedByteBuffer.putInt(length).position(0);
        return reusedByteBuffer;
    }

    /**
     * compression snappy. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @return byteBuffer, length + compression data
     * @throws IOException IOException
     */
    public static ByteBuffer compressionSnappy(ByteBuffer byteBuffer) throws IOException {
        return compressionSnappy(byteBuffer, null);
    }

    /**
     * decompression snappy. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @param reusedByteBuffer reusedByteBuffer
     * @return byteBuffer
     * @throws IOException IOException
     */
    public static ByteBuffer decompressionSnappy(ByteBuffer byteBuffer, ByteBuffer reusedByteBuffer)
            throws IOException {
        final int uncompressedLength = Snappy.uncompressedLength(byteBuffer);
        reusedByteBuffer = IOUtils.reAllocate(reusedByteBuffer, uncompressedLength);
        Snappy.uncompress(byteBuffer, reusedByteBuffer);
        return reusedByteBuffer;
    }

    /**
     * decompression snappy. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @return byteBuffer
     * @throws IOException IOException
     */
    public static ByteBuffer decompressionSnappy(ByteBuffer byteBuffer) throws IOException {
        return decompressionSnappy(byteBuffer, null);
    }

    /**
     * reAllocate buffer.
     *
     * @param oldBuffer old buffer
     * @param capacity capacity
     * @param limit limit
     * @param direct direct
     * @return new buffer
     */
    public static ByteBuffer reAllocate(
            ByteBuffer oldBuffer, int capacity, int limit, boolean direct) {

        if (capacity < limit) {
            capacity = limit;
        }
        if (oldBuffer == null || oldBuffer.capacity() < limit) {
            oldBuffer =
                    direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
        } else {
            oldBuffer.clear();
        }
        oldBuffer.limit(limit);
        return oldBuffer;
    }

    /**
     * reAllocate buffer.
     *
     * @param oldBuffer old buffer
     * @param capacity capacity
     * @param limit limit
     * @return new buffer
     */
    public static ByteBuffer reAllocate(ByteBuffer oldBuffer, int capacity, int limit) {
        return reAllocate(oldBuffer, capacity, limit, true);
    }

    /**
     * reAllocate buffer.
     *
     * @param oldBuffer old buffer
     * @param capacity capacity
     * @return new buffer
     */
    public static ByteBuffer reAllocate(ByteBuffer oldBuffer, int capacity) {
        return reAllocate(oldBuffer, capacity, capacity, true);
    }

    /**
     * close quietly.
     *
     * @param closeables closeables
     */
    public static void closeQuietly(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Exception e) {
                LOG.debug("close error", e);
            }
        }
    }

    /**
     * read channel to byte buffer.
     *
     * @param channel file channel
     * @param byteBuffer buffer
     * @param position file position
     * @throws IOException IOException
     */
    public static ByteBuffer readFully(FileChannel channel, ByteBuffer byteBuffer, long position)
            throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException(
                    "The file channel position cannot be negative, but it is " + position);
        }
        long offset = position;
        int readCount;
        do {
            readCount = channel.read(byteBuffer, offset);
            offset += readCount;
        } while (readCount != -1 && byteBuffer.hasRemaining());
        return byteBuffer;
    }

    /**
     * write bytebuffer to channel.
     *
     * @param channel channel
     * @param byteBuffer ByteBuffer
     * @return write count
     * @throws IOException IOException
     */
    public static int writeFull(GatheringByteChannel channel, ByteBuffer byteBuffer)
            throws IOException {
        if (byteBuffer == null) {
            return 0;
        }
        int count = 0;
        while (byteBuffer.hasRemaining()) {
            count += channel.write(byteBuffer);
        }
        return count;
    }

    /**
     * delete child dir.
     *
     * @param dir dir
     * @return boolean delete
     */
    public static boolean deleteDir(File dir) {
        if (dir.isFile()) {
            return dir.delete();
        }
        final File[] childFiles = dir.listFiles();
        boolean childDelete = true;
        if (childFiles != null) {
            for (File child : childFiles) {
                childDelete = childDelete & deleteDir(child);
            }
        }
        return childDelete && dir.delete();
    }

    /**
     * mkdir.
     *
     * @param dir dir
     * @return boolean make success
     */
    public static boolean makeDir(File dir) {
        if (dir.exists()) {
            return dir.isDirectory();
        }
        if (dir.getParentFile().exists()) {
            return dir.mkdir();
        } else {
            return makeDir(dir.getParentFile()) && dir.mkdir();
        }
    }

    /**
     * copy byte buffer.
     *
     * @param byteBuffer byteBuffer
     * @return byte array
     */
    public static byte[] copy(ByteBuffer byteBuffer) {
        final byte[] copy = new byte[byteBuffer.remaining()];
        byteBuffer.get(copy);
        return copy;
    }
}
