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

package org.zicat.tributary.channel.utils;

import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

/** IOUtils. */
public class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);
    private static final int INT_LENGTH = 4;

    /**
     * compression none.
     *
     * @param byteBuffer byteBuffer
     * @param compressionBlock compressionBlock
     * @return length + compression data
     */
    public static ByteBuffer compressionNone(ByteBuffer byteBuffer, ByteBuffer compressionBlock) {
        compressionBlock =
                IOUtils.reAllocate(compressionBlock, byteBuffer.remaining() + INT_LENGTH);
        compressionBlock.putInt(byteBuffer.remaining()).put(byteBuffer).flip();
        return compressionBlock;
    }

    /**
     * compression zstd.
     *
     * @param byteBuffer byteBuffer
     * @param compressionBlock compressionBlock
     * @return byteBuffer, length + compression data
     */
    public static ByteBuffer compressionZSTD(ByteBuffer byteBuffer, ByteBuffer compressionBlock) {
        final ByteBuffer zstdBuffer = Zstd.compress(byteBuffer, 3);
        compressionBlock =
                IOUtils.reAllocate(compressionBlock, zstdBuffer.remaining() + INT_LENGTH);
        compressionBlock.putInt(zstdBuffer.remaining()).put(zstdBuffer).flip();
        return compressionBlock;
    }

    /**
     * compression snappy.
     *
     * @param byteBuffer byteBuffer
     * @param compressionBlock compressionBlock
     * @return byteBuffer, length + compression data
     * @throws IOException IOException
     */
    public static ByteBuffer compressionSnappy(ByteBuffer byteBuffer, ByteBuffer compressionBlock)
            throws IOException {
        final int size = byteBuffer.remaining();
        final int maxCompressedLength = Snappy.maxCompressedLength(size);
        if (!byteBuffer.isDirect()) {
            final byte[] buf = new byte[maxCompressedLength];
            final int offset = 0;
            final int compressedByteSize =
                    Snappy.rawCompress(
                            byteBuffer.array(), byteBuffer.position(), size, buf, offset);
            final int limit = compressedByteSize + INT_LENGTH;
            compressionBlock = IOUtils.reAllocate(compressionBlock, limit, limit, false);
            compressionBlock.putInt(compressedByteSize);
            compressionBlock.put(buf, offset, compressedByteSize);
            compressionBlock.flip();
        } else {
            compressionBlock =
                    IOUtils.reAllocate(compressionBlock, maxCompressedLength + INT_LENGTH);
            compressionBlock.position(INT_LENGTH);
            Snappy.compress(byteBuffer, compressionBlock);
            final int length = compressionBlock.remaining();
            compressionBlock.position(0);
            compressionBlock.putInt(length).position(0);
        }
        return compressionBlock;
    }

    /**
     * decompression snappy.
     *
     * @param byteBuffer byteBuffer
     * @return byteBuffer
     * @throws IOException IOException
     */
    public static ByteBuffer decompressionSnappy(ByteBuffer byteBuffer, ByteBuffer compressionBlock)
            throws IOException {
        if (!byteBuffer.isDirect()) {
            final int dataSize = byteBuffer.remaining();
            final int size =
                    Snappy.uncompressedLength(byteBuffer.array(), byteBuffer.position(), dataSize);
            final byte[] result = new byte[size];
            Snappy.uncompress(byteBuffer.array(), byteBuffer.position(), dataSize, result, 0);
            return ByteBuffer.wrap(result);
        } else {
            final int uncompressedLength = Snappy.uncompressedLength(byteBuffer);
            compressionBlock = IOUtils.reAllocate(compressionBlock, uncompressedLength);
            Snappy.uncompress(byteBuffer, compressionBlock);
            return compressionBlock;
        }
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
                LOG.warn("close error", e);
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
}
