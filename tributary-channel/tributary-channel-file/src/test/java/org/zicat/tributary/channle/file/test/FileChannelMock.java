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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/** FileChannelMock. */
public class FileChannelMock extends FileChannel {

    private byte[] data;
    private long position;

    public FileChannelMock(int headSize) {
        if (headSize < 0) {
            throw new IllegalArgumentException("headSize must be >= 0");
        }
        data = new byte[headSize];
        position = headSize;
    }

    @Override
    public int read(ByteBuffer dst, long position) {
        if (data == null || position >= data.length) {
            return -1;
        }
        int bytesToRead = (int) Math.min(dst.remaining(), data.length - position);
        dst.put(data, (int) position, bytesToRead);
        return bytesToRead;
    }

    @Override
    public int write(ByteBuffer src) {
        final byte[] value = new byte[src.remaining()];
        src.get(value);
        if (data == null) {
            data = value;
        } else {
            final byte[] newData = new byte[data.length + value.length];
            System.arraycopy(data, 0, newData, 0, data.length);
            System.arraycopy(value, 0, newData, data.length, value.length);
            data = newData;
        }
        position = data.length;
        return value.length;
    }

    @Override
    public int write(ByteBuffer src, long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public FileChannel position(long newPosition) {
        position = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        return data.length;
    }

    @Override
    public FileChannel truncate(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void force(boolean metaData) {}

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void implCloseChannel() {}
}
