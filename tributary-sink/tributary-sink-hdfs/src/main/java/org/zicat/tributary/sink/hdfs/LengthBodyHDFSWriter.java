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

package org.zicat.tributary.sink.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.zicat.tributary.common.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/** LengthBodyHDFSWriter. */
public class LengthBodyHDFSWriter implements HDFSWriter {

    protected FSDataOutputStream fsOut;
    private ByteBuffer buffer = null;

    public LengthBodyHDFSWriter() {}

    @Override
    public void open(FileSystem fileSystem, Path path) throws IOException {
        fsOut = fileSystem.create(path);
    }

    @Override
    public void append(byte[] bs, int offset, int length) throws IOException {
        final int bufferSize = length + 4;
        buffer = IOUtils.reAllocate(buffer, bufferSize, bufferSize, false);
        buffer.putInt(length).put(bs, offset, length).flip();
        write(buffer);
    }

    /**
     * write byte buffer.
     *
     * @param byteBuffer byteBuffer
     * @throws IOException IOException
     */
    protected void write(ByteBuffer byteBuffer) throws IOException {
        if (byteBuffer.isDirect()) {
            byte[] copy = new byte[byteBuffer.remaining()];
            byteBuffer.get(copy);
            fsOut.write(copy);
        } else {
            fsOut.write(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining());
        }
    }

    @Override
    public void sync() throws IOException {
        fsOut.flush();
        fsOut.hflush();
    }

    @Override
    public void close() throws IOException {
        try {
            sync();
        } finally {
            IOUtils.closeQuietly(fsOut);
        }
    }
}
