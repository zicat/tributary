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
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.zicat.tributary.queue.utils.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/** HDFSCompressedDataStreamWriter. @ Copy From Apache Flume */
public class HDFSCompressedDataStream extends AbstractHDFSWriter {

    protected Compressor compressor;
    protected FSDataOutputStream fsOut;
    protected CompressionOutputStream cmpOut;
    protected boolean isFinished = false;
    private ByteBuffer buffer = null;

    @Override
    public void open(FileSystem fileSystem, Path path, CompressionCodec codec) throws IOException {

        compressor = CodecPool.getCompressor(codec, fileSystem.getConf());
        fsOut = fileSystem.create(path);
        cmpOut = codec.createOutputStream(fsOut, compressor);
        isFinished = false;
    }

    @Override
    public void append(byte[] bs, int offset, int length) throws IOException {
        if (isFinished) {
            cmpOut.resetState();
            isFinished = false;
        }
        final int bufferSize = length + 4;
        buffer = IOUtils.reAllocate(buffer, bufferSize, bufferSize, false);
        buffer.putInt(length).put(bs, offset, length).flip();
        cmpOut.write(buffer.array(), buffer.position(), buffer.remaining());
    }

    @Override
    public void sync() throws IOException {
        if (!isFinished) {
            cmpOut.finish();
            isFinished = true;
        }
        fsOut.flush();
        hflushOrSync(fsOut);
    }

    @Override
    public void close() throws IOException {
        try {
            if (cmpOut != null) {
                try {
                    sync();
                } finally {
                    IOUtils.closeQuietly(cmpOut);
                    cmpOut = null;
                }
            }
        } finally {
            CodecPool.returnCompressor(compressor);
            compressor = null;
        }
    }
}
