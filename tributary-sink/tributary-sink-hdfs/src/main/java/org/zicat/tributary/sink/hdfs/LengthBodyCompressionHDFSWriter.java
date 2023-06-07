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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.zicat.tributary.common.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/** LengthBodyHDFSWriter. */
public class LengthBodyCompressionHDFSWriter extends LengthBodyHDFSWriter {

    protected final CompressionCodec codec;
    protected Compressor compressor;
    protected CompressionOutputStream cmpOut;
    protected boolean isFinished = false;

    public LengthBodyCompressionHDFSWriter(CompressionCodec codec) {
        this.codec = codec;
    }

    @Override
    public void open(FileSystem fileSystem, Path path) throws IOException {
        super.open(fileSystem, path);
        compressor = CodecPool.getCompressor(codec, fileSystem.getConf());
        cmpOut = codec.createOutputStream(fsOut, compressor);
        isFinished = false;
    }

    @Override
    public void append(byte[] bs, int offset, int length) throws IOException {
        if (isFinished) {
            cmpOut.resetState();
            isFinished = false;
        }
        super.append(bs, offset, length);
    }

    @Override
    protected void write(ByteBuffer byteBuffer) throws IOException {
        if (byteBuffer.isDirect()) {
            byte[] copy = new byte[byteBuffer.remaining()];
            byteBuffer.get(copy);
            cmpOut.write(copy);
        } else {
            cmpOut.write(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining());
        }
    }

    @Override
    public void sync() throws IOException {
        if (!isFinished) {
            cmpOut.finish();
            isFinished = true;
        }
        super.sync();
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
