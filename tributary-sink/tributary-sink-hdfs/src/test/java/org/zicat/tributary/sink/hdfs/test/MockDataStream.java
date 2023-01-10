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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.zicat.tributary.sink.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.zicat.tributary.sink.hdfs.HDFSCompressedDataStream;

import java.io.IOException;

/** MockDataStream. */
class MockDataStream extends HDFSCompressedDataStream {

    private final FileSystem fs;

    MockDataStream(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public void open(FileSystem fileSystem, Path path, CompressionCodec codec) throws IOException {
        Configuration conf = new Configuration();
        doOpen(conf, path, fs, codec);
    }

    protected void doOpen(Configuration conf, Path dstPath, FileSystem hdfs, CompressionCodec codec)
            throws IOException {

        fsOut = hdfs.create(dstPath);
        compressor = CodecPool.getCompressor(codec, conf);
        cmpOut = codec.createOutputStream(fsOut, compressor);
        isFinished = false;
    }
}
