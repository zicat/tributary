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
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.Closeable;
import java.io.IOException;

/** HDFSWriter. */
public interface HDFSWriter extends Closeable {

    /**
     * open file with codec.
     *
     * @param fileSystem fileSystem
     * @param path path
     * @param codec codec
     * @throws IOException IOException
     */
    void open(FileSystem fileSystem, Path path, CompressionCodec codec) throws IOException;

    /**
     * append bs to file.
     *
     * @param bs bs
     * @param offset offset
     * @param length length
     * @throws IOException IOException
     */
    void append(byte[] bs, int offset, int length) throws IOException;

    /**
     * append bs.
     *
     * @param bs bs
     * @throws IOException IOException
     */
    default void append(byte[] bs) throws IOException {
        append(bs, 0, bs.length);
    }

    /**
     * sync stream.
     *
     * @throws IOException IOException
     */
    void sync() throws IOException;
}
