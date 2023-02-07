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

package org.zicat.tributary.channel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/** SegmentStorage. */
public interface SegmentStorage extends Closeable {

    /**
     * write byte buffer to storage.
     *
     * @param byteBuffer byteBuffer
     */
    void writeFull(ByteBuffer byteBuffer) throws IOException;

    /**
     * read full byte buffer from offset.
     *
     * @param byteBuffer byteBuffer
     * @param offset offset
     * @throws IOException IOException
     */
    void readFull(ByteBuffer byteBuffer, long offset) throws IOException;

    /**
     * force flush.
     *
     * @param force force
     */
    void persist(boolean force) throws IOException;

    /** @return false if recycle fail */
    boolean recycle();

    /**
     * get compression type.
     *
     * @return CompressionType
     */
    CompressionType compressionType();
}
