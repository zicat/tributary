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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

/** LengthBodyHDFSWriterFactory. */
public class LengthBodyHDFSWriterFactory implements HDFSWriterFactory {

    private final CompressionCodec codec;

    public LengthBodyHDFSWriterFactory(CompressionCodec codec) {
        this.codec = codec;
    }

    public LengthBodyHDFSWriterFactory() {
        this(defaultCodes());
    }

    /**
     * default codes.
     *
     * @return CompressionCodec
     */
    private static CompressionCodec defaultCodes() {
        final SnappyCodec snappyCodec = new SnappyCodec();
        snappyCodec.setConf(new Configuration());
        return snappyCodec;
    }

    @Override
    public String fileExtension() {
        return codec.getDefaultExtension();
    }

    @Override
    public HDFSWriter create() {
        return new LengthBodyHDFSWriter(codec);
    }
}
